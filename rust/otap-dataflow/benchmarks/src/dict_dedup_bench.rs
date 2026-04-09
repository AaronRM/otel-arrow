// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Benchmark comparing dictionary deduplication strategies for Quiver's
//! `StreamAccumulator` dictionary unification.
//!
//! Run with: `cargo run --release -p benchmarks --bin dict_dedup_bench`

// This is a standalone benchmark binary that prints results to stdout.
#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::builder::{
    FixedSizeBinaryDictionaryBuilder, PrimitiveDictionaryBuilder,
    StringDictionaryBuilder, TimestampNanosecondBuilder,
    UInt32Builder,
};
use arrow_array::types::{Int64Type, UInt16Type, UInt8Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray,
};
use arrow_buffer::ArrowNativeType;
use arrow_cast::cast;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use rand::distr::Alphanumeric;

/// Fixed seed for reproducible benchmark results.
const BENCH_SEED: u64 = 0x3A02_9E81_F650_7B4C;

/// Number of warmup iterations before measurement.
const WARMUP_ITERS: usize = 5;
/// Number of measured iterations.
const BENCH_ITERS: usize = 20;

// ─────────────────────────────────────────────────────────────────────────────
// Strategy implementations
// ─────────────────────────────────────────────────────────────────────────────

/// Result of a single unification run.
struct UnifyResult {
    /// Batches with unified dictionaries, ready for IPC writing.
    batches: Vec<RecordBatch>,
    /// Updated schema (may differ from input if key type was widened).
    schema: SchemaRef,
    /// Number of values in the unified dictionary.
    unified_dict_len: usize,
    /// Whether key type was widened beyond the original.
    key_widened: bool,
    /// Whether native fallback was triggered (dict stripped).
    native_fallback: bool,
}

/// Strategy A: Concatenate without dedup (current production behavior).
fn unify_concat(schema: &SchemaRef, batches: &[RecordBatch]) -> UnifyResult {
    // This mirrors the production code in stream_accumulator.rs.
    // For each dict column, concatenate values arrays and remap keys.
    let num_cols = schema.fields().len();
    let num_batches = batches.len();

    let mut column_data: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_cols);
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(num_cols);
    let mut any_widened = false;
    let mut any_native_fallback = false;
    let mut total_dict_len = 0usize;

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        if let DataType::Dictionary(key_type, value_type) = field.data_type() {
            let mut value_refs: Vec<&dyn Array> = Vec::with_capacity(num_batches);
            let mut batch_offsets: Vec<usize> = Vec::with_capacity(num_batches);
            let mut total_values: usize = 0;

            for batch in batches {
                let values = dict_values(batch.column(col_idx), key_type);
                batch_offsets.push(total_values);
                total_values += values.len();
                value_refs.push(values);
            }

            let unified_values: ArrayRef = if total_values == 0 {
                arrow_array::new_empty_array(value_type)
            } else {
                concat(&value_refs).expect("concat failed")
            };
            total_dict_len += unified_values.len();

            let effective_key = widen_key_type(key_type, total_values);

            if exceeds_max_dict_key(&effective_key) {
                any_native_fallback = true;
                let native_type = value_type.as_ref().clone();
                let cols: Vec<ArrayRef> = batches
                    .iter()
                    .map(|b| cast(b.column(col_idx), &native_type).expect("cast failed"))
                    .collect();
                column_data.push(cols);
                new_fields.push(Arc::new(field.as_ref().clone().with_data_type(native_type)));
            } else {
                if effective_key != **key_type {
                    any_widened = true;
                }
                let cols: Vec<ArrayRef> = batches
                    .iter()
                    .zip(batch_offsets.iter())
                    .map(|(batch, &offset)| {
                        remap_dict_keys(
                            batch.column(col_idx),
                            key_type,
                            &effective_key,
                            &unified_values,
                            offset,
                        )
                    })
                    .collect();
                column_data.push(cols);
                if effective_key != **key_type {
                    new_fields.push(Arc::new(field.as_ref().clone().with_data_type(
                        DataType::Dictionary(Box::new(effective_key), value_type.clone()),
                    )));
                } else {
                    new_fields.push(Arc::clone(field));
                }
            }
        } else {
            let cols: Vec<ArrayRef> =
                batches.iter().map(|b| Arc::clone(b.column(col_idx))).collect();
            column_data.push(cols);
            new_fields.push(Arc::clone(field));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batches: Vec<RecordBatch> = (0..num_batches)
        .map(|bi| {
            let cols: Vec<ArrayRef> =
                (0..num_cols).map(|ci| Arc::clone(&column_data[ci][bi])).collect();
            RecordBatch::try_new(Arc::clone(&new_schema), cols).expect("batch build failed")
        })
        .collect();

    UnifyResult {
        batches: new_batches,
        schema: new_schema,
        unified_dict_len: total_dict_len,
        key_widened: any_widened,
        native_fallback: any_native_fallback,
    }
}

/// Strategy B: Always dedup via HashMap.
fn unify_dedup(schema: &SchemaRef, batches: &[RecordBatch]) -> UnifyResult {
    let num_cols = schema.fields().len();
    let num_batches = batches.len();

    let mut column_data: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_cols);
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(num_cols);
    let mut any_widened = false;
    let mut any_native_fallback = false;
    let mut total_dict_len = 0usize;

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        if let DataType::Dictionary(key_type, value_type) = field.data_type() {
            // Build dedup map: value -> index in unified array.
            let (unified_values, key_mappings) =
                dedup_dict_values(batches, col_idx, key_type, value_type);
            let deduped_count = unified_values.len();
            total_dict_len += deduped_count;

            let effective_key = widen_key_type(key_type, deduped_count);

            if exceeds_max_dict_key(&effective_key) {
                any_native_fallback = true;
                let native_type = value_type.as_ref().clone();
                let cols: Vec<ArrayRef> = batches
                    .iter()
                    .map(|b| cast(b.column(col_idx), &native_type).expect("cast failed"))
                    .collect();
                column_data.push(cols);
                new_fields.push(Arc::new(field.as_ref().clone().with_data_type(native_type)));
            } else {
                if effective_key != **key_type {
                    any_widened = true;
                }
                let cols: Vec<ArrayRef> = batches
                    .iter()
                    .zip(key_mappings.iter())
                    .map(|(batch, mapping)| {
                        remap_dict_keys_with_mapping(
                            batch.column(col_idx),
                            key_type,
                            &effective_key,
                            &unified_values,
                            mapping,
                        )
                    })
                    .collect();
                column_data.push(cols);
                if effective_key != **key_type {
                    new_fields.push(Arc::new(field.as_ref().clone().with_data_type(
                        DataType::Dictionary(Box::new(effective_key), value_type.clone()),
                    )));
                } else {
                    new_fields.push(Arc::clone(field));
                }
            }
        } else {
            let cols: Vec<ArrayRef> =
                batches.iter().map(|b| Arc::clone(b.column(col_idx))).collect();
            column_data.push(cols);
            new_fields.push(Arc::clone(field));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batches: Vec<RecordBatch> = (0..num_batches)
        .map(|bi| {
            let cols: Vec<ArrayRef> =
                (0..num_cols).map(|ci| Arc::clone(&column_data[ci][bi])).collect();
            RecordBatch::try_new(Arc::clone(&new_schema), cols).expect("batch build failed")
        })
        .collect();

    UnifyResult {
        batches: new_batches,
        schema: new_schema,
        unified_dict_len: total_dict_len,
        key_widened: any_widened,
        native_fallback: any_native_fallback,
    }
}

/// Strategy C: Dedup only when total values would exceed key type capacity
/// (i.e., when widening or native fallback would be triggered).
fn unify_threshold(schema: &SchemaRef, batches: &[RecordBatch]) -> UnifyResult {
    let num_cols = schema.fields().len();
    let num_batches = batches.len();

    let mut column_data: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_cols);
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(num_cols);
    let mut any_widened = false;
    let mut any_native_fallback = false;
    let mut total_dict_len = 0usize;

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        if let DataType::Dictionary(key_type, value_type) = field.data_type() {
            // First, compute total values without dedup to check threshold.
            let mut total_values: usize = 0;
            for batch in batches {
                total_values += dict_values(batch.column(col_idx), key_type).len();
            }

            let capacity = key_type_capacity(key_type);
            let needs_dedup = total_values > capacity;

            if needs_dedup {
                // Dedup path (same as strategy B for this column).
                let (unified_values, key_mappings) =
                    dedup_dict_values(batches, col_idx, key_type, value_type);
                let deduped_count = unified_values.len();
                total_dict_len += deduped_count;

                let effective_key = widen_key_type(key_type, deduped_count);

                if exceeds_max_dict_key(&effective_key) {
                    any_native_fallback = true;
                    let native_type = value_type.as_ref().clone();
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .map(|b| cast(b.column(col_idx), &native_type).expect("cast failed"))
                        .collect();
                    column_data.push(cols);
                    new_fields
                        .push(Arc::new(field.as_ref().clone().with_data_type(native_type)));
                } else {
                    if effective_key != **key_type {
                        any_widened = true;
                    }
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .zip(key_mappings.iter())
                        .map(|(batch, mapping)| {
                            remap_dict_keys_with_mapping(
                                batch.column(col_idx),
                                key_type,
                                &effective_key,
                                &unified_values,
                                mapping,
                            )
                        })
                        .collect();
                    column_data.push(cols);
                    if effective_key != **key_type {
                        new_fields.push(Arc::new(field.as_ref().clone().with_data_type(
                            DataType::Dictionary(Box::new(effective_key), value_type.clone()),
                        )));
                    } else {
                        new_fields.push(Arc::clone(field));
                    }
                }
            } else {
                // Concat path (same as strategy A for this column).
                let mut value_refs: Vec<&dyn Array> = Vec::with_capacity(num_batches);
                let mut batch_offsets: Vec<usize> = Vec::with_capacity(num_batches);
                let mut running = 0usize;

                for batch in batches {
                    let values = dict_values(batch.column(col_idx), key_type);
                    batch_offsets.push(running);
                    running += values.len();
                    value_refs.push(values);
                }

                let unified_values: ArrayRef = if running == 0 {
                    arrow_array::new_empty_array(value_type)
                } else {
                    concat(&value_refs).expect("concat failed")
                };
                total_dict_len += unified_values.len();

                // No widening needed since total_values <= capacity.
                let cols: Vec<ArrayRef> = batches
                    .iter()
                    .zip(batch_offsets.iter())
                    .map(|(batch, &offset)| {
                        remap_dict_keys(
                            batch.column(col_idx),
                            key_type,
                            key_type,
                            &unified_values,
                            offset,
                        )
                    })
                    .collect();
                column_data.push(cols);
                new_fields.push(Arc::clone(field));
            }
        } else {
            let cols: Vec<ArrayRef> =
                batches.iter().map(|b| Arc::clone(b.column(col_idx))).collect();
            column_data.push(cols);
            new_fields.push(Arc::clone(field));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batches: Vec<RecordBatch> = (0..num_batches)
        .map(|bi| {
            let cols: Vec<ArrayRef> =
                (0..num_cols).map(|ci| Arc::clone(&column_data[ci][bi])).collect();
            RecordBatch::try_new(Arc::clone(&new_schema), cols).expect("batch build failed")
        })
        .collect();

    UnifyResult {
        batches: new_batches,
        schema: new_schema,
        unified_dict_len: total_dict_len,
        key_widened: any_widened,
        native_fallback: any_native_fallback,
    }
}

/// Strategy D: Selective dedup — only dedup columns where the average per-batch
/// dictionary size is below a threshold (i.e., low-cardinality columns).
///
/// This targets exactly the columns Albert identified: severity_text (~4 values),
/// schema_url (1 value), scope.name (~3 values), attribute keys (~50 values).
/// These are cheap to dedup and benefit significantly from it.
///
/// High-cardinality columns like trace_id (one value per row) are skipped —
/// they're expensive to dedup and have 0% overlap anyway.
const SELECTIVE_DEDUP_PER_BATCH_THRESHOLD: usize = 256;

fn unify_selective(schema: &SchemaRef, batches: &[RecordBatch]) -> UnifyResult {
    unify_selective_with_threshold(schema, batches, SELECTIVE_DEDUP_PER_BATCH_THRESHOLD)
}

fn unify_selective_with_threshold(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    threshold: usize,
) -> UnifyResult {
    let num_cols = schema.fields().len();
    let num_batches = batches.len();

    let mut column_data: Vec<Vec<ArrayRef>> = Vec::with_capacity(num_cols);
    let mut new_fields: Vec<Arc<Field>> = Vec::with_capacity(num_cols);
    let mut any_widened = false;
    let mut any_native_fallback = false;
    let mut total_dict_len = 0usize;

    for col_idx in 0..num_cols {
        let field = &schema.fields()[col_idx];
        if let DataType::Dictionary(key_type, value_type) = field.data_type() {
            // Compute average per-batch dictionary cardinality.
            let mut total_values: usize = 0;
            for batch in batches {
                total_values += dict_values(batch.column(col_idx), key_type).len();
            }
            let avg_per_batch = if num_batches > 0 {
                total_values / num_batches
            } else {
                0
            };

            let is_low_cardinality = avg_per_batch <= threshold;

            if is_low_cardinality {
                // Dedup path: cheap for low-cardinality columns.
                let (unified_values, key_mappings) =
                    dedup_dict_values(batches, col_idx, key_type, value_type);
                let deduped_count = unified_values.len();
                total_dict_len += deduped_count;

                let effective_key = widen_key_type(key_type, deduped_count);

                if exceeds_max_dict_key(&effective_key) {
                    any_native_fallback = true;
                    let native_type = value_type.as_ref().clone();
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .map(|b| cast(b.column(col_idx), &native_type).expect("cast failed"))
                        .collect();
                    column_data.push(cols);
                    new_fields
                        .push(Arc::new(field.as_ref().clone().with_data_type(native_type)));
                } else {
                    if effective_key != **key_type {
                        any_widened = true;
                    }
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .zip(key_mappings.iter())
                        .map(|(batch, mapping)| {
                            remap_dict_keys_with_mapping(
                                batch.column(col_idx),
                                key_type,
                                &effective_key,
                                &unified_values,
                                mapping,
                            )
                        })
                        .collect();
                    column_data.push(cols);
                    if effective_key != **key_type {
                        new_fields.push(Arc::new(field.as_ref().clone().with_data_type(
                            DataType::Dictionary(Box::new(effective_key), value_type.clone()),
                        )));
                    } else {
                        new_fields.push(Arc::clone(field));
                    }
                }
            } else {
                // Concat path for high-cardinality columns (e.g., trace_id).
                let mut value_refs: Vec<&dyn Array> = Vec::with_capacity(num_batches);
                let mut batch_offsets: Vec<usize> = Vec::with_capacity(num_batches);
                let mut running = 0usize;

                for batch in batches {
                    let values = dict_values(batch.column(col_idx), key_type);
                    batch_offsets.push(running);
                    running += values.len();
                    value_refs.push(values);
                }

                let unified_values: ArrayRef = if running == 0 {
                    arrow_array::new_empty_array(value_type)
                } else {
                    concat(&value_refs).expect("concat failed")
                };
                total_dict_len += unified_values.len();

                let effective_key = widen_key_type(key_type, running);

                if exceeds_max_dict_key(&effective_key) {
                    any_native_fallback = true;
                    let native_type = value_type.as_ref().clone();
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .map(|b| cast(b.column(col_idx), &native_type).expect("cast failed"))
                        .collect();
                    column_data.push(cols);
                    new_fields
                        .push(Arc::new(field.as_ref().clone().with_data_type(native_type)));
                } else {
                    if effective_key != **key_type {
                        any_widened = true;
                    }
                    let cols: Vec<ArrayRef> = batches
                        .iter()
                        .zip(batch_offsets.iter())
                        .map(|(batch, &offset)| {
                            remap_dict_keys(
                                batch.column(col_idx),
                                key_type,
                                &effective_key,
                                &unified_values,
                                offset,
                            )
                        })
                        .collect();
                    column_data.push(cols);
                    if effective_key != **key_type {
                        new_fields.push(Arc::new(field.as_ref().clone().with_data_type(
                            DataType::Dictionary(Box::new(effective_key), value_type.clone()),
                        )));
                    } else {
                        new_fields.push(Arc::clone(field));
                    }
                }
            }
        } else {
            let cols: Vec<ArrayRef> =
                batches.iter().map(|b| Arc::clone(b.column(col_idx))).collect();
            column_data.push(cols);
            new_fields.push(Arc::clone(field));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    let new_batches: Vec<RecordBatch> = (0..num_batches)
        .map(|bi| {
            let cols: Vec<ArrayRef> =
                (0..num_cols).map(|ci| Arc::clone(&column_data[ci][bi])).collect();
            RecordBatch::try_new(Arc::clone(&new_schema), cols).expect("batch build failed")
        })
        .collect();

    UnifyResult {
        batches: new_batches,
        schema: new_schema,
        unified_dict_len: total_dict_len,
        key_widened: any_widened,
        native_fallback: any_native_fallback,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dedup helper: builds deduplicated values array + per-batch key mappings
// ─────────────────────────────────────────────────────────────────────────────

/// Enum wrapping a zero-copy borrowed byte key for HashMap lookups.
/// Avoids allocating a `Vec<u8>` per dictionary value.
#[derive(Eq, PartialEq, Hash, Clone)]
enum ValueKey<'a> {
    Null,
    Bytes(&'a [u8]),
    /// For Int32 and Duration(ns)/Int64 values, store the raw 8 bytes inline.
    Inline([u8; 8]),
}

/// Extracts a zero-copy comparable key for a single array value.
fn value_key<'a>(array: &'a dyn Array, idx: usize) -> ValueKey<'a> {
    if array.is_null(idx) {
        return ValueKey::Null;
    }
    if let Some(sa) = array.as_any().downcast_ref::<StringArray>() {
        return ValueKey::Bytes(sa.value(idx).as_bytes());
    }
    if let Some(fa) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return ValueKey::Bytes(fa.value(idx));
    }
    if let Some(ia) = array.as_any().downcast_ref::<Int32Array>() {
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&ia.value(idx).to_le_bytes());
        buf[4] = 1; // discriminator so Int32(0) != null/short values
        return ValueKey::Inline(buf);
    }
    // Int64 / Duration(ns) — stored as PrimitiveArray<Int64Type>
    if let Some(la) = array
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>()
    {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&la.value(idx).to_le_bytes());
        return ValueKey::Inline(buf);
    }
    // Fallback: serialize via debug (should not be hit for our schemas).
    let sliced = array.slice(idx, 1);
    let bytes = format!("{sliced:?}").into_bytes();
    // Must allocate here; leak into 'a to keep the borrow checker happy.
    // This path is not performance-critical (only hit for unexpected types).
    ValueKey::Bytes(Vec::leak(bytes))
}

/// For each batch, produces a mapping from original key index -> unified
/// deduplicated index. Returns the unified values array and per-batch mappings.
fn dedup_dict_values(
    batches: &[RecordBatch],
    col_idx: usize,
    key_type: &DataType,
    value_type: &DataType,
) -> (ArrayRef, Vec<Vec<usize>>) {
    // Collect all values from all batches, building a dedup map.
    let mut seen: HashMap<ValueKey<'_>, usize> = HashMap::new();
    let mut unique_indices: Vec<(usize, usize)> = Vec::new(); // (batch_idx, value_idx)
    let mut key_mappings: Vec<Vec<usize>> = Vec::with_capacity(batches.len());

    for (batch_idx, batch) in batches.iter().enumerate() {
        let values = dict_values(batch.column(col_idx), key_type);
        let num_values = values.len();
        let mut mapping = Vec::with_capacity(num_values);

        for val_idx in 0..num_values {
            let key = value_key(values, val_idx);
            let next_id = seen.len();
            let unified_idx = *seen.entry(key).or_insert_with(|| {
                unique_indices.push((batch_idx, val_idx));
                next_id
            });
            mapping.push(unified_idx);
        }
        key_mappings.push(mapping);
    }

    // Build the unified values array from unique entries.
    let unified_values = if unique_indices.is_empty() {
        arrow_array::new_empty_array(value_type)
    } else {
        let refs: Vec<&dyn Array> = unique_indices
            .iter()
            .map(|&(bi, _)| dict_values(batches[bi].column(col_idx), key_type))
            .collect();
        // Slice each to a single element and concat.
        let sliced: Vec<ArrayRef> = unique_indices
            .iter()
            .enumerate()
            .map(|(i, &(_, vi))| refs[i].slice(vi, 1))
            .collect();
        let sliced_refs: Vec<&dyn Array> = sliced.iter().map(|a| a.as_ref()).collect();
        concat(&sliced_refs).expect("dedup concat failed")
    };

    (unified_values, key_mappings)
}

// ─────────────────────────────────────────────────────────────────────────────
// Arrow helpers (mirrors of production code, standalone for benchmark)
// ─────────────────────────────────────────────────────────────────────────────

const MAX_DICT_KEY_TYPE: DataType = DataType::UInt16;

fn exceeds_max_dict_key(key_type: &DataType) -> bool {
    key_type_capacity(key_type) > key_type_capacity(&MAX_DICT_KEY_TYPE)
}

fn key_type_capacity(dt: &DataType) -> usize {
    match dt {
        DataType::Int8 => i8::MAX as usize + 1,
        DataType::Int16 => i16::MAX as usize + 1,
        DataType::Int32 => i32::MAX as usize + 1,
        DataType::Int64 => i64::MAX as usize + 1,
        DataType::UInt8 => u8::MAX as usize + 1,
        DataType::UInt16 => u16::MAX as usize + 1,
        DataType::UInt32 => u32::MAX as usize + 1,
        DataType::UInt64 => usize::MAX,
        _ => 0,
    }
}

fn widen_key_type(original: &DataType, total_values: usize) -> DataType {
    if total_values <= key_type_capacity(original) {
        return original.clone();
    }
    let chain: &[DataType] = match original {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            &[DataType::Int16, DataType::Int32, DataType::Int64]
        }
        _ => &[DataType::UInt16, DataType::UInt32, DataType::UInt64],
    };
    for candidate in chain {
        if total_values <= key_type_capacity(candidate) {
            return candidate.clone();
        }
    }
    // Unreachable in practice (UInt64 covers all cases).
    DataType::UInt64
}

fn dict_values<'a>(col: &'a ArrayRef, key_type: &DataType) -> &'a dyn Array {
    match key_type {
        DataType::UInt8 => col
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .expect("UInt8 dict")
            .values()
            .as_ref(),
        DataType::UInt16 => col
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .expect("UInt16 dict")
            .values()
            .as_ref(),
        other => panic!("unsupported key type for benchmark: {other:?}"),
    }
}

fn extract_offset_keys(col: &ArrayRef, key_type: &DataType, offset: usize) -> Vec<Option<u64>> {
    match key_type {
        DataType::UInt8 => {
            let dict = col
                .as_any()
                .downcast_ref::<DictionaryArray<UInt8Type>>()
                .expect("UInt8 dict");
            dict.keys()
                .iter()
                .map(|k| k.map(|v| v.as_usize() as u64 + offset as u64))
                .collect()
        }
        DataType::UInt16 => {
            let dict = col
                .as_any()
                .downcast_ref::<DictionaryArray<UInt16Type>>()
                .expect("UInt16 dict");
            dict.keys()
                .iter()
                .map(|k| k.map(|v| v.as_usize() as u64 + offset as u64))
                .collect()
        }
        other => panic!("unsupported key type: {other:?}"),
    }
}

fn build_dict_array(
    keys: &[Option<u64>],
    target_key_type: &DataType,
    unified_values: &ArrayRef,
) -> ArrayRef {
    match target_key_type {
        DataType::UInt8 => {
            let typed: arrow_array::PrimitiveArray<UInt8Type> =
                keys.iter().map(|k| k.map(|v| v as u8)).collect();
            Arc::new(DictionaryArray::new(typed, Arc::clone(unified_values)))
        }
        DataType::UInt16 => {
            let typed: arrow_array::PrimitiveArray<UInt16Type> =
                keys.iter().map(|k| k.map(|v| v as u16)).collect();
            Arc::new(DictionaryArray::new(typed, Arc::clone(unified_values)))
        }
        other => panic!("unsupported target key type: {other:?}"),
    }
}

fn remap_dict_keys(
    col: &ArrayRef,
    original_key_type: &DataType,
    target_key_type: &DataType,
    unified_values: &ArrayRef,
    offset: usize,
) -> ArrayRef {
    let keys = extract_offset_keys(col, original_key_type, offset);
    build_dict_array(&keys, target_key_type, unified_values)
}

/// Remaps keys using a per-batch mapping (for dedup strategies).
fn remap_dict_keys_with_mapping(
    col: &ArrayRef,
    original_key_type: &DataType,
    target_key_type: &DataType,
    unified_values: &ArrayRef,
    mapping: &[usize],
) -> ArrayRef {
    let keys = extract_offset_keys(col, original_key_type, 0);
    let remapped: Vec<Option<u64>> = keys
        .iter()
        .map(|k| k.map(|v| mapping[v as usize] as u64))
        .collect();
    build_dict_array(&remapped, target_key_type, unified_values)
}

// ─────────────────────────────────────────────────────────────────────────────
// Data generation
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum ValueType {
    Utf8,
    FixedSizeBinary16,
    Int32,
}

impl std::fmt::Display for ValueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Utf8 => write!(f, "Utf8"),
            Self::FixedSizeBinary16 => write!(f, "FSB16"),
            Self::Int32 => write!(f, "Int32"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum KeyType {
    UInt8,
    UInt16,
}

impl std::fmt::Display for KeyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
        }
    }
}

impl KeyType {
    fn to_arrow(&self) -> DataType {
        match self {
            Self::UInt8 => DataType::UInt8,
            Self::UInt16 => DataType::UInt16,
        }
    }
}

/// Test parameters for a single benchmark case.
#[derive(Debug, Clone)]
struct TestCase {
    name: String,
    num_batches: usize,
    per_batch_cardinality: usize,
    overlap_ratio: f64, // 0.0 = all unique, 1.0 = identical
    value_type: ValueType,
    key_type: KeyType,
    rows_per_batch: usize,
}

/// Generates a pool of unique values for the value type.
fn generate_value_pool(vtype: ValueType, count: usize, rng: &mut StdRng) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| match vtype {
            ValueType::Utf8 => {
                let len = rng.random_range(5..30);
                let s: String = format!(
                    "val_{i}_{}",
                    (0..len)
                        .map(|_| rng.sample(Alphanumeric) as char)
                        .collect::<String>()
                );
                s.into_bytes()
            }
            ValueType::FixedSizeBinary16 => {
                let mut buf = vec![0u8; 16];
                rng.fill(&mut buf[..]);
                // Embed index for uniqueness guarantee.
                buf[0..4].copy_from_slice(&(i as u32).to_le_bytes());
                buf
            }
            ValueType::Int32 => (i as i32).to_le_bytes().to_vec(),
        })
        .collect()
}

/// Builds batches for a test case.
fn build_test_batches(tc: &TestCase) -> (SchemaRef, Vec<RecordBatch>) {
    let arrow_value_type = match tc.value_type {
        ValueType::Utf8 => DataType::Utf8,
        ValueType::FixedSizeBinary16 => DataType::FixedSizeBinary(16),
        ValueType::Int32 => DataType::Int32,
    };
    let arrow_key_type = tc.key_type.to_arrow();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "col",
        DataType::Dictionary(Box::new(arrow_key_type.clone()), Box::new(arrow_value_type)),
        true,
    )]));

    // Generate the global value pool. The total unique values across all batches
    // depends on overlap_ratio.
    //
    // With overlap_ratio = 1.0, all batches use the same `per_batch_cardinality` values.
    // With overlap_ratio = 0.0, each batch uses a disjoint set.
    // With overlap_ratio = r, per-batch: r*card values are shared, (1-r)*card are unique.

    let shared_count = (tc.per_batch_cardinality as f64 * tc.overlap_ratio).round() as usize;
    let unique_per_batch = tc.per_batch_cardinality - shared_count;
    let total_unique = shared_count + unique_per_batch * tc.num_batches;

    let mut rng = StdRng::seed_from_u64(BENCH_SEED);
    let pool = generate_value_pool(tc.value_type, total_unique, &mut rng);
    let mut batches = Vec::with_capacity(tc.num_batches);

    for batch_idx in 0..tc.num_batches {
        // Build this batch's dictionary values: shared values + batch-specific values.
        let batch_values: Vec<&[u8]> = (0..shared_count)
            .map(|i| pool[i].as_slice())
            .chain(
                (0..unique_per_batch)
                    .map(|i| pool[shared_count + batch_idx * unique_per_batch + i].as_slice()),
            )
            .collect();

        let col: ArrayRef = match (tc.value_type, &tc.key_type) {
            (ValueType::Utf8, KeyType::UInt8) => {
                let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    builder.append_value(std::str::from_utf8(batch_values[idx]).unwrap());
                }
                Arc::new(builder.finish())
            }
            (ValueType::Utf8, KeyType::UInt16) => {
                let mut builder = StringDictionaryBuilder::<UInt16Type>::new();
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    builder.append_value(std::str::from_utf8(batch_values[idx]).unwrap());
                }
                Arc::new(builder.finish())
            }
            (ValueType::FixedSizeBinary16, KeyType::UInt8) => {
                let mut builder =
                    FixedSizeBinaryDictionaryBuilder::<UInt8Type>::new(16);
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    builder.append_value(batch_values[idx]);
                }
                Arc::new(builder.finish())
            }
            (ValueType::FixedSizeBinary16, KeyType::UInt16) => {
                let mut builder =
                    FixedSizeBinaryDictionaryBuilder::<UInt16Type>::new(16);
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    builder.append_value(batch_values[idx]);
                }
                Arc::new(builder.finish())
            }
            (ValueType::Int32, KeyType::UInt8) => {
                let mut builder = PrimitiveDictionaryBuilder::<UInt8Type, arrow_array::types::Int32Type>::new();
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    let val = i32::from_le_bytes(batch_values[idx].try_into().unwrap());
                    builder.append_value(val);
                }
                Arc::new(builder.finish())
            }
            (ValueType::Int32, KeyType::UInt16) => {
                let mut builder = PrimitiveDictionaryBuilder::<UInt16Type, arrow_array::types::Int32Type>::new();
                for _ in 0..tc.rows_per_batch {
                    let idx = rng.random_range(0..batch_values.len());
                    let val = i32::from_le_bytes(batch_values[idx].try_into().unwrap());
                    builder.append_value(val);
                }
                Arc::new(builder.finish())
            }
        };

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![col]).expect("batch");
        batches.push(batch);
    }

    (schema, batches)
}

// ─────────────────────────────────────────────────────────────────────────────
// IPC size measurement
// ─────────────────────────────────────────────────────────────────────────────

fn ipc_file_size(schema: &SchemaRef, batches: &[RecordBatch]) -> usize {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer = FileWriter::try_new_with_options(&mut buf, schema, options).expect("writer");
    for batch in batches {
        writer.write(batch).expect("write batch");
    }
    writer.finish().expect("finish");
    buf.len()
}

/// Reads back IPC file to verify correctness and return total row count.
fn verify_ipc(schema: &SchemaRef, batches: &[RecordBatch]) -> usize {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::default();
    let mut writer = FileWriter::try_new_with_options(&mut buf, schema, options).expect("writer");
    for batch in batches {
        writer.write(batch).expect("write batch");
    }
    writer.finish().expect("finish");

    let cursor = Cursor::new(buf);
    let reader = FileReader::try_new(cursor, None).expect("valid IPC");
    let mut total_rows = 0;
    for rb in reader {
        total_rows += rb.expect("valid batch").num_rows();
    }
    total_rows
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark execution
// ─────────────────────────────────────────────────────────────────────────────

struct BenchResult {
    strategy: &'static str,
    time_us: f64,
    time_us_stddev: f64,
    unified_dict_len: usize,
    key_widened: bool,
    native_fallback: bool,
    ipc_size_bytes: usize,
}

/// Runs a strategy function with warmup and per-iteration timing, returning
/// mean and stddev in microseconds along with the last result.
fn bench_strategy(
    strategy_fn: fn(&SchemaRef, &[RecordBatch]) -> UnifyResult,
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> (f64, f64, UnifyResult) {
    // Warmup
    for _ in 0..WARMUP_ITERS {
        let _ = strategy_fn(schema, batches);
    }
    // Per-iteration timing
    let mut times_us = Vec::with_capacity(BENCH_ITERS);
    let mut last = None;
    for _ in 0..BENCH_ITERS {
        let start = Instant::now();
        let r = strategy_fn(schema, batches);
        let elapsed_us = start.elapsed().as_nanos() as f64 / 1_000.0;
        times_us.push(elapsed_us);
        last = Some(r);
    }
    let n = times_us.len() as f64;
    let mean = times_us.iter().sum::<f64>() / n;
    let variance = times_us.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1.0);
    let stddev = variance.sqrt();
    (mean, stddev, last.expect("BENCH_ITERS > 0"))
}

fn run_case(tc: &TestCase) -> Vec<BenchResult> {
    let (schema, batches) = build_test_batches(tc);

    let mut results = Vec::new();

    for (name, strategy_fn) in [
        (
            "concat",
            unify_concat as fn(&SchemaRef, &[RecordBatch]) -> UnifyResult,
        ),
        ("dedup", unify_dedup as fn(&_, &[_]) -> _),
        ("threshold", unify_threshold as fn(&_, &[_]) -> _),
        ("selective", unify_selective as fn(&_, &[_]) -> _),
    ] {
        let (mean, stddev, r) = bench_strategy(strategy_fn, &schema, &batches);
        let total_rows = verify_ipc(&r.schema, &r.batches);
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, expected_rows, "{name}: row count mismatch");
        let ipc_size = ipc_file_size(&r.schema, &r.batches);
        results.push(BenchResult {
            strategy: name,
            time_us: mean,
            time_us_stddev: stddev,
            unified_dict_len: r.unified_dict_len,
            key_widened: r.key_widened,
            native_fallback: r.native_fallback,
            ipc_size_bytes: ipc_size,
        });
    }

    results
}

// ─────────────────────────────────────────────────────────────────────────────
// OTAP-realistic workloads
// ─────────────────────────────────────────────────────────────────────────────

/// Builds multi-column batches simulating a realistic OTAP Logs payload.
///
/// Based on actual OTAP schema from crates/pdata/src/encode/record/logs.rs:
///   - 14 columns total (7 dictionary-encoded, 4 plain, 3 struct-nested-dict)
///   - resource: Struct { id: UInt16, schema_url: Dict<U8,Utf8>, dropped: UInt32 }
///   - scope: Struct { id: UInt16, name: Dict<U8,Utf8>, version: Dict<U8,Utf8>, dropped: UInt32 }
///   - trace_id: Dict<U16, FSB(16)> — unique per row, 0% overlap
///   - span_id: Dict<U16, FSB(8)> — unique per row, 0% overlap
///   - severity_number: Dict<U8, Int32> — ~6 values, 100% overlap
///   - severity_text: Dict<U8, Utf8> — 3 values, 100% overlap
///   - schema_url: Dict<U8, Utf8> — 1 value, 100% overlap
///   - event_name: Dict<U8, Utf8> — ~10 values, 90% overlap
///   - time_unix_nano: Timestamp(ns) — plain
///   - observed_time_unix_nano: Timestamp(ns) — plain
///   - dropped_attributes_count: UInt32 — plain
///   - flags: UInt32 — plain
///
/// The OTAP encoder selects key type based on per-batch cardinality.
/// trace_id/span_id are unique per row, so at production batch sizes
/// (1,000-8,192 rows) they need UInt16 keys (capacity 65,536).
///
/// At scale, 10 batches × 8,192 rows = 81,920 unique trace_ids —
/// exceeding UInt16 capacity (65,536). This triggers native fallback,
/// which is the most interesting dedup scenario: can dedup save us here?
/// (Answer: no, because overlap is 0%.)
fn build_otap_logs_batches(
    num_batches: usize,
    rows_per_batch: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        // 0: time_unix_nano (plain timestamp — not dict-encoded)
        Field::new(
            "time_unix_nano",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
        // 1: observed_time_unix_nano (plain timestamp)
        Field::new(
            "observed_time_unix_nano",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
        // 2: severity_number — 6 distinct values, 100% overlap
        Field::new(
            "severity_number",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32)),
            true,
        ),
        // 3: severity_text — 3 values, 100% overlap
        Field::new(
            "severity_text",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 4: trace_id — FSB(16), unique per row, 0% overlap.
        //    UInt16 keys: real OTAP encoder selects key width by cardinality.
        //    At 1000+ rows/batch, UInt8 (256 max) is insufficient.
        Field::new(
            "trace_id",
            DataType::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(DataType::FixedSizeBinary(16)),
            ),
            true,
        ),
        // 5: span_id — FSB(8), unique per row, 0% overlap. UInt16 keys.
        Field::new(
            "span_id",
            DataType::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(DataType::FixedSizeBinary(8)),
            ),
            true,
        ),
        // 6: schema_url — 1 value, 100% overlap
        Field::new(
            "schema_url",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 7: event_name — ~10 values, 90% overlap
        Field::new(
            "event_name",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 8: dropped_attributes_count (plain UInt32)
        Field::new("dropped_attributes_count", DataType::UInt32, false),
        // 9: flags (plain UInt32)
        Field::new("flags", DataType::UInt32, false),
    ]));

    let severity_nums: &[i32] = &[1, 5, 9, 13, 17, 21]; // TRACE..FATAL
    let severity_texts = ["DEBUG", "INFO", "WARN", "ERROR"];
    let schema_url = "https://opentelemetry.io/schemas/1.21.0";
    let event_names_shared = [
        "http.request",
        "http.response",
        "db.query",
        "rpc.call",
        "exception",
        "log",
        "cache.hit",
        "cache.miss",
        "queue.enqueue",
    ];
    let mut rng = StdRng::seed_from_u64(BENCH_SEED);
    let base_ts: i64 = 1_700_000_000_000_000_000; // 2023-11-14 ~epoch

    let mut batches = Vec::with_capacity(num_batches);
    for batch_idx in 0..num_batches {
        // 0: time_unix_nano
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(rows_per_batch);
        // 1: observed_time_unix_nano
        let mut obs_ts_builder = TimestampNanosecondBuilder::with_capacity(rows_per_batch);
        // 2: severity_number
        let mut sev_num_builder =
            PrimitiveDictionaryBuilder::<UInt8Type, arrow_array::types::Int32Type>::new();
        // 3: severity_text
        let mut sev_text_builder = StringDictionaryBuilder::<UInt8Type>::new();
        // 4: trace_id — UInt16 keys, unique per row per batch.
        //    Each batch will have rows_per_batch unique entries in its dictionary.
        let mut tid_builder = FixedSizeBinaryDictionaryBuilder::<UInt16Type>::new(16);
        // 5: span_id — UInt16 keys, unique per row per batch.
        let mut sid_builder = FixedSizeBinaryDictionaryBuilder::<UInt16Type>::new(8);
        // 6: schema_url
        let mut url_builder = StringDictionaryBuilder::<UInt8Type>::new();
        // 7: event_name
        let batch_unique_event = format!(
            "custom.event.batch{batch_idx}.{}",
            (0..6)
                .map(|_| rng.sample(Alphanumeric) as char)
                .collect::<String>()
        );
        let mut event_builder = StringDictionaryBuilder::<UInt8Type>::new();
        // 8: dropped_attributes_count
        let mut dropped_builder = UInt32Builder::with_capacity(rows_per_batch);
        // 9: flags
        let mut flags_builder = UInt32Builder::with_capacity(rows_per_batch);

        // Pre-generate unique trace_ids and span_ids for this batch.
        let mut trace_ids: Vec<[u8; 16]> = Vec::with_capacity(rows_per_batch);
        for i in 0..rows_per_batch {
            let mut buf = [0u8; 16];
            buf[0..4].copy_from_slice(&(batch_idx as u32).to_le_bytes());
            buf[4..8].copy_from_slice(&(i as u32).to_le_bytes());
            rng.fill(&mut buf[8..16]);
            trace_ids.push(buf);
        }
        let mut span_ids: Vec<[u8; 8]> = Vec::with_capacity(rows_per_batch);
        for i in 0..rows_per_batch {
            let mut buf = [0u8; 8];
            buf[0..4].copy_from_slice(&(batch_idx as u32).to_le_bytes());
            buf[4..8].copy_from_slice(&(i as u32).to_le_bytes());
            span_ids.push(buf);
        }

        for row in 0..rows_per_batch {
            let row_ts = base_ts + (batch_idx * rows_per_batch + row) as i64 * 1_000_000;
            ts_builder.append_value(row_ts);
            obs_ts_builder.append_value(row_ts + 500_000); // 0.5ms later

            let sev_idx = rng.random_range(0..severity_nums.len());
            sev_num_builder.append_value(severity_nums[sev_idx]);
            sev_text_builder.append_value(severity_texts[sev_idx % severity_texts.len()]);

            tid_builder.append_value(&trace_ids[row]);
            sid_builder.append_value(&span_ids[row]);

            url_builder.append_value(schema_url);

            // event_name: 90% from shared pool, 10% batch-unique
            let r: f64 = rng.random();
            if r < 0.9 {
                let idx = rng.random_range(0..event_names_shared.len());
                event_builder.append_value(event_names_shared[idx]);
            } else {
                event_builder.append_value(&batch_unique_event);
            }

            dropped_builder.append_value(0);
            flags_builder.append_value(0);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(ts_builder.finish()),
                Arc::new(obs_ts_builder.finish()),
                Arc::new(sev_num_builder.finish()),
                Arc::new(sev_text_builder.finish()),
                Arc::new(tid_builder.finish()),
                Arc::new(sid_builder.finish()),
                Arc::new(url_builder.finish()),
                Arc::new(event_builder.finish()),
                Arc::new(dropped_builder.finish()),
                Arc::new(flags_builder.finish()),
            ],
        )
        .expect("otap batch");
        batches.push(batch);
    }

    (schema, batches)
}

// ─────────────────────────────────────────────────────────────────────────────
// OTAP Metrics simulation
// ─────────────────────────────────────────────────────────────────────────────

/// Builds batches simulating a realistic OTAP Metrics main-table payload.
///
/// Based on actual OTAP schema from crates/pdata/src/encode/record/metrics.rs:
///   - metric_name: Dict<U8, Utf8> — 50-200 unique names, ~100% overlap
///   - description: Dict<U8, Utf8> — ~1:1 with metric_name, ~100% overlap
///   - unit: Dict<U8, Utf8> — ~15 unique units, 100% overlap
///   - scope_schema_url: Dict<U8, Utf8> — 1 value, 100% overlap
///   - aggregation_temporality: Dict<U8, Int32> — 2 values, 100% overlap
///   - metric_type: UInt8 — plain (not dict-encoded)
///   - is_monotonic: Boolean — plain
///   - id: UInt16 — plain
///
/// Key difference from Logs: **no high-cardinality 0%-overlap columns**.
/// All dict columns are low-to-moderate cardinality with high overlap.
/// This is where dedup should provide the most benefit relative to cost.
fn build_otap_metrics_batches(
    num_batches: usize,
    rows_per_batch: usize,
    num_unique_metrics: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    use arrow_array::builder::{BooleanBuilder, UInt8Builder, UInt16Builder};

    let schema = Arc::new(Schema::new(vec![
        // 0: id (plain UInt16)
        Field::new("id", DataType::UInt16, false),
        // 1: metric_type (plain UInt8, not dict-encoded in real OTAP)
        Field::new("metric_type", DataType::UInt8, false),
        // 2: name — Dict<U8, Utf8>, moderate cardinality, ~100% overlap
        Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        ),
        // 3: description — Dict<U8, Utf8>, ~1:1 with name
        Field::new(
            "description",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 4: unit — Dict<U8, Utf8>, ~15 unique values, 100% overlap
        Field::new(
            "unit",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 5: scope_schema_url — Dict<U8, Utf8>, 1 value, 100% overlap
        Field::new(
            "scope_schema_url",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 6: aggregation_temporality — Dict<U8, Int32>, 2 values, 100% overlap
        Field::new(
            "aggregation_temporality",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32)),
            true,
        ),
        // 7: is_monotonic (plain Boolean)
        Field::new("is_monotonic", DataType::Boolean, true),
    ]));

    // Generate realistic metric names, descriptions, and units.
    let units = [
        "ms", "s", "bytes", "kB", "MB", "1", "%", "requests",
        "errors", "connections", "threads", "Hz", "celsius", "ops", "packets",
    ];
    let metric_names: Vec<String> = (0..num_unique_metrics)
        .map(|i| {
            let category = match i % 5 {
                0 => "http.server",
                1 => "system.cpu",
                2 => "system.memory",
                3 => "runtime.gc",
                _ => "db.client",
            };
            format!("{category}.metric_{i}")
        })
        .collect();
    let descriptions: Vec<String> = metric_names
        .iter()
        .map(|n| format!("Description for {n}"))
        .collect();
    let schema_url = "https://opentelemetry.io/schemas/1.21.0";
    // aggregation_temporality: 1=DELTA, 2=CUMULATIVE
    let agg_temps: &[i32] = &[1, 2];
    // metric_type: 1=Gauge, 2=Sum, 3=Histogram
    let metric_types: &[u8] = &[1, 2, 3];

    let mut rng = StdRng::seed_from_u64(BENCH_SEED);
    let mut batches = Vec::with_capacity(num_batches);

    for _batch_idx in 0..num_batches {
        let mut id_builder = UInt16Builder::with_capacity(rows_per_batch);
        let mut type_builder = UInt8Builder::with_capacity(rows_per_batch);
        let mut name_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut desc_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut unit_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut url_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut agg_builder =
            PrimitiveDictionaryBuilder::<UInt8Type, arrow_array::types::Int32Type>::new();
        let mut mono_builder = BooleanBuilder::with_capacity(rows_per_batch);

        for row in 0..rows_per_batch {
            let metric_idx = rng.random_range(0..num_unique_metrics);

            id_builder.append_value(row as u16);
            type_builder.append_value(metric_types[metric_idx % metric_types.len()]);
            name_builder.append_value(&metric_names[metric_idx]);
            desc_builder.append_value(&descriptions[metric_idx]);
            unit_builder.append_value(units[metric_idx % units.len()]);
            url_builder.append_value(schema_url);
            agg_builder.append_value(agg_temps[metric_idx % agg_temps.len()]);
            mono_builder.append_value(metric_idx % 3 == 0);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(desc_builder.finish()),
                Arc::new(unit_builder.finish()),
                Arc::new(url_builder.finish()),
                Arc::new(agg_builder.finish()),
                Arc::new(mono_builder.finish()),
            ],
        )
        .expect("metrics batch");
        batches.push(batch);
    }

    (schema, batches)
}

fn run_otap_metrics_case(
    num_batches: usize,
    rows_per_batch: usize,
    num_unique_metrics: usize,
) -> Vec<BenchResult> {
    let (schema, batches) =
        build_otap_metrics_batches(num_batches, rows_per_batch, num_unique_metrics);
    run_all_strategies(&schema, &batches)
}

// ─────────────────────────────────────────────────────────────────────────────
// OTAP Traces simulation
// ─────────────────────────────────────────────────────────────────────────────

/// Builds batches simulating a realistic OTAP Traces (spans) main-table payload.
///
/// Based on actual schema from crates/pdata/src/encode/record/traces.rs:
///   - trace_id: FixedSizeBinary(16) — plain (NOT dict-encoded)
///   - span_id: FixedSizeBinary(8) — plain (NOT dict-encoded)
///   - parent_span_id: FixedSizeBinary(8) — plain
///   - name: Dict<U8, Utf8> — span operation names, 20-500+ unique
///   - kind: Dict<U8, Int32> — 5 enum values, 100% overlap
///   - schema_url: Dict<U8, Utf8> — 1 value, 100% overlap
///   - trace_state: Dict<U8, Utf8> — ~5-20 values, ~90% overlap
///   - duration_time_unix_nano: Dict<U16, Duration(ns)> — high cardinality
///   - start_time_unix_nano: Timestamp(ns) — plain
///   - dropped_attributes_count: UInt32 — plain
///
/// Key difference from Logs: trace_id/span_id are NOT dictionary-encoded,
/// so there's no 0%-overlap high-cardinality dict poison. But `name` (span
/// operations) can be moderate-high cardinality, and `duration` is dict-encoded
/// with high cardinality and 0% overlap (unique durations per span).
fn build_otap_traces_batches(
    num_batches: usize,
    rows_per_batch: usize,
    num_unique_span_names: usize,
) -> (SchemaRef, Vec<RecordBatch>) {
    use arrow_array::builder::{
        FixedSizeBinaryBuilder, UInt32Builder,
    };

    let schema = Arc::new(Schema::new(vec![
        // 0: start_time_unix_nano (plain timestamp)
        Field::new(
            "start_time_unix_nano",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            false,
        ),
        // 1: trace_id — plain FSB(16), NOT dict-encoded
        Field::new(
            "trace_id",
            DataType::FixedSizeBinary(16),
            false,
        ),
        // 2: span_id — plain FSB(8), NOT dict-encoded
        Field::new(
            "span_id",
            DataType::FixedSizeBinary(8),
            false,
        ),
        // 3: parent_span_id — plain FSB(8)
        Field::new(
            "parent_span_id",
            DataType::FixedSizeBinary(8),
            true,
        ),
        // 4: name — Dict<U8, Utf8>, span operation names
        Field::new(
            "name",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            false,
        ),
        // 5: kind — Dict<U8, Int32>, 5 enum values
        Field::new(
            "kind",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Int32)),
            true,
        ),
        // 6: schema_url — Dict<U8, Utf8>, 1 value, 100% overlap
        Field::new(
            "schema_url",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 7: trace_state — Dict<U8, Utf8>, ~10 values, ~90% overlap
        Field::new(
            "trace_state",
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
            true,
        ),
        // 8: duration_time_unix_nano — Dict<U16, Duration(ns)>, high cardinality
        Field::new(
            "duration_time_unix_nano",
            DataType::Dictionary(
                Box::new(DataType::UInt16),
                Box::new(DataType::Duration(arrow_schema::TimeUnit::Nanosecond)),
            ),
            false,
        ),
        // 9: dropped_attributes_count (plain UInt32)
        Field::new("dropped_attributes_count", DataType::UInt32, false),
    ]));

    let span_names: Vec<String> = (0..num_unique_span_names)
        .map(|i| {
            let service = match i % 4 {
                0 => "GET",
                1 => "POST",
                2 => "grpc",
                _ => "internal",
            };
            let endpoint = match i % 8 {
                0 => "/api/users",
                1 => "/api/orders",
                2 => "/api/products",
                3 => "/health",
                4 => ".UserService/GetUser",
                5 => ".OrderService/CreateOrder",
                6 => "db.query",
                _ => "cache.get",
            };
            format!("{service} {endpoint}/{}", i / 8)
        })
        .collect();

    // Span kind: INTERNAL=0, SERVER=1, CLIENT=2, PRODUCER=3, CONSUMER=4
    let span_kinds: &[i32] = &[0, 1, 2, 3, 4];
    let schema_url = "https://opentelemetry.io/schemas/1.21.0";
    let trace_states_shared = [
        "sampled=true",
        "sampled=false",
        "rate=0.1",
        "rate=0.5",
        "rate=1.0",
        "priority=high",
        "priority=low",
        "debug=true",
    ];
    let base_ts: i64 = 1_700_000_000_000_000_000;

    let mut rng = StdRng::seed_from_u64(BENCH_SEED);
    let mut batches = Vec::with_capacity(num_batches);

    for batch_idx in 0..num_batches {
        let mut ts_builder = TimestampNanosecondBuilder::with_capacity(rows_per_batch);
        let mut tid_builder = FixedSizeBinaryBuilder::with_capacity(rows_per_batch, 16);
        let mut sid_builder = FixedSizeBinaryBuilder::with_capacity(rows_per_batch, 8);
        let mut psid_builder = FixedSizeBinaryBuilder::with_capacity(rows_per_batch, 8);
        let mut name_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut kind_builder =
            PrimitiveDictionaryBuilder::<UInt8Type, arrow_array::types::Int32Type>::new();
        let mut url_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut ts_state_builder = StringDictionaryBuilder::<UInt8Type>::new();
        let mut dur_builder = PrimitiveDictionaryBuilder::<UInt16Type, arrow_array::types::DurationNanosecondType>::new();
        let mut dropped_builder = UInt32Builder::with_capacity(rows_per_batch);

        for row in 0..rows_per_batch {
            let row_ts = base_ts + (batch_idx * rows_per_batch + row) as i64 * 1_000_000;
            ts_builder.append_value(row_ts);

            // trace_id: plain, unique per span
            let mut tid_buf = [0u8; 16];
            tid_buf[0..4].copy_from_slice(&(batch_idx as u32).to_le_bytes());
            tid_buf[4..8].copy_from_slice(&(row as u32).to_le_bytes());
            rng.fill(&mut tid_buf[8..16]);
            tid_builder.append_value(&tid_buf).unwrap();

            // span_id: plain, unique per span
            let mut sid_buf = [0u8; 8];
            sid_buf[0..4].copy_from_slice(&(batch_idx as u32).to_le_bytes());
            sid_buf[4..8].copy_from_slice(&(row as u32).to_le_bytes());
            sid_builder.append_value(&sid_buf).unwrap();

            // parent_span_id: 80% have a parent
            let r: f64 = rng.random();
            if r < 0.8 {
                let mut psid_buf = [0u8; 8];
                rng.fill(&mut psid_buf[..]);
                psid_builder.append_value(&psid_buf).unwrap();
            } else {
                psid_builder.append_null();
            }

            // name: from shared pool
            let name_idx = rng.random_range(0..num_unique_span_names);
            name_builder.append_value(&span_names[name_idx]);

            // kind
            kind_builder.append_value(span_kinds[rng.random_range(0..span_kinds.len())]);

            // schema_url
            url_builder.append_value(schema_url);

            // trace_state: 90% from shared pool, 10% batch-specific
            let r2: f64 = rng.random();
            if r2 < 0.9 {
                let idx = rng.random_range(0..trace_states_shared.len());
                ts_state_builder.append_value(trace_states_shared[idx]);
            } else {
                ts_state_builder
                    .append_value(&format!("batch={batch_idx},row={}", row % 5));
            }

            // duration: unique-ish (microsecond-level variation)
            let base_dur = rng.random_range(100_000i64..500_000_000); // 0.1ms to 500ms
            dur_builder.append_value(base_dur);

            dropped_builder.append_value(0);
        }

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(ts_builder.finish()),
                Arc::new(tid_builder.finish()),
                Arc::new(sid_builder.finish()),
                Arc::new(psid_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(kind_builder.finish()),
                Arc::new(url_builder.finish()),
                Arc::new(ts_state_builder.finish()),
                Arc::new(dur_builder.finish()),
                Arc::new(dropped_builder.finish()),
            ],
        )
        .expect("traces batch");
        batches.push(batch);
    }

    (schema, batches)
}

fn run_otap_traces_case(
    num_batches: usize,
    rows_per_batch: usize,
    num_unique_span_names: usize,
) -> Vec<BenchResult> {
    let (schema, batches) =
        build_otap_traces_batches(num_batches, rows_per_batch, num_unique_span_names);
    run_all_strategies(&schema, &batches)
}

/// Per-column breakdown for OTAP workloads: shows which columns dominate cost.
fn run_otap_per_column_analysis(num_batches: usize, rows_per_batch: usize) {
    let (schema, batches) = build_otap_logs_batches(num_batches, rows_per_batch);

    println!(
        "\n  Per-column analysis (nb={num_batches} rows={rows_per_batch}):"
    );
    println!(
        "  {:<25} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>7}",
        "column", "concat", "dedup", "threshld", "selectv", "dict_cat", "dict_dd", "dedup?"
    );
    println!("  {}", "-".repeat(110));

    for col_idx in 0..schema.fields().len() {
        let field = &schema.fields()[col_idx];
        if let DataType::Dictionary(_key_type, _value_type) = field.data_type() {
            // Extract just this column into single-column batches.
            let col_schema = Arc::new(Schema::new(vec![field.as_ref().clone()]));
            let col_batches: Vec<RecordBatch> = batches
                .iter()
                .map(|b| {
                    RecordBatch::try_new(Arc::clone(&col_schema), vec![Arc::clone(b.column(col_idx))])
                        .unwrap()
                })
                .collect();

            let iters = 20;
            let t_concat = {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = unify_concat(&col_schema, &col_batches);
                }
                start.elapsed().as_micros() as f64 / iters as f64
            };
            let r_concat = unify_concat(&col_schema, &col_batches);

            let t_dedup = {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = unify_dedup(&col_schema, &col_batches);
                }
                start.elapsed().as_micros() as f64 / iters as f64
            };
            let _r_dedup = unify_dedup(&col_schema, &col_batches);

            let t_threshold = {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = unify_threshold(&col_schema, &col_batches);
                }
                start.elapsed().as_micros() as f64 / iters as f64
            };

            let t_selective = {
                let start = Instant::now();
                for _ in 0..iters {
                    let _ = unify_selective(&col_schema, &col_batches);
                }
                start.elapsed().as_micros() as f64 / iters as f64
            };
            let r_selective = unify_selective(&col_schema, &col_batches);

            println!(
                "  {:<25} | {:>7.1}µ | {:>7.1}µ | {:>7.1}µ | {:>7.1}µ | {:>8} | {:>8} | {:>7}",
                field.name(),
                t_concat,
                t_dedup,
                t_threshold,
                t_selective,
                r_concat.unified_dict_len,
                r_selective.unified_dict_len,
                if r_concat.unified_dict_len != r_selective.unified_dict_len {
                    "YES"
                } else {
                    "no"
                },
            );
        } else {
            println!(
                "  {:<25} |      n/a |      n/a |      n/a |      n/a |      n/a |      n/a |     n/a",
                field.name(),
            );
        }
    }
}

fn run_otap_case(num_batches: usize, rows_per_batch: usize) -> Vec<BenchResult> {
    let (schema, batches) = build_otap_logs_batches(num_batches, rows_per_batch);
    run_all_strategies(&schema, &batches)
}

/// Shared bench runner for all four strategies.
fn run_all_strategies(schema: &SchemaRef, batches: &[RecordBatch]) -> Vec<BenchResult> {
    let mut results = Vec::new();

    for (name, strategy_fn) in [
        (
            "concat",
            unify_concat as fn(&SchemaRef, &[RecordBatch]) -> UnifyResult,
        ),
        ("dedup", unify_dedup as fn(&_, &[_]) -> _),
        ("threshold", unify_threshold as fn(&_, &[_]) -> _),
        ("selective", unify_selective as fn(&_, &[_]) -> _),
    ] {
        let (mean, stddev, r) = bench_strategy(strategy_fn, schema, batches);
        let total_rows = verify_ipc(&r.schema, &r.batches);
        let expected_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, expected_rows, "{name}: row count mismatch");
        let ipc_size = ipc_file_size(&r.schema, &r.batches);
        results.push(BenchResult {
            strategy: name,
            time_us: mean,
            time_us_stddev: stddev,
            unified_dict_len: r.unified_dict_len,
            key_widened: r.key_widened,
            native_fallback: r.native_fallback,
            ipc_size_bytes: ipc_size,
        });
    }

    results
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────

fn main() {
    println!("Dictionary Deduplication Benchmark for Quiver StreamAccumulator");
    println!("================================================================\n");

    // ── Synthetic test matrix ──
    let batch_counts = [2, 5, 10, 20, 50];
    let cardinalities = [10, 100, 256, 1000, 5000];
    let overlaps = [0.0, 0.5, 0.9, 1.0];
    let value_types = [ValueType::Utf8, ValueType::FixedSizeBinary16, ValueType::Int32];
    let key_types = [KeyType::UInt8, KeyType::UInt16];
    let rows_per_batch = 100;

    // CSV header
    println!("=== SYNTHETIC TEST MATRIX ===");
    println!(
        "{:<50} | {:>9} | {:>8} | {:>5} | {:>8} | {:>7} | {:>7} | {:>10} | {:>9}",
        "test_case", "strategy", "time_us", "±std", "dict_len", "widen", "native", "ipc_bytes", "ipc_delta"
    );
    println!("{}", "-".repeat(140));

    let mut all_results: Vec<(String, Vec<BenchResult>)> = Vec::new();

    // Run a representative subset to keep runtime reasonable.
    // Full matrix would be: 5*5*4*3*2 = 600 cases * 3 strategies.
    // Subset: fix value_type=Utf8, key_type=UInt8 for the full batch*card*overlap sweep,
    // then test other value/key types at a few representative points.

    // Full sweep for Utf8/UInt8
    for &nb in &batch_counts {
        for &card in &cardinalities {
            for &overlap in &overlaps {
                // Skip cases where total values would be enormous.
                let shared = (card as f64 * overlap).round() as usize;
                let unique_per = card - shared;
                let total_possible = shared + unique_per * nb;
                if total_possible > 100_000 {
                    continue;
                }

                let tc = TestCase {
                    name: format!(
                        "Utf8/U8 nb={nb} card={card} ovl={:.0}%",
                        overlap * 100.0
                    ),
                    num_batches: nb,
                    per_batch_cardinality: card,
                    overlap_ratio: overlap,
                    value_type: ValueType::Utf8,
                    key_type: KeyType::UInt8,
                    rows_per_batch,
                };

                let results = run_case(&tc);
                print_results(&tc.name, &results);
                all_results.push((tc.name, results));
            }
        }
    }

    // Representative cases for other value/key types
    for &vt in &value_types {
        for &kt in &key_types {
            if matches!(vt, ValueType::Utf8) && matches!(kt, KeyType::UInt8) {
                continue; // Already covered above.
            }
            for &(nb, card, overlap) in &[
                (5, 100, 0.5),
                (10, 256, 0.9),
                (20, 1000, 0.5),
                (5, 256, 0.0),
                (10, 100, 1.0),
            ] {
                let shared = (card as f64 * overlap).round() as usize;
                let unique_per = card - shared;
                let total_possible = shared + unique_per * nb;
                if total_possible > 100_000 {
                    continue;
                }

                let tc = TestCase {
                    name: format!(
                        "{vt}/{kt} nb={nb} card={card} ovl={:.0}%",
                        overlap * 100.0
                    ),
                    num_batches: nb,
                    per_batch_cardinality: card,
                    overlap_ratio: overlap,
                    value_type: vt,
                    key_type: kt,
                    rows_per_batch,
                };

                let results = run_case(&tc);
                print_results(&tc.name, &results);
                all_results.push((tc.name, results));
            }
        }
    }

    // ── OTAP-realistic workloads ──
    println!("\n=== OTAP-REALISTIC WORKLOADS (Logs payload simulation) ===");
    println!("Schema: 10 columns (6 dict-encoded, 4 plain). Production batch sizes.");
    println!(
        "{:<50} | {:>9} | {:>8} | {:>5} | {:>8} | {:>7} | {:>7} | {:>10} | {:>9}",
        "test_case", "strategy", "time_us", "±std", "dict_len", "widen", "native", "ipc_bytes", "ipc_delta"
    );
    println!("{}", "-".repeat(140));

    // Test at multiple production-realistic scales.
    // Real OTAP: 100-8192 rows/batch, 5-20 batches/segment.
    // OTel Collector default send_batch_size = 8192.
    //
    // trace_id/span_id use UInt16 keys (capacity 65,536).
    // At nb=10 × rows=8192, trace_id has 81,920 unique values →
    //   exceeds UInt16 max → native fallback triggered!
    for &(nb, rows) in &[
        (5, 100),    // small batches
        (10, 100),   // typical segment, small batch
        (5, 500),    // moderate batch
        (10, 500),   // moderate × typical
        (5, 1000),   // production batch size
        (10, 1000),  // production: 10k unique trace_ids
        (20, 1000),  // large segment: 20k unique trace_ids
        (5, 2000),   // large batch size
        (10, 2000),  // 20k unique trace_ids
        (5, 5000),   // very large batches
        (10, 5000),  // 50k unique — approaching UInt16 limit
        (5, 8192),   // OTel Collector default batch size
        (10, 8192),  // STRESS: 81,920 unique trace_ids → exceeds UInt16!
        (8, 8192),   // boundary: 65,536 = exactly UInt16 capacity
        // High batch counts — where low-cardinality dict bloat matters.
        // At 100+ batches, severity_text (4 vals/batch) → 400+ unified entries.
        // At 500+ batches, every low-card column overflows UInt8.
        (100, 1000), // high batch count, production rows
        (500, 100),  // very high batch count, small rows (time-triggered segments)
        (500, 1000), // stress: 500K rows, low-card dicts at 2000+ entries
    ] {
        let name = format!("OTAP-Logs nb={nb} rows={rows}");
        let results = run_otap_case(nb, rows);
        print_results(&name, &results);
        all_results.push((name, results));
    }

    // ── OTAP Metrics workloads ──
    println!("\n=== OTAP METRICS WORKLOADS ===");
    println!("Schema: 8 columns (5 dict-encoded, 3 plain). All dict columns are low-to-moderate");
    println!("cardinality with ~100% overlap. No high-cardinality 0%-overlap columns.");
    println!(
        "{:<50} | {:>9} | {:>8} | {:>5} | {:>8} | {:>7} | {:>7} | {:>10} | {:>9}",
        "test_case", "strategy", "time_us", "±std", "dict_len", "widen", "native", "ipc_bytes", "ipc_delta"
    );
    println!("{}", "-".repeat(140));

    // Metrics: vary batch count and unique metric count.
    // rows_per_batch = number of metric data points per batch.
    // num_unique_metrics = distinct metric names (e.g., 50 for a moderate app).
    for &(nb, rows, metrics) in &[
        (10, 500, 50),     // moderate: 50 metrics, 10 batches
        (10, 500, 200),    // moderate: 200 metrics (large app)
        (100, 500, 50),    // high batch count, 50 metrics
        (100, 500, 200),   // high batch count, 200 metrics
        (500, 100, 50),    // very high batch count, small batches
        (500, 500, 50),    // stress: 500 batches
        (500, 500, 200),   // stress: 500 batches, 200 metrics
    ] {
        let name = format!("OTAP-Metrics nb={nb} rows={rows} metrics={metrics}");
        let results = run_otap_metrics_case(nb, rows, metrics);
        print_results(&name, &results);
        all_results.push((name, results));
    }

    // ── OTAP Traces workloads ──
    println!("\n=== OTAP TRACES WORKLOADS ===");
    println!("Schema: 10 columns (5 dict-encoded, 5 plain). trace_id/span_id are plain FSB.");
    println!("duration_time_unix_nano is Dict<U16,Duration(ns)> with high cardinality, 0% overlap.");
    println!(
        "{:<50} | {:>9} | {:>8} | {:>5} | {:>8} | {:>7} | {:>7} | {:>10} | {:>9}",
        "test_case", "strategy", "time_us", "±std", "dict_len", "widen", "native", "ipc_bytes", "ipc_delta"
    );
    println!("{}", "-".repeat(140));

    for &(nb, rows, spans) in &[
        (10, 500, 50),     // moderate: 50 span names
        (10, 500, 200),    // moderate: 200 span names (microservices)
        (100, 500, 50),    // high batch count
        (100, 500, 200),   // high batch count, many operations
        (500, 100, 50),    // very high batch count, small batches
        (500, 500, 50),    // stress
    ] {
        let name = format!("OTAP-Traces nb={nb} rows={rows} spans={spans}");
        let results = run_otap_traces_case(nb, rows, spans);
        print_results(&name, &results);
        all_results.push((name, results));
    }

    // Per-column breakdown at key sizes to identify which columns drive cost.
    println!("\n=== PER-COLUMN ANALYSIS (Logs) ===");
    run_otap_per_column_analysis(10, 1000);
    run_otap_per_column_analysis(100, 1000);

    // ── Threshold sweep ──
    // Test different selective thresholds to find the optimal cutoff.
    println!("\n=== SELECTIVE THRESHOLD SWEEP ===");
    println!("Testing per-batch cardinality thresholds: which cutoff best separates");
    println!("low-cardinality (worth dedup) from high-cardinality (skip) columns.\n");

    let thresholds = [1, 4, 10, 16, 32, 64, 128, 256, 512, 1024, 4096, 65536];

    let sweep_header = |label: &str| {
        println!(
            "  {:>10} | {:>9} | {:>6} | {:>10} | {:>10} | {:>10} | {:>7}  {label}",
            "threshold", "time_us", "±std", "dict_len", "ipc_bytes", "ipc_delta", "widen"
        );
        println!("  {}", "-".repeat(85));
    };

    let run_sweep = |schema: &SchemaRef, batches: &[RecordBatch], label: &str| {
        let r_concat = unify_concat(schema, batches);
        let concat_ipc = ipc_file_size(&r_concat.schema, &r_concat.batches);
        println!(
            "  {label} (concat_ipc={concat_ipc}, dict_concat={})",
            r_concat.unified_dict_len
        );
        sweep_header(label);
        for &t in &thresholds {
            for _ in 0..WARMUP_ITERS {
                let _ = unify_selective_with_threshold(schema, batches, t);
            }
            let mut times_us = Vec::with_capacity(BENCH_ITERS);
            let mut last = None;
            for _ in 0..BENCH_ITERS {
                let start = Instant::now();
                let r = unify_selective_with_threshold(schema, batches, t);
                times_us.push(start.elapsed().as_nanos() as f64 / 1_000.0);
                last = Some(r);
            }
            let n = times_us.len() as f64;
            let mean = times_us.iter().sum::<f64>() / n;
            let variance = times_us.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
            let stddev = variance.sqrt();
            let r = last.expect("BENCH_ITERS > 0");
            let ipc = ipc_file_size(&r.schema, &r.batches);
            let delta = ipc as i64 - concat_ipc as i64;
            println!(
                "  {:>10} | {:>8.1}µ | {:>5.1}µ | {:>10} | {:>10} | {:>+10} | {:>7}",
                t,
                mean,
                stddev,
                r.unified_dict_len,
                ipc,
                delta,
                r.key_widened,
            );
        }
        println!();
    };

    // Logs sweeps
    println!("--- Logs threshold sweep ---");
    {
        let (s, b) = build_otap_logs_batches(100, 1000);
        run_sweep(&s, &b, "Logs nb=100 rows=1000");
    }
    {
        let (s, b) = build_otap_logs_batches(500, 100);
        run_sweep(&s, &b, "Logs nb=500 rows=100");
    }

    // Metrics sweeps
    println!("--- Metrics threshold sweep ---");
    {
        let (s, b) = build_otap_metrics_batches(100, 500, 50);
        run_sweep(&s, &b, "Metrics nb=100 rows=500 metrics=50");
    }
    {
        let (s, b) = build_otap_metrics_batches(100, 500, 200);
        run_sweep(&s, &b, "Metrics nb=100 rows=500 metrics=200");
    }

    // Traces sweeps
    println!("--- Traces threshold sweep ---");
    {
        let (s, b) = build_otap_traces_batches(100, 500, 50);
        run_sweep(&s, &b, "Traces nb=100 rows=500 spans=50");
    }
    {
        let (s, b) = build_otap_traces_batches(100, 500, 200);
        run_sweep(&s, &b, "Traces nb=100 rows=500 spans=200");
    }

    // ── Summary analysis ──
    println!("\n=== ANALYSIS SUMMARY ===\n");
    print_analysis(&all_results);
}

fn print_results(name: &str, results: &[BenchResult]) {
    let baseline_ipc = results[0].ipc_size_bytes;
    for r in results {
        let delta: i64 = r.ipc_size_bytes as i64 - baseline_ipc as i64;
        let delta_str = if delta == 0 {
            "   0".to_string()
        } else {
            format!("{delta:+}")
        };
        println!(
            "{:<50} | {:>9} | {:>8.1} | {:>5.1} | {:>8} | {:>7} | {:>7} | {:>10} | {:>9}",
            name,
            r.strategy,
            r.time_us,
            r.time_us_stddev,
            r.unified_dict_len,
            r.key_widened,
            r.native_fallback,
            r.ipc_size_bytes,
            delta_str,
        );
    }
}

fn print_analysis(all_results: &[(String, Vec<BenchResult>)]) {
    // Q1: CPU cost of always-dedup vs concat
    let mut total_concat_us = 0.0f64;
    let mut total_dedup_us = 0.0f64;
    let mut total_threshold_us = 0.0f64;
    let mut total_selective_us = 0.0f64;
    let mut count = 0usize;

    // Q2: Space savings
    let mut cases_dedup_saves_space = 0usize;
    let mut cases_selective_saves_space = 0usize;
    let mut total_space_saved_dedup = 0i64;
    let mut total_space_saved_selective = 0i64;
    let mut total_ipc_baseline = 0i64;

    // Q3: Selective vs always-dedup comparison
    let mut selective_matches_dedup_space = 0usize;
    let mut selective_faster_than_dedup = 0usize;

    // Q4: Widening avoidance
    let mut concat_widens = 0usize;
    let mut dedup_widens = 0usize;
    let mut selective_widens = 0usize;
    let mut concat_native_fb = 0usize;
    let mut dedup_native_fb = 0usize;
    let mut selective_native_fb = 0usize;

    for (name, results) in all_results {
        if results.len() < 4 {
            continue;
        }
        let concat_r = &results[0];
        let dedup_r = &results[1];
        let _threshold_r = &results[2];
        let selective_r = &results[3];

        total_concat_us += concat_r.time_us;
        total_dedup_us += dedup_r.time_us;
        total_threshold_us += _threshold_r.time_us;
        total_selective_us += selective_r.time_us;
        count += 1;

        let dedup_saved = concat_r.ipc_size_bytes as i64 - dedup_r.ipc_size_bytes as i64;
        let selective_saved = concat_r.ipc_size_bytes as i64 - selective_r.ipc_size_bytes as i64;
        total_ipc_baseline += concat_r.ipc_size_bytes as i64;
        if dedup_saved > 0 {
            cases_dedup_saves_space += 1;
            total_space_saved_dedup += dedup_saved;
        }
        if selective_saved > 0 {
            cases_selective_saves_space += 1;
            total_space_saved_selective += selective_saved;
        }

        if selective_r.ipc_size_bytes <= dedup_r.ipc_size_bytes {
            selective_matches_dedup_space += 1;
        }
        if selective_r.time_us < dedup_r.time_us * 0.9 {
            selective_faster_than_dedup += 1;
        }

        if concat_r.key_widened {
            concat_widens += 1;
        }
        if dedup_r.key_widened {
            dedup_widens += 1;
        }
        if selective_r.key_widened {
            selective_widens += 1;
        }
        if concat_r.native_fallback {
            concat_native_fb += 1;
        }
        if dedup_r.native_fallback {
            dedup_native_fb += 1;
        }
        if selective_r.native_fallback {
            selective_native_fb += 1;
        }

        // Print notable results (where selective dedup stands out).
        if selective_saved > 1000 || concat_r.key_widened != selective_r.key_widened {
            println!(
                "  Notable: {name} -- selective_saved={selective_saved}B, \
                 concat_ipc={} sel_ipc={}, \
                 concat={:.0}us sel={:.0}us dedup={:.0}us",
                concat_r.ipc_size_bytes,
                selective_r.ipc_size_bytes,
                concat_r.time_us,
                selective_r.time_us,
                dedup_r.time_us,
            );
        }
    }

    println!();
    println!("─── Q1: CPU cost comparison ───");
    let avg_concat = total_concat_us / count as f64;
    let avg_dedup = total_dedup_us / count as f64;
    let avg_threshold = total_threshold_us / count as f64;
    let avg_selective = total_selective_us / count as f64;
    println!("  Avg concat time:    {avg_concat:.1} us");
    println!("  Avg dedup time:     {avg_dedup:.1} us  ({:+.1}%)", (avg_dedup - avg_concat) / avg_concat * 100.0);
    println!("  Avg threshold time: {avg_threshold:.1} us  ({:+.1}%)", (avg_threshold - avg_concat) / avg_concat * 100.0);
    println!("  Avg selective time: {avg_selective:.1} us  ({:+.1}%)", (avg_selective - avg_concat) / avg_concat * 100.0);

    println!();
    println!("─── Q2: Space savings from dedup ───");
    println!("  Dedup saves space:     {cases_dedup_saves_space}/{count} cases");
    println!("  Selective saves space: {cases_selective_saves_space}/{count} cases");
    if total_ipc_baseline > 0 {
        println!(
            "  Total dedup savings:     {} bytes ({:.1}% of baseline)",
            total_space_saved_dedup,
            total_space_saved_dedup as f64 / total_ipc_baseline as f64 * 100.0
        );
        println!(
            "  Total selective savings: {} bytes ({:.1}% of baseline)",
            total_space_saved_selective,
            total_space_saved_selective as f64 / total_ipc_baseline as f64 * 100.0
        );
    }

    println!();
    println!("─── Q3: Selective vs always-dedup ───");
    println!(
        "  Selective matches/beats dedup IPC size: {selective_matches_dedup_space}/{count}"
    );
    println!(
        "  Selective >10% faster than dedup: {selective_faster_than_dedup}/{count}"
    );

    println!();
    println!("─── Q4: Key widening / native fallback comparison ───");
    println!("  Concat widens:      {concat_widens}/{count}");
    println!("  Dedup widens:       {dedup_widens}/{count}");
    println!("  Selective widens:   {selective_widens}/{count}");
    println!("  Concat native FB:   {concat_native_fb}/{count}");
    println!("  Dedup native FB:    {dedup_native_fb}/{count}");
    println!("  Selective native FB:{selective_native_fb}/{count}");

    println!();
    println!("─── CSV output for post-processing ───");
    println!("test_case,strategy,time_us,time_us_stddev,dict_len,key_widened,native_fallback,ipc_bytes");
    for (name, results) in all_results {
        for r in results {
            println!(
                "{},{},{:.1},{:.1},{},{},{},{}",
                name.replace(',', ";"),
                r.strategy,
                r.time_us,
                r.time_us_stddev,
                r.unified_dict_len,
                r.key_widened,
                r.native_fallback,
                r.ipc_size_bytes,
            );
        }
    }
}
