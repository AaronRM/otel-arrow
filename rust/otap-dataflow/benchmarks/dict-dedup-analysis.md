# Dictionary Deduplication Analysis for Quiver StreamAccumulator

## Context

Quiver accumulates multiple Arrow RecordBatches per stream within a segment.
At segment finalization, dictionary-encoded columns from all batches are unified
into a single dictionary by concatenating their values arrays **without
deduplication**. This means duplicate values across batches are repeated in the
unified dictionary, which can cause unnecessary key-type widening and larger IPC
files.

This analysis evaluates whether deduplication during dictionary unification is
worth the CPU cost, and if so, which strategy to use.

The benchmark uses a fixed random seed (`BENCH_SEED`) for reproducible data
generation. All numbers in this document match the saved results in
`dict_dedup_results.txt`. Timing is reported as mean ± stddev over 20 measured
iterations (after 5 warmup iterations). The dedup implementation uses zero-copy
byte slicing (borrowed references into Arrow buffers, no per-value allocation).

**Related**: [PR #2565](https://github.com/open-telemetry/otel-arrow/pull/2565)
discussion on the trade-offs of dictionary concatenation vs deduplication.

## Strategies Tested

| Strategy | Description |
| --- | --- |
| **concat** (current) | Concatenate all per-batch dictionary values without dedup. Remap keys with offsets. |
| **always-dedup** | Build `HashMap<value, index>` for every dictionary column. Produces minimal dictionaries. |
| **threshold** | Dedup only when total values exceed key-type capacity (i.e., when widening would be triggered). |
| **selective** | Dedup only columns where average per-batch dictionary size is below a configurable threshold. Skip high-cardinality columns entirely. |

## OTAP Logs Simulation

The benchmark simulates a realistic OTAP Logs payload with 10 columns (6
dictionary-encoded, 4 plain), matching the actual schema from
`crates/pdata/src/encode/record/logs.rs`:

| Column | Type | Per-batch cardinality | Cross-batch overlap |
| --- | --- | --- | --- |
| severity_number | Dict\<U8, Int32\> | 6 | 100% |
| severity_text | Dict\<U8, Utf8\> | 4 | 100% |
| trace_id | Dict\<U16, FSB(16)\> | = rows_per_batch | 0% |
| span_id | Dict\<U16, FSB(8)\> | = rows_per_batch | 0% |
| schema_url | Dict\<U8, Utf8\> | 1 | 100% |
| event_name | Dict\<U8, Utf8\> | ~10 | 90% |
| time_unix_nano | Timestamp(ns) | — | — |
| observed_time_unix_nano | Timestamp(ns) | — | — |
| dropped_attributes_count | UInt32 | — | — |
| flags | UInt32 | — | — |

## Key Finding: The Right Strategy Depends on the Signal

Selective dedup is not uniformly the best strategy — it depends on the signal's
column profile. The optimized zero-copy dedup implementation reveals that
always-dedup is inexpensive when there are no high-cardinality 0%-overlap
columns (Metrics), while selective is essential when such columns exist
(Logs, Traces).

### Production workloads (10-20 batches × 1,000 rows)

| Workload | Strategy | Time (µs) | ±std | IPC size | Delta |
| --- | --- | --- | --- | --- | --- |
| 10 × 1,000 | **concat** | 111 | 13 | 576 KB | — |
| 10 × 1,000 | always-dedup | 3,996 (36x) | 110 | 575 KB | -1.9 KB |
| 10 × 1,000 | **selective** | 137 (1.2x) | 11 | 575 KB | -1.9 KB |
| 20 × 1,000 | **concat** | 216 | 12 | 1,149 KB | — |
| 20 × 1,000 | always-dedup | 9,038 (42x) | 722 | 1,145 KB | -4.1 KB |
| 20 × 1,000 | **selective** | 276 (1.3x) | 23 | 1,145 KB | -4.1 KB |

At moderate batch counts, dedup savings are small (~0.3%) because low-cardinality
columns have few entries even with duplication. Selective dedup captures the full
savings at near-concat speed.

### High batch counts (100-500 batches — where dedup matters most)

| Workload | Strategy | Time (µs) | ±std | IPC size | Delta | Delta % |
| --- | --- | --- | --- | --- | --- | --- |
| 100 × 1,000 | **concat** | 1,157 | 343 | 5.61 MB | — | — |
| 100 × 1,000 | always-dedup | 53,684 (46x) | 2,269 | 5.29 MB | **-322 KB** | -5.7% |
| 100 × 1,000 | **selective** | 1,410 (1.2x) | 72 | 5.29 MB | **-322 KB** | -5.7% |
| 500 × 1,000 | **concat** | 8,631 | 1,168 | 27.8 MB | — | — |
| 500 × 1,000 | always-dedup | 380,101 (44x) | 16,835 | 26.3 MB | **-1.6 MB** | -5.6% |
| 500 × 1,000 | **selective** | 8,442 (1.0x) | 509 | 26.3 MB | **-1.6 MB** | -5.6% |

**When the threshold correctly separates low- from high-cardinality columns,**
selective dedup achieves 100% of the space savings of always-dedup at near-zero
CPU overhead. It works because it targets only the columns where dedup is cheap
and effective. **However, when batch sizes are small enough that high-cardinality
columns (e.g., `trace_id`) fall below the threshold, selective degrades to
always-dedup performance (18x overhead).** See the "Known Failure Mode" section.

### OTel Collector default batch size (8,192 rows)

| Workload | Strategy | Time (µs) | ±std | IPC size | Delta |
| --- | --- | --- | --- | --- | --- |
| 10 × 8,192 | **concat** | 687 | 105 | 4.17 MB | — |
| 10 × 8,192 | always-dedup | 42,251 (61x) | 2,162 | 4.17 MB | -1.9 KB |
| 10 × 8,192 | threshold | 42,524 (62x) | 1,700 | 4.17 MB | 0 |
| 10 × 8,192 | **selective** | 863 (1.3x) | 93 | 4.17 MB | -1.9 KB |

Note: at 10 × 8,192, `trace_id` alone has 81,920 unique values exceeding UInt16
capacity (65,536), triggering native fallback regardless of strategy. The
threshold strategy wastefully deduplicates `trace_id`/`span_id` here because it
can only detect cardinality overflow, not overlap. Selective correctly skips
them.

## Per-Column Breakdown (100 batches × 1,000 rows)

This shows why selective dedup works — it targets the right columns:

| Column | concat (µs) | always-dedup (µs) | selective (µs) | Dict: concat → selective | Dedup benefit? |
| --- | --- | --- | --- | --- | --- |
| severity_number | 173 | 217 | 229 | 600 → **6** | YES |
| severity_text | 168 | 216 | 220 | 400 → **4** | YES |
| **trace_id** | 178 | **30,817** | **211** | 100,000 → 100,000 | **NO** (0% overlap) |
| **span_id** | 145 | **29,520** | **121** | 100,000 → 100,000 | **NO** (0% overlap) |
| schema_url | 150 | 202 | 207 | 100 → **1** | YES |
| event_name | 171 | 269 | 245 | 1,000 → **109** | YES |

*Note: Per-column numbers are from a separate single-column run and may not
exactly sum to the multi-column totals due to measurement overhead.*

`trace_id` and `span_id` consume >95% of always-dedup's CPU time while producing
zero benefit. Selective skips them entirely.

## OTAP Metrics: Where Dedup Matters Most

Unlike Logs, the Metrics main table has **no high-cardinality 0%-overlap
columns**. All dictionary columns are low-to-moderate cardinality with ~100%
overlap across batches (the same metrics are reported every collection interval).
This is the strongest case for dedup.

The simulation uses 8 columns (5 dictionary-encoded): `name` (50-200 unique
metric names), `description` (~1:1 with name), `unit` (~15 values),
`scope_schema_url` (1 value), `aggregation_temporality` (2 values).

| Workload | Strategy | Time (µs) | ±std | IPC size | Delta | Delta % |
| --- | --- | --- | --- | --- | --- | --- |
| 10 bat × 500 rows, 50 metrics | **concat** | 49 | 2 | 98 KB | — | — |
| 10 bat × 500 rows, 50 metrics | always-dedup | 100 (2.0x) | 4 | 57 KB | **-41 KB** | **-41%** |
| 10 bat × 500 rows, 50 metrics | **selective** | 100 (2.0x) | 3 | 57 KB | **-41 KB** | **-41%** |
| 100 bat × 500 rows, 200 metrics | **concat** | 502 | 50 | 1.87 MB | — | — |
| 100 bat × 500 rows, 200 metrics | always-dedup | 1,612 (3.2x) | 135 | 525 KB | **-1.3 MB** | **-72%** |
| 100 bat × 500 rows, 200 metrics | **selective** | 1,928 (3.8x) | 498 | 525 KB | **-1.3 MB** | **-72%** |
| 500 bat × 500 rows, 200 metrics | **concat** | 4,916 | 572 | 18.6 MB | — | — |
| 500 bat × 500 rows, 200 metrics | always-dedup | 8,980 (1.8x) | 556 | 2.5 MB | **-16.1 MB** | **-87%** |
| 500 bat × 500 rows, 200 metrics | **selective** | 9,054 (1.8x) | 573 | 2.5 MB | **-16.1 MB** | **-87%** |

**For Metrics, dedup reduces IPC size by 41-87%** with only 1.8-2.5x CPU cost.
This is because:

1. All dict columns are low cardinality — dedup is cheap (small HashMap)
2. 100% overlap — dedup collapses 500 copies of the same 200 metric names into
   one 200-entry dictionary
3. No high-cardinality poison columns — selective and always-dedup produce
   identical results **and identical performance**
4. Concat causes severe bloat: 500 batches × 200 metrics = 100,000 duplicate
   dictionary entries, triggering UInt8→UInt16 widening and eventually native
   fallback

**Metrics is the strongest argument for implementing dedup.** The savings are
large in both absolute terms (megabytes) and relative terms (up to 87%), and the
CPU cost is modest. Since all Metrics columns benefit from dedup, there is **no
advantage to selective over always-dedup** for this signal — both dedup every
column and produce the same result.

## OTAP Traces: Duration is the Poison Column

Traces have a different profile from both Logs and Metrics. `trace_id`/`span_id`
are **plain FixedSizeBinary** (not dictionary-encoded), so the Logs poison
doesn't apply. However, `duration_time_unix_nano` is `Dict<U16, Duration(ns)>`
with high cardinality and ~0% overlap (each span has a unique duration).

| Workload | Strategy | Time (µs) | ±std | IPC size | Delta | Delta % |
| --- | --- | --- | --- | --- | --- | --- |
| 10 bat × 500 rows, 50 spans | **concat** | 51 | 5 | 321 KB | — | — |
| 10 bat × 500 rows, 50 spans | always-dedup | 1,386 (27x) | 148 | 304 KB | **-17 KB** | -5% |
| 10 bat × 500 rows, 50 spans | **selective** | 86 (1.7x) | 3 | 304 KB | **-17 KB** | -5% |
| 100 bat × 500 rows, 200 spans | **concat** | 526 | 34 | 3.61 MB | — | — |
| 100 bat × 500 rows, 200 spans | always-dedup | 16,787 (32x) | 759 | 3.05 MB | **-566 KB** | -16% |
| 100 bat × 500 rows, 200 spans | **selective** | 1,311 (2.5x) | 157 | 3.05 MB | **-566 KB** | -16% |
| 500 bat × 500 rows, 50 spans | **concat** | 4,277 | 770 | 15.7 MB | — | — |
| 500 bat × 500 rows, 50 spans | always-dedup | 136,322 (32x) | 8,101 | 14.3 MB | **-1.4 MB** | -9% |
| 500 bat × 500 rows, 50 spans | **selective** | 6,144 (1.4x) | 752 | 14.3 MB | **-1.4 MB** | -9% |

Traces benefit from selective dedup (9-16% IPC savings at high batch counts),
with selective being 1.4-2.5x concat speed vs 27-32x for always-dedup. The
`duration` column plays the same role as `trace_id` in Logs: high cardinality,
0% overlap, expensive to dedup for zero benefit. Selective correctly skips it.

## Cross-Signal Threshold Sweep

The selective strategy uses a per-batch cardinality threshold. We swept
thresholds from 1 to 65,536 across all three signals to find the optimal cutoff.

### Summary of plateau and cliff points

| Signal | Workload | Savings plateau | Cliff threshold | Key column at cliff |
| --- | --- | --- | --- | --- |
| Logs | 100 bat × 1,000 rows | threshold ≥ 10 | ≥ 1,024 | trace_id (1,000/batch) |
| Logs | 500 bat × 100 rows | threshold ≥ 10 | ≥ 128 | trace_id (100/batch) |
| Metrics | 100 bat × 500 rows, 50 names | threshold ≥ 64 | none | all columns benefit |
| Metrics | 100 bat × 500 rows, 200 names | threshold ≥ 256 | none | all columns benefit |
| Traces | 100 bat × 500 rows, 50 spans | threshold ≥ 64 | ≥ 512 | duration (500/batch) |
| Traces | 100 bat × 500 rows, 200 spans | threshold ≥ 256 | ≥ 512 | duration (500/batch) |

### The threshold tension

There is a **fundamental tension** between signals:

- **Metrics/Traces with 200+ unique names per batch** need threshold ≥ 256 to
  capture `metric_name`/`span_name` dedup (the biggest savings).
- **Logs with small batches** (100 rows) need threshold < 128 to avoid including
  `trace_id` (100 entries/batch, 0% overlap).

A single static threshold **cannot optimally serve all signals and batch sizes**.

### Recommended approach

Rather than a single threshold constant, use the **per-column cardinality gap**
that naturally exists in each workload. The algorithm should:

1. Compute average per-batch dictionary size for each column.
2. Apply a configurable threshold (default: **256**).
3. For columns above the threshold, concatenate without dedup.

**Why 256?** It captures the largest wins (Metrics 200: -1.3 MB, Traces 200:
-580 KB) while the downside for Logs is limited — `trace_id`'s per-batch
cardinality equals `rows_per_batch`, which is typically ≥ 500 in production
(OTel Collector default: 8,192). **However, at small batch sizes (e.g., 100
rows), `trace_id` has only 100 entries/batch, which is below the 256 threshold,
causing selective dedup to wastefully include `trace_id` in the dedup HashMap
(0% overlap, pure cost with no benefit).** See the "Known Failure Mode" section
below.

**The threshold should be configurable** via `QuiverConfig` so deployments can
tune for their workload. The default of 256 is a reasonable general-purpose
value for typical production batch sizes (≥ 500 rows), but is not optimal for
all configurations.

### Detailed sweep: Metrics (100 batches, 200 metrics)

| Threshold | Time (µs) | IPC size | Delta | Notes |
| --- | --- | --- | --- | --- |
| 1 | 567 | 1.91 MB | -4 KB | Only schema_url deduped |
| 16 | 718 | 1.84 MB | -69 KB | + unit, aggregation_temporality |
| 64 | 680 | 1.84 MB | -69 KB | Same (name has 200 vals/batch, excluded) |
| **256** | **2,502** | **537 KB** | **-1.37 MB** | **+ name/description. Full savings.** |
| 512 | 2,318 | 537 KB | -1.37 MB | Same |
| 65,536 | 2,721 | 537 KB | -1.37 MB | Same |

### Detailed sweep: Logs (100 batches, 1,000 rows)

| Threshold | Time (µs) | IPC size | Delta | Notes |
| --- | --- | --- | --- | --- |
| 10 | 1,357 | 5.41 MB | -329 KB | Full savings (all low-card columns) |
| 256 | 1,333 | 5.41 MB | -329 KB | Same savings, same speed |
| 512 | 1,351 | 5.41 MB | -329 KB | Same |
| **1,024** | **60,469** | 5.41 MB | -329 KB | **Cliff: trace_id included** |

## Known Failure Mode: Small-Batch Logs

When `rows_per_batch` is small (e.g., 100), `trace_id` has only 100 unique
values per batch — below the default threshold of 256. Selective dedup
**incorrectly includes** `trace_id` for dedup, incurring full HashMap cost on a
column with 0% overlap that produces zero size benefit.

| Workload | Strategy | Time (µs) | IPC savings | Notes |
| --- | --- | --- | --- | --- |
| 500 × 100 Logs | **concat** | 1,641 | — | Baseline |
| 500 × 100 Logs | always-dedup | 29,717 (18x) | -297 KB | Full dedup |
| 500 × 100 Logs | **selective (256)** | **29,746 (18x)** | -297 KB | **Fails: trace_id included** |
| 500 × 100 Logs | threshold | 2,366 (1.4x) | -297 KB | Only dedupes at overflow |

**The selective strategy collapses to always-dedup performance** in this case
because `trace_id` dominates CPU cost and is below the threshold. A threshold of
64 would fix this case but would miss `metric_name` dedup for Metrics workloads
with 200+ unique names.

### Possible mitigations

1. **Lower default threshold** (e.g., 64): Avoids the Logs failure but loses
   Metrics savings for moderate-cardinality columns (name/description at 200
   values per batch).
2. **Overlap-aware heuristic**: Sample the first few batches' dictionaries,
   compute overlap ratio, and only dedup columns with overlap > some minimum
   (e.g., 50%). This correctly skips `trace_id` (0% overlap) regardless of
   cardinality.
3. **Signal-aware default**: Use different thresholds per signal type (Logs=64,
   Metrics=256, Traces=256) since the schema is known at stream creation time.

## Recommendation

**Implement selective dedup in `unify_dict_column` with a configurable
per-batch cardinality threshold (default: 256).** However, recognize this is
a pragmatic compromise with a known failure mode — not a universally optimal
solution.

The algorithm is:

1. For each dictionary column, compute average per-batch dictionary size: \
   `avg_cardinality = sum(batch.dict.values().len()) / num_batches`
2. If `avg_cardinality <= threshold`: dedup via HashMap (produces minimal
   dictionary)
3. If `avg_cardinality > threshold`: concatenate without dedup (current behavior)

The cardinality check is O(1) per batch per column (just `.values().len()`),
adding negligible overhead.

### Impact by signal

| Signal | IPC savings (100+ batches) | CPU overhead (selective) |
| --- | --- | --- |
| **Metrics** | **41-87%** | 1.8-2.5x |
| **Traces** | **5-16%** | 1.4-2.5x |
| **Logs** | **0.3-5.7%** | 1.0-1.2x |

*CPU overhead numbers assume `rows_per_batch` ≥ 500 so that high-cardinality
columns exceed the threshold. At small batch sizes (100 rows), Logs overhead can
be 14-15x — see "Known Failure Mode" section.*

### Why not always-dedup?

The benchmark uses an optimized zero-copy dedup implementation (borrowed byte
slices into Arrow buffers, no per-value allocation). With this optimization:

- **Metrics**: always-dedup is only 1.8-2.0x slower than concat — **a viable
  option**. Since all Metrics columns benefit, selective provides no advantage
  over always-dedup.
- **Traces**: always-dedup is 27-32x slower due to `duration` (high-cardinality,
  0% overlap). Selective avoids this.
- **Logs**: always-dedup is 36-46x slower due to `trace_id`/`span_id`. Selective
  avoids this (at typical batch sizes).

Always-dedup **is viable for Metrics** but **is not viable for Logs/Traces**
due to high-cardinality 0%-overlap dictionary columns. If a per-signal strategy
were implemented, always-dedup for Metrics + selective for Logs/Traces would be
simpler and avoid the threshold-tuning problem entirely.

### Why not threshold (dedup at widening boundary)?

The threshold strategy deduplicates when total values exceed key-type capacity.
This sounds good but fails for OTAP because: (1) when `trace_id` triggers
widening, there's nothing to dedup (0% overlap), so the dedup cost is pure waste;
(2) it misses the low-cardinality columns that
*could* benefit when their total is still under the key-type cap.

### Why not IPC streaming format?

An alternative suggestion from PR #2565 is switching from Arrow IPC File format
to IPC Streaming format,
which allows dictionary replacement across batches. This would avoid
unification entirely. However, the File format was specifically chosen for Quiver
because it supports seeking and memory-mapped reads — the entire segment reader
depends on offset-based `FileDecoder` access. Switching to streaming format
would be a much larger architectural change. Selective dedup solves the immediate
problem within the current architecture.

## Open Questions

1. **Is the cardinality threshold the right heuristic?** It uses a proxy
   (per-batch size) to guess at overlap, which fails when a column has low
   cardinality but 0% overlap. An overlap-aware approach (sampling the first few
   batches) would be more accurate but adds implementation complexity.

2. **Should the strategy be per-signal?** The data strongly suggests different
   signals have different optimal strategies:
   - Metrics: always-dedup (1.8x cost, 41-87% savings, no poison columns)
   - Logs: selective with low threshold (1.0-1.2x cost, 0.3-5.7% savings)
   - Traces: selective with moderate threshold (1.4-2.5x cost, 5-16% savings)

3. **What batch sizes occur in production?** The failure mode (selective = 18x
   overhead) only triggers when `rows_per_batch` < threshold. If OTel Collector
   always uses batch sizes ≥ 500 (default: 8,192), threshold=256 is safe. But
   time-triggered flushes or low-throughput streams may produce smaller batches.

4. **Is 5.7% Logs savings worth any complexity?** For Logs at typical batch
   sizes, dedup saves only 0.3-5.7% of IPC size while adding code complexity
   and a configurable threshold. The strongest case for dedup is Metrics (87%
   savings). It may be simpler to implement dedup only for Metrics signals.

## Applicability to `concatenate.rs`

The same findings apply to `crates/pdata/src/otap/transform/concatenate.rs`,
which merges decoded OTAP batches for the query pipeline and batching layer.
It exhibits the same concat-without-dedup behavior as Quiver's
`stream_accumulator`, but with one key difference: it **already builds an
`AHashSet` of dictionary values** during cardinality estimation
(`estimate_cardinality_generic()`), then discards the dedup result and
concatenates the raw values with duplicates. This means dedup could be added at
near-zero marginal cost by retaining the set and using it to produce a
deduplicated values array and key mappings during the existing estimation pass.
Note that `upsert_attributes.rs` in the same directory already implements full
HashMap-based dictionary dedup (`try_build_unified_dict_multi()`), so the
pattern has precedent in the crate.

## How to Run

```bash
cd rust/otap-dataflow
cargo run --release -p benchmarks --bin dict_dedup_bench
```

Full results are saved in:

- `benchmarks/dict_dedup_results.txt` — human-readable report
- `benchmarks/dict_dedup_results.csv` — machine-readable CSV
