# Quiver (Experimental) -- Arrow-Based Persistence for OTAP Dataflow

Quiver is a standalone, embeddable Arrow-based segment store packaged as a
reusable Rust crate. It provides durable buffering with crash recovery for
telemetry pipelines. While developed for
[otap-dataflow](https://github.com/open-telemetry/otel-arrow), it is designed
to be reusable in any system that needs durable buffering around Apache Arrow.

See [ARCHITECTURE.md](ARCHITECTURE.md) for full design details including file
formats, recovery semantics, and retention policies.

## Status

**Experimental** -- This crate is under active development and the API may
change. Not yet suitable for production use.

## Overview

Quiver solves a specific problem: **telemetry pipelines need a way to survive
crashes and downstream outages without losing data.** It acts as a durable
buffer between receivers and exporters, persisting Arrow-encoded telemetry to
disk and managing delivery to one or more subscribers.

### How It Works (High Level)

```
                       +-------------------------------------------+
                       |             QuiverEngine                   |
                       |                                           |
  RecordBundle ------->|  +-------+    +--------------+            |
  (ingest)             |  |  WAL  |--->| Open Segment |            |
                       |  +-------+    |  (in-memory  |            |
                       |    crash      |  accumulator) |           |
                       |    recovery   +------+-------+            |
                       |                      |                    |
                       |               finalize (size/time/flush)  |
                       |                      |                    |
                       |                      v                    |
                       |             +------------------+          |
                       |             |  Segment Store   |          |
                       |             |  (immutable .qseg|          |
                       |             |   files on disk) |          |
                       |             +--------+---------+          |
                       |                      |                    |
                       |          +-----------+-----------+        |
                       |          |                       |        |
                       |   +------+------+   +------+------+      |
                       |   | Subscriber A|   | Subscriber B|      |
                       |   | (progress   |   | (progress   |      |
                       |   |  tracking)  |   |  tracking)  |      |
                       |   +-------------+   +-------------+      |
                       +-------------------------------------------+
```

1. **Ingest**: `RecordBundle`s arrive and are appended to the WAL (for crash
   recovery) and buffered in the open segment's in-memory accumulators.
2. **Finalize**: When the open segment exceeds a size or duration threshold, it
   is written to disk as an immutable `.qseg` segment file. The WAL is then
   eligible for truncation.
3. **Consume**: Subscribers (exporters) claim bundles from finalized segments
   via `next_bundle()`. Each subscriber tracks its own progress independently.
4. **Acknowledge**: After processing, the subscriber calls `ack()` (success) or
   `reject()` (permanent failure). Calling `defer()` or dropping the handle
   returns the bundle for retry.
5. **Cleanup**: Once every subscriber has acknowledged all bundles in a segment,
   the segment is deleted.

### Key Design Principles

- **Arrow-native**: Segments are containers of Arrow IPC streams -- standard
  tooling can read individual streams directly.
- **Immutable segments**: Once finalized, segment files never change (enforced
  via read-only permissions and CRC integrity checks).
- **Single writer**: Each `QuiverEngine` instance owns one writer (no
  cross-instance locking).
- **Multi-subscriber**: Independent consumers with per-subscriber progress files
  and at-least-once delivery semantics.
- **Bounded resources**: Configurable caps on WAL size, segment count, and disk
  budget with backpressure or drop-oldest policies.
- **Crash recovery**: WAL replay on startup restores the open segment; progress
  files restore subscriber state without log replay.

## Features

- **Write-Ahead Log (WAL)**: Crash recovery with configurable flush policies
  (`DurabilityMode::Wal` or `DurabilityMode::SegmentOnly` for ~3x throughput)
- **Segment Storage**: Immutable Arrow IPC files (`.qseg`) with optional
  memory-mapped reads for zero-copy access
- **Multi-Subscriber**: Independent consumers with at-least-once delivery and
  out-of-order acknowledgement support
- **Progress Tracking**: CRC-protected binary progress files per subscriber
  with atomic write-fsync-rename updates
- **Disk Budget**: Watermark-based capacity enforcement with backpressure or
  drop-oldest overflow policies
- **Automatic Cleanup**: Segments deleted once all subscribers complete; WAL
  truncated after segment finalization

## Quick Start

```bash
cd rust/otap-dataflow
cargo test -p otap-df-quiver      # unit tests + doc tests
cargo bench -p otap-df-quiver     # Criterion benchmarks
```

## Core Components

### QuiverEngine

The central entry point. Coordinates all subsystems and provides the public
API for ingestion, consumption, maintenance, and shutdown.

```
QuiverEngine
  |-- WalWriter            (append-only crash-recovery log)
  |-- OpenSegment          (in-memory accumulator for current segment)
  |-- SegmentStore         (manages finalized .qseg files on disk)
  |-- SubscriberRegistry   (subscriber lifecycle + progress tracking)
  |-- DiskBudget           (capacity enforcement)
  '-- PersistenceMetrics   (observability counters)
```

**Key methods:**

| Method | Async | Description |
|--------|-------|-------------|
| `open(config, budget)` | Yes | Initialize engine, replay WAL, load subscribers |
| `builder(config)` | -- | Fluent builder with `with_budget()` / `build()` |
| `ingest(bundle)` | Yes | Append to WAL + accumulate; returns after fsync |
| `next_bundle(id, timeout, cancel)` | Yes | Wait for next available bundle |
| `poll_next_bundle(id)` | No | Non-blocking check for available data |
| `claim_bundle(id, ref)` | No | Claim a specific bundle by reference |
| `maintain()` | Yes | Flush progress files, clean up completed segments |
| `flush()` | Yes | Force-finalize the current open segment |
| `shutdown()` | Yes | Finalize + cleanup for graceful shutdown |
| `register_subscriber(id)` | No | Add a subscriber (auto-resume or start latest) |
| `activate_subscriber(id)` | No | Mark subscriber active (orphan detection cutoff) |

### RecordBundle (Trait)

The generic ingestion unit. A `RecordBundle` is a fixed-width array of
optional payload slots, where each slot holds an Arrow `RecordBatch`. In OTAP,
slots map to payload types like `Logs`, `LogAttrs`, `ScopeAttrs`, etc.

### Segment File (`.qseg`)

Immutable files containing multiple Arrow IPC streams with a stream directory
and batch manifest. The format supports schema evolution within a segment --
when a payload's schema changes, a new stream is allocated. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the full file layout.

```
+---------------------------+
| Stream Data Region        |
|   Stream 0: Arrow IPC     |
|   Stream 1: Arrow IPC     |
|   ...                     |
+---------------------------+
| Stream Directory (IPC)    |
+---------------------------+
| Batch Manifest (IPC)      |
+---------------------------+
| Footer (version-specific) |
+---------------------------+
| Trailer (16 bytes, CRC)   |
+---------------------------+
```

### Write-Ahead Log (WAL)

Append-only log for crash recovery. Each incoming `RecordBundle` is serialized
as a length-prefixed, CRC-protected entry. On startup, the WAL is replayed to
reconstruct the in-memory open segment state.

```
wal/
  quiver.wal           <-- active append target
  quiver.wal.1         <-- rotated (oldest)
  quiver.wal.2         <-- rotated
  quiver.wal.cursor    <-- consumer progress (24 bytes, CRC-protected)
```

### Subscriber System

Each subscriber maintains independent progress through the segment stream.
Progress is tracked via per-segment bitmaps (supporting out-of-order
acknowledgement) and persisted in binary files (`quiver.sub.<id>`).

**Bundle lifecycle:**

```
                  +---> Acked (terminal)
                  |
Pending --> Claimed
                  |
                  +---> Rejected (terminal, logged as Dropped)
                  |
                  +---> Pending (via defer or implicit drop)
```

### Disk Budget

Watermark-based capacity enforcement:

- **Soft cap** = `hard_cap - segment_headroom`: gates new ingestion
- **Hard cap**: absolute ceiling; headroom reserved for in-flight finalization
- When exceeded with `Backpressure` policy: `ingest()` returns
  `QuiverError::StorageAtCapacity`
- When exceeded with `DropOldest` policy: oldest segments are evicted

## Usage

```rust,no_run
use quiver::{QuiverEngine, QuiverConfig, DiskBudget, RetentionPolicy, SubscriberId, CancellationToken};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use a durable filesystem path, not /tmp (which may be tmpfs)
    let data_dir = PathBuf::from("/var/lib/quiver/data");
    let config = QuiverConfig::default().with_data_dir(&data_dir);

    // Configure disk budget (10 GB cap with backpressure).
    // for_config() reads segment/WAL sizes from the config and validates
    // that hard_cap >= wal_max + 2 * segment_target.
    let budget = Arc::new(DiskBudget::for_config(
        10 * 1024 * 1024 * 1024,  // 10 GB hard cap
        &config,
        RetentionPolicy::Backpressure,
    )?);
    let engine = QuiverEngine::open(config, budget).await?;

    // Register a subscriber
    let sub_id = SubscriberId::new("my-exporter")?;
    engine.register_subscriber(sub_id.clone())?;
    engine.activate_subscriber(&sub_id)?;

    // Create a cancellation token for graceful shutdown
    let shutdown = CancellationToken::new();

    // Ingest data (bundles from upstream)
    // engine.ingest(&bundle).await?;

    // Consume bundles with timeout and cancellation support
    loop {
        match engine.next_bundle(&sub_id, Some(Duration::from_secs(5)), Some(&shutdown)).await {
            Ok(Some(handle)) => {
                // Process the bundle...
                handle.ack();  // Acknowledge successful processing
            }
            Ok(None) => continue,  // Timeout, check shutdown condition
            Err(e) if e.is_cancelled() => break,  // Graceful shutdown
            Err(e) => return Err(e.into()),
        }
    }

    // Periodic maintenance
    engine.maintain().await?;

    Ok(())
}
```

### Handling Backpressure

When the disk budget is exhausted, `ingest()` returns
`QuiverError::StorageAtCapacity`. The embedding layer should handle this by
slowing ingestion:

```rust,no_run
use quiver::QuiverError;

match engine.ingest(&bundle).await {
    Ok(()) => { /* success */ }
    Err(e) if e.is_at_capacity() => {
        // Backpressure: wait for subscribers to catch up and segments to be cleaned
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        engine.maintain().await?;  // Try to clean up completed segments
        // Retry ingestion...
    }
    Err(e) => return Err(e),  // Other errors are fatal
}
```

## Configuration

### Defaults

| Setting | Default | Description |
|---------|---------|-------------|
| `durability` | `Wal` | WAL-based crash recovery (`SegmentOnly` for ~3x throughput) |
| `segment.target_size` | 32 MB | Target segment size before finalization |
| `segment.max_open_duration` | 5 s | Max time before forced finalization |
| `wal.max_size` | 128 MB | Aggregate WAL footprint cap |
| `wal.rotation_target` | 64 MB | Active WAL file size before rotation |
| `wal.flush_interval` | 25 ms | Fsync cadence |
| `wal.max_rotated_files` | 10 | Max WAL files (active + rotated) |
| `retention.policy` | `Backpressure` | Behavior when disk budget exceeded |
| `read_mode` | `Mmap` | Memory-mapped or standard I/O reads |

### Durability Modes

| Mode | Throughput | Data Loss on Crash | Use Case |
|------|------------|-------------------|----------|
| `Wal` (default) | Baseline | Since last WAL fsync | Production, critical data |
| `SegmentOnly` | ~3x higher | Entire open segment | High-throughput, loss-tolerant |

## On-Disk Layout

```
<data_dir>/
  wal/
    quiver.wal              # Active WAL (append-only)
    quiver.wal.1            # Rotated WAL files
    quiver.wal.cursor       # Consumer cursor sidecar (24 bytes)
  0000000000000000.qseg     # Segment files (zero-padded sequence)
  0000000000000001.qseg
  quiver.sub.exporter-otlp  # Per-subscriber progress files
  quiver.sub.backup-s3
```

## Error Handling

Key error variants on `QuiverError`:

| Variant | Meaning | Helper |
|---------|---------|--------|
| `StorageAtCapacity` | Disk budget exceeded | `is_at_capacity()` |
| `Cancelled` | Graceful shutdown requested | `is_cancelled()` |
| `Wal` | WAL I/O or format error | -- |
| `Segment` | Segment I/O or format error | -- |
| `InvalidConfig` | Configuration validation failed | -- |

All I/O errors are wrapped with context. Use `is_recoverable()` to determine
if retry is appropriate.

## Cargo Features

| Feature | Default | Description |
|---------|---------|-------------|
| `mmap` | Yes | Memory-mapped segment reads for zero-copy access |
| `serde` | No | Serialization support for configuration types |
| `otap-dataflow-integrations` | Yes | Integration with otap-dataflow types |
