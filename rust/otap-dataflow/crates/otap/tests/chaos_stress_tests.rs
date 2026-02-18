// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Long-running chaos stress tests for the durable buffer.
//!
//! These tests exercise extreme scenarios using the chaos exporter:
//! backpressure engagement/recovery, drop-oldest eviction, multi-core
//! pipelines, large capacities, degraded connectivity, and rapid mode
//! switching.
//!
//! All tests are marked `#[ignore]` and are **not** run in CI by default.
//! Run them explicitly with:
//!
//! ```bash
//! # Run all chaos stress tests
//! cargo nextest run -p otap-df-otap --run-ignored only -E 'test(chaos_stress)'
//!
//! # Run a specific test
//! cargo nextest run -p otap-df-otap --run-ignored only -E 'test(stress_backpressure)'
//! ```
//!
//! Each test uses the chaos exporter's JSON control file to switch modes
//! on the fly (online → offline → flaky → online), driving the durable
//! buffer through realistic failure and recovery scenarios.

use otap_df_config::observed_state::{ObservedStateSettings, SendPolicy};
use otap_df_config::pipeline::{PipelineConfig, PipelineConfigBuilder, PipelineType};
use otap_df_config::{DeployedPipelineKey, PipelineGroupId, PipelineId};
use otap_df_engine::context::ControllerContext;
use otap_df_engine::control::{PipelineControlMsg, pipeline_ctrl_msg_channel};
use otap_df_engine::entity_context::set_pipeline_entity_key;
use otap_df_otap::chaos_exporter::CHAOS_EXPORTER_URN;
use otap_df_otap::durable_buffer_processor::DURABLE_BUFFER_URN;
use otap_df_otap::fake_data_generator::OTAP_FAKE_DATA_GENERATOR_URN;
use otap_df_otap::OTAP_PIPELINE_FACTORY;
use otap_df_state::store::ObservedStateStore;
use otap_df_telemetry::InternalTelemetrySystem;
use serde_json::json;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// ─────────────────────────────────────────────────────────────────────────────
// Control File Helpers
// ─────────────────────────────────────────────────────────────────────────────

// ── Realistic network latency constants ──────────────────────────────────────
//
// Real-world OTAP/gRPC round-trip times for reference:
//   - Same region:    5–20 ms
//   - Cross-region:   50–150 ms
//   - Cross-continent: 100–300 ms
//   - Connection refused (port closed): ~1–10 ms
//   - Connection timeout (host unreachable): 30–120 s (OS TCP SYN retransmit)
//
// We simulate "same region" for online and "connection timeout" for offline.

/// Baseline online latency: 20 ms ± 10 ms jitter (simulates same-region gRPC).
const ONLINE_LATENCY_MS: u64 = 20;
const ONLINE_JITTER_MS: u64 = 10;

/// Offline latency: 30 s ± 10 s jitter (simulates real TCP SYN timeout to an
/// unreachable host — the OS retransmits SYN packets before giving up, which
/// typically takes 30–120 s depending on the platform and sysctl settings).
const OFFLINE_LATENCY_MS: u64 = 30_000;
const OFFLINE_JITTER_MS: u64 = 10_000;

/// Set chaos exporter to online mode with realistic network latency.
fn set_online(path: &Path) {
    let state = json!({
        "mode": "online",
        "latency_ms": ONLINE_LATENCY_MS,
        "jitter_ms": ONLINE_JITTER_MS,
    });
    std::fs::write(path, state.to_string()).expect("failed to write control file");
}

/// Set chaos exporter to online mode with zero latency (fast drain).
#[allow(dead_code)]
fn set_online_fast(path: &Path) {
    std::fs::write(path, r#"{"mode":"online"}"#).expect("failed to write control file");
}

/// Set chaos exporter to offline mode with realistic connection-timeout latency.
/// Each NACK takes 30 ± 10 seconds, simulating a TCP SYN timeout to an unreachable host.
fn set_offline(path: &Path) {
    let state = json!({
        "mode": "offline",
        "latency_ms": OFFLINE_LATENCY_MS,
        "jitter_ms": OFFLINE_JITTER_MS,
    });
    std::fs::write(path, state.to_string()).expect("failed to write control file");
}

/// Set chaos exporter to flaky mode with configurable failure parameters.
fn set_flaky(path: &Path, failure_rate: f64, latency_ms: u64, jitter_ms: u64) {
    let state = json!({
        "mode": "flaky",
        "failure_rate": failure_rate,
        "latency_ms": latency_ms,
        "jitter_ms": jitter_ms,
    });
    std::fs::write(path, state.to_string()).expect("failed to write control file");
}

// ─────────────────────────────────────────────────────────────────────────────
// Measurement Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Recursively measure total disk usage of a directory in bytes.
fn dir_size_bytes(path: &Path) -> u64 {
    if !path.exists() {
        return 0;
    }
    walk_dir_size(path)
}

fn walk_dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += walk_dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Format bytes as a human-readable string for log messages.
fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GiB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MiB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KiB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline Configuration Builder
// ─────────────────────────────────────────────────────────────────────────────

/// Builder for chaos stress test pipeline configurations.
///
/// Always uses the chaos exporter. Configures the fake data generator,
/// durable buffer, and chaos exporter with sensible stress-test defaults.
struct ChaosTestConfig {
    buffer_path: std::path::PathBuf,
    control_file: std::path::PathBuf,
    retention_size_cap: String,
    size_cap_policy: &'static str,
    signals_per_second: usize,
    max_batch_size: usize,
    check_interval: &'static str,
    log_every_n: u64,
    log_body_size: Option<usize>,
}

impl ChaosTestConfig {
    fn new(buffer_path: std::path::PathBuf, control_file: std::path::PathBuf) -> Self {
        Self {
            buffer_path,
            control_file,
            retention_size_cap: "256 MB".into(),
            size_cap_policy: "backpressure",
            signals_per_second: 5000,
            max_batch_size: 100,
            check_interval: "50ms",
            log_every_n: 1000,
            log_body_size: None,
        }
    }

    fn retention_size_cap(mut self, cap: impl Into<String>) -> Self {
        self.retention_size_cap = cap.into();
        self
    }

    fn size_cap_policy(mut self, policy: &'static str) -> Self {
        self.size_cap_policy = policy;
        self
    }

    fn signals_per_second(mut self, rate: usize) -> Self {
        self.signals_per_second = rate;
        self
    }

    fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    #[allow(dead_code)]
    fn log_every_n(mut self, n: u64) -> Self {
        self.log_every_n = n;
        self
    }

    fn log_body_size(mut self, size: usize) -> Self {
        self.log_body_size = Some(size);
        self
    }

    fn build(
        self,
        pipeline_group_id: &PipelineGroupId,
        pipeline_id: &PipelineId,
    ) -> PipelineConfig {
        let mut traffic_config = json!({
                "signals_per_second": self.signals_per_second,
                "max_signal_count": null,
                "max_batch_size": self.max_batch_size,
                "metric_weight": 0,
                "trace_weight": 0,
                "log_weight": 100,
        });
        if let Some(body_size) = self.log_body_size {
            traffic_config["log_body_size"] = json!(body_size);
        }
        let receiver_config = json!({
            "traffic_config": traffic_config,
            "data_source": "static"
        });

        let buffer_config = json!({
            "path": self.buffer_path.to_string_lossy(),
            "poll_interval": "20ms",
            "retention_size_cap": self.retention_size_cap,
            "size_cap_policy": self.size_cap_policy,
            "max_segment_open_duration": "50ms",
            "initial_retry_interval": "100ms",
            "max_retry_interval": "500ms",
            "retry_multiplier": 2.0,
            "max_in_flight": 100,
        });

        let chaos_config = json!({
            "control_file": self.control_file.to_string_lossy(),
            "check_interval": self.check_interval,
            "log_every_n": self.log_every_n,
        });

        PipelineConfigBuilder::new()
            .add_receiver(
                "fake_receiver",
                OTAP_FAKE_DATA_GENERATOR_URN,
                Some(receiver_config),
            )
            .add_processor("durable_buffer", DURABLE_BUFFER_URN, Some(buffer_config))
            .add_exporter("chaos_exporter", CHAOS_EXPORTER_URN, Some(chaos_config))
            .one_of("fake_receiver", ["durable_buffer"])
            .one_of("durable_buffer", ["chaos_exporter"])
            .build(
                PipelineType::Otap,
                pipeline_group_id.clone(),
                pipeline_id.clone(),
            )
            .expect("failed to build chaos stress test pipeline config")
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline Runners
// ─────────────────────────────────────────────────────────────────────────────

/// Run a single-core chaos pipeline.
///
/// The `phase_orchestrator` closure runs in a separate thread and controls
/// the test scenario by manipulating the chaos exporter's control file.
/// When the orchestrator returns, the pipeline is shut down and the
/// orchestrator's return value is forwarded to the caller for assertions.
fn run_chaos_pipeline<F, R>(
    config: PipelineConfig,
    pipeline_group_id: &PipelineGroupId,
    pipeline_id: &PipelineId,
    shutdown_deadline: Duration,
    phase_orchestrator: F,
) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    run_chaos_pipeline_cores(
        config,
        pipeline_group_id,
        pipeline_id,
        1,
        shutdown_deadline,
        phase_orchestrator,
    )
}

/// Run a multi-core chaos pipeline.
///
/// Spawns `num_cores` pipeline instances, each with its own core_id and
/// per-core Quiver data directory (`core_0/`, `core_1/`, …). All cores
/// share the same buffer path root and chaos exporter control file.
///
/// The `phase_orchestrator` controls the test scenario. When it returns,
/// all cores are shut down and the orchestrator's return value is forwarded.
fn run_chaos_pipeline_cores<F, R>(
    config: PipelineConfig,
    pipeline_group_id: &PipelineGroupId,
    pipeline_id: &PipelineId,
    num_cores: usize,
    shutdown_deadline: Duration,
    phase_orchestrator: F,
) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let telemetry_system = InternalTelemetrySystem::default();
    let registry = telemetry_system.registry();
    let controller_ctx = ControllerContext::new(registry.clone());
    let metrics_reporter = telemetry_system.reporter();
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Phase orchestrator thread: runs test phases, then signals shutdown.
    let orch_flag = shutdown_flag.clone();
    let orchestrator_handle = std::thread::spawn(move || {
        let result = phase_orchestrator();
        orch_flag.store(true, Ordering::Release);
        result
    });

    // Spawn one pipeline per core.
    let core_handles: Vec<_> = (0..num_cores)
        .map(|core_id| {
            let config = config.clone();
            let gid = pipeline_group_id.clone();
            let pid = pipeline_id.clone();
            let ctx = controller_ctx.clone();
            let flag = shutdown_flag.clone();
            let mr = metrics_reporter.clone();
            let reg = registry.clone();

            std::thread::spawn(move || {
                let pipeline_ctx =
                    ctx.pipeline_context_with(gid.clone(), pid.clone(), core_id, num_cores, 0);
                let pipeline_entity_key = pipeline_ctx.register_pipeline_entity();
                let runtime_pipeline = OTAP_PIPELINE_FACTORY
                    .build(pipeline_ctx.clone(), config.clone(), None)
                    .unwrap_or_else(|e| panic!("core {core_id}: build failed: {e}"));

                let settings = config.pipeline_settings().clone();
                let (tx, rx) = pipeline_ctrl_msg_channel(
                    settings.default_pipeline_ctrl_msg_channel_size,
                );
                let tx_shutdown = tx.clone();

                // Per-core shutdown poller: checks the shared flag and sends
                // a Shutdown control message when the orchestrator is done.
                let core_flag = flag;
                let deadline = shutdown_deadline;
                let _shutdown_poller = std::thread::spawn(move || {
                    while !core_flag.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    let _ = tx_shutdown.try_send(PipelineControlMsg::Shutdown {
                        deadline: Instant::now() + deadline,
                        reason: "chaos stress test complete".to_owned(),
                    });
                });

                let observed_state_store =
                    ObservedStateStore::new(&ObservedStateSettings::default(), reg);
                let pipeline_key = DeployedPipelineKey {
                    pipeline_group_id: gid,
                    pipeline_id: pid,
                    core_id,
                };
                let event_reporter = observed_state_store.reporter(SendPolicy::default());

                let run_result = {
                    let _guard = set_pipeline_entity_key(
                        pipeline_ctx.metrics_registry(),
                        pipeline_entity_key,
                    );
                    runtime_pipeline.run_forever(
                        pipeline_key,
                        pipeline_ctx,
                        event_reporter,
                        mr,
                        tx,
                        rx,
                    )
                };

                // Accept clean shutdown or channel-closed race (same as
                // durable_buffer_processor_tests.rs).
                match &run_result {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("Channel is closed") => {}
                    Err(e) => panic!("core {core_id}: pipeline error: {e}"),
                }
            })
        })
        .collect();

    // Join all core threads.
    for (i, handle) in core_handles.into_iter().enumerate() {
        handle.join().unwrap_or_else(|_| panic!("core {i} panicked"));
    }

    // Join orchestrator and retrieve result.
    let result = orchestrator_handle
        .join()
        .expect("phase orchestrator panicked");

    // Verify telemetry cleanup (same assertions as durable_buffer_processor_tests).
    assert_eq!(
        registry.metric_set_count(),
        0,
        "metric sets should be cleaned up after shutdown"
    );
    assert_eq!(
        registry.entity_count(),
        0,
        "entities should be cleaned up after shutdown"
    );

    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress Tests
// ─────────────────────────────────────────────────────────────────────────────

/// Backpressure: online → offline (buffer fills) → online (recovery drain).
///
/// Verifies that:
/// - Data accumulates in the durable buffer during offline period
/// - Backpressure engages when buffer approaches retention_size_cap
/// - Pipeline recovers cleanly when the exporter comes back online
/// - Buffer drains (at least partially) during recovery
///
/// Duration: ~4 minutes.
#[test]
#[ignore]
fn chaos_stress_backpressure_offline_recovery() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-bp".into();
    let pipeline_id: PipelineId = "bp-offline-recovery".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("256 MB")
        .size_cap_policy("backpressure")
        .signals_per_second(5000)
        .max_batch_size(100)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let (size_after_offline, size_after_recovery) = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            // Phase 1: Online warm-up — establish steady-state ACK flow.
            set_online(&ctrl);
            eprintln!("[stress_bp] Phase 1: Online warm-up (5s)");
            std::thread::sleep(Duration::from_secs(5));

            // Phase 2: Offline — exporter NACKs everything, buffer fills,
            // backpressure should engage.
            set_offline(&ctrl);
            eprintln!("[stress_bp] Phase 2: Offline (120s) — buffer filling...");
            std::thread::sleep(Duration::from_secs(120));
            let size_offline = dir_size_bytes(&buf);
            eprintln!(
                "[stress_bp] Buffer size after offline: {}",
                format_bytes(size_offline)
            );

            // Phase 3: Recovery — exporter ACKs, buffer should drain.
            set_online(&ctrl);
            eprintln!("[stress_bp] Phase 3: Online recovery (60s) — draining...");
            std::thread::sleep(Duration::from_secs(60));
            let size_recovery = dir_size_bytes(&buf);
            eprintln!(
                "[stress_bp] Buffer size after recovery: {}",
                format_bytes(size_recovery)
            );

            (size_offline, size_recovery)
        },
    );

    // Buffer should have accumulated data during the offline period.
    assert!(
        size_after_offline > 1_000_000,
        "buffer should have accumulated data during offline, got {}",
        format_bytes(size_after_offline)
    );

    // Buffer should have drained (at least partially) during recovery.
    assert!(
        size_after_recovery < size_after_offline,
        "buffer should have drained during recovery: offline={}, recovery={}",
        format_bytes(size_after_offline),
        format_bytes(size_after_recovery)
    );

    // Per-core directory should exist.
    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Drop-oldest: online → offline (buffer fills, oldest evicted) → online.
///
/// Same flow as the backpressure test but with `drop_oldest` policy.
/// Instead of backpressuring the receiver, old segments are evicted to
/// make room for new data.
///
/// Verifies that:
/// - Data accumulates during offline
/// - Pipeline does NOT stall (drop_oldest allows continued ingestion)
/// - Pipeline recovers cleanly
///
/// Duration: ~4 minutes.
#[test]
#[ignore]
fn chaos_stress_drop_oldest_offline_recovery() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-drop".into();
    let pipeline_id: PipelineId = "drop-oldest-recovery".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("256 MB")
        .size_cap_policy("drop_oldest")
        .signals_per_second(5000)
        .max_batch_size(100)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let size_after_offline = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            // Phase 1: Online warm-up.
            set_online(&ctrl);
            eprintln!("[stress_drop] Phase 1: Online warm-up (5s)");
            std::thread::sleep(Duration::from_secs(5));

            // Phase 2: Offline — buffer fills, old segments evicted.
            set_offline(&ctrl);
            eprintln!("[stress_drop] Phase 2: Offline (120s) — filling + evicting...");
            std::thread::sleep(Duration::from_secs(120));
            let size = dir_size_bytes(&buf);
            eprintln!(
                "[stress_drop] Buffer size after offline: {}",
                format_bytes(size)
            );

            // Phase 3: Recovery.
            set_online(&ctrl);
            eprintln!("[stress_drop] Phase 3: Online recovery (60s)");
            std::thread::sleep(Duration::from_secs(60));

            size
        },
    );

    // Buffer should have data (capped at ~256 MB by drop_oldest eviction).
    assert!(
        size_after_offline > 1_000_000,
        "buffer should have data during offline, got {}",
        format_bytes(size_after_offline)
    );

    // Buffer should be bounded by retention_size_cap (256 MB = 256,000,000).
    // Allow some headroom for WAL and in-flight segments.
    let cap_with_headroom = 300_000_000u64; // ~286 MiB
    assert!(
        size_after_offline < cap_with_headroom,
        "buffer should be bounded by retention cap, got {} (cap ~256 MB)",
        format_bytes(size_after_offline)
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Multi-cycle endurance: repeated offline/online transitions.
///
/// Exercises the pipeline through 5 complete offline → online cycles to
/// detect resource leaks, state corruption, or degraded performance over
/// repeated transitions.
///
/// Duration: ~10 minutes.
#[test]
#[ignore]
fn chaos_stress_multi_cycle_endurance() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-cycle".into();
    let pipeline_id: PipelineId = "multi-cycle".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("256 MB")
        .size_cap_policy("backpressure")
        .signals_per_second(10_000)
        .max_batch_size(1000)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let cycle_measurements = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            let num_cycles = 5;
            let mut measurements: Vec<(u64, u64)> = Vec::new();

            for cycle in 1..=num_cycles {
                // Online phase: drain accumulated data.
                set_online(&ctrl);
                eprintln!("[stress_cycle] Cycle {cycle}/{num_cycles}: Online (30s)");
                std::thread::sleep(Duration::from_secs(30));
                let size_online = dir_size_bytes(&buf);

                // Offline phase: accumulate data in buffer.
                set_offline(&ctrl);
                eprintln!("[stress_cycle] Cycle {cycle}/{num_cycles}: Offline (60s)");
                std::thread::sleep(Duration::from_secs(60));
                let size_offline = dir_size_bytes(&buf);

                eprintln!(
                    "[stress_cycle] Cycle {cycle}: online={}, offline={}",
                    format_bytes(size_online),
                    format_bytes(size_offline),
                );
                measurements.push((size_online, size_offline));
            }

            // Final drain.
            set_online(&ctrl);
            eprintln!("[stress_cycle] Final drain (60s)");
            std::thread::sleep(Duration::from_secs(60));

            measurements
        },
    );

    // Every offline phase should have accumulated some data.
    for (i, (_online, offline)) in cycle_measurements.iter().enumerate() {
        assert!(
            *offline > 0,
            "cycle {} offline phase should have buffered data, got 0",
            i + 1
        );
    }

    // All 5 cycles should have completed (pipeline survived all transitions).
    assert_eq!(
        cycle_measurements.len(),
        5,
        "should have completed all 5 cycles"
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Flaky/degraded connectivity with escalating failure rates.
///
/// Simulates progressively worsening network conditions, then recovery:
///   1. Mild degradation (10% failures, 50ms latency)
///   2. Moderate degradation (50% failures, 200ms latency)
///   3. Severe degradation (90% failures, 500ms latency)
///   4. Full recovery (online)
///
/// Verifies the pipeline handles all degradation levels gracefully,
/// with retries and backpressure adapting to conditions.
///
/// Duration: ~5 minutes.
#[test]
#[ignore]
fn chaos_stress_flaky_degraded_connectivity() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-flaky".into();
    let pipeline_id: PipelineId = "degraded-connectivity".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("256 MB")
        .size_cap_policy("backpressure")
        .signals_per_second(2000)
        .max_batch_size(50)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let phase_sizes = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            let mut sizes = Vec::new();

            // Phase 1: Mild degradation.
            set_flaky(&ctrl, 0.1, 50, 20);
            eprintln!("[stress_flaky] Phase 1: 10% failure, 50±20ms latency (60s)");
            std::thread::sleep(Duration::from_secs(60));
            sizes.push(dir_size_bytes(&buf));

            // Phase 2: Moderate degradation.
            set_flaky(&ctrl, 0.5, 200, 100);
            eprintln!("[stress_flaky] Phase 2: 50% failure, 200±100ms latency (60s)");
            std::thread::sleep(Duration::from_secs(60));
            sizes.push(dir_size_bytes(&buf));

            // Phase 3: Severe degradation.
            set_flaky(&ctrl, 0.9, 500, 200);
            eprintln!("[stress_flaky] Phase 3: 90% failure, 500±200ms latency (60s)");
            std::thread::sleep(Duration::from_secs(60));
            sizes.push(dir_size_bytes(&buf));

            // Phase 4: Full recovery.
            set_online(&ctrl);
            eprintln!("[stress_flaky] Phase 4: Online recovery (60s)");
            std::thread::sleep(Duration::from_secs(60));
            sizes.push(dir_size_bytes(&buf));

            for (i, sz) in sizes.iter().enumerate() {
                eprintln!("[stress_flaky] Phase {} size: {}", i + 1, format_bytes(*sz));
            }

            sizes
        },
    );

    // Pipeline should have survived all degradation phases.
    assert_eq!(phase_sizes.len(), 4, "all 4 phases should have completed");

    // Severe degradation (90% NACK) should accumulate more data than mild (10%).
    // This validates that the degradation levels actually affect behavior.
    assert!(
        phase_sizes[2] > phase_sizes[0],
        "severe degradation should buffer more than mild: severe={}, mild={}",
        format_bytes(phase_sizes[2]),
        format_bytes(phase_sizes[0])
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Multi-core backpressure: 4 cores, shared budget, offline/online cycle.
///
/// Runs 4 pipeline instances concurrently, each with its own Quiver engine
/// (core_0/ through core_3/). The total retention budget (1 GiB) is divided
/// equally across cores (~256 MiB each).
///
/// Verifies that:
/// - All 4 cores start and run correctly
/// - Per-core buffer directories are created
/// - Pipeline recovers cleanly across all cores
///
/// Duration: ~4 minutes.
#[test]
#[ignore]
fn chaos_stress_multi_core_backpressure() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-multicore".into();
    let pipeline_id: PipelineId = "multicore-bp".into();
    let num_cores = 4;

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        // 1 GiB total = 256 MiB per core (above 192 MiB minimum).
        .retention_size_cap("1 GiB")
        .size_cap_policy("backpressure")
        .signals_per_second(5000)
        .max_batch_size(100)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let (size_offline, size_recovery) = run_chaos_pipeline_cores(
        config,
        &pipeline_group_id,
        &pipeline_id,
        num_cores,
        Duration::from_secs(10),
        move || {
            // Phase 1: Online warm-up.
            set_online(&ctrl);
            eprintln!("[stress_mc] Phase 1: Online warm-up, {num_cores} cores (5s)");
            std::thread::sleep(Duration::from_secs(5));

            // Phase 2: Offline — all cores buffer data independently.
            set_offline(&ctrl);
            eprintln!("[stress_mc] Phase 2: Offline (120s) — all cores buffering...");
            std::thread::sleep(Duration::from_secs(120));
            let size = dir_size_bytes(&buf);
            eprintln!("[stress_mc] Total buffer size: {}", format_bytes(size));

            // Phase 3: Recovery.
            set_online(&ctrl);
            eprintln!("[stress_mc] Phase 3: Online recovery (60s)");
            std::thread::sleep(Duration::from_secs(60));
            let size_rec = dir_size_bytes(&buf);
            eprintln!(
                "[stress_mc] Buffer after recovery: {}",
                format_bytes(size_rec)
            );

            (size, size_rec)
        },
    );

    // All 4 per-core directories should exist.
    for core_id in 0..num_cores {
        assert!(
            buffer_path.join(format!("core_{core_id}")).exists(),
            "core_{core_id} directory should exist"
        );
    }

    // Buffer should have accumulated data during offline.
    assert!(
        size_offline > 1_000_000,
        "multi-core buffer should have data during offline, got {}",
        format_bytes(size_offline)
    );

    // Buffer should have drained during recovery.
    assert!(
        size_recovery < size_offline,
        "buffer should drain: offline={}, recovery={}",
        format_bytes(size_offline),
        format_bytes(size_recovery)
    );
}

/// Large capacity: 2 GiB buffer under sustained high-throughput offline load.
///
/// Tests that the durable buffer correctly operates with a larger capacity
/// and handles sustained write pressure. Uses 10,000 signals/sec to fill
/// the buffer as quickly as possible.
///
/// Duration: ~7 minutes.
#[test]
#[ignore]
fn chaos_stress_large_capacity_fill() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-large".into();
    let pipeline_id: PipelineId = "large-capacity".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("2 GiB")
        .size_cap_policy("backpressure")
        .signals_per_second(10_000)
        .max_batch_size(200)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let (size_after_fill, size_after_drain) = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(15),
        move || {
            // Phase 1: Offline — sustained high-throughput writes.
            set_offline(&ctrl);
            eprintln!("[stress_large] Phase 1: Offline fill (240s) @ 10k signals/sec");
            // Log progress every 60 seconds.
            for chunk in 1..=4 {
                std::thread::sleep(Duration::from_secs(60));
                let size = dir_size_bytes(&buf);
                eprintln!(
                    "[stress_large] Progress ({chunk}/4): {}",
                    format_bytes(size)
                );
            }
            let size_fill = dir_size_bytes(&buf);
            eprintln!(
                "[stress_large] Buffer after fill: {}",
                format_bytes(size_fill)
            );

            // Phase 2: Recovery drain.
            set_online(&ctrl);
            eprintln!("[stress_large] Phase 2: Online drain (120s)");
            std::thread::sleep(Duration::from_secs(120));
            let size_drain = dir_size_bytes(&buf);
            eprintln!(
                "[stress_large] Buffer after drain: {}",
                format_bytes(size_drain)
            );

            (size_fill, size_drain)
        },
    );

    // Buffer should have substantial data after the fill phase.
    // With 10k signals/sec for 240s, we expect significant accumulation.
    assert!(
        size_after_fill > 100_000_000, // > 100 MB
        "buffer should have significant data after 240s fill, got {}",
        format_bytes(size_after_fill),
    );

    // Drain should have reduced the buffer.
    assert!(
        size_after_drain < size_after_fill,
        "drain should reduce buffer: fill={}, drain={}",
        format_bytes(size_after_fill),
        format_bytes(size_after_drain)
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Rapid mode switching: fast transitions between online/offline/flaky.
///
/// Exercises the chaos exporter's hot-reload mechanism under stress by
/// switching modes every 2–3 seconds for 30 transitions. This tests:
/// - Control file hot-reload reliability
/// - State machine transitions under rapid changes
/// - No crashes from rapid ACK/NACK switching
///
/// Duration: ~100 seconds.
#[test]
#[ignore]
fn chaos_stress_rapid_mode_switching() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-rapid".into();
    let pipeline_id: PipelineId = "rapid-switching".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("256 MB")
        .size_cap_policy("backpressure")
        .signals_per_second(5000)
        .max_batch_size(100)
        .build(&pipeline_group_id, &pipeline_id);

    let ctrl = control_file.clone();
    let transitions_completed = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            let modes: Vec<(&str, Box<dyn Fn(&Path)>)> = vec![
                ("online", Box::new(|p: &Path| set_online(p))),
                ("offline", Box::new(|p: &Path| set_offline(p))),
                (
                    "flaky-mild",
                    Box::new(|p: &Path| set_flaky(p, 0.2, 50, 20)),
                ),
                (
                    "flaky-moderate",
                    Box::new(|p: &Path| set_flaky(p, 0.5, 200, 100)),
                ),
                (
                    "flaky-severe",
                    Box::new(|p: &Path| set_flaky(p, 0.8, 500, 200)),
                ),
            ];

            let num_transitions = 30;
            for i in 0..num_transitions {
                let (name, setter) = &modes[i % modes.len()];
                setter(&ctrl);
                eprintln!("[stress_rapid] Transition {}/{}: {}", i + 1, num_transitions, name);
                // 2–3 second intervals (cycle between 2s and 3s).
                let delay = if i % 2 == 0 { 2 } else { 3 };
                std::thread::sleep(Duration::from_secs(delay));
            }

            // Final drain period.
            set_online(&ctrl);
            eprintln!("[stress_rapid] Final online drain (15s)");
            std::thread::sleep(Duration::from_secs(15));

            num_transitions
        },
    );

    // All transitions should have completed without crashing.
    assert_eq!(
        transitions_completed, 30,
        "all 30 mode transitions should complete"
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Terabyte capacity config validation: verify 1 TiB is accepted.
///
/// This test does NOT attempt to fill 1 TiB (that would take hours).
/// It validates that:
/// - The pipeline accepts a 1 TiB retention_size_cap configuration
/// - Data flows correctly at this capacity setting
/// - The pipeline starts, runs, and shuts down cleanly
///
/// Duration: ~30 seconds.
#[test]
#[ignore]
fn chaos_stress_terabyte_capacity_validation() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-tb".into();
    let pipeline_id: PipelineId = "terabyte-validation".into();

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("1 TiB")
        .size_cap_policy("backpressure")
        .signals_per_second(5000)
        .max_batch_size(100)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let size_after_run = run_chaos_pipeline(
        config,
        &pipeline_group_id,
        &pipeline_id,
        Duration::from_secs(10),
        move || {
            // Online for 20s — data should flow through and be ACKed.
            set_online(&ctrl);
            eprintln!("[stress_tb] Online with 1 TiB capacity (20s)");
            std::thread::sleep(Duration::from_secs(20));

            // Brief offline to confirm buffer operates at this capacity.
            set_offline(&ctrl);
            eprintln!("[stress_tb] Brief offline (5s)");
            std::thread::sleep(Duration::from_secs(5));
            let size = dir_size_bytes(&buf);
            eprintln!(
                "[stress_tb] Buffer after brief offline: {}",
                format_bytes(size)
            );

            // Recovery.
            set_online(&ctrl);
            eprintln!("[stress_tb] Recovery (5s)");
            std::thread::sleep(Duration::from_secs(5));

            size
        },
    );

    // Pipeline ran successfully with 1 TiB capacity. Buffer should have
    // some data from the brief offline.
    assert!(
        size_after_run > 0,
        "buffer should have data after brief offline at 1 TiB capacity"
    );

    assert!(
        buffer_path.join("core_0").exists(),
        "Quiver core_0 directory should exist"
    );
}

/// Multi-core with drop_oldest: 4 cores under sustained eviction pressure.
///
/// Combines multi-core execution with drop_oldest policy to verify that
/// per-core eviction works correctly when all cores are independently
/// filling their buffers.
///
/// Duration: ~5 minutes.
#[test]
#[ignore]
fn chaos_stress_multi_core_drop_oldest() {
    let temp_dir = tempdir().expect("failed to create temp dir");
    let buffer_path = temp_dir.path().join("buffer");
    let control_file = temp_dir.path().join("chaos-exporter.json");
    let pipeline_group_id: PipelineGroupId = "stress-mc-drop".into();
    let pipeline_id: PipelineId = "multicore-drop-oldest".into();
    let num_cores = 4;

    let config = ChaosTestConfig::new(buffer_path.clone(), control_file.clone())
        .retention_size_cap("1 GiB")
        .size_cap_policy("drop_oldest")
        .signals_per_second(5000)
        .max_batch_size(100)
        .log_body_size(4096)
        .build(&pipeline_group_id, &pipeline_id);

    let buf = buffer_path.clone();
    let ctrl = control_file.clone();
    let (size_during_eviction, _) = run_chaos_pipeline_cores(
        config,
        &pipeline_group_id,
        &pipeline_id,
        num_cores,
        Duration::from_secs(10),
        move || {
            // Phase 1: Online warm-up.
            set_online(&ctrl);
            eprintln!("[stress_mc_drop] Phase 1: Online warm-up, {num_cores} cores (5s)");
            std::thread::sleep(Duration::from_secs(5));

            // Phase 2: Offline — all cores fill, old segments evicted.
            set_offline(&ctrl);
            eprintln!("[stress_mc_drop] Phase 2: Offline (120s) — filling + evicting...");
            std::thread::sleep(Duration::from_secs(120));
            let size = dir_size_bytes(&buf);
            eprintln!(
                "[stress_mc_drop] Total buffer size (should be capped): {}",
                format_bytes(size)
            );

            // Phase 3: Recovery.
            set_online(&ctrl);
            eprintln!("[stress_mc_drop] Phase 3: Recovery (60s)");
            std::thread::sleep(Duration::from_secs(60));
            let size_rec = dir_size_bytes(&buf);
            eprintln!(
                "[stress_mc_drop] Buffer after recovery: {}",
                format_bytes(size_rec)
            );

            (size, size_rec)
        },
    );

    // All per-core directories should exist.
    for core_id in 0..num_cores {
        assert!(
            buffer_path.join(format!("core_{core_id}")).exists(),
            "core_{core_id} directory should exist"
        );
    }

    // Buffer should be bounded by the total retention cap (~1 GiB).
    // Allow headroom for WAL and in-flight data.
    let cap_with_headroom = 1_200_000_000u64; // ~1.12 GiB
    assert!(
        size_during_eviction < cap_with_headroom,
        "multi-core drop_oldest buffer should be bounded, got {} (cap ~1 GiB)",
        format_bytes(size_during_eviction),
    );
}
