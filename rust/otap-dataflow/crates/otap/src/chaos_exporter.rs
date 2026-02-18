// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! A chaos-engineering exporter for manual testing of backpressure,
//! network outages, degraded connectivity, and recovery scenarios.
//!
//! The exporter reads a **JSON control file** at a configurable interval
//! and adjusts its behavior accordingly. Edit the file from another
//! terminal (or script) to change behavior on the fly — no restart needed.
//!
//! # Simulation Modes
//!
//! | Mode      | Behavior |
//! |-----------|----------|
//! | `online`  | ACKs every message (optionally with added latency) |
//! | `offline` | NACKs every message (simulates total outage) |
//! | `flaky`   | Randomly ACKs or NACKs based on `failure_rate` |
//!
//! # Control File Format (JSON)
//!
//! All fields are optional (defaults shown):
//!
//! ```json
//! {
//!   "mode": "online",
//!   "failure_rate": 0.5,
//!   "latency_ms": 0,
//!   "jitter_ms": 0
//! }
//! ```
//!
//! - **`mode`**: `"online"` | `"offline"` | `"flaky"` (default: `"online"`)
//! - **`failure_rate`**: 0.0–1.0, probability of NACK in `flaky` mode (default: 0.5)
//! - **`latency_ms`**: base delay in ms before responding (default: 0)
//! - **`jitter_ms`**: random jitter added to latency, uniform [0, jitter_ms] (default: 0)
//!
//! When the control file does not exist, the exporter defaults to `online` mode
//! with no latency. If the file exists but cannot be parsed, the previous state
//! is retained and a warning is logged.
//!
//! # Pipeline Configuration
//!
//! ```yaml
//! nodes:
//!   my_exporter:
//!     type: chaos:exporter
//!     config:
//!       # Path to the JSON control file (required)
//!       control_file: /tmp/chaos-exporter.json
//!       # How often to re-read the control file (default: 500ms)
//!       check_interval: 500ms
//!       # Log a status line every N messages; 0 = quiet (default: 100)
//!       log_every_n: 100
//! ```
//!
//! # Typical Test Workflow
//!
//! ```bash
//! # 1. Start online (create control file or let it default)
//! echo '{}' > /tmp/chaos-exporter.json
//!
//! # 2. Run the pipeline
//! cargo run --release -- --pipeline configs/fake-durable-buffer-chaos.yaml
//!
//! # 3. Observe steady-state ACKs, then simulate an outage:
//! echo '{"mode":"offline"}' > /tmp/chaos-exporter.json
//!
//! # 4. Watch the buffer fill up, backpressure engage...
//!
//! # 5. Simulate degraded recovery (50% packet loss, 200ms latency):
//! echo '{"mode":"flaky","failure_rate":0.5,"latency_ms":200}' > /tmp/chaos-exporter.json
//!
//! # 6. Simulate slow but reliable endpoint:
//! echo '{"mode":"online","latency_ms":500,"jitter_ms":100}' > /tmp/chaos-exporter.json
//!
//! # 7. Full recovery:
//! echo '{}' > /tmp/chaos-exporter.json
//!
//! # 8. Ctrl-C to shut down
//! ```

use crate::OTAP_EXPORTER_FACTORIES;
use crate::pdata::OtapPdata;
use async_trait::async_trait;
use linkme::distributed_slice;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ExporterConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::{AckMsg, NackMsg, NodeControlMsg};
use otap_df_engine::error::Error;
use otap_df_engine::exporter::ExporterWrapper;
use otap_df_engine::local::exporter::{EffectHandler, Exporter};
use otap_df_engine::message::{Message, MessageChannel};
use otap_df_engine::node::NodeId;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otap_df_telemetry::{otel_info, otel_warn};
use rand::{Rng, RngExt};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// URN for the chaos exporter.
pub const CHAOS_EXPORTER_URN: &str = "urn:otel:chaos:exporter";

// ─────────────────────────────────────────────────────────────────────────────
// Control File Schema (hot-reloaded at runtime)
// ─────────────────────────────────────────────────────────────────────────────

/// Runtime simulation state, read from the control file.
#[derive(Debug, Clone, Deserialize, PartialEq)]
struct ControlState {
    /// Simulation mode.
    #[serde(default)]
    mode: SimulationMode,

    /// Probability of NACK in `flaky` mode (0.0–1.0). Ignored in other modes.
    #[serde(default = "default_failure_rate")]
    failure_rate: f64,

    /// Base latency in milliseconds added before every ACK or NACK response.
    /// Simulates slow endpoint or network round-trip time.
    #[serde(default)]
    latency_ms: u64,

    /// Random jitter in milliseconds added on top of `latency_ms`.
    /// Actual delay = `latency_ms + uniform(0, jitter_ms)`.
    #[serde(default)]
    jitter_ms: u64,
}

impl Default for ControlState {
    fn default() -> Self {
        Self {
            mode: SimulationMode::Online,
            failure_rate: default_failure_rate(),
            latency_ms: 0,
            jitter_ms: 0,
        }
    }
}

impl std::fmt::Display for ControlState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.mode {
            SimulationMode::Online => write!(f, "ONLINE")?,
            SimulationMode::Offline => write!(f, "OFFLINE")?,
            SimulationMode::Flaky => write!(f, "FLAKY(fail={:.0}%)", self.failure_rate * 100.0)?,
        }
        if self.latency_ms > 0 || self.jitter_ms > 0 {
            write!(f, " +{}ms", self.latency_ms)?;
            if self.jitter_ms > 0 {
                write!(f, "±{}ms", self.jitter_ms)?;
            }
        }
        Ok(())
    }
}

/// Simulation mode for the chaos exporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SimulationMode {
    /// ACKs every message. Default behavior.
    #[default]
    Online,
    /// NACKs every message. Simulates total downstream outage.
    Offline,
    /// Randomly ACKs or NACKs based on `failure_rate`. Simulates
    /// degraded connectivity, packet loss, or intermittent failures.
    Flaky,
}

const fn default_failure_rate() -> f64 {
    0.5
}

// ─────────────────────────────────────────────────────────────────────────────
// Exporter Node Configuration (static, set at pipeline start)
// ─────────────────────────────────────────────────────────────────────────────

/// Pipeline node configuration for the chaos exporter.
#[derive(Debug, Clone, Deserialize)]
struct ChaosExporterConfig {
    /// Path to the JSON control file. The exporter periodically reads this
    /// file to update its simulation behavior. When the file does not exist,
    /// defaults to `online` mode with no latency.
    control_file: PathBuf,

    /// How often to re-read the control file.
    /// Default: 500ms.
    #[serde(with = "humantime_serde", default = "default_check_interval")]
    check_interval: Duration,

    /// Log a status line every N messages (0 = silent). Default: 100.
    #[serde(default = "default_log_every_n")]
    log_every_n: u64,
}

const fn default_check_interval() -> Duration {
    Duration::from_millis(500)
}

const fn default_log_every_n() -> u64 {
    100
}

// ─────────────────────────────────────────────────────────────────────────────
// Exporter Implementation
// ─────────────────────────────────────────────────────────────────────────────

struct ChaosExporter {
    control_file: PathBuf,
    check_interval: Duration,
    log_every_n: u64,
}

impl ChaosExporter {
    /// Read and parse the control file. Returns `None` if the file doesn't
    /// exist (which means "use defaults"). Returns `Some(Err(...))` if the
    /// file exists but can't be parsed.
    fn read_control_file(path: &std::path::Path) -> Option<Result<ControlState, String>> {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let contents = contents.trim();
                if contents.is_empty() {
                    // Empty file → defaults (online)
                    Some(Ok(ControlState::default()))
                } else {
                    Some(
                        serde_json::from_str::<ControlState>(contents)
                            .map_err(|e| format!("invalid control file JSON: {e}")),
                    )
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => Some(Err(format!("failed to read control file: {e}"))),
        }
    }

    /// Compute the response delay for the current control state.
    fn compute_response_delay(state: &ControlState, rng: &mut impl Rng) -> Duration {
        let jitter = if state.jitter_ms > 0 {
            rng.random_range(0..=state.jitter_ms)
        } else {
            0
        };
        Duration::from_millis(state.latency_ms + jitter)
    }

    /// Decide whether this message should be ACKed based on the current mode.
    fn should_ack_message(state: &ControlState, rng: &mut impl Rng) -> bool {
        match state.mode {
            SimulationMode::Online => true,
            SimulationMode::Offline => false,
            SimulationMode::Flaky => rng.random_range(0.0..1.0) >= state.failure_rate,
        }
    }
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static CHAOS_EXPORTER_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: CHAOS_EXPORTER_URN,
    create: |_pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             exporter_config: &ExporterConfig| {
        let config: ChaosExporterConfig =
            serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otap_df_config::error::Error::InvalidUserConfig {
                    error: format!("Failed to parse chaos-exporter configuration: {e}"),
                }
            })?;

        Ok(ExporterWrapper::local(
            ChaosExporter {
                control_file: config.control_file,
                check_interval: config.check_interval,
                log_every_n: config.log_every_n,
            },
            node,
            node_config,
            exporter_config,
        ))
    },
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

#[async_trait(?Send)]
impl Exporter<OtapPdata> for ChaosExporter {
    async fn start(
        self: Box<Self>,
        mut msg_chan: MessageChannel<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut state = ControlState::default();
        let mut last_check = Instant::now();
        let mut ack_count: u64 = 0;
        let mut nack_count: u64 = 0;
        let mut rng = rand::rng();

        // Initial read of control file
        if let Some(result) = Self::read_control_file(&self.control_file) {
            match result {
                Ok(s) => state = s,
                Err(e) => otel_warn!("chaos_exporter.init", error = %e),
            }
        }

        otel_info!(
            "chaos_exporter.start",
            control_file = %self.control_file.display(),
            initial_state = %state,
        );

        loop {
            match msg_chan.recv().await? {
                Message::Control(NodeControlMsg::Shutdown { .. }) => {
                    otel_info!(
                        "chaos_exporter.shutdown",
                        total_acks = ack_count,
                        total_nacks = nack_count,
                        final_state = %state,
                    );
                    break;
                }
                Message::PData(data) => {
                    // Periodically re-read the control file
                    let now = Instant::now();
                    if now.duration_since(last_check) >= self.check_interval {
                        last_check = now;
                        let new_state = match Self::read_control_file(&self.control_file) {
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                otel_warn!(
                                    "chaos_exporter.control_file_error",
                                    error = %e,
                                    message = "keeping previous state",
                                );
                                state.clone()
                            }
                            None => ControlState::default(),
                        };

                        if new_state != state {
                            otel_info!(
                                "chaos_exporter.state_change",
                                from = %state,
                                to = %new_state,
                                acks_so_far = ack_count,
                                nacks_so_far = nack_count,
                            );
                            state = new_state;
                        }
                    }

                    // Apply latency if configured
                    let delay = Self::compute_response_delay(&state, &mut rng);
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }

                    // Decide ACK or NACK
                    if Self::should_ack_message(&state, &mut rng) {
                        ack_count += 1;
                        if self.log_every_n > 0 && ack_count % self.log_every_n == 0 {
                            otel_info!(
                                "chaos_exporter.progress",
                                ack_count,
                                nack_count,
                                items = data.num_items(),
                                state = %state,
                            );
                        }
                        effect_handler.notify_ack(AckMsg::new(data)).await?;
                    } else {
                        nack_count += 1;
                        if self.log_every_n > 0 && nack_count % self.log_every_n == 0 {
                            otel_warn!(
                                "chaos_exporter.progress",
                                ack_count,
                                nack_count,
                                items = data.num_items(),
                                state = %state,
                            );
                        }
                        effect_handler
                            .notify_nack(NackMsg::new(
                                format!("chaos exporter {}", state),
                                data,
                            ))
                            .await?;
                    }
                }
                _ => {}
            }
        }

        Ok(TerminalState::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;
    use otap_df_engine::Interests;
    use serde_json::json;
    use tempfile::tempdir;

    fn make_config(control_file: &std::path::Path) -> serde_json::Value {
        json!({
            "control_file": control_file.to_string_lossy(),
            "check_interval": "10ms",
            "log_every_n": 0,
        })
    }

    #[test]
    fn test_chaos_exporter_no_subscription() {
        let dir = tempdir().unwrap();
        let control = dir.path().join("control.json");
        // No control file → defaults to online → ACKs
        test_exporter_no_subscription(&CHAOS_EXPORTER_FACTORY, make_config(&control));
    }

    #[test]
    fn test_chaos_exporter_online_acks() {
        let dir = tempdir().unwrap();
        let control = dir.path().join("control.json");
        std::fs::write(&control, r#"{"mode":"online"}"#).unwrap();
        test_exporter_with_subscription(
            &CHAOS_EXPORTER_FACTORY,
            make_config(&control),
            Interests::ACKS,
            Interests::ACKS,
        );
    }

    #[test]
    fn test_chaos_exporter_no_file_defaults_online() {
        let dir = tempdir().unwrap();
        let control = dir.path().join("control.json");
        // File does not exist → defaults to online → ACKs
        test_exporter_with_subscription(
            &CHAOS_EXPORTER_FACTORY,
            make_config(&control),
            Interests::ACKS,
            Interests::ACKS,
        );
    }

    // ── Control state parsing ────────────────────────────────────────────

    #[test]
    fn test_control_state_defaults() {
        let state: ControlState = serde_json::from_str("{}").unwrap();
        assert_eq!(state.mode, SimulationMode::Online);
        assert!((state.failure_rate - 0.5).abs() < f64::EPSILON);
        assert_eq!(state.latency_ms, 0);
        assert_eq!(state.jitter_ms, 0);
    }

    #[test]
    fn test_control_state_offline() {
        let state: ControlState = serde_json::from_str(r#"{"mode":"offline"}"#).unwrap();
        assert_eq!(state.mode, SimulationMode::Offline);
    }

    #[test]
    fn test_control_state_flaky_with_params() {
        let state: ControlState = serde_json::from_str(
            r#"{"mode":"flaky","failure_rate":0.3,"latency_ms":200,"jitter_ms":50}"#,
        )
        .unwrap();
        assert_eq!(state.mode, SimulationMode::Flaky);
        assert!((state.failure_rate - 0.3).abs() < f64::EPSILON);
        assert_eq!(state.latency_ms, 200);
        assert_eq!(state.jitter_ms, 50);
    }

    #[test]
    fn test_control_state_display() {
        let online = ControlState::default();
        assert_eq!(format!("{online}"), "ONLINE");

        let offline = ControlState {
            mode: SimulationMode::Offline,
            ..Default::default()
        };
        assert_eq!(format!("{offline}"), "OFFLINE");

        let flaky_with_latency = ControlState {
            mode: SimulationMode::Flaky,
            failure_rate: 0.3,
            latency_ms: 200,
            jitter_ms: 50,
        };
        assert_eq!(format!("{flaky_with_latency}"), "FLAKY(fail=30%) +200ms±50ms");

        let online_with_latency = ControlState {
            latency_ms: 100,
            ..Default::default()
        };
        assert_eq!(format!("{online_with_latency}"), "ONLINE +100ms");
    }

    // ── Decision logic ──────────────────────────────────────────────────

    #[test]
    fn test_should_ack_online_always_true() {
        let state = ControlState {
            mode: SimulationMode::Online,
            ..Default::default()
        };
        let mut rng = rand::rng();
        for _ in 0..100 {
            assert!(ChaosExporter::should_ack_message(&state, &mut rng));
        }
    }

    #[test]
    fn test_should_ack_offline_always_false() {
        let state = ControlState {
            mode: SimulationMode::Offline,
            ..Default::default()
        };
        let mut rng = rand::rng();
        for _ in 0..100 {
            assert!(!ChaosExporter::should_ack_message(&state, &mut rng));
        }
    }

    #[test]
    fn test_should_ack_flaky_produces_mix() {
        let state = ControlState {
            mode: SimulationMode::Flaky,
            failure_rate: 0.5,
            ..Default::default()
        };
        let mut rng = rand::rng();
        let results: Vec<bool> = (0..1000)
            .map(|_| ChaosExporter::should_ack_message(&state, &mut rng))
            .collect();
        let ack_count = results.iter().filter(|&&v| v).count();
        // With 50% failure rate over 1000 tries, expect roughly 400-600 ACKs
        assert!(
            (400..=600).contains(&ack_count),
            "expected ~500 ACKs out of 1000, got {ack_count}"
        );
    }

    #[test]
    fn test_response_delay_zero_when_no_latency() {
        let state = ControlState::default();
        let mut rng = rand::rng();
        assert_eq!(
            ChaosExporter::compute_response_delay(&state, &mut rng),
            Duration::ZERO
        );
    }

    #[test]
    fn test_response_delay_with_latency_and_jitter() {
        let state = ControlState {
            latency_ms: 100,
            jitter_ms: 50,
            ..Default::default()
        };
        let mut rng = rand::rng();
        for _ in 0..100 {
            let delay = ChaosExporter::compute_response_delay(&state, &mut rng);
            assert!(delay >= Duration::from_millis(100));
            assert!(delay <= Duration::from_millis(150));
        }
    }

    // ── Control file reading ────────────────────────────────────────────

    #[test]
    fn test_read_control_file_missing_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        assert!(ChaosExporter::read_control_file(&path).is_none());
    }

    #[test]
    fn test_read_control_file_empty_returns_defaults() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control.json");
        std::fs::write(&path, "").unwrap();
        let result = ChaosExporter::read_control_file(&path).unwrap().unwrap();
        assert_eq!(result, ControlState::default());
    }

    #[test]
    fn test_read_control_file_valid_json() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control.json");
        std::fs::write(&path, r#"{"mode":"offline","latency_ms":500}"#).unwrap();
        let result = ChaosExporter::read_control_file(&path).unwrap().unwrap();
        assert_eq!(result.mode, SimulationMode::Offline);
        assert_eq!(result.latency_ms, 500);
    }

    #[test]
    fn test_read_control_file_invalid_json_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("control.json");
        std::fs::write(&path, "not json").unwrap();
        let result = ChaosExporter::read_control_file(&path).unwrap();
        assert!(result.is_err());
    }
}
