use std::collections::VecDeque;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

const REPORT_INTERVAL_SECS: u64 = 10;
const LATENCY_SAMPLE_CAPACITY: usize = 20_000;
const LOSS_SLO_MAX: f64 = 0.0001;
const P99_SLO_MAX_MS: u64 = 60;

#[derive(Debug, Clone)]
pub enum TelemetryEvent {
    WsConnectAttempt,
    WsConnected,
    WsConnectFailure,
    WsReadError,
    WsClosed,
    WsIdleTimeout,
    WsReconnectScheduled,
    DepthEventReceived,
    AggTradeEventReceived,
    OutOfOrderCorrection,
    LateEventCorrection,
    ReorderBufferGuardTriggered,
    TradeAlignmentForcedOpen,
    SequenceGapDetected,
    ResyncTriggered,
    ResyncCompleted,
    SignalEmitted { ingest_to_signal_ms: u64, stale_state: bool },
    ParserError,
    MetricsWriteError,
    RawWriteError,
    S3UploadSuccess { rows: usize },
    S3UploadFailure,
}

#[derive(Debug, Default)]
struct TelemetryState {
    ws_connect_attempts: u64,
    ws_connected_sessions: u64,
    ws_connect_failures: u64,
    ws_read_errors: u64,
    ws_closed_events: u64,
    ws_idle_timeouts: u64,
    ws_reconnect_scheduled: u64,
    total_depth_events: u64,
    total_agg_trade_events: u64,
    out_of_order_corrections: u64,
    late_event_corrections: u64,
    reorder_buffer_guard_triggers: u64,
    trade_alignment_forced_open: u64,
    sequence_gap_events: u64,
    resync_started: u64,
    resync_completed: u64,
    parser_errors: u64,
    metrics_write_errors: u64,
    raw_write_errors: u64,
    s3_upload_successes: u64,
    s3_upload_failures: u64,
    signals_emitted: u64,
    stale_signals_emitted: u64,
    uploaded_rows: u64,
    latency_samples: VecDeque<u64>,
}

impl TelemetryState {
    fn apply(&mut self, event: TelemetryEvent) {
        match event {
            TelemetryEvent::WsConnectAttempt => {
                self.ws_connect_attempts = self.ws_connect_attempts.saturating_add(1);
            }
            TelemetryEvent::WsConnected => {
                self.ws_connected_sessions = self.ws_connected_sessions.saturating_add(1);
            }
            TelemetryEvent::WsConnectFailure => {
                self.ws_connect_failures = self.ws_connect_failures.saturating_add(1);
            }
            TelemetryEvent::WsReadError => {
                self.ws_read_errors = self.ws_read_errors.saturating_add(1);
            }
            TelemetryEvent::WsClosed => {
                self.ws_closed_events = self.ws_closed_events.saturating_add(1);
            }
            TelemetryEvent::WsIdleTimeout => {
                self.ws_idle_timeouts = self.ws_idle_timeouts.saturating_add(1);
            }
            TelemetryEvent::WsReconnectScheduled => {
                self.ws_reconnect_scheduled = self.ws_reconnect_scheduled.saturating_add(1);
            }
            TelemetryEvent::DepthEventReceived => {
                self.total_depth_events = self.total_depth_events.saturating_add(1);
            }
            TelemetryEvent::AggTradeEventReceived => {
                self.total_agg_trade_events = self.total_agg_trade_events.saturating_add(1);
            }
            TelemetryEvent::OutOfOrderCorrection => {
                self.out_of_order_corrections = self.out_of_order_corrections.saturating_add(1);
            }
            TelemetryEvent::LateEventCorrection => {
                self.late_event_corrections = self.late_event_corrections.saturating_add(1);
            }
            TelemetryEvent::ReorderBufferGuardTriggered => {
                self.reorder_buffer_guard_triggers =
                    self.reorder_buffer_guard_triggers.saturating_add(1);
            }
            TelemetryEvent::TradeAlignmentForcedOpen => {
                self.trade_alignment_forced_open = self.trade_alignment_forced_open.saturating_add(1);
            }
            TelemetryEvent::SequenceGapDetected => {
                self.sequence_gap_events = self.sequence_gap_events.saturating_add(1);
            }
            TelemetryEvent::ResyncTriggered => {
                self.resync_started = self.resync_started.saturating_add(1);
            }
            TelemetryEvent::ResyncCompleted => {
                self.resync_completed = self.resync_completed.saturating_add(1);
            }
            TelemetryEvent::SignalEmitted {
                ingest_to_signal_ms,
                stale_state,
            } => {
                self.signals_emitted = self.signals_emitted.saturating_add(1);
                if stale_state {
                    self.stale_signals_emitted = self.stale_signals_emitted.saturating_add(1);
                }
                if self.latency_samples.len() >= LATENCY_SAMPLE_CAPACITY {
                    self.latency_samples.pop_front();
                }
                self.latency_samples.push_back(ingest_to_signal_ms);
            }
            TelemetryEvent::ParserError => {
                self.parser_errors = self.parser_errors.saturating_add(1);
            }
            TelemetryEvent::MetricsWriteError => {
                self.metrics_write_errors = self.metrics_write_errors.saturating_add(1);
            }
            TelemetryEvent::RawWriteError => {
                self.raw_write_errors = self.raw_write_errors.saturating_add(1);
            }
            TelemetryEvent::S3UploadSuccess { rows } => {
                self.s3_upload_successes = self.s3_upload_successes.saturating_add(1);
                self.uploaded_rows = self.uploaded_rows.saturating_add(rows as u64);
            }
            TelemetryEvent::S3UploadFailure => {
                self.s3_upload_failures = self.s3_upload_failures.saturating_add(1);
            }
        }
    }

    fn loss_rate(&self) -> f64 {
        if self.total_depth_events == 0 {
            return 0.0;
        }
        self.sequence_gap_events as f64 / self.total_depth_events as f64
    }

    fn p99_latency_ms(&self) -> Option<u64> {
        percentile(&self.latency_samples, 0.99)
    }
}

pub async fn run_telemetry(mut telemetry_rx: mpsc::Receiver<TelemetryEvent>) -> Result<()> {
    let mut state = TelemetryState {
        latency_samples: VecDeque::with_capacity(LATENCY_SAMPLE_CAPACITY),
        ..TelemetryState::default()
    };

    let mut ticker = tokio::time::interval(Duration::from_secs(REPORT_INTERVAL_SECS));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                report_state(&state);
            }
            event = telemetry_rx.recv() => {
                match event {
                    Some(event) => state.apply(event),
                    None => break,
                }
            }
        }
    }

    report_state(&state);
    Ok(())
}

fn report_state(state: &TelemetryState) {
    let loss_rate = state.loss_rate();
    let p99_latency_ms = state.p99_latency_ms();

    info!(
        ws_connect_attempts = state.ws_connect_attempts,
        ws_connected_sessions = state.ws_connected_sessions,
        ws_connect_failures = state.ws_connect_failures,
        ws_read_errors = state.ws_read_errors,
        ws_closed_events = state.ws_closed_events,
        ws_idle_timeouts = state.ws_idle_timeouts,
        ws_reconnect_scheduled = state.ws_reconnect_scheduled,
        total_depth_events = state.total_depth_events,
        total_agg_trade_events = state.total_agg_trade_events,
        out_of_order_corrections = state.out_of_order_corrections,
        late_event_corrections = state.late_event_corrections,
        reorder_buffer_guard_triggers = state.reorder_buffer_guard_triggers,
        trade_alignment_forced_open = state.trade_alignment_forced_open,
        sequence_gap_events = state.sequence_gap_events,
        loss_rate,
        resync_started = state.resync_started,
        resync_completed = state.resync_completed,
        signals_emitted = state.signals_emitted,
        stale_signals_emitted = state.stale_signals_emitted,
        parser_errors = state.parser_errors,
        metrics_write_errors = state.metrics_write_errors,
        raw_write_errors = state.raw_write_errors,
        s3_upload_successes = state.s3_upload_successes,
        s3_upload_failures = state.s3_upload_failures,
        uploaded_rows = state.uploaded_rows,
        p99_latency_ms,
        "runtime telemetry"
    );

    if loss_rate > LOSS_SLO_MAX {
        warn!(
            loss_rate,
            threshold = LOSS_SLO_MAX,
            "loss rate is above target threshold"
        );
    }

    if let Some(p99_latency_ms) = p99_latency_ms {
        if p99_latency_ms > P99_SLO_MAX_MS {
            warn!(
                p99_latency_ms,
                threshold_ms = P99_SLO_MAX_MS,
                "p99 ingest-to-signal latency is above target threshold"
            );
        }
    }
}

fn percentile(samples: &VecDeque<u64>, quantile: f64) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }

    let mut sorted = samples.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();

    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted.get(index).copied()
}
