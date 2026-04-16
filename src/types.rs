use std::time::Instant;

use clickhouse::Row;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum CancelHeuristic {
    Lifo,
    ProRata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeChunk {
    pub size: f64,
    pub added_at_exchange_ts: i64,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct NormalizedDepthUpdate {
    pub event_type: String,
    pub event_time_ms: i64,
    pub transaction_time_ms: Option<i64>,
    pub symbol: String,
    pub pair: Option<String>,
    pub first_update_id: u64,
    pub final_update_id: u64,
    pub prev_final_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct NormalizedBookTicker {
    pub event_type: String,
    pub update_id: u64,
    pub event_time_ms: i64,
    pub transaction_time_ms: Option<i64>,
    pub symbol: String,
    pub pair: Option<String>,
    pub best_bid_price: String,
    pub best_bid_qty: String,
    pub best_ask_price: String,
    pub best_ask_qty: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct NormalizedAggTrade {
    pub event_type: String,
    pub event_time_ms: i64,
    pub trade_time_ms: i64,
    pub symbol: String,
    pub pair: Option<String>,
    pub aggregate_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub buyer_is_maker: bool,
}

#[derive(Debug, Clone)]
pub struct DepthSnapshot {
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct DepthEvent {
    pub payload: NormalizedDepthUpdate,
    pub recv_ts_ms: i64,
    pub recv_instant: Instant,
}

#[derive(Debug, Clone)]
pub struct BookTickerEvent {
    pub payload: NormalizedBookTicker,
    pub recv_ts_ms: i64,
    pub raw_json: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AggTradeEvent {
    pub payload: NormalizedAggTrade,
    pub recv_ts_ms: i64,
    pub recv_instant: Instant,
    pub raw_json: String,
}

#[derive(Debug, Clone)]
pub enum UnifiedEvent {
    Depth(DepthEvent),
    AggTrade(AggTradeEvent),
}

impl UnifiedEvent {
    pub fn exchange_ts_ms(&self) -> i64 {
        match self {
            Self::Depth(event) => event.payload.event_time_ms,
            Self::AggTrade(event) => event.payload.event_time_ms,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            Self::Depth(event) => event.payload.symbol.as_str(),
            Self::AggTrade(event) => event.payload.symbol.as_str(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Row)]
pub struct SignalMetric {
    pub schema_version: u16,
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub event_id: String,
    pub event_ts_ms: i64,
    pub recv_ts_ms: i64,
    pub stale_state: bool,
    pub trade_alignment_forced_open: bool,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid_price: f64,
    pub spread_bps: f64,
    pub bid_notional_5pct: f64,
    pub ask_notional_5pct: f64,
    pub imbalance_5pct: f64,
    pub microprice: f64,
    pub ofi_l1: f64,
    pub m_ofi_top5: f64,
    pub vwaa_l1_bid_ms: f64,
    pub vwaa_l1_ask_ms: f64,
    pub vwaa_l1_imbalance: f64,
    pub vwaa_top5_bid_ms: f64,
    pub vwaa_top5_ask_ms: f64,
    pub vwaa_top5_imbalance: f64,
    pub cvd_5s: f64,
    pub cvd_1m: f64,
    pub cvd_5m: f64,
    pub spoof_flag: bool,
    pub spoof_score: f64,
    pub spoof_reason_code: String,
    pub depth_update_u: u64,
    pub depth_update_pu: u64,
    pub ingest_to_signal_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBookTickerRow {
    pub schema_version: u16,
    pub symbol: String,
    pub event_ts_ms: i64,
    pub recv_ts_ms: i64,
    pub raw_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshotEvent {
    pub schema_version: u16,
    pub exchange: String,
    pub market: String,
    pub symbol: String,
    pub snapshot_ts_ms: i64,
    pub last_update_id: u64,
    pub snapshot_source: String,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

pub fn now_utc_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
