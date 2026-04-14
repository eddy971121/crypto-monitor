use std::{env, path::PathBuf};

use anyhow::Result;

use crate::types::CancelHeuristic;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetricsBackend {
    Jsonl,
    ClickHouse,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub canonical_symbol: String,
    pub symbol: String,
    pub stream_symbol: String,
    pub ws_base_url: String,
    pub rest_base_url: String,
    pub depth_limit: u16,
    pub channel_capacity: usize,
    pub enable_depth_stream: bool,
    pub enable_book_ticker_stream: bool,
    pub enable_agg_trade_stream: bool,
    pub cancel_heuristic: CancelHeuristic,
    pub health_report_interval_secs: u64,
    pub ntp_slew_enabled: bool,
    pub ntp_server: String,
    pub ntp_poll_interval_secs: u64,
    pub ntp_timeout_ms: u64,
    pub ntp_slew_alpha: f64,
    pub ntp_slew_max_step_ms: i64,
    pub trade_alignment_max_lag_ms: i64,
    pub execution_match_lookahead_ms: i64,
    pub reorder_resync_max_buffer_lag_ms: i64,
    pub snapshot_dump_interval_secs: u64,
    pub raw_spool_dir: PathBuf,
    pub raw_parquet_chunk_rows: usize,
    pub metrics_dir: PathBuf,
    pub metrics_backend: MetricsBackend,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub clickhouse_batch_size: usize,
    pub clickhouse_flush_ms: u64,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    pub metrics_retention_days: i64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let canonical_symbol = env_or_default("APP_SYMBOL", "BTCUSD_PERP");
        let (canonical_symbol, symbol, stream_symbol) =
            resolve_binance_coinm_instrument(&canonical_symbol)?;

        let ws_base_url = env_or_default("APP_WS_BASE_URL", "wss://dstream.binance.com");
        let rest_base_url = env_or_default("APP_REST_BASE_URL", "https://dapi.binance.com");
        let depth_limit = parse_env_or_default("APP_DEPTH_LIMIT", 1000_u16)?;
        let channel_capacity = parse_env_or_default("APP_CHANNEL_CAPACITY", 20000_usize)?;
        let enable_depth_stream = parse_env_or_default("APP_ENABLE_DEPTH_STREAM", true)?;
        let enable_book_ticker_stream =
            parse_env_or_default("APP_ENABLE_BOOK_TICKER_STREAM", true)?;
        let enable_agg_trade_stream = parse_env_or_default("APP_ENABLE_AGG_TRADE_STREAM", true)?;
        let cancel_heuristic = parse_cancel_heuristic(&env_or_default("APP_CANCEL_HEURISTIC", "lifo"))?;
        let health_report_interval_secs =
            parse_env_or_default("APP_HEALTH_REPORT_INTERVAL_SECS", 30_u64)?;
        let ntp_slew_enabled = parse_env_or_default("APP_NTP_SLEW_ENABLED", true)?;
        let ntp_server = env_or_default("APP_NTP_SERVER", "time.windows.com:123");
        let ntp_poll_interval_secs = parse_env_or_default("APP_NTP_POLL_INTERVAL_SECS", 30_u64)?;
        let ntp_timeout_ms = parse_env_or_default("APP_NTP_TIMEOUT_MS", 1500_u64)?;
        let ntp_slew_alpha = parse_env_or_default("APP_NTP_SLEW_ALPHA", 0.25_f64)?;
        let ntp_slew_max_step_ms = parse_env_or_default("APP_NTP_SLEW_MAX_STEP_MS", 25_i64)?;
        let trade_alignment_max_lag_ms =
            parse_env_or_default("APP_TRADE_ALIGNMENT_MAX_LAG_MS", 250_i64)?;
        let execution_match_lookahead_ms =
            parse_env_or_default("APP_EXECUTION_MATCH_LOOKAHEAD_MS", 40_i64)?;
        let reorder_resync_max_buffer_lag_ms =
            parse_env_or_default("APP_REORDER_RESYNC_MAX_LAG_MS", 3000_i64)?;
        let snapshot_dump_interval_secs =
            parse_env_or_default("APP_SNAPSHOT_DUMP_INTERVAL_SECS", 3600_u64)?;

        if !enable_depth_stream && !enable_book_ticker_stream && !enable_agg_trade_stream {
            return Err(anyhow::anyhow!(
                "at least one stream must be enabled: APP_ENABLE_DEPTH_STREAM, APP_ENABLE_BOOK_TICKER_STREAM, or APP_ENABLE_AGG_TRADE_STREAM"
            ));
        }

        if ntp_poll_interval_secs == 0 {
            return Err(anyhow::anyhow!(
                "APP_NTP_POLL_INTERVAL_SECS must be greater than 0"
            ));
        }

        if ntp_timeout_ms == 0 {
            return Err(anyhow::anyhow!(
                "APP_NTP_TIMEOUT_MS must be greater than 0"
            ));
        }

        if !(0.0..=1.0).contains(&ntp_slew_alpha) || ntp_slew_alpha == 0.0 {
            return Err(anyhow::anyhow!(
                "APP_NTP_SLEW_ALPHA must be in the range (0.0, 1.0]"
            ));
        }

        if ntp_slew_max_step_ms <= 0 {
            return Err(anyhow::anyhow!(
                "APP_NTP_SLEW_MAX_STEP_MS must be greater than 0"
            ));
        }

        if trade_alignment_max_lag_ms <= 0 {
            return Err(anyhow::anyhow!(
                "APP_TRADE_ALIGNMENT_MAX_LAG_MS must be greater than 0"
            ));
        }

        if execution_match_lookahead_ms < 0 {
            return Err(anyhow::anyhow!(
                "APP_EXECUTION_MATCH_LOOKAHEAD_MS must be greater than or equal to 0"
            ));
        }

        if reorder_resync_max_buffer_lag_ms <= 0 {
            return Err(anyhow::anyhow!(
                "APP_REORDER_RESYNC_MAX_LAG_MS must be greater than 0"
            ));
        }

        let data_root = PathBuf::from(env_or_default("APP_DATA_ROOT", "data"));
        let raw_spool_dir = PathBuf::from(env_or_default_path(
            "APP_RAW_SPOOL_DIR",
            data_root.join("raw").to_string_lossy().as_ref(),
        ));
        let raw_parquet_chunk_rows =
            parse_env_or_default("APP_RAW_PARQUET_CHUNK_ROWS", 10_000_usize)?;
        let metrics_dir = PathBuf::from(env_or_default_path(
            "APP_METRICS_DIR",
            data_root.join("metrics").to_string_lossy().as_ref(),
        ));
        let metrics_backend = parse_metrics_backend(&env_or_default("APP_METRICS_BACKEND", "jsonl"))?;
        let clickhouse_url = env_or_default("APP_CLICKHOUSE_URL", "http://127.0.0.1:8123");
        let clickhouse_database = env_or_default("APP_CLICKHOUSE_DATABASE", "crypto_monitor");
        let clickhouse_table = env_or_default("APP_CLICKHOUSE_TABLE", "metrics");
        let clickhouse_batch_size = parse_env_or_default("APP_CLICKHOUSE_BATCH_SIZE", 500_usize)?;
        let clickhouse_flush_ms = parse_env_or_default("APP_CLICKHOUSE_FLUSH_MS", 500_u64)?;

        if clickhouse_batch_size == 0 {
            return Err(anyhow::anyhow!("APP_CLICKHOUSE_BATCH_SIZE must be greater than 0"));
        }

        if clickhouse_flush_ms == 0 {
            return Err(anyhow::anyhow!("APP_CLICKHOUSE_FLUSH_MS must be greater than 0"));
        }

        if raw_parquet_chunk_rows == 0 {
            return Err(anyhow::anyhow!(
                "APP_RAW_PARQUET_CHUNK_ROWS must be greater than 0"
            ));
        }

        let s3_bucket = env::var("APP_S3_BUCKET")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let s3_prefix = env_or_default("APP_S3_PREFIX", "crypto-monitor/binance-coinm");
        let metrics_retention_days = parse_env_or_default("APP_METRICS_RETENTION_DAYS", 30_i64)?;

        Ok(Self {
            canonical_symbol,
            symbol,
            stream_symbol,
            ws_base_url,
            rest_base_url,
            depth_limit,
            channel_capacity,
            enable_depth_stream,
            enable_book_ticker_stream,
            enable_agg_trade_stream,
            cancel_heuristic,
            health_report_interval_secs,
            ntp_slew_enabled,
            ntp_server,
            ntp_poll_interval_secs,
            ntp_timeout_ms,
            ntp_slew_alpha,
            ntp_slew_max_step_ms,
            trade_alignment_max_lag_ms,
            execution_match_lookahead_ms,
            reorder_resync_max_buffer_lag_ms,
            snapshot_dump_interval_secs,
            raw_spool_dir,
            raw_parquet_chunk_rows,
            metrics_dir,
            metrics_backend,
            clickhouse_url,
            clickhouse_database,
            clickhouse_table,
            clickhouse_batch_size,
            clickhouse_flush_ms,
            s3_bucket,
            s3_prefix,
            metrics_retention_days,
        })
    }

    pub fn symbol_stream_key(&self) -> String {
        self.stream_symbol.clone()
    }
}

fn env_or_default(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_or_default_path(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn parse_env_or_default<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match env::var(key) {
        Ok(value) => value.trim().parse::<T>().map_err(|error| {
            anyhow::anyhow!("failed to parse environment variable {key}: {error}")
        }),
        Err(_) => Ok(default),
    }
}

fn parse_metrics_backend(value: &str) -> Result<MetricsBackend> {
    match value.trim().to_ascii_lowercase().as_str() {
        "jsonl" | "file" => Ok(MetricsBackend::Jsonl),
        "clickhouse" | "ch" => Ok(MetricsBackend::ClickHouse),
        other => Err(anyhow::anyhow!(
            "invalid APP_METRICS_BACKEND value: {other}; expected jsonl or clickhouse"
        )),
    }
}

fn resolve_binance_coinm_instrument(input: &str) -> Result<(String, String, String)> {
    let normalized = input.trim().to_ascii_uppercase();

    match normalized.as_str() {
        "BTCUSD_PERP" => Ok((
            "BTCUSD_PERP".to_string(),
            "BTCUSD_PERP".to_string(),
            "btcusd_perp".to_string(),
        )),
        other => Err(anyhow::anyhow!(
            "unsupported APP_SYMBOL for Binance COIN-M: {other}; currently supported: BTCUSD_PERP"
        )),
    }
}

fn parse_cancel_heuristic(value: &str) -> Result<CancelHeuristic> {
    match value.trim().to_ascii_lowercase().as_str() {
        "lifo" => Ok(CancelHeuristic::Lifo),
        "prorata" | "pro_rata" | "pro-rata" => Ok(CancelHeuristic::ProRata),
        other => Err(anyhow::anyhow!(
            "invalid APP_CANCEL_HEURISTIC value: {other}; expected lifo or prorata"
        )),
    }
}
