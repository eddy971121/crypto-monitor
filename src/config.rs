use std::{env, path::PathBuf};

use anyhow::Result;

use crate::types::CancelHeuristic;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MetricsBackend {
    Jsonl,
    ClickHouse,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ContractType {
    Linear,
    Inverse { contract_size: f64 },
}

impl ContractType {
    pub fn quote_notional(self, price: f64, quantity: f64) -> f64 {
        if price <= 0.0 || quantity <= 0.0 {
            return 0.0;
        }

        match self {
            Self::Linear => price * quantity,
            Self::Inverse { contract_size } => quantity * contract_size,
        }
    }

    #[allow(dead_code)]
    pub fn base_amount(self, price: f64, quantity: f64) -> f64 {
        if price <= 0.0 || quantity <= 0.0 {
            return 0.0;
        }

        match self {
            Self::Linear => quantity,
            Self::Inverse { contract_size } => (quantity * contract_size) / price,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MarketCategory {
    Spot,
    CoinM,
    UsdM,
}

impl MarketCategory {
    fn default_symbol(self) -> &'static str {
        match self {
            Self::Spot => "BTCUSD",
            Self::CoinM => "BTCUSD_PERP",
            Self::UsdM => "BTCUSDT",
        }
    }

    fn default_ws_base_url(self) -> &'static str {
        match self {
            Self::Spot => "wss://stream.binance.com:9443",
            Self::CoinM => "wss://dstream.binance.com",
            Self::UsdM => "wss://fstream.binance.com",
        }
    }

    fn default_rest_base_url(self) -> &'static str {
        match self {
            Self::Spot => "https://api.binance.com",
            Self::CoinM => "https://dapi.binance.com",
            Self::UsdM => "https://fapi.binance.com",
        }
    }

    fn default_depth_snapshot_path(self) -> &'static str {
        match self {
            Self::Spot => "/api/v3/depth",
            Self::CoinM => "/dapi/v1/depth",
            Self::UsdM => "/fapi/v1/depth",
        }
    }

    fn default_market_name(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::CoinM => "coin-m-futures",
            Self::UsdM => "usd-m-futures",
        }
    }

    fn default_contract_mode(self) -> ContractMode {
        match self {
            Self::Spot => ContractMode::Linear,
            Self::CoinM => ContractMode::Inverse,
            Self::UsdM => ContractMode::Linear,
        }
    }

    pub fn uses_prev_final_update_id(self) -> bool {
        !matches!(self, Self::Spot)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ContractMode {
    Linear,
    Inverse,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub exchange: String,
    pub market_category: MarketCategory,
    pub market: String,
    pub contract_type: ContractType,
    pub canonical_symbol: String,
    pub symbol: String,
    pub stream_symbol: String,
    pub ws_base_url: String,
    pub rest_base_url: String,
    pub depth_snapshot_path: String,
    pub depth_limit: u16,
    pub channel_capacity: usize,
    pub enable_depth_stream: bool,
    pub enable_book_ticker_stream: bool,
    pub enable_agg_trade_stream: bool,
    pub cancel_heuristic: CancelHeuristic,
    pub health_report_interval_secs: u64,
    pub ws_read_idle_timeout_secs: u64,
    pub ws_reconnect_backoff_max_secs: u64,
    pub ws_reconnect_jitter_bps: u16,
    pub systemd_notify_enabled: bool,
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
        let exchange = normalize_identity_component(
            env_or_default("APP_EXCHANGE", "binance"),
            "APP_EXCHANGE",
        )?;
        if exchange != "binance" {
            return Err(anyhow::anyhow!(
                "unsupported APP_EXCHANGE value: {exchange}; currently supported: binance"
            ));
        }

        let market_category =
            parse_market_category(&env_or_default("APP_MARKET", "coinm"))?;
        let market = normalize_identity_component(
            env_or_default("APP_MARKET_NAME", market_category.default_market_name()),
            "APP_MARKET_NAME",
        )?;

        let canonical_symbol = env_or_default("APP_SYMBOL", market_category.default_symbol());
        let (canonical_symbol, symbol, stream_symbol) =
            resolve_binance_instrument(market_category, &canonical_symbol)?;

        let ws_base_url = env_or_default("APP_WS_BASE_URL", market_category.default_ws_base_url());
        let rest_base_url = env_or_default(
            "APP_REST_BASE_URL",
            market_category.default_rest_base_url(),
        );
        let depth_snapshot_path = env_or_default(
            "APP_DEPTH_SNAPSHOT_PATH",
            market_category.default_depth_snapshot_path(),
        );

        if !depth_snapshot_path.starts_with('/') {
            return Err(anyhow::anyhow!(
                "APP_DEPTH_SNAPSHOT_PATH must start with '/'; got {}",
                depth_snapshot_path
            ));
        }

        let contract_mode = parse_contract_mode(&env_or_default(
            "APP_CONTRACT_TYPE",
            contract_mode_name(market_category.default_contract_mode()),
        ))?;
        let contract_size = parse_env_or_default("APP_CONTRACT_SIZE", 100.0_f64)?;
        if contract_size <= 0.0 {
            return Err(anyhow::anyhow!(
                "APP_CONTRACT_SIZE must be greater than 0"
            ));
        }
        let contract_type = match contract_mode {
            ContractMode::Linear => ContractType::Linear,
            ContractMode::Inverse => ContractType::Inverse { contract_size },
        };

        let depth_limit = parse_env_or_default("APP_DEPTH_LIMIT", 1000_u16)?;
        let channel_capacity = parse_env_or_default("APP_CHANNEL_CAPACITY", 20000_usize)?;
        let enable_depth_stream = parse_env_or_default("APP_ENABLE_DEPTH_STREAM", true)?;
        let enable_book_ticker_stream =
            parse_env_or_default("APP_ENABLE_BOOK_TICKER_STREAM", true)?;
        let enable_agg_trade_stream = parse_env_or_default("APP_ENABLE_AGG_TRADE_STREAM", true)?;
        let cancel_heuristic = parse_cancel_heuristic(&env_or_default("APP_CANCEL_HEURISTIC", "lifo"))?;
        let health_report_interval_secs =
            parse_env_or_default("APP_HEALTH_REPORT_INTERVAL_SECS", 30_u64)?;
        let ws_read_idle_timeout_secs =
            parse_env_or_default("APP_WS_READ_IDLE_TIMEOUT_SECS", 45_u64)?;
        let ws_reconnect_backoff_max_secs =
            parse_env_or_default("APP_WS_RECONNECT_BACKOFF_MAX_SECS", 30_u64)?;
        let ws_reconnect_jitter_bps =
            parse_env_or_default("APP_WS_RECONNECT_JITTER_BPS", 1000_u16)?;
        let systemd_notify_enabled =
            parse_env_or_default("APP_SYSTEMD_NOTIFY_ENABLED", true)?;
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

        if ws_read_idle_timeout_secs == 0 {
            return Err(anyhow::anyhow!(
                "APP_WS_READ_IDLE_TIMEOUT_SECS must be greater than 0"
            ));
        }

        if ws_reconnect_backoff_max_secs == 0 {
            return Err(anyhow::anyhow!(
                "APP_WS_RECONNECT_BACKOFF_MAX_SECS must be greater than 0"
            ));
        }

        if ws_reconnect_jitter_bps > 5000 {
            return Err(anyhow::anyhow!(
                "APP_WS_RECONNECT_JITTER_BPS must be in the range [0, 5000] (0-50%)"
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
        let default_s3_prefix = format!("crypto-monitor/{}/{}", exchange, market);
        let s3_prefix = env_or_default("APP_S3_PREFIX", default_s3_prefix.as_str());
        let metrics_retention_days = parse_env_or_default("APP_METRICS_RETENTION_DAYS", 30_i64)?;

        Ok(Self {
            exchange,
            market_category,
            market,
            contract_type,
            canonical_symbol,
            symbol,
            stream_symbol,
            ws_base_url,
            rest_base_url,
            depth_snapshot_path,
            depth_limit,
            channel_capacity,
            enable_depth_stream,
            enable_book_ticker_stream,
            enable_agg_trade_stream,
            cancel_heuristic,
            health_report_interval_secs,
            ws_read_idle_timeout_secs,
            ws_reconnect_backoff_max_secs,
            ws_reconnect_jitter_bps,
            systemd_notify_enabled,
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

    pub fn depth_event_id(&self, symbol: &str, final_update_id: u64) -> String {
        format!(
            "{}:{}:{}:{}",
            self.exchange, self.market, symbol, final_update_id
        )
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

fn resolve_binance_instrument(
    market_category: MarketCategory,
    input: &str,
) -> Result<(String, String, String)> {
    let normalized = input.trim().to_ascii_uppercase();

    match market_category {
        MarketCategory::Spot => match normalized.as_str() {
            "BTCUSD" => Ok((
                "BTCUSD".to_string(),
                "BTCUSD".to_string(),
                "btcusd".to_string(),
            )),
            "ETHUSD" => Ok((
                "ETHUSD".to_string(),
                "ETHUSD".to_string(),
                "ethusd".to_string(),
            )),
            other => Err(anyhow::anyhow!(
                "unsupported APP_SYMBOL for Binance SPOT: {other}; currently supported: BTCUSD, ETHUSD"
            )),
        },
        MarketCategory::CoinM => match normalized.as_str() {
            "BTCUSD_PERP" => Ok((
                "BTCUSD_PERP".to_string(),
                "BTCUSD_PERP".to_string(),
                "btcusd_perp".to_string(),
            )),
            "ETHUSD_PERP" => Ok((
                "ETHUSD_PERP".to_string(),
                "ETHUSD_PERP".to_string(),
                "ethusd_perp".to_string(),
            )),
            other => Err(anyhow::anyhow!(
                "unsupported APP_SYMBOL for Binance COIN-M: {other}; currently supported: BTCUSD_PERP, ETHUSD_PERP"
            )),
        },
        MarketCategory::UsdM => match normalized.as_str() {
            "BTCUSDT" => Ok((
                "BTCUSDT".to_string(),
                "BTCUSDT".to_string(),
                "btcusdt".to_string(),
            )),
            "ETHUSDT" => Ok((
                "ETHUSDT".to_string(),
                "ETHUSDT".to_string(),
                "ethusdt".to_string(),
            )),
            other => Err(anyhow::anyhow!(
                "unsupported APP_SYMBOL for Binance USD-M: {other}; currently supported: BTCUSDT, ETHUSDT"
            )),
        },
    }
}

fn parse_market_category(value: &str) -> Result<MarketCategory> {
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" | "spot-market" | "spot_market" => Ok(MarketCategory::Spot),
        "coinm" | "coin-m" | "coin_m" | "coin-m-futures" => Ok(MarketCategory::CoinM),
        "usdm" | "usd-m" | "usd_m" | "usd-m-futures" => Ok(MarketCategory::UsdM),
        other => Err(anyhow::anyhow!(
            "invalid APP_MARKET value: {other}; expected spot, coinm, or usdm"
        )),
    }
}

fn parse_contract_mode(value: &str) -> Result<ContractMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "linear" => Ok(ContractMode::Linear),
        "inverse" => Ok(ContractMode::Inverse),
        other => Err(anyhow::anyhow!(
            "invalid APP_CONTRACT_TYPE value: {other}; expected linear or inverse"
        )),
    }
}

fn contract_mode_name(mode: ContractMode) -> &'static str {
    match mode {
        ContractMode::Linear => "linear",
        ContractMode::Inverse => "inverse",
    }
}

fn normalize_identity_component(value: String, key: &str) -> Result<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(anyhow::anyhow!(
            "{key} must be non-empty after trimming whitespace"
        ));
    }
    Ok(normalized)
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
