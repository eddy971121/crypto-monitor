use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use clickhouse::{Client as ClickHouseClient, Row};
use serde::Deserialize;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{info, warn};

use crate::config::{AppConfig, MetricsBackend};
use crate::telemetry::TelemetryEvent;
use crate::types::SignalMetric;

const DEDUP_VIEW_NAME: &str = "signal_metrics_vw";
const SIGNAL_METRIC_SCHEMA: [(&str, &str); 34] = [
    ("schema_version", "UInt16"),
    ("exchange", "String"),
    ("market", "String"),
    ("symbol", "String"),
    ("event_id", "String"),
    ("event_ts_ms", "Int64"),
    ("recv_ts_ms", "Int64"),
    ("stale_state", "Bool"),
    ("trade_alignment_forced_open", "Bool"),
    ("best_bid", "Float64"),
    ("best_ask", "Float64"),
    ("mid_price", "Float64"),
    ("spread_bps", "Float64"),
    ("bid_notional_5pct", "Float64"),
    ("ask_notional_5pct", "Float64"),
    ("imbalance_5pct", "Float64"),
    ("microprice", "Float64"),
    ("ofi_l1", "Float64"),
    ("m_ofi_top5", "Float64"),
    ("vwaa_l1_bid_ms", "Float64"),
    ("vwaa_l1_ask_ms", "Float64"),
    ("vwaa_l1_imbalance", "Float64"),
    ("vwaa_top5_bid_ms", "Float64"),
    ("vwaa_top5_ask_ms", "Float64"),
    ("vwaa_top5_imbalance", "Float64"),
    ("cvd_5s", "Float64"),
    ("cvd_1m", "Float64"),
    ("cvd_5m", "Float64"),
    ("spoof_flag", "Bool"),
    ("spoof_score", "Float64"),
    ("spoof_reason_code", "String"),
    ("depth_update_u", "UInt64"),
    ("depth_update_pu", "UInt64"),
    ("ingest_to_signal_ms", "UInt64"),
];

pub async fn ensure_clickhouse_preflight(config: &AppConfig) -> Result<()> {
    if config.metrics_backend != MetricsBackend::ClickHouse {
        return Ok(());
    }

    let (database, table) = clickhouse_identifiers(config)?;
    let client = build_clickhouse_client(&config.clickhouse_url, None);

    ensure_clickhouse_schema(&client, &database, &table).await?;
    ensure_clickhouse_dedup_view(&client, &database, &table).await?;
    validate_clickhouse_schema_contract(&client, &database, &table).await?;

    Ok(())
}

fn clickhouse_identifiers(config: &AppConfig) -> Result<(String, String)> {
    let database = validate_identifier(&config.clickhouse_database, "APP_CLICKHOUSE_DATABASE")?;
    let table = validate_identifier(&config.clickhouse_table, "APP_CLICKHOUSE_TABLE")?;
    Ok((database, table))
}

fn build_clickhouse_client(clickhouse_url: &str, database: Option<&str>) -> ClickHouseClient {
    let client = ClickHouseClient::default().with_url(clickhouse_url.trim_end_matches('/'));

    match database {
        Some(database) => client.with_database(database),
        None => client,
    }
}

pub async fn run_metrics_writer(
    config: AppConfig,
    metrics_rx: mpsc::Receiver<SignalMetric>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    match config.metrics_backend {
        MetricsBackend::Jsonl => run_file_metrics_writer(config, metrics_rx, telemetry_tx).await,
        MetricsBackend::ClickHouse => {
            run_clickhouse_metrics_writer(config, metrics_rx, telemetry_tx).await
        }
    }
}

async fn run_file_metrics_writer(
    config: AppConfig,
    mut metrics_rx: mpsc::Receiver<SignalMetric>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    info!("metrics backend selected: jsonl");
    let mut writer = MetricsFileWriter::new(config.metrics_dir.clone());

    while let Some(metric) = metrics_rx.recv().await {
        if let Err(error) = writer.write_metric(&metric).await {
            warn!(%error, "failed to write metrics row");
            let _ = telemetry_tx.send(TelemetryEvent::MetricsWriteError).await;
        }
    }

    writer.flush().await?;
    Ok(())
}

async fn run_clickhouse_metrics_writer(
    config: AppConfig,
    mut metrics_rx: mpsc::Receiver<SignalMetric>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    info!("metrics backend selected: clickhouse");

    let (database, table) = clickhouse_identifiers(&config)?;
    let client = build_clickhouse_client(&config.clickhouse_url, Some(database.as_str()));

    ensure_clickhouse_schema(&client, &database, &table).await?;
    ensure_clickhouse_dedup_view(&client, &database, &table).await?;
    validate_clickhouse_schema_contract(&client, &database, &table).await?;

    let mut batch = Vec::with_capacity(config.clickhouse_batch_size);
    let mut pending_event_ids = HashSet::new();
    let mut recent_event_cache = RecentEventCache::new(config.clickhouse_batch_size.max(1) * 400);
    let mut flush_interval = time::interval(Duration::from_millis(config.clickhouse_flush_ms));

    loop {
        tokio::select! {
            metric = metrics_rx.recv() => {
                match metric {
                    Some(metric) => {
                        if recent_event_cache.contains(metric.event_id.as_str())
                            || pending_event_ids.contains(metric.event_id.as_str())
                        {
                            continue;
                        }

                        pending_event_ids.insert(metric.event_id.clone());
                        batch.push(metric);

                        if batch.len() >= config.clickhouse_batch_size {
                            flush_clickhouse_batch(
                                &client,
                                &table,
                                &mut batch,
                                &mut pending_event_ids,
                                &mut recent_event_cache,
                                &telemetry_tx,
                            )
                            .await;
                        }
                    }
                    None => break,
                }
            }
            _ = flush_interval.tick() => {
                if !batch.is_empty() {
                    flush_clickhouse_batch(
                        &client,
                        &table,
                        &mut batch,
                        &mut pending_event_ids,
                        &mut recent_event_cache,
                        &telemetry_tx,
                    )
                    .await;
                }
            }
        }
    }

    if !batch.is_empty() {
        flush_clickhouse_batch(
            &client,
            &table,
            &mut batch,
            &mut pending_event_ids,
            &mut recent_event_cache,
            &telemetry_tx,
        )
        .await;
    }

    Ok(())
}

async fn flush_clickhouse_batch(
    client: &ClickHouseClient,
    table: &str,
    batch: &mut Vec<SignalMetric>,
    pending_event_ids: &mut HashSet<String>,
    recent_event_cache: &mut RecentEventCache,
    telemetry_tx: &mpsc::Sender<TelemetryEvent>,
) {
    if batch.is_empty() {
        return;
    }

    let mut write_batch = std::mem::take(batch);
    if let Err(error) = insert_clickhouse_metrics(client, table, &write_batch).await {
        warn!(%error, rows = write_batch.len(), "failed to write metrics batch to clickhouse");
        batch.append(&mut write_batch);
        let _ = telemetry_tx.send(TelemetryEvent::MetricsWriteError).await;
        return;
    }

    for metric in write_batch {
        pending_event_ids.remove(metric.event_id.as_str());
        recent_event_cache.insert(metric.event_id);
    }
}

async fn ensure_clickhouse_schema(
    client: &ClickHouseClient,
    database: &str,
    table: &str,
) -> Result<()> {
    let create_db_query = format!("CREATE DATABASE IF NOT EXISTS {database}");
    run_clickhouse_query(client, &create_db_query).await?;

    let create_table_query = format!(
        "CREATE TABLE IF NOT EXISTS {database}.{table} (
            schema_version UInt16,
            exchange String,
            market String,
            symbol String,
            event_id String,
            event_ts_ms Int64,
            recv_ts_ms Int64,
            stale_state Bool,
            trade_alignment_forced_open Bool,
            best_bid Float64,
            best_ask Float64,
            mid_price Float64,
            spread_bps Float64,
            bid_notional_5pct Float64,
            ask_notional_5pct Float64,
            imbalance_5pct Float64,
            microprice Float64,
            ofi_l1 Float64,
            m_ofi_top5 Float64,
            vwaa_l1_bid_ms Float64,
            vwaa_l1_ask_ms Float64,
            vwaa_l1_imbalance Float64,
            vwaa_top5_bid_ms Float64,
            vwaa_top5_ask_ms Float64,
            vwaa_top5_imbalance Float64,
            cvd_5s Float64,
            cvd_1m Float64,
            cvd_5m Float64,
            spoof_flag Bool,
            spoof_score Float64,
            spoof_reason_code String,
            depth_update_u UInt64,
            depth_update_pu UInt64,
            ingest_to_signal_ms UInt64
        )
        ENGINE = ReplacingMergeTree(recv_ts_ms)
        ORDER BY (symbol, event_id)"
    );

    run_clickhouse_query(client, &create_table_query).await?;

    let alter_queries = [
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS ofi_l1 Float64"),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS m_ofi_top5 Float64"),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_l1_bid_ms Float64"
        ),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_l1_ask_ms Float64"
        ),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_l1_imbalance Float64"
        ),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_top5_bid_ms Float64"
        ),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_top5_ask_ms Float64"
        ),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS vwaa_top5_imbalance Float64"
        ),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS cvd_5s Float64"),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS cvd_1m Float64"),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS cvd_5m Float64"),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS trade_alignment_forced_open Bool"
        ),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS spoof_flag Bool"),
        format!("ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS spoof_score Float64"),
        format!(
            "ALTER TABLE {database}.{table} ADD COLUMN IF NOT EXISTS spoof_reason_code String"
        ),
    ];

    for query in alter_queries {
        run_clickhouse_query(client, query.as_str()).await?;
    }

    Ok(())
}

async fn ensure_clickhouse_dedup_view(
    client: &ClickHouseClient,
    database: &str,
    table: &str,
) -> Result<()> {
    let create_view_query = format!(
        "CREATE VIEW IF NOT EXISTS {database}.{DEDUP_VIEW_NAME} AS
        SELECT
            argMax(schema_version, recv_ts_ms) AS schema_version,
            argMax(exchange, recv_ts_ms) AS exchange,
            argMax(market, recv_ts_ms) AS market,
            symbol,
            event_id,
            argMax(event_ts_ms, recv_ts_ms) AS event_ts_ms,
            max(recv_ts_ms) AS recv_ts_ms,
            argMax(stale_state, recv_ts_ms) AS stale_state,
            argMax(trade_alignment_forced_open, recv_ts_ms) AS trade_alignment_forced_open,
            argMax(best_bid, recv_ts_ms) AS best_bid,
            argMax(best_ask, recv_ts_ms) AS best_ask,
            argMax(mid_price, recv_ts_ms) AS mid_price,
            argMax(spread_bps, recv_ts_ms) AS spread_bps,
            argMax(bid_notional_5pct, recv_ts_ms) AS bid_notional_5pct,
            argMax(ask_notional_5pct, recv_ts_ms) AS ask_notional_5pct,
            argMax(imbalance_5pct, recv_ts_ms) AS imbalance_5pct,
            argMax(microprice, recv_ts_ms) AS microprice,
            argMax(ofi_l1, recv_ts_ms) AS ofi_l1,
            argMax(m_ofi_top5, recv_ts_ms) AS m_ofi_top5,
            argMax(vwaa_l1_bid_ms, recv_ts_ms) AS vwaa_l1_bid_ms,
            argMax(vwaa_l1_ask_ms, recv_ts_ms) AS vwaa_l1_ask_ms,
            argMax(vwaa_l1_imbalance, recv_ts_ms) AS vwaa_l1_imbalance,
            argMax(vwaa_top5_bid_ms, recv_ts_ms) AS vwaa_top5_bid_ms,
            argMax(vwaa_top5_ask_ms, recv_ts_ms) AS vwaa_top5_ask_ms,
            argMax(vwaa_top5_imbalance, recv_ts_ms) AS vwaa_top5_imbalance,
            argMax(cvd_5s, recv_ts_ms) AS cvd_5s,
            argMax(cvd_1m, recv_ts_ms) AS cvd_1m,
            argMax(cvd_5m, recv_ts_ms) AS cvd_5m,
            argMax(spoof_flag, recv_ts_ms) AS spoof_flag,
            argMax(spoof_score, recv_ts_ms) AS spoof_score,
            argMax(spoof_reason_code, recv_ts_ms) AS spoof_reason_code,
            argMax(depth_update_u, recv_ts_ms) AS depth_update_u,
            argMax(depth_update_pu, recv_ts_ms) AS depth_update_pu,
            argMax(ingest_to_signal_ms, recv_ts_ms) AS ingest_to_signal_ms
        FROM {database}.{table}
        GROUP BY symbol, event_id"
    );

    run_clickhouse_query(client, &create_view_query).await
}

async fn validate_clickhouse_schema_contract(
    client: &ClickHouseClient,
    database: &str,
    table: &str,
) -> Result<()> {
    let rows = client
        .query("SELECT ?fields FROM system.columns WHERE database = ? AND table = ?")
        .bind(database)
        .bind(table)
        .fetch_all::<ClickHouseColumnInfo>()
        .await
        .context("failed to query clickhouse system.columns for schema validation")?;

    if rows.is_empty() {
        return Err(anyhow::anyhow!(
            "clickhouse schema contract check failed: {database}.{table} has no columns"
        ));
    }

    let actual_columns: HashMap<String, String> = rows
        .into_iter()
        .map(|row| (row.name, row.column_type))
        .collect();

    for (column_name, expected_type) in SIGNAL_METRIC_SCHEMA {
        let Some(actual_type) = actual_columns.get(column_name) else {
            return Err(anyhow::anyhow!(
                "clickhouse schema contract check failed: missing required column {column_name} on {database}.{table}"
            ));
        };

        if !clickhouse_type_matches(expected_type, actual_type) {
            return Err(anyhow::anyhow!(
                "clickhouse schema contract check failed: {database}.{table}.{column_name} expected type {expected_type} but found {actual_type}"
            ));
        }
    }

    Ok(())
}

async fn insert_clickhouse_metrics(
    client: &ClickHouseClient,
    table: &str,
    metrics: &[SignalMetric],
) -> Result<()> {
    let mut insert = client
        .insert(table)
        .context("failed to open clickhouse insert")?;

    for metric in metrics {
        insert
            .write(metric)
            .await
            .context("failed to write clickhouse metric row")?;
    }

    insert
        .end()
        .await
        .context("failed to finalize clickhouse insert")
}

async fn run_clickhouse_query(
    client: &ClickHouseClient,
    query: &str,
) -> Result<()> {
    client
        .query(query)
        .execute()
        .await
        .with_context(|| format!("clickhouse query failed: {query}"))?;

    Ok(())
}

#[derive(Debug, Deserialize, Row)]
struct ClickHouseColumnInfo {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}

fn clickhouse_type_matches(expected: &str, actual: &str) -> bool {
    let normalized_expected = normalize_clickhouse_type(expected);
    let normalized_actual = normalize_clickhouse_type(actual);

    if normalized_expected == normalized_actual {
        return true;
    }

    (normalized_expected == "bool" && normalized_actual == "uint8")
        || (normalized_expected == "uint8" && normalized_actual == "bool")
}

fn normalize_clickhouse_type(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace(' ', "")
}

fn validate_identifier(value: &str, env_key: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow::anyhow!(
            "{env_key} cannot be empty when clickhouse backend is enabled"
        ));
    }

    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(anyhow::anyhow!(
            "{env_key} contains invalid characters; only [A-Za-z0-9_] are allowed"
        ));
    }

    Ok(trimmed.to_string())
}

struct RecentEventCache {
    capacity: usize,
    queue: VecDeque<String>,
    set: HashSet<String>,
}

impl RecentEventCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            queue: VecDeque::with_capacity(capacity.max(1)),
            set: HashSet::with_capacity(capacity.max(1)),
        }
    }

    fn contains(&self, event_id: &str) -> bool {
        self.set.contains(event_id)
    }

    fn insert(&mut self, event_id: String) {
        if self.set.contains(event_id.as_str()) {
            return;
        }

        if self.queue.len() >= self.capacity {
            if let Some(evicted) = self.queue.pop_front() {
                self.set.remove(evicted.as_str());
            }
        }

        self.set.insert(event_id.clone());
        self.queue.push_back(event_id);
    }
}

struct MetricsFileWriter {
    metrics_dir: PathBuf,
    current_date: Option<String>,
    file: Option<tokio::fs::File>,
}

impl MetricsFileWriter {
    fn new(metrics_dir: PathBuf) -> Self {
        Self {
            metrics_dir,
            current_date: None,
            file: None,
        }
    }

    async fn write_metric(&mut self, metric: &SignalMetric) -> Result<()> {
        let date_key = date_key_from_ts(metric.event_ts_ms);
        self.rotate_if_needed(&date_key).await?;

        let line = serde_json::to_string(metric).context("failed to serialize metric row")?;
        if let Some(file) = &mut self.file {
            file.write_all(line.as_bytes()).await?;
            file.write_all(b"\n").await?;
        }

        Ok(())
    }

    async fn rotate_if_needed(&mut self, date_key: &str) -> Result<()> {
        if self.current_date.as_deref() == Some(date_key) {
            return Ok(());
        }

        self.flush().await?;

        tokio::fs::create_dir_all(&self.metrics_dir)
            .await
            .with_context(|| format!("failed to create {}", self.metrics_dir.display()))?;

        let file_path = metrics_file_path(&self.metrics_dir, date_key);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .await
            .with_context(|| format!("failed to open metrics file {}", file_path.display()))?;

        self.current_date = Some(date_key.to_string());
        self.file = Some(file);

        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(file) = &mut self.file {
            file.flush().await?;
        }
        Ok(())
    }
}

fn date_key_from_ts(ts_ms: i64) -> String {
    match Utc.timestamp_millis_opt(ts_ms).single() {
        Some(timestamp) => timestamp.date_naive().format("%Y-%m-%d").to_string(),
        None => Utc::now().date_naive().format("%Y-%m-%d").to_string(),
    }
}

pub fn metrics_file_path(metrics_dir: &Path, date_key: &str) -> PathBuf {
    metrics_dir.join(format!("metrics-{}.jsonl", date_key))
}
