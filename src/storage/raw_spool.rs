use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt16Array};
use arrow_schema::{DataType, Field, Schema};
use chrono::{TimeZone, Utc};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::telemetry::TelemetryEvent;
use crate::types::{BookTickerEvent, RawBookTickerRow};

const RAW_PARQUET_MANIFEST_SCHEMA_VERSION: u16 = 1;
const FIRST_PARQUET_PART_INDEX: u32 = 1;
const RAW_PARTITION_EXCHANGE: &str = "binance";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawParquetPartEntry {
    pub part_index: u32,
    pub file_name: String,
    pub row_count: usize,
    pub uploaded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawParquetManifest {
    pub schema_version: u16,
    pub date_key: String,
    pub next_part_index: u32,
    pub parts: Vec<RawParquetPartEntry>,
}

impl RawParquetManifest {
    fn new(date_key: String, next_part_index: u32, parts: Vec<RawParquetPartEntry>) -> Self {
        Self {
            schema_version: RAW_PARQUET_MANIFEST_SCHEMA_VERSION,
            date_key,
            next_part_index,
            parts,
        }
    }

    pub fn all_uploaded(&self) -> bool {
        self.parts.iter().all(|part| part.uploaded)
    }
}

pub async fn run_raw_spooler(
    config: AppConfig,
    mut book_ticker_rx: mpsc::Receiver<BookTickerEvent>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let mut writer = RawBookTickerParquetWriter::new(
        config.raw_spool_dir.clone(),
        config.raw_parquet_chunk_rows,
        RAW_PARTITION_EXCHANGE.to_string(),
        config.stream_symbol.clone(),
    );

    while let Some(event) = book_ticker_rx.recv().await {
        if event.payload.symbol != config.symbol {
            continue;
        }

        let row = RawBookTickerRow {
            schema_version: 1,
            symbol: event.payload.symbol,
            event_ts_ms: event.payload.event_time_ms,
            recv_ts_ms: event.recv_ts_ms,
            raw_json: event.raw_json,
        };

        if let Err(error) = writer.write_row(&row).await {
            warn!(%error, "failed to write raw bookTicker parquet row");
            let _ = telemetry_tx.send(TelemetryEvent::RawWriteError).await;
        }
    }

    writer.flush().await?;
    Ok(())
}

struct RawBookTickerParquetWriter {
    spool_dir: PathBuf,
    chunk_rows: usize,
    exchange: String,
    symbol: String,
    active_date: Option<String>,
    manifest: Option<RawParquetManifest>,
    recv_ts_ms: Vec<i64>,
    event_ts_ms: Vec<i64>,
    symbols: Vec<String>,
    payloads: Vec<String>,
    schema_versions: Vec<u16>,
}

impl RawBookTickerParquetWriter {
    fn new(spool_dir: PathBuf, chunk_rows: usize, exchange: String, symbol: String) -> Self {
        Self {
            spool_dir,
            chunk_rows: chunk_rows.max(1),
            exchange,
            symbol,
            active_date: None,
            manifest: None,
            recv_ts_ms: Vec::with_capacity(chunk_rows.max(1)),
            event_ts_ms: Vec::with_capacity(chunk_rows.max(1)),
            symbols: Vec::with_capacity(chunk_rows.max(1)),
            payloads: Vec::with_capacity(chunk_rows.max(1)),
            schema_versions: Vec::with_capacity(chunk_rows.max(1)),
        }
    }

    async fn write_row(&mut self, row: &RawBookTickerRow) -> Result<()> {
        let date_key = date_key_from_ts(row.recv_ts_ms);
        self.rotate_if_needed(date_key.as_str()).await?;

        self.recv_ts_ms.push(row.recv_ts_ms);
        self.event_ts_ms.push(row.event_ts_ms);
        self.symbols.push(row.symbol.clone());
        self.payloads.push(row.raw_json.clone());
        self.schema_versions.push(row.schema_version);

        if self.recv_ts_ms.len() >= self.chunk_rows {
            self.flush_chunk().await?;
        }

        Ok(())
    }

    async fn rotate_if_needed(&mut self, date_key: &str) -> Result<()> {
        if self.active_date.as_deref() == Some(date_key) {
            return Ok(());
        }

        self.flush().await?;
        self.prepare_date(date_key).await
    }

    async fn prepare_date(&mut self, date_key: &str) -> Result<()> {
        let date_dir = partition_date_dir(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
        );

        tokio::fs::create_dir_all(&date_dir)
            .await
            .with_context(|| format!("failed to create {}", date_dir.display()))?;

        cleanup_temp_part_files(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
        )
        .await?;

        let manifest = match load_raw_parquet_manifest(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                let parts = list_raw_parquet_parts(
                    &self.spool_dir,
                    self.exchange.as_str(),
                    self.symbol.as_str(),
                    date_key,
                )
                .await?;
                let manifest = build_manifest_from_parts(date_key, parts);
                save_raw_parquet_manifest(
                    &self.spool_dir,
                    self.exchange.as_str(),
                    self.symbol.as_str(),
                    &manifest,
                )
                .await?;
                manifest
            }
        };

        if manifest.date_key != date_key {
            return Err(anyhow::anyhow!(
                "raw parquet manifest date mismatch: expected {date_key}, found {}",
                manifest.date_key
            ));
        }

        let manifest = reconcile_manifest_with_disk_parts(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
            manifest,
        )
        .await?;

        self.active_date = Some(date_key.to_string());
        self.manifest = Some(manifest);

        Ok(())
    }

    async fn flush_chunk(&mut self) -> Result<()> {
        if self.recv_ts_ms.is_empty() {
            return Ok(());
        }

        let date_key = self
            .active_date
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("raw spool date not initialized"))?;
        let manifest = self
            .manifest
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("raw spool manifest not initialized"))?;

        let part_index = manifest.next_part_index;
        let chunk_start_ts_ms = chunk_start_ts_ms(&self.event_ts_ms, &self.recv_ts_ms);
        let temp_path = parquet_part_temp_file_path(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );
        let final_path = parquet_part_file_path(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );

        let write_result = write_parquet_part_file(
            &temp_path,
            &self.recv_ts_ms,
            &self.event_ts_ms,
            &self.symbols,
            &self.payloads,
            &self.schema_versions,
        );

        let row_count = match write_result {
            Ok(row_count) => row_count,
            Err(error) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error).with_context(|| {
                    format!("failed to write parquet temp file {}", temp_path.display())
                });
            }
        };

        tokio::fs::rename(&temp_path, &final_path)
            .await
            .with_context(|| {
                format!(
                    "failed to atomically rename {} to {}",
                    temp_path.display(),
                    final_path.display()
                )
            })?;

        let file_name = relative_path_from_spool_root(&self.spool_dir, &final_path)?;

        manifest.parts.push(RawParquetPartEntry {
            part_index,
            file_name,
            row_count,
            uploaded: false,
        });
        manifest.next_part_index = part_index.saturating_add(1);

        save_raw_parquet_manifest(
            &self.spool_dir,
            self.exchange.as_str(),
            self.symbol.as_str(),
            manifest,
        )
        .await?;

        info!(
            path = %final_path.display(),
            rows = row_count,
            "flushed raw bookTicker parquet spool chunk"
        );

        self.clear_chunk_buffers();
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_chunk().await?;

        if let Some(manifest) = &self.manifest {
            save_raw_parquet_manifest(
                &self.spool_dir,
                self.exchange.as_str(),
                self.symbol.as_str(),
                manifest,
            )
            .await?;
        }

        Ok(())
    }

    fn clear_chunk_buffers(&mut self) {
        self.recv_ts_ms.clear();
        self.event_ts_ms.clear();
        self.symbols.clear();
        self.payloads.clear();
        self.schema_versions.clear();
    }
}

fn write_parquet_part_file(
    output_path: &Path,
    recv_ts_ms: &[i64],
    event_ts_ms: &[i64],
    symbols: &[String],
    payloads: &[String],
    schema_versions: &[u16],
) -> Result<usize> {
    let row_count = recv_ts_ms.len();

    if row_count == 0 {
        return Ok(0);
    }

    if event_ts_ms.len() != row_count
        || symbols.len() != row_count
        || payloads.len() != row_count
        || schema_versions.len() != row_count
    {
        return Err(anyhow::anyhow!(
            "raw parquet chunk columns have mismatched lengths"
        ));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ms", DataType::Int64, false),
        Field::new("event_ts_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("raw_json", DataType::Utf8, false),
        Field::new("schema_version", DataType::UInt16, false),
    ]));

    let output_file = std::fs::File::create(output_path)
        .with_context(|| format!("failed to create parquet output {}", output_path.display()))?;
    let mut parquet_writer =
        ArrowWriter::try_new(output_file, schema.clone(), None).context("failed to open parquet writer")?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(recv_ts_ms.to_vec())),
            Arc::new(Int64Array::from(event_ts_ms.to_vec())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(StringArray::from(payloads.to_vec())),
            Arc::new(UInt16Array::from(schema_versions.to_vec())),
        ],
    )
    .context("failed to build arrow record batch")?;

    parquet_writer
        .write(&batch)
        .context("failed writing parquet batch")?;

    parquet_writer
        .close()
        .context("failed to close parquet writer")?;

    Ok(row_count)
}

pub fn date_key_from_ts(ts_ms: i64) -> String {
    match Utc.timestamp_millis_opt(ts_ms).single() {
        Some(timestamp) => timestamp.date_naive().format("%Y-%m-%d").to_string(),
        None => Utc::now().date_naive().format("%Y-%m-%d").to_string(),
    }
}

fn chunk_start_ts_ms(event_ts_ms: &[i64], recv_ts_ms: &[i64]) -> i64 {
    event_ts_ms
        .first()
        .copied()
        .or_else(|| recv_ts_ms.first().copied())
        .unwrap_or_else(|| Utc::now().timestamp_millis())
}

fn relative_path_from_spool_root(spool_dir: &Path, path: &Path) -> Result<String> {
    let relative = path.strip_prefix(spool_dir).with_context(|| {
        format!(
            "failed to strip raw spool root {} from {}",
            spool_dir.display(),
            path.display()
        )
    })?;

    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn normalize_partition_value(value: &str) -> String {
    let normalized: String = value
        .trim()
        .chars()
        .map(|ch| {
            let lower = ch.to_ascii_lowercase();
            if lower.is_ascii_alphanumeric() || lower == '_' || lower == '-' {
                lower
            } else {
                '_'
            }
        })
        .collect();

    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

pub fn partition_date_dir(spool_dir: &Path, exchange: &str, symbol: &str, date_key: &str) -> PathBuf {
    spool_dir
        .join(format!("exchange={}", normalize_partition_value(exchange)))
        .join(format!("symbol={}", normalize_partition_value(symbol)))
        .join(format!("date={date_key}"))
}

pub fn raw_file_path(spool_dir: &Path, exchange: &str, symbol: &str, date_key: &str) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join("bookticker.jsonl")
}

pub fn parquet_file_path(spool_dir: &Path, exchange: &str, symbol: &str, date_key: &str) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join("bookticker.parquet")
}

pub fn parquet_part_file_name(
    exchange: &str,
    symbol: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> String {
    let exchange_key = normalize_partition_value(exchange);
    let symbol_key = normalize_partition_value(symbol);
    format!(
        "bookticker_{chunk_start_ts_ms}_{exchange_key}_{symbol_key}_part-{part_index:06}.parquet"
    )
}

pub fn parquet_part_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join(parquet_part_file_name(
        exchange,
        symbol,
        chunk_start_ts_ms,
        part_index,
    ))
}

fn is_new_part_file_name(file_name: &str) -> bool {
    file_name.starts_with("bookticker_")
        && file_name.contains("_part-")
        && file_name.ends_with(".parquet")
}

fn is_legacy_part_file_name(file_name: &str, date_key: &str) -> bool {
    file_name.starts_with(format!("bookticker-{date_key}-part-").as_str())
        && file_name.ends_with(".parquet")
}

pub fn parse_parquet_part_index(file_name: &str, date_key: &str) -> Option<u32> {
    if is_new_part_file_name(file_name) {
        let stem = file_name.strip_suffix(".parquet")?;
        let (_, index_text) = stem.rsplit_once("_part-")?;
        return index_text.parse::<u32>().ok();
    }

    if is_legacy_part_file_name(file_name, date_key) {
        let prefix = format!("bookticker-{date_key}-part-");
        let part_text = file_name.strip_prefix(prefix.as_str())?.strip_suffix(".parquet")?;
        return part_text.parse::<u32>().ok();
    }

    None
}

fn parquet_part_temp_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    let exchange_key = normalize_partition_value(exchange);
    let symbol_key = normalize_partition_value(symbol);
    let file_name = format!(
        "bookticker_{chunk_start_ts_ms}_{exchange_key}_{symbol_key}_part-{part_index:06}.tmp.parquet"
    );
    partition_date_dir(spool_dir, exchange, symbol, date_key).join(file_name)
}

pub fn manifest_file_path(spool_dir: &Path, exchange: &str, symbol: &str, date_key: &str) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join("bookticker_manifest.json")
}

fn manifest_temp_file_path(spool_dir: &Path, exchange: &str, symbol: &str, date_key: &str) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join("bookticker_manifest.tmp.json")
}

pub async fn load_raw_parquet_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    let path = manifest_file_path(spool_dir, exchange, symbol, date_key);
    let content = match tokio::fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to read raw parquet manifest {}", path.display()))
        }
    };

    let manifest: RawParquetManifest = serde_json::from_str(content.as_str()).with_context(|| {
        format!("failed to parse raw parquet manifest {}", path.display())
    })?;

    Ok(Some(manifest))
}

pub async fn save_raw_parquet_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    manifest: &RawParquetManifest,
) -> Result<()> {
    let path = manifest_file_path(spool_dir, exchange, symbol, manifest.date_key.as_str());
    let temp_path = manifest_temp_file_path(spool_dir, exchange, symbol, manifest.date_key.as_str());
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "failed to resolve manifest parent directory for {}",
            path.display()
        )
    })?;

    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create {}", parent.display()))?;

    let content = serde_json::to_string_pretty(manifest).context("failed to serialize raw parquet manifest")?;

    tokio::fs::write(&temp_path, content)
        .await
        .with_context(|| format!("failed to write raw parquet manifest temp file {}", temp_path.display()))?;

    if tokio::fs::metadata(&path).await.is_ok() {
        tokio::fs::remove_file(&path)
            .await
            .with_context(|| format!("failed to replace raw parquet manifest {}", path.display()))?;
    }

    tokio::fs::rename(&temp_path, &path)
        .await
        .with_context(|| format!("failed to rename raw parquet manifest {}", path.display()))?;

    Ok(())
}

pub async fn list_raw_parquet_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Vec<RawParquetPartEntry>> {
    if tokio::fs::metadata(spool_dir).await.is_err() {
        return Ok(Vec::new());
    }

    let mut parts = Vec::new();
    let date_dir = partition_date_dir(spool_dir, exchange, symbol, date_key);
    parts.extend(list_raw_parquet_parts_in_directory(
        spool_dir,
        date_dir.as_path(),
        date_key,
    ).await?);

    // Keep legacy flat-file discovery so prior days can still upload after migration.
    parts.extend(
        list_raw_parquet_parts_in_directory(spool_dir, spool_dir, date_key)
            .await?
            .into_iter()
            .filter(|part| !part.file_name.contains('/')),
    );

    parts.sort_by_key(|entry| entry.part_index);
    parts.dedup_by(|left, right| left.file_name == right.file_name);
    Ok(parts)
}

async fn list_raw_parquet_parts_in_directory(
    spool_root: &Path,
    search_dir: &Path,
    date_key: &str,
) -> Result<Vec<RawParquetPartEntry>> {
    if tokio::fs::metadata(search_dir).await.is_err() {
        return Ok(Vec::new());
    }

    let mut entries = tokio::fs::read_dir(search_dir)
        .await
        .with_context(|| format!("failed to read raw spool directory {}", search_dir.display()))?;

    let mut parts = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        if !is_new_part_file_name(file_name) && !is_legacy_part_file_name(file_name, date_key) {
            continue;
        }

        let Some(part_index) = parse_parquet_part_index(file_name, date_key) else {
            continue;
        };

        let relative_file_name = relative_path_from_spool_root(spool_root, path.as_path())?;

        parts.push(RawParquetPartEntry {
            part_index,
            file_name: relative_file_name,
            row_count: 0,
            uploaded: false,
        });
    }

    parts.sort_by_key(|entry| entry.part_index);
    Ok(parts)
}

pub fn build_manifest_from_parts(date_key: &str, mut parts: Vec<RawParquetPartEntry>) -> RawParquetManifest {
    parts.sort_by_key(|entry| entry.part_index);
    let next_part_index = parts
        .iter()
        .map(|entry| entry.part_index)
        .max()
        .map(|max_part| max_part.saturating_add(1))
        .unwrap_or(FIRST_PARQUET_PART_INDEX);

    RawParquetManifest::new(date_key.to_string(), next_part_index, parts)
}

async fn reconcile_manifest_with_disk_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    mut manifest: RawParquetManifest,
) -> Result<RawParquetManifest> {
    let discovered_parts = list_raw_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
    if discovered_parts.is_empty() {
        return Ok(manifest);
    }

    let mut known_files = manifest
        .parts
        .iter()
        .map(|part| part.file_name.clone())
        .collect::<HashSet<_>>();
    let mut orphaned_count = 0_usize;

    for discovered_part in discovered_parts {
        if known_files.contains(discovered_part.file_name.as_str()) {
            continue;
        }

        known_files.insert(discovered_part.file_name.clone());
        manifest.parts.push(discovered_part);
        orphaned_count = orphaned_count.saturating_add(1);
    }

    if orphaned_count == 0 {
        return Ok(manifest);
    }

    manifest.parts.sort_by_key(|part| part.part_index);
    manifest.next_part_index = manifest
        .parts
        .iter()
        .map(|part| part.part_index)
        .max()
        .map(|max_part| max_part.saturating_add(1))
        .unwrap_or(FIRST_PARQUET_PART_INDEX);

    warn!(
        date_key,
        exchange,
        symbol,
        orphaned_count,
        "detected orphaned raw bookTicker parquet parts; appending to manifest upload queue"
    );

    save_raw_parquet_manifest(spool_dir, exchange, symbol, &manifest).await?;
    Ok(manifest)
}

async fn cleanup_temp_part_files(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<()> {
    let date_dir = partition_date_dir(spool_dir, exchange, symbol, date_key);

    cleanup_temp_part_files_in_directory(date_dir.as_path(), date_key).await?;

    cleanup_temp_part_files_in_directory(spool_dir, date_key).await?;
    Ok(())
}

async fn cleanup_temp_part_files_in_directory(search_dir: &Path, date_key: &str) -> Result<()> {
    if tokio::fs::metadata(search_dir).await.is_err() {
        return Ok(());
    }

    let mut entries = tokio::fs::read_dir(search_dir)
        .await
        .with_context(|| format!("failed to read raw spool directory {}", search_dir.display()))?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        let is_new = file_name.starts_with("bookticker_")
            && file_name.contains("_part-")
            && file_name.ends_with(".tmp.parquet");
        let is_legacy = file_name
            .starts_with(format!("bookticker-{date_key}-part-").as_str())
            && file_name.ends_with(".tmp.parquet");

        if !is_new && !is_legacy {
            continue;
        }

        tokio::fs::remove_file(&path)
            .await
            .with_context(|| format!("failed to cleanup stale temp parquet {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parquet_part_file_name, parse_parquet_part_index};

    #[test]
    fn part_file_name_embeds_exchange_symbol_and_timestamp() {
        let file_name = parquet_part_file_name("binance", "btcusd_perp", 1_763_000_000_000, 7);
        assert_eq!(
            file_name,
            "bookticker_1763000000000_binance_btcusd_perp_part-000007.parquet"
        );
    }

    #[test]
    fn parse_part_index_accepts_new_and_legacy_file_names() {
        let new_name = "bookticker_1763000000000_binance_btcusd_perp_part-000042.parquet";
        let legacy_name = "bookticker-2026-04-14-part-000042.parquet";

        assert_eq!(parse_parquet_part_index(new_name, "2026-04-14"), Some(42));
        assert_eq!(parse_parquet_part_index(legacy_name, "2026-04-14"), Some(42));
    }
}
