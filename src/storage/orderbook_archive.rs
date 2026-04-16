use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt16Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::storage::raw_spool::{
    RawParquetManifest, RawParquetPartEntry, build_manifest_from_parts, date_key_from_ts,
    partition_date_dir,
};
use crate::telemetry::TelemetryEvent;
use crate::types::{DepthEvent, OrderbookSnapshotEvent};

const DEPTH_DATASET_PREFIX: &str = "depth-delta";
const SNAPSHOT_DATASET_PREFIX: &str = "snapshot";

pub async fn run_orderbook_archive_spooler(
    config: AppConfig,
    mut depth_rx: mpsc::Receiver<DepthEvent>,
    mut snapshot_rx: mpsc::Receiver<OrderbookSnapshotEvent>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let mut depth_writer = DepthDeltaParquetWriter::new(
        config.raw_spool_dir.clone(),
        config.raw_parquet_chunk_rows,
        config.exchange.clone(),
        config.stream_symbol.clone(),
        config.exchange.clone(),
        config.market.clone(),
    );
    let mut snapshot_writer = SnapshotParquetWriter::new(
        config.raw_spool_dir.clone(),
        config.exchange.clone(),
        config.stream_symbol.clone(),
    );

    let mut depth_closed = false;
    let mut snapshot_closed = false;

    while !depth_closed || !snapshot_closed {
        tokio::select! {
            maybe_event = depth_rx.recv(), if !depth_closed => {
                match maybe_event {
                    Some(event) => {
                        if event.payload.symbol != config.symbol {
                            continue;
                        }

                        if let Err(error) = depth_writer.write_event(&event).await {
                            warn!(%error, "failed to write raw depth delta parquet rows");
                            let _ = telemetry_tx.send(TelemetryEvent::RawWriteError).await;
                        }
                    }
                    None => {
                        depth_closed = true;
                    }
                }
            }
            maybe_event = snapshot_rx.recv(), if !snapshot_closed => {
                match maybe_event {
                    Some(event) => {
                        if event.symbol != config.symbol {
                            continue;
                        }

                        if let Err(error) = snapshot_writer.write_event(&event).await {
                            warn!(%error, "failed to write raw orderbook snapshot parquet row");
                            let _ = telemetry_tx.send(TelemetryEvent::RawWriteError).await;
                        }
                    }
                    None => {
                        snapshot_closed = true;
                    }
                }
            }
        }
    }

    depth_writer.flush().await?;
    snapshot_writer.flush().await?;

    Ok(())
}

struct DepthDeltaParquetWriter {
    spool_dir: PathBuf,
    chunk_rows: usize,
    partition_exchange: String,
    partition_symbol: String,
    row_exchange: String,
    row_market: String,
    active_date: Option<String>,
    manifest: Option<RawParquetManifest>,
    recv_ts_ms: Vec<i64>,
    event_ts_ms: Vec<i64>,
    exchange: Vec<String>,
    market: Vec<String>,
    symbols: Vec<String>,
    is_bid: Vec<bool>,
    price: Vec<f64>,
    size: Vec<f64>,
    first_update_id: Vec<u64>,
    final_update_id: Vec<u64>,
    schema_versions: Vec<u16>,
}

impl DepthDeltaParquetWriter {
    fn new(
        spool_dir: PathBuf,
        chunk_rows: usize,
        partition_exchange: String,
        partition_symbol: String,
        row_exchange: String,
        row_market: String,
    ) -> Self {
        let capacity = chunk_rows.max(1);
        Self {
            spool_dir,
            chunk_rows: capacity,
            partition_exchange,
            partition_symbol,
            row_exchange,
            row_market,
            active_date: None,
            manifest: None,
            recv_ts_ms: Vec::with_capacity(capacity),
            event_ts_ms: Vec::with_capacity(capacity),
            exchange: Vec::with_capacity(capacity),
            market: Vec::with_capacity(capacity),
            symbols: Vec::with_capacity(capacity),
            is_bid: Vec::with_capacity(capacity),
            price: Vec::with_capacity(capacity),
            size: Vec::with_capacity(capacity),
            first_update_id: Vec::with_capacity(capacity),
            final_update_id: Vec::with_capacity(capacity),
            schema_versions: Vec::with_capacity(capacity),
        }
    }

    async fn write_event(&mut self, event: &DepthEvent) -> Result<()> {
        let date_key = date_key_from_ts(event.recv_ts_ms);
        self.rotate_if_needed(date_key.as_str()).await?;

        self.push_levels(event, true, &event.payload.bids);
        self.push_levels(event, false, &event.payload.asks);

        if self.recv_ts_ms.len() >= self.chunk_rows {
            self.flush_chunk().await?;
        }

        Ok(())
    }

    fn push_levels(&mut self, event: &DepthEvent, is_bid: bool, levels: &[[String; 2]]) {
        for level in levels {
            let price = match level[0].parse::<f64>() {
                Ok(value) if value > 0.0 => value,
                Ok(_) => continue,
                Err(error) => {
                    warn!(%error, value = level[0].as_str(), "invalid depth delta price");
                    continue;
                }
            };

            let size = match level[1].parse::<f64>() {
                Ok(value) if value >= 0.0 => value,
                Ok(_) => continue,
                Err(error) => {
                    warn!(%error, value = level[1].as_str(), "invalid depth delta size");
                    continue;
                }
            };

            self.recv_ts_ms.push(event.recv_ts_ms);
            self.event_ts_ms.push(event.payload.event_time_ms);
            self.exchange.push(self.row_exchange.clone());
            self.market.push(self.row_market.clone());
            self.symbols.push(event.payload.symbol.clone());
            self.is_bid.push(is_bid);
            self.price.push(price);
            self.size.push(size);
            self.first_update_id.push(event.payload.first_update_id);
            self.final_update_id.push(event.payload.final_update_id);
            self.schema_versions.push(1);
        }
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
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
        );

        tokio::fs::create_dir_all(&date_dir)
            .await
            .with_context(|| format!("failed to create {}", date_dir.display()))?;

        cleanup_temp_part_files_for_dataset(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            DEPTH_DATASET_PREFIX,
        )
        .await?;

        let manifest = match load_depth_delta_manifest(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                let parts = list_depth_delta_parquet_parts(
                    &self.spool_dir,
                    self.partition_exchange.as_str(),
                    self.partition_symbol.as_str(),
                    date_key,
                )
                .await?;
                let manifest = build_manifest_from_parts(date_key, parts);
                save_depth_delta_manifest(
                    &self.spool_dir,
                    self.partition_exchange.as_str(),
                    self.partition_symbol.as_str(),
                    &manifest,
                )
                .await?;
                manifest
            }
        };

        if manifest.date_key != date_key {
            return Err(anyhow::anyhow!(
                "depth delta manifest date mismatch: expected {date_key}, found {}",
                manifest.date_key
            ));
        }

        let manifest = reconcile_manifest_with_dataset_parts(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            DEPTH_DATASET_PREFIX,
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
            .ok_or_else(|| anyhow::anyhow!("depth delta spool date not initialized"))?;
        let manifest = self
            .manifest
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("depth delta manifest not initialized"))?;

        let part_index = manifest.next_part_index;
        let chunk_start_ts_ms = chunk_start_ts_ms(&self.event_ts_ms, &self.recv_ts_ms);
        let temp_path = depth_delta_parquet_part_temp_file_path(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );
        let final_path = depth_delta_parquet_part_file_path(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );

        let write_result = write_depth_delta_parquet_part_file(
            &temp_path,
            &self.recv_ts_ms,
            &self.event_ts_ms,
            &self.exchange,
            &self.market,
            &self.symbols,
            &self.is_bid,
            &self.price,
            &self.size,
            &self.first_update_id,
            &self.final_update_id,
            &self.schema_versions,
        );

        let row_count = match write_result {
            Ok(row_count) => row_count,
            Err(error) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error).with_context(|| {
                    format!("failed to write depth delta parquet temp file {}", temp_path.display())
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

        save_depth_delta_manifest(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            manifest,
        )
        .await?;

        info!(
            path = %final_path.display(),
            rows = row_count,
            "flushed raw depth delta parquet spool chunk"
        );

        self.clear_chunk_buffers();
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_chunk().await?;

        if let Some(manifest) = &self.manifest {
            save_depth_delta_manifest(
                &self.spool_dir,
                self.partition_exchange.as_str(),
                self.partition_symbol.as_str(),
                manifest,
            )
            .await?;
        }

        Ok(())
    }

    fn clear_chunk_buffers(&mut self) {
        self.recv_ts_ms.clear();
        self.event_ts_ms.clear();
        self.exchange.clear();
        self.market.clear();
        self.symbols.clear();
        self.is_bid.clear();
        self.price.clear();
        self.size.clear();
        self.first_update_id.clear();
        self.final_update_id.clear();
        self.schema_versions.clear();
    }
}

struct SnapshotParquetWriter {
    spool_dir: PathBuf,
    partition_exchange: String,
    partition_symbol: String,
    active_date: Option<String>,
    manifest: Option<RawParquetManifest>,
    snapshot_ts_ms: Vec<i64>,
    exchange: Vec<String>,
    market: Vec<String>,
    symbols: Vec<String>,
    last_update_id: Vec<u64>,
    snapshot_source: Vec<String>,
    bids_levels: Vec<Vec<(f64, f64)>>,
    asks_levels: Vec<Vec<(f64, f64)>>,
    schema_versions: Vec<u16>,
}

impl SnapshotParquetWriter {
    fn new(spool_dir: PathBuf, partition_exchange: String, partition_symbol: String) -> Self {
        Self {
            spool_dir,
            partition_exchange,
            partition_symbol,
            active_date: None,
            manifest: None,
            snapshot_ts_ms: Vec::with_capacity(1),
            exchange: Vec::with_capacity(1),
            market: Vec::with_capacity(1),
            symbols: Vec::with_capacity(1),
            last_update_id: Vec::with_capacity(1),
            snapshot_source: Vec::with_capacity(1),
            bids_levels: Vec::with_capacity(1),
            asks_levels: Vec::with_capacity(1),
            schema_versions: Vec::with_capacity(1),
        }
    }

    async fn write_event(&mut self, event: &OrderbookSnapshotEvent) -> Result<()> {
        let date_key = date_key_from_ts(event.snapshot_ts_ms);
        self.rotate_if_needed(date_key.as_str()).await?;

        self.snapshot_ts_ms.push(event.snapshot_ts_ms);
        self.exchange.push(event.exchange.clone());
        self.market.push(event.market.clone());
        self.symbols.push(event.symbol.clone());
        self.last_update_id.push(event.last_update_id);
        self.snapshot_source.push(event.snapshot_source.clone());
        self.bids_levels.push(event.bids.clone());
        self.asks_levels.push(event.asks.clone());
        self.schema_versions.push(event.schema_version);

        // Snapshots are infrequent; flush per-event for durability and simpler recovery.
        self.flush_chunk().await
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
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
        );

        tokio::fs::create_dir_all(&date_dir)
            .await
            .with_context(|| format!("failed to create {}", date_dir.display()))?;

        cleanup_temp_part_files_for_dataset(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            SNAPSHOT_DATASET_PREFIX,
        )
        .await?;

        let manifest = match load_snapshot_manifest(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                let parts = list_snapshot_parquet_parts(
                    &self.spool_dir,
                    self.partition_exchange.as_str(),
                    self.partition_symbol.as_str(),
                    date_key,
                )
                .await?;
                let manifest = build_manifest_from_parts(date_key, parts);
                save_snapshot_manifest(
                    &self.spool_dir,
                    self.partition_exchange.as_str(),
                    self.partition_symbol.as_str(),
                    &manifest,
                )
                .await?;
                manifest
            }
        };

        if manifest.date_key != date_key {
            return Err(anyhow::anyhow!(
                "snapshot manifest date mismatch: expected {date_key}, found {}",
                manifest.date_key
            ));
        }

        let manifest = reconcile_manifest_with_dataset_parts(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            SNAPSHOT_DATASET_PREFIX,
            manifest,
        )
        .await?;

        self.active_date = Some(date_key.to_string());
        self.manifest = Some(manifest);

        Ok(())
    }

    async fn flush_chunk(&mut self) -> Result<()> {
        if self.snapshot_ts_ms.is_empty() {
            return Ok(());
        }

        let date_key = self
            .active_date
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("snapshot spool date not initialized"))?;
        let manifest = self
            .manifest
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("snapshot manifest not initialized"))?;

        let part_index = manifest.next_part_index;
        let chunk_start_ts_ms = chunk_start_ts_ms(&self.snapshot_ts_ms, &self.snapshot_ts_ms);
        let temp_path = snapshot_parquet_part_temp_file_path(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );
        let final_path = snapshot_parquet_part_file_path(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            date_key,
            chunk_start_ts_ms,
            part_index,
        );

        let write_result = write_snapshot_parquet_part_file(
            &temp_path,
            &self.snapshot_ts_ms,
            &self.exchange,
            &self.market,
            &self.symbols,
            &self.last_update_id,
            &self.snapshot_source,
            &self.bids_levels,
            &self.asks_levels,
            &self.schema_versions,
        );

        let row_count = match write_result {
            Ok(row_count) => row_count,
            Err(error) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                return Err(error).with_context(|| {
                    format!("failed to write snapshot parquet temp file {}", temp_path.display())
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

        save_snapshot_manifest(
            &self.spool_dir,
            self.partition_exchange.as_str(),
            self.partition_symbol.as_str(),
            manifest,
        )
        .await?;

        info!(
            path = %final_path.display(),
            rows = row_count,
            "flushed raw orderbook snapshot parquet spool chunk"
        );

        self.clear_chunk_buffers();
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_chunk().await?;

        if let Some(manifest) = &self.manifest {
            save_snapshot_manifest(
                &self.spool_dir,
                self.partition_exchange.as_str(),
                self.partition_symbol.as_str(),
                manifest,
            )
            .await?;
        }

        Ok(())
    }

    fn clear_chunk_buffers(&mut self) {
        self.snapshot_ts_ms.clear();
        self.exchange.clear();
        self.market.clear();
        self.symbols.clear();
        self.last_update_id.clear();
        self.snapshot_source.clear();
        self.bids_levels.clear();
        self.asks_levels.clear();
        self.schema_versions.clear();
    }
}

fn write_depth_delta_parquet_part_file(
    output_path: &Path,
    recv_ts_ms: &[i64],
    event_ts_ms: &[i64],
    exchange: &[String],
    market: &[String],
    symbols: &[String],
    is_bid: &[bool],
    price: &[f64],
    size: &[f64],
    first_update_id: &[u64],
    final_update_id: &[u64],
    schema_versions: &[u16],
) -> Result<usize> {
    let row_count = recv_ts_ms.len();

    if row_count == 0 {
        return Ok(0);
    }

    if event_ts_ms.len() != row_count
        || exchange.len() != row_count
        || market.len() != row_count
        || symbols.len() != row_count
        || is_bid.len() != row_count
        || price.len() != row_count
        || size.len() != row_count
        || first_update_id.len() != row_count
        || final_update_id.len() != row_count
        || schema_versions.len() != row_count
    {
        return Err(anyhow::anyhow!(
            "depth delta parquet chunk columns have mismatched lengths"
        ));
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ms", DataType::Int64, false),
        Field::new("event_ts_ms", DataType::Int64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("is_bid", DataType::Boolean, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("first_update_id", DataType::UInt64, false),
        Field::new("final_update_id", DataType::UInt64, false),
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
            Arc::new(StringArray::from(exchange.to_vec())),
            Arc::new(StringArray::from(market.to_vec())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(BooleanArray::from(is_bid.to_vec())),
            Arc::new(Float64Array::from(price.to_vec())),
            Arc::new(Float64Array::from(size.to_vec())),
            Arc::new(UInt64Array::from(first_update_id.to_vec())),
            Arc::new(UInt64Array::from(final_update_id.to_vec())),
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

fn write_snapshot_parquet_part_file(
    output_path: &Path,
    snapshot_ts_ms: &[i64],
    exchange: &[String],
    market: &[String],
    symbols: &[String],
    last_update_id: &[u64],
    snapshot_source: &[String],
    bids_levels: &[Vec<(f64, f64)>],
    asks_levels: &[Vec<(f64, f64)>],
    schema_versions: &[u16],
) -> Result<usize> {
    let row_count = snapshot_ts_ms.len();

    if row_count == 0 {
        return Ok(0);
    }

    if exchange.len() != row_count
        || market.len() != row_count
        || symbols.len() != row_count
        || last_update_id.len() != row_count
        || snapshot_source.len() != row_count
        || bids_levels.len() != row_count
        || asks_levels.len() != row_count
        || schema_versions.len() != row_count
    {
        return Err(anyhow::anyhow!(
            "snapshot parquet chunk columns have mismatched lengths"
        ));
    }

    let list_item_field = Arc::new(Field::new("item", DataType::Float64, true));

    let schema = Arc::new(Schema::new(vec![
        Field::new("snapshot_ts_ms", DataType::Int64, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("last_update_id", DataType::UInt64, false),
        Field::new("snapshot_source", DataType::Utf8, false),
        Field::new("bid_prices", DataType::List(list_item_field.clone()), false),
        Field::new("bid_sizes", DataType::List(list_item_field.clone()), false),
        Field::new("ask_prices", DataType::List(list_item_field.clone()), false),
        Field::new("ask_sizes", DataType::List(list_item_field), false),
        Field::new("schema_version", DataType::UInt16, false),
    ]));

    let (bid_prices_rows, bid_sizes_rows) = split_price_size_levels(bids_levels);
    let (ask_prices_rows, ask_sizes_rows) = split_price_size_levels(asks_levels);
    let bid_prices_array = build_float64_list_array(&bid_prices_rows);
    let bid_sizes_array = build_float64_list_array(&bid_sizes_rows);
    let ask_prices_array = build_float64_list_array(&ask_prices_rows);
    let ask_sizes_array = build_float64_list_array(&ask_sizes_rows);

    let output_file = std::fs::File::create(output_path)
        .with_context(|| format!("failed to create parquet output {}", output_path.display()))?;
    let mut parquet_writer =
        ArrowWriter::try_new(output_file, schema.clone(), None).context("failed to open parquet writer")?;

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(snapshot_ts_ms.to_vec())),
            Arc::new(StringArray::from(exchange.to_vec())),
            Arc::new(StringArray::from(market.to_vec())),
            Arc::new(StringArray::from(symbols.to_vec())),
            Arc::new(UInt64Array::from(last_update_id.to_vec())),
            Arc::new(StringArray::from(snapshot_source.to_vec())),
            bid_prices_array,
            bid_sizes_array,
            ask_prices_array,
            ask_sizes_array,
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

fn split_price_size_levels(rows: &[Vec<(f64, f64)>]) -> (Vec<Vec<f64>>, Vec<Vec<f64>>) {
    let mut prices_rows = Vec::with_capacity(rows.len());
    let mut sizes_rows = Vec::with_capacity(rows.len());

    for levels in rows {
        let mut prices = Vec::with_capacity(levels.len());
        let mut sizes = Vec::with_capacity(levels.len());
        for (price, size) in levels {
            prices.push(*price);
            sizes.push(*size);
        }

        prices_rows.push(prices);
        sizes_rows.push(sizes);
    }

    (prices_rows, sizes_rows)
}

fn build_float64_list_array(rows: &[Vec<f64>]) -> ArrayRef {
    let mut list_builder = ListBuilder::new(Float64Builder::new());

    for values in rows {
        for value in values {
            list_builder.values().append_value(*value);
        }
        list_builder.append(true);
    }

    Arc::new(list_builder.finish())
}

fn chunk_start_ts_ms(primary_ts_ms: &[i64], fallback_ts_ms: &[i64]) -> i64 {
    primary_ts_ms
        .first()
        .copied()
        .or_else(|| fallback_ts_ms.first().copied())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
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

fn dataset_parquet_part_file_name(
    dataset_prefix: &str,
    exchange: &str,
    symbol: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> String {
    let exchange_key = normalize_partition_value(exchange);
    let symbol_key = normalize_partition_value(symbol);
    format!(
        "{dataset_prefix}_{chunk_start_ts_ms}_{exchange_key}_{symbol_key}_part-{part_index:06}.parquet"
    )
}

fn dataset_parquet_part_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key).join(dataset_parquet_part_file_name(
        dataset_prefix,
        exchange,
        symbol,
        chunk_start_ts_ms,
        part_index,
    ))
}

fn dataset_parquet_part_temp_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    let exchange_key = normalize_partition_value(exchange);
    let symbol_key = normalize_partition_value(symbol);
    let file_name = format!(
        "{dataset_prefix}_{chunk_start_ts_ms}_{exchange_key}_{symbol_key}_part-{part_index:06}.tmp.parquet"
    );
    partition_date_dir(spool_dir, exchange, symbol, date_key).join(file_name)
}

fn is_new_dataset_part_file_name(file_name: &str, dataset_prefix: &str) -> bool {
    file_name.starts_with(format!("{dataset_prefix}_").as_str())
        && file_name.contains("_part-")
        && file_name.ends_with(".parquet")
}

fn is_legacy_dataset_part_file_name(file_name: &str, dataset_prefix: &str, date_key: &str) -> bool {
    file_name.starts_with(format!("{dataset_prefix}-{date_key}-part-").as_str())
        && file_name.ends_with(".parquet")
}

fn parse_dataset_parquet_part_index(
    file_name: &str,
    dataset_prefix: &str,
    date_key: &str,
) -> Option<u32> {
    if is_new_dataset_part_file_name(file_name, dataset_prefix) {
        let stem = file_name.strip_suffix(".parquet")?;
        let (_, index_text) = stem.rsplit_once("_part-")?;
        return index_text.parse::<u32>().ok();
    }

    if is_legacy_dataset_part_file_name(file_name, dataset_prefix, date_key) {
        let prefix = format!("{dataset_prefix}-{date_key}-part-");
        let part_text = file_name.strip_prefix(prefix.as_str())?.strip_suffix(".parquet")?;
        return part_text.parse::<u32>().ok();
    }

    None
}

fn dataset_manifest_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    dataset_prefix: &str,
    date_key: &str,
) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key)
        .join(format!("{dataset_prefix}_manifest.json"))
}

fn dataset_manifest_temp_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    dataset_prefix: &str,
    date_key: &str,
) -> PathBuf {
    partition_date_dir(spool_dir, exchange, symbol, date_key)
        .join(format!("{dataset_prefix}_manifest.tmp.json"))
}

pub fn depth_delta_parquet_part_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    dataset_parquet_part_file_path(
        spool_dir,
        exchange,
        symbol,
        date_key,
        DEPTH_DATASET_PREFIX,
        chunk_start_ts_ms,
        part_index,
    )
}

fn depth_delta_parquet_part_temp_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    dataset_parquet_part_temp_file_path(
        spool_dir,
        exchange,
        symbol,
        date_key,
        DEPTH_DATASET_PREFIX,
        chunk_start_ts_ms,
        part_index,
    )
}

pub fn snapshot_parquet_part_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    dataset_parquet_part_file_path(
        spool_dir,
        exchange,
        symbol,
        date_key,
        SNAPSHOT_DATASET_PREFIX,
        chunk_start_ts_ms,
        part_index,
    )
}

fn snapshot_parquet_part_temp_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    chunk_start_ts_ms: i64,
    part_index: u32,
) -> PathBuf {
    dataset_parquet_part_temp_file_path(
        spool_dir,
        exchange,
        symbol,
        date_key,
        SNAPSHOT_DATASET_PREFIX,
        chunk_start_ts_ms,
        part_index,
    )
}

pub fn depth_delta_manifest_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> PathBuf {
    dataset_manifest_file_path(spool_dir, exchange, symbol, DEPTH_DATASET_PREFIX, date_key)
}

pub fn snapshot_manifest_file_path(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> PathBuf {
    dataset_manifest_file_path(spool_dir, exchange, symbol, SNAPSHOT_DATASET_PREFIX, date_key)
}

pub async fn load_depth_delta_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    load_manifest_for_dataset(spool_dir, exchange, symbol, date_key, DEPTH_DATASET_PREFIX).await
}

pub async fn load_snapshot_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    load_manifest_for_dataset(spool_dir, exchange, symbol, date_key, SNAPSHOT_DATASET_PREFIX)
        .await
}

pub async fn save_depth_delta_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    manifest: &RawParquetManifest,
) -> Result<()> {
    save_manifest_for_dataset(spool_dir, exchange, symbol, manifest, DEPTH_DATASET_PREFIX).await
}

pub async fn save_snapshot_manifest(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    manifest: &RawParquetManifest,
) -> Result<()> {
    save_manifest_for_dataset(spool_dir, exchange, symbol, manifest, SNAPSHOT_DATASET_PREFIX)
        .await
}

pub async fn list_depth_delta_parquet_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Vec<RawParquetPartEntry>> {
    list_dataset_parquet_parts(spool_dir, exchange, symbol, date_key, DEPTH_DATASET_PREFIX).await
}

pub async fn list_snapshot_parquet_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Vec<RawParquetPartEntry>> {
    list_dataset_parquet_parts(spool_dir, exchange, symbol, date_key, SNAPSHOT_DATASET_PREFIX)
        .await
}

async fn load_manifest_for_dataset(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
) -> Result<Option<RawParquetManifest>> {
    let path = dataset_manifest_file_path(spool_dir, exchange, symbol, dataset_prefix, date_key);
    let content = match tokio::fs::read_to_string(&path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to read {dataset_prefix} parquet manifest {}", path.display())
            });
        }
    };

    let manifest: RawParquetManifest = serde_json::from_str(content.as_str()).with_context(|| {
        format!("failed to parse {dataset_prefix} parquet manifest {}", path.display())
    })?;

    Ok(Some(manifest))
}

async fn save_manifest_for_dataset(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    manifest: &RawParquetManifest,
    dataset_prefix: &str,
) -> Result<()> {
    let path = dataset_manifest_file_path(
        spool_dir,
        exchange,
        symbol,
        dataset_prefix,
        manifest.date_key.as_str(),
    );
    let temp_path = dataset_manifest_temp_file_path(
        spool_dir,
        exchange,
        symbol,
        dataset_prefix,
        manifest.date_key.as_str(),
    );
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "failed to resolve {dataset_prefix} manifest parent directory for {}",
            path.display()
        )
    })?;

    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create {}", parent.display()))?;

    let content =
        serde_json::to_string_pretty(manifest).context("failed to serialize parquet manifest")?;

    tokio::fs::write(&temp_path, content)
        .await
        .with_context(|| {
            format!(
                "failed to write {dataset_prefix} parquet manifest temp file {}",
                temp_path.display()
            )
        })?;

    if tokio::fs::metadata(&path).await.is_ok() {
        tokio::fs::remove_file(&path).await.with_context(|| {
            format!(
                "failed to replace {dataset_prefix} parquet manifest {}",
                path.display()
            )
        })?;
    }

    tokio::fs::rename(&temp_path, &path).await.with_context(|| {
        format!(
            "failed to rename {dataset_prefix} parquet manifest {}",
            path.display()
        )
    })?;

    Ok(())
}

async fn list_dataset_parquet_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
) -> Result<Vec<RawParquetPartEntry>> {
    if tokio::fs::metadata(spool_dir).await.is_err() {
        return Ok(Vec::new());
    }

    let mut parts = Vec::new();
    let date_dir = partition_date_dir(spool_dir, exchange, symbol, date_key);
    parts.extend(
        list_dataset_parquet_parts_in_directory(spool_dir, date_dir.as_path(), dataset_prefix, date_key)
            .await?,
    );

    // Keep legacy flat-file discovery so historical days can still upload after migration.
    parts.extend(
        list_dataset_parquet_parts_in_directory(spool_dir, spool_dir, dataset_prefix, date_key)
            .await?
            .into_iter()
            .filter(|part| !part.file_name.contains('/')),
    );

    parts.sort_by_key(|entry| entry.part_index);
    parts.dedup_by(|left, right| left.file_name == right.file_name);
    Ok(parts)
}

async fn list_dataset_parquet_parts_in_directory(
    spool_root: &Path,
    search_dir: &Path,
    dataset_prefix: &str,
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

        if !is_new_dataset_part_file_name(file_name, dataset_prefix)
            && !is_legacy_dataset_part_file_name(file_name, dataset_prefix, date_key)
        {
            continue;
        }

        let Some(part_index) =
            parse_dataset_parquet_part_index(file_name, dataset_prefix, date_key)
        else {
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

async fn reconcile_manifest_with_dataset_parts(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
    mut manifest: RawParquetManifest,
) -> Result<RawParquetManifest> {
    let discovered_parts =
        list_dataset_parquet_parts(spool_dir, exchange, symbol, date_key, dataset_prefix).await?;
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
        .unwrap_or(1);

    warn!(
        dataset = dataset_prefix,
        exchange,
        symbol,
        date_key,
        orphaned_count,
        "detected orphaned parquet parts; appending to manifest upload queue"
    );

    save_manifest_for_dataset(spool_dir, exchange, symbol, &manifest, dataset_prefix).await?;
    Ok(manifest)
}

async fn cleanup_temp_part_files_for_dataset(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
    dataset_prefix: &str,
) -> Result<()> {
    let date_dir = partition_date_dir(spool_dir, exchange, symbol, date_key);

    cleanup_temp_part_files_for_dataset_in_directory(date_dir.as_path(), dataset_prefix, date_key)
        .await?;

    cleanup_temp_part_files_for_dataset_in_directory(spool_dir, dataset_prefix, date_key).await?;
    Ok(())
}

async fn cleanup_temp_part_files_for_dataset_in_directory(
    search_dir: &Path,
    dataset_prefix: &str,
    date_key: &str,
) -> Result<()> {
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

        let is_new = file_name.starts_with(format!("{dataset_prefix}_").as_str())
            && file_name.contains("_part-")
            && file_name.ends_with(".tmp.parquet");
        let is_legacy = file_name
            .starts_with(format!("{dataset_prefix}-{date_key}-part-").as_str())
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
    use std::fs::File;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::{
        dataset_parquet_part_file_name, parse_dataset_parquet_part_index,
        write_snapshot_parquet_part_file,
    };

    #[test]
    fn snapshot_parallel_array_schema_writes_parquet() {
        let mut output_path = std::env::temp_dir();
        output_path.push(format!(
            "snapshot-writer-test-{}-{}.parquet",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        ));

        let result = write_snapshot_parquet_part_file(
            output_path.as_path(),
            &[1_776_170_000_000],
            &["binance".to_string()],
            &["coin-m-futures".to_string()],
            &["BTCUSD_PERP".to_string()],
            &[123_456],
            &["rest_bootstrap".to_string()],
            &[vec![(100.0, 2.0), (99.5, 1.0)]],
            &[vec![(101.0, 3.0), (101.5, 2.0)]],
            &[1],
        );

        if let Err(error) = &result {
            panic!("snapshot parquet writer failed: {error:#}");
        }

        let file = File::open(output_path.as_path()).expect("failed to open snapshot parquet file");
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("failed to build parquet reader");
        let schema = builder.schema();
        let field_names = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();

        assert!(field_names.contains(&"bid_prices"));
        assert!(field_names.contains(&"bid_sizes"));
        assert!(field_names.contains(&"ask_prices"));
        assert!(field_names.contains(&"ask_sizes"));
        assert!(!field_names.contains(&"bids"));
        assert!(!field_names.contains(&"asks"));

        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn dataset_part_file_name_embeds_timestamp_exchange_and_symbol() {
        let file_name = dataset_parquet_part_file_name(
            "depth-delta",
            "binance",
            "btcusd_perp",
            1_763_000_000_000,
            11,
        );

        assert_eq!(
            file_name,
            "depth-delta_1763000000000_binance_btcusd_perp_part-000011.parquet"
        );
    }

    #[test]
    fn dataset_part_index_parser_accepts_new_and_legacy_names() {
        let new_name = "snapshot_1763000000000_binance_btcusd_perp_part-000005.parquet";
        let legacy_name = "snapshot-2026-04-14-part-000005.parquet";

        assert_eq!(
            parse_dataset_parquet_part_index(new_name, "snapshot", "2026-04-14"),
            Some(5)
        );
        assert_eq!(
            parse_dataset_parquet_part_index(legacy_name, "snapshot", "2026-04-14"),
            Some(5)
        );
    }
}
