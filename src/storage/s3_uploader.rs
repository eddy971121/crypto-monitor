use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arrow_array::{Int64Array, RecordBatch, StringArray, UInt16Array};
use arrow_schema::{DataType, Field, Schema};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Duration as ChronoDuration, NaiveDate, Timelike, Utc};
use parquet::arrow::ArrowWriter;
use tokio::fs;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::storage::metrics_writer::metrics_file_path;
use crate::storage::orderbook_archive::{
    depth_delta_manifest_file_path, list_depth_delta_parquet_parts, load_depth_delta_manifest,
    load_snapshot_manifest, save_depth_delta_manifest, save_snapshot_manifest,
    snapshot_manifest_file_path, list_snapshot_parquet_parts,
};
use crate::storage::raw_spool::{
    build_manifest_from_parts, list_raw_parquet_parts, load_raw_parquet_manifest,
    manifest_file_path, parquet_file_path, raw_file_path, save_raw_parquet_manifest,
    RawParquetManifest, RawParquetPartEntry,
};
use crate::telemetry::TelemetryEvent;
use crate::types::RawBookTickerRow;

const PARTITION_EXCHANGE: &str = "binance";

#[derive(Debug, Clone, Copy)]
struct PartUploadResult {
    found_parts: bool,
    uploaded_rows: usize,
}

pub async fn run_daily_maintenance(
    config: AppConfig,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    if let Err(error) = process_previous_day(&config, &telemetry_tx).await {
        warn!(%error, "initial raw upload job failed");
        let _ = telemetry_tx.send(TelemetryEvent::S3UploadFailure).await;
    }

    if let Err(error) = prune_metrics_files(&config).await {
        warn!(%error, "initial metrics retention prune failed");
    }

    loop {
        let sleep_duration = duration_until_next_utc_midnight();
        tokio::time::sleep(sleep_duration).await;

        if let Err(error) = process_previous_day(&config, &telemetry_tx).await {
            warn!(%error, "daily raw upload job failed");
            let _ = telemetry_tx.send(TelemetryEvent::S3UploadFailure).await;
        }

        if let Err(error) = prune_metrics_files(&config).await {
            warn!(%error, "metrics retention prune failed");
        }
    }
}

pub async fn run_upload_once(config: AppConfig, target_date: NaiveDate) -> Result<()> {
    let (telemetry_tx, _telemetry_rx) = mpsc::channel(1);
    process_for_date(&config, &telemetry_tx, target_date).await
}

async fn process_previous_day(
    config: &AppConfig,
    telemetry_tx: &mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let target_date = Utc::now().date_naive() - ChronoDuration::days(1);
    process_for_date(config, telemetry_tx, target_date).await
}

async fn process_for_date(
    config: &AppConfig,
    telemetry_tx: &mpsc::Sender<TelemetryEvent>,
    target_date: NaiveDate,
) -> Result<()> {
    let date_key = target_date.format("%Y-%m-%d").to_string();

    let mut uploaded_rows = 0_usize;

    let book_ticker_upload_result = process_previous_day_parquet_parts(config, date_key.as_str()).await?;
    uploaded_rows = uploaded_rows.saturating_add(book_ticker_upload_result.uploaded_rows);

    let depth_delta_upload_result = process_previous_day_depth_delta_parts(config, date_key.as_str()).await?;
    uploaded_rows = uploaded_rows.saturating_add(depth_delta_upload_result.uploaded_rows);

    let snapshot_upload_result = process_previous_day_snapshot_parts(config, date_key.as_str()).await?;
    uploaded_rows = uploaded_rows.saturating_add(snapshot_upload_result.uploaded_rows);

    if !book_ticker_upload_result.found_parts {
        uploaded_rows = uploaded_rows
            .saturating_add(process_previous_day_legacy_jsonl(config, date_key.as_str()).await?);
    }

    uploaded_rows = uploaded_rows.saturating_add(process_previous_day_metrics_file(config, date_key.as_str()).await?);

    if uploaded_rows > 0 {
        let _ = telemetry_tx
            .send(TelemetryEvent::S3UploadSuccess { rows: uploaded_rows })
            .await;
    }

    Ok(())
}

async fn process_previous_day_parquet_parts(
    config: &AppConfig,
    date_key: &str,
) -> Result<PartUploadResult> {
    let mut manifest =
        match load_or_rebuild_manifest_for_upload(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                return Ok(PartUploadResult {
                    found_parts: false,
                    uploaded_rows: 0,
                });
            }
        };

    if manifest.parts.is_empty() {
        return Ok(PartUploadResult {
            found_parts: false,
            uploaded_rows: 0,
        });
    }

    manifest.parts.sort_by_key(|part| part.part_index);

    let Some(bucket) = &config.s3_bucket else {
        warn!("APP_S3_BUCKET is not set; keeping local parquet spool parts until bucket is configured");
        return Ok(PartUploadResult {
            found_parts: true,
            uploaded_rows: 0,
        });
    };

    let mut uploaded_rows = 0_usize;

    for index in 0..manifest.parts.len() {
        let part = manifest.parts[index].clone();
        let part_path = config.raw_spool_dir.join(part.file_name.as_str());

        if part.uploaded {
            if path_exists(&part_path).await {
                if let Err(error) = fs::remove_file(&part_path).await {
                    warn!(%error, path = %part_path.display(), "failed to cleanup uploaded local parquet part");
                }
            }
            continue;
        }

        if !path_exists(&part_path).await {
            return Err(anyhow::anyhow!(
                "raw parquet manifest references missing part file {}",
                part_path.display()
            ));
        }

        let object_key = object_key_for_part(config, date_key, part.file_name.as_str());

        upload_and_verify(bucket, object_key.as_str(), &part_path).await?;

        manifest.parts[index].uploaded = true;
        save_raw_parquet_manifest(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            &manifest,
        )
        .await?;

        if let Err(error) = fs::remove_file(&part_path).await {
            warn!(%error, path = %part_path.display(), "uploaded parquet part but local cleanup failed");
        }

        uploaded_rows = uploaded_rows.saturating_add(part.row_count);

        info!(
            bucket,
            key = %object_key,
            rows = part.row_count,
            "uploaded raw bookTicker parquet part"
        );
    }

    if manifest.all_uploaded() {
        let manifest_path = manifest_file_path(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        );
        if path_exists(&manifest_path).await {
            fs::remove_file(&manifest_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to remove completed raw parquet manifest {}",
                        manifest_path.display()
                    )
                })?;
        }

        info!(date_key, "uploaded all raw bookTicker parquet parts for day");
    }

    Ok(PartUploadResult {
        found_parts: true,
        uploaded_rows,
    })
}

async fn process_previous_day_depth_delta_parts(
    config: &AppConfig,
    date_key: &str,
) -> Result<PartUploadResult> {
    let mut manifest =
        match load_or_rebuild_depth_delta_manifest_for_upload(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                return Ok(PartUploadResult {
                    found_parts: false,
                    uploaded_rows: 0,
                });
            }
        };

    if manifest.parts.is_empty() {
        return Ok(PartUploadResult {
            found_parts: false,
            uploaded_rows: 0,
        });
    }

    manifest.parts.sort_by_key(|part| part.part_index);

    let Some(bucket) = &config.s3_bucket else {
        warn!("APP_S3_BUCKET is not set; keeping local depth-delta parquet spool parts until bucket is configured");
        return Ok(PartUploadResult {
            found_parts: true,
            uploaded_rows: 0,
        });
    };

    let mut uploaded_rows = 0_usize;

    for index in 0..manifest.parts.len() {
        let part = manifest.parts[index].clone();
        let part_path = config.raw_spool_dir.join(part.file_name.as_str());

        if part.uploaded {
            if path_exists(&part_path).await {
                if let Err(error) = fs::remove_file(&part_path).await {
                    warn!(%error, path = %part_path.display(), "failed to cleanup uploaded local depth-delta parquet part");
                }
            }
            continue;
        }

        if !path_exists(&part_path).await {
            return Err(anyhow::anyhow!(
                "depth-delta parquet manifest references missing part file {}",
                part_path.display()
            ));
        }

        let object_key = object_key_for_part(config, date_key, part.file_name.as_str());

        upload_and_verify(bucket, object_key.as_str(), &part_path).await?;

        manifest.parts[index].uploaded = true;
        save_depth_delta_manifest(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            &manifest,
        )
        .await?;

        if let Err(error) = fs::remove_file(&part_path).await {
            warn!(%error, path = %part_path.display(), "uploaded depth-delta parquet part but local cleanup failed");
        }

        uploaded_rows = uploaded_rows.saturating_add(part.row_count);

        info!(
            bucket,
            key = %object_key,
            rows = part.row_count,
            "uploaded raw depth-delta parquet part"
        );
    }

    if manifest.all_uploaded() {
        let manifest_path = depth_delta_manifest_file_path(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        );
        if path_exists(&manifest_path).await {
            fs::remove_file(&manifest_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to remove completed depth-delta parquet manifest {}",
                        manifest_path.display()
                    )
                })?;
        }

        info!(date_key, "uploaded all raw depth-delta parquet parts for day");
    }

    Ok(PartUploadResult {
        found_parts: true,
        uploaded_rows,
    })
}

async fn process_previous_day_snapshot_parts(
    config: &AppConfig,
    date_key: &str,
) -> Result<PartUploadResult> {
    let mut manifest =
        match load_or_rebuild_snapshot_manifest_for_upload(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        )
        .await?
        {
            Some(manifest) => manifest,
            None => {
                return Ok(PartUploadResult {
                    found_parts: false,
                    uploaded_rows: 0,
                });
            }
        };

    if manifest.parts.is_empty() {
        return Ok(PartUploadResult {
            found_parts: false,
            uploaded_rows: 0,
        });
    }

    manifest.parts.sort_by_key(|part| part.part_index);

    let Some(bucket) = &config.s3_bucket else {
        warn!("APP_S3_BUCKET is not set; keeping local snapshot parquet spool parts until bucket is configured");
        return Ok(PartUploadResult {
            found_parts: true,
            uploaded_rows: 0,
        });
    };

    let mut uploaded_rows = 0_usize;

    for index in 0..manifest.parts.len() {
        let part = manifest.parts[index].clone();
        let part_path = config.raw_spool_dir.join(part.file_name.as_str());

        if part.uploaded {
            if path_exists(&part_path).await {
                if let Err(error) = fs::remove_file(&part_path).await {
                    warn!(%error, path = %part_path.display(), "failed to cleanup uploaded local snapshot parquet part");
                }
            }
            continue;
        }

        if !path_exists(&part_path).await {
            return Err(anyhow::anyhow!(
                "snapshot parquet manifest references missing part file {}",
                part_path.display()
            ));
        }

        let object_key = object_key_for_part(config, date_key, part.file_name.as_str());

        upload_and_verify(bucket, object_key.as_str(), &part_path).await?;

        manifest.parts[index].uploaded = true;
        save_snapshot_manifest(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            &manifest,
        )
        .await?;

        if let Err(error) = fs::remove_file(&part_path).await {
            warn!(%error, path = %part_path.display(), "uploaded snapshot parquet part but local cleanup failed");
        }

        uploaded_rows = uploaded_rows.saturating_add(part.row_count);

        info!(
            bucket,
            key = %object_key,
            rows = part.row_count,
            "uploaded raw snapshot parquet part"
        );
    }

    if manifest.all_uploaded() {
        let manifest_path = snapshot_manifest_file_path(
            &config.raw_spool_dir,
            PARTITION_EXCHANGE,
            config.stream_symbol.as_str(),
            date_key,
        );
        if path_exists(&manifest_path).await {
            fs::remove_file(&manifest_path)
                .await
                .with_context(|| {
                    format!(
                        "failed to remove completed snapshot parquet manifest {}",
                        manifest_path.display()
                    )
                })?;
        }

        info!(date_key, "uploaded all raw snapshot parquet parts for day");
    }

    Ok(PartUploadResult {
        found_parts: true,
        uploaded_rows,
    })
}

async fn load_or_rebuild_manifest_for_upload(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    if let Some(manifest) = load_raw_parquet_manifest(spool_dir, exchange, symbol, date_key).await? {
        let discovered_parts = list_raw_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
        let (manifest, orphaned_count) =
            reconcile_manifest_with_discovered_parts(manifest, discovered_parts);
        if orphaned_count > 0 {
            warn!(
                date_key,
                exchange,
                symbol,
                orphaned_count,
                "detected orphaned raw bookTicker parquet parts; appending to manifest upload queue"
            );
            save_raw_parquet_manifest(spool_dir, exchange, symbol, &manifest).await?;
        }

        return Ok(Some(manifest));
    }

    let parts = list_raw_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
    if parts.is_empty() {
        return Ok(None);
    }

    let manifest = build_manifest_from_parts(date_key, parts);
    save_raw_parquet_manifest(spool_dir, exchange, symbol, &manifest).await?;
    Ok(Some(manifest))
}

async fn load_or_rebuild_depth_delta_manifest_for_upload(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    if let Some(manifest) = load_depth_delta_manifest(spool_dir, exchange, symbol, date_key).await? {
        let discovered_parts = list_depth_delta_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
        let (manifest, orphaned_count) =
            reconcile_manifest_with_discovered_parts(manifest, discovered_parts);
        if orphaned_count > 0 {
            warn!(
                date_key,
                exchange,
                symbol,
                orphaned_count,
                "detected orphaned raw depth-delta parquet parts; appending to manifest upload queue"
            );
            save_depth_delta_manifest(spool_dir, exchange, symbol, &manifest).await?;
        }

        return Ok(Some(manifest));
    }

    let parts = list_depth_delta_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
    if parts.is_empty() {
        return Ok(None);
    }

    let manifest = build_manifest_from_parts(date_key, parts);
    save_depth_delta_manifest(spool_dir, exchange, symbol, &manifest).await?;
    Ok(Some(manifest))
}

async fn load_or_rebuild_snapshot_manifest_for_upload(
    spool_dir: &Path,
    exchange: &str,
    symbol: &str,
    date_key: &str,
) -> Result<Option<RawParquetManifest>> {
    if let Some(manifest) = load_snapshot_manifest(spool_dir, exchange, symbol, date_key).await? {
        let discovered_parts = list_snapshot_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
        let (manifest, orphaned_count) =
            reconcile_manifest_with_discovered_parts(manifest, discovered_parts);
        if orphaned_count > 0 {
            warn!(
                date_key,
                exchange,
                symbol,
                orphaned_count,
                "detected orphaned raw snapshot parquet parts; appending to manifest upload queue"
            );
            save_snapshot_manifest(spool_dir, exchange, symbol, &manifest).await?;
        }

        return Ok(Some(manifest));
    }

    let parts = list_snapshot_parquet_parts(spool_dir, exchange, symbol, date_key).await?;
    if parts.is_empty() {
        return Ok(None);
    }

    let manifest = build_manifest_from_parts(date_key, parts);
    save_snapshot_manifest(spool_dir, exchange, symbol, &manifest).await?;
    Ok(Some(manifest))
}

fn reconcile_manifest_with_discovered_parts(
    mut manifest: RawParquetManifest,
    discovered_parts: Vec<RawParquetPartEntry>,
) -> (RawParquetManifest, usize) {
    if discovered_parts.is_empty() {
        return (manifest, 0);
    }

    let mut orphaned_count = 0_usize;
    for discovered_part in discovered_parts {
        let already_present = manifest
            .parts
            .iter()
            .any(|part| part.file_name == discovered_part.file_name);
        if already_present {
            continue;
        }

        manifest.parts.push(discovered_part);
        orphaned_count = orphaned_count.saturating_add(1);
    }

    if orphaned_count == 0 {
        return (manifest, 0);
    }

    manifest.parts.sort_by_key(|part| part.part_index);
    manifest.next_part_index = manifest
        .parts
        .iter()
        .map(|part| part.part_index)
        .max()
        .map(|max_part| max_part.saturating_add(1))
        .unwrap_or(1);

    (manifest, orphaned_count)
}

fn object_key_for_part(config: &AppConfig, date_key: &str, file_name: &str) -> String {
    let relative = file_name.replace('\\', "/");
    if relative.contains('/') {
        return format!("{}/{}", config.s3_prefix, relative);
    }

    format!(
        "{}/exchange={}/symbol={}/date={}/{}",
        config.s3_prefix,
        PARTITION_EXCHANGE,
        config.stream_symbol,
        date_key,
        relative,
    )
}

async fn process_previous_day_metrics_file(config: &AppConfig, date_key: &str) -> Result<usize> {
    let metrics_path = metrics_file_path(&config.metrics_dir, date_key);
    if !path_exists(&metrics_path).await {
        return Ok(0);
    }

    let Some(bucket) = &config.s3_bucket else {
        warn!("APP_S3_BUCKET is not set; keeping local metrics file until bucket is configured");
        return Ok(0);
    };

    let object_key = format!(
        "{}/exchange={}/symbol={}/date={}/metrics/metrics-{}.jsonl",
        config.s3_prefix,
        PARTITION_EXCHANGE,
        config.stream_symbol,
        date_key,
        date_key,
    );

    upload_and_verify(bucket, object_key.as_str(), &metrics_path).await?;

    let row_count = count_file_lines(&metrics_path).await?;

    fs::remove_file(&metrics_path)
        .await
        .with_context(|| format!("failed to remove metrics file {}", metrics_path.display()))?;

    info!(
        bucket,
        key = %object_key,
        rows = row_count,
        "uploaded metrics jsonl file and removed local source"
    );

    Ok(row_count)
}

async fn process_previous_day_legacy_jsonl(
    config: &AppConfig,
    date_key: &str,
) -> Result<usize> {
    let raw_path = raw_file_path(
        &config.raw_spool_dir,
        PARTITION_EXCHANGE,
        config.stream_symbol.as_str(),
        date_key,
    );
    if !path_exists(&raw_path).await {
        return Ok(0);
    }

    let parquet_path = parquet_file_path(
        &config.raw_spool_dir,
        PARTITION_EXCHANGE,
        config.stream_symbol.as_str(),
        date_key,
    );

    let raw_path_for_blocking = raw_path.clone();
    let parquet_path_for_blocking = parquet_path.clone();
    let rows = tokio::task::spawn_blocking(move || {
        convert_raw_jsonl_to_parquet(&raw_path_for_blocking, &parquet_path_for_blocking)
    })
    .await
    .context("parquet conversion task panicked")??;

    if rows == 0 {
        warn!(path = %raw_path.display(), "raw file had zero rows; skipping upload");
    }

    let Some(bucket) = &config.s3_bucket else {
        warn!("APP_S3_BUCKET is not set; keeping local files until bucket is configured");
        return Ok(0);
    };

    let object_key = format!(
        "{}/exchange={}/symbol={}/date={}/bookticker.parquet",
        config.s3_prefix,
        PARTITION_EXCHANGE,
        config.stream_symbol,
        date_key,
    );

    upload_and_verify(bucket, object_key.as_str(), &parquet_path).await?;

    fs::remove_file(&raw_path)
        .await
        .with_context(|| format!("failed to remove raw file {}", raw_path.display()))?;
    fs::remove_file(&parquet_path)
        .await
        .with_context(|| format!("failed to remove parquet file {}", parquet_path.display()))?;

    info!(
        bucket,
        key = %object_key,
        rows,
        "uploaded legacy raw bookTicker parquet and removed local source files"
    );

    Ok(rows)
}

async fn upload_and_verify(bucket: &str, key: &str, parquet_path: &Path) -> Result<()> {
    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&aws_config);

    let body = ByteStream::from_path(parquet_path.to_path_buf())
        .await
        .with_context(|| format!("failed to stream parquet file {}", parquet_path.display()))?;

    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .context("S3 put_object failed")?;

    let local_size = fs::metadata(parquet_path)
        .await
        .with_context(|| format!("failed to read parquet metadata {}", parquet_path.display()))?
        .len();

    let head = s3_client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("S3 head_object verification failed")?;

    let uploaded_size = head.content_length.unwrap_or_default().max(0) as u64;
    if uploaded_size != local_size {
        return Err(anyhow::anyhow!(
            "uploaded object size mismatch: local={} uploaded={}",
            local_size,
            uploaded_size
        ));
    }

    Ok(())
}

async fn prune_metrics_files(config: &AppConfig) -> Result<()> {
    if !path_exists(&config.metrics_dir).await {
        return Ok(());
    }

    let cutoff_date = Utc::now().date_naive() - ChronoDuration::days(config.metrics_retention_days);
    let mut entries = fs::read_dir(&config.metrics_dir)
        .await
        .with_context(|| format!("failed to read metrics directory {}", config.metrics_dir.display()))?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };

        let Some(file_date) = parse_metrics_date(file_name) else {
            continue;
        };

        if file_date < cutoff_date {
            fs::remove_file(&path)
                .await
                .with_context(|| format!("failed to remove old metrics file {}", path.display()))?;
            info!(path = %path.display(), "removed metrics file due to retention policy");
        }
    }

    Ok(())
}

fn parse_metrics_date(file_name: &str) -> Option<NaiveDate> {
    if !file_name.starts_with("metrics-") || !file_name.ends_with(".jsonl") {
        return None;
    }

    let date_part = file_name.strip_prefix("metrics-")?.strip_suffix(".jsonl")?;

    NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
}

async fn count_file_lines(path: &Path) -> Result<usize> {
    let content = fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(content.lines().filter(|line| !line.trim().is_empty()).count())
}

async fn path_exists(path: &Path) -> bool {
    fs::metadata(path).await.is_ok()
}

fn duration_until_next_utc_midnight() -> Duration {
    let now = Utc::now();
    let seconds_today = i64::from(now.num_seconds_from_midnight());
    let seconds_until_midnight = 86_400_i64.saturating_sub(seconds_today);
    let nanos_until_next_second = 1_000_000_000_u32.saturating_sub(now.nanosecond());

    Duration::from_secs(seconds_until_midnight as u64)
        + Duration::from_nanos(u64::from(nanos_until_next_second))
}

fn convert_raw_jsonl_to_parquet(input_path: &Path, output_path: &Path) -> Result<usize> {
    let input_file = std::fs::File::open(input_path)
        .with_context(|| format!("failed to open raw input file {}", input_path.display()))?;
    let reader = BufReader::new(input_file);

    let mut recv_ts = Vec::new();
    let mut event_ts = Vec::new();
    let mut symbols = Vec::new();
    let mut payloads = Vec::new();
    let mut schema_versions = Vec::new();

    for (line_index, line_result) in reader.lines().enumerate() {
        let line = line_result.context("failed reading raw jsonl line")?;
        if line.trim().is_empty() {
            continue;
        }

        let rows = match parse_raw_rows_from_line(line.as_str()) {
            Ok(rows) => rows,
            Err(error) => {
                let line_preview: String = line.chars().take(160).collect();
                warn!(
                    line_number = line_index + 1,
                    preview = line_preview.as_str(),
                    %error,
                    "skipping malformed legacy raw jsonl line"
                );
                continue;
            }
        };

        for row in rows {
            recv_ts.push(row.recv_ts_ms);
            event_ts.push(row.event_ts_ms);
            symbols.push(row.symbol);
            payloads.push(row.raw_json);
            schema_versions.push(row.schema_version);
        }
    }

    let row_count = recv_ts.len();

    let schema = Arc::new(Schema::new(vec![
        Field::new("recv_ts_ms", DataType::Int64, false),
        Field::new("event_ts_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("raw_json", DataType::Utf8, false),
        Field::new("schema_version", DataType::UInt16, false),
    ]));

    let output_file = std::fs::File::create(output_path)
        .with_context(|| format!("failed to create parquet output {}", output_path.display()))?;
    let mut parquet_writer = ArrowWriter::try_new(output_file, schema.clone(), None)
        .context("failed to open parquet writer")?;

    if !recv_ts.is_empty() {
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(recv_ts)),
                Arc::new(Int64Array::from(event_ts)),
                Arc::new(StringArray::from(symbols)),
                Arc::new(StringArray::from(payloads)),
                Arc::new(UInt16Array::from(schema_versions)),
            ],
        )
        .context("failed to build arrow record batch")?;

        parquet_writer
            .write(&batch)
            .context("failed writing parquet batch")?;
    }

    parquet_writer
        .close()
        .context("failed to close parquet writer")?;

    Ok(row_count)
}

fn parse_raw_rows_from_line(line: &str) -> Result<Vec<RawBookTickerRow>> {
    let mut rows = Vec::new();
    let deserializer = serde_json::Deserializer::from_str(line);

    for parsed_row in deserializer.into_iter::<RawBookTickerRow>() {
        rows.push(parsed_row.context("failed to decode raw row")?);
    }

    if rows.is_empty() {
        return Err(anyhow::anyhow!("raw jsonl line did not contain a decodable row"));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::parse_raw_rows_from_line;

    #[test]
    fn parse_raw_rows_from_line_handles_concatenated_json_objects() {
        let line = concat!(
            r#"{"schema_version":1,"symbol":"BTCUSD_PERP","event_ts_ms":10,"recv_ts_ms":20,"raw_json":"{}"}"#,
            r#"{"schema_version":1,"symbol":"BTCUSD_PERP","event_ts_ms":11,"recv_ts_ms":21,"raw_json":"{}"}"#
        );

        let rows = parse_raw_rows_from_line(line).expect("failed to parse concatenated rows");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].event_ts_ms, 10);
        assert_eq!(rows[1].event_ts_ms, 11);
    }

    #[test]
    fn parse_raw_rows_from_line_rejects_non_json_content() {
        let error = parse_raw_rows_from_line("not-json").expect_err("expected parse failure");
        assert!(error.to_string().contains("failed to decode raw row"));
    }
}
