use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::clock;
use crate::connectors;
use crate::config::AppConfig;
use crate::health;
use crate::orderbook;
use crate::signal;
use crate::storage;
use crate::telemetry::{self, TelemetryEvent};
use crate::types::{
    AggTradeEvent, BookTickerEvent, DepthEvent, OrderbookSnapshotEvent, SignalMetric,
    UnifiedEvent,
};

pub async fn run(config: AppConfig) -> Result<()> {
    tokio::fs::create_dir_all(&config.raw_spool_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create raw spool directory {}",
                config.raw_spool_dir.display()
            )
        })?;

    tokio::fs::create_dir_all(&config.metrics_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create metrics directory {}",
                config.metrics_dir.display()
            )
        })?;

    storage::metrics_writer::ensure_clickhouse_preflight(&config)
        .await
        .context("clickhouse preflight failed")?;

    let (depth_tx, mut depth_rx) = mpsc::channel::<DepthEvent>(config.channel_capacity);
    let (book_ticker_tx, book_ticker_rx) = mpsc::channel::<BookTickerEvent>(config.channel_capacity);
    let (agg_trade_tx, mut agg_trade_rx) = mpsc::channel::<AggTradeEvent>(config.channel_capacity);
    let (unified_tx, unified_rx) = mpsc::channel::<UnifiedEvent>(config.channel_capacity);
    let (metrics_tx, metrics_rx) = mpsc::channel::<SignalMetric>(config.channel_capacity);
    let (raw_depth_tx, raw_depth_rx) = mpsc::channel::<DepthEvent>(config.channel_capacity);
    let (snapshot_tx, snapshot_rx) =
        mpsc::channel::<OrderbookSnapshotEvent>(config.channel_capacity);
    let (telemetry_tx, telemetry_rx) = mpsc::channel::<TelemetryEvent>(config.channel_capacity);
    let (signal_tx, mut signal_rx) = signal::channel(config.channel_capacity);

    let ntp_config = config.clone();
    tokio::spawn(async move {
        if let Err(error) = clock::run_ntp_slew(ntp_config).await {
            error!(%error, "ntp slew task exited");
        }
    });

    let health_config = config.clone();
    let health_task = tokio::spawn(async move { health::run_health_reporter(health_config).await });

    let telemetry_task = tokio::spawn(async move { telemetry::run_telemetry(telemetry_rx).await });

    let collector_config = config.clone();
    let collector_telemetry_tx = telemetry_tx.clone();
    let collector_task = tokio::spawn(async move {
        connectors::run_collector(
            collector_config,
            depth_tx,
            book_ticker_tx,
            agg_trade_tx,
            collector_telemetry_tx,
        )
        .await
    });

    let merge_task = tokio::spawn(async move {
        let mut depth_closed = false;
        let mut agg_trade_closed = false;

        loop {
            tokio::select! {
                event = depth_rx.recv(), if !depth_closed => {
                    match event {
                        Some(event) => {
                            if unified_tx.send(UnifiedEvent::Depth(event)).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            depth_closed = true;
                        }
                    }
                }
                event = agg_trade_rx.recv(), if !agg_trade_closed => {
                    match event {
                        Some(event) => {
                            if unified_tx.send(UnifiedEvent::AggTrade(event)).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            agg_trade_closed = true;
                        }
                    }
                }
            }

            if depth_closed && agg_trade_closed {
                break;
            }
        }

        Ok(())
    });

    let orderbook_config = config.clone();
    let orderbook_telemetry_tx = telemetry_tx.clone();
    let orderbook_task = tokio::spawn(async move {
        orderbook::run_engine(
            orderbook_config,
            unified_rx,
            metrics_tx,
            signal_tx,
            raw_depth_tx,
            snapshot_tx,
            orderbook_telemetry_tx,
        )
        .await
    });

    let metrics_config = config.clone();
    let metrics_telemetry_tx = telemetry_tx.clone();
    let metrics_task = tokio::spawn(async move {
        storage::metrics_writer::run_metrics_writer(metrics_config, metrics_rx, metrics_telemetry_tx)
            .await
    });

    let raw_config = config.clone();
    let raw_telemetry_tx = telemetry_tx.clone();
    let raw_task = tokio::spawn(async move {
        storage::raw_spool::run_raw_spooler(raw_config, book_ticker_rx, raw_telemetry_tx).await
    });

    let archive_config = config.clone();
    let archive_telemetry_tx = telemetry_tx.clone();
    let archive_task = tokio::spawn(async move {
        storage::orderbook_archive::run_orderbook_archive_spooler(
            archive_config,
            raw_depth_rx,
            snapshot_rx,
            archive_telemetry_tx,
        )
        .await
    });

    let uploader_config = config.clone();
    let uploader_telemetry_tx = telemetry_tx.clone();
    let uploader_task = tokio::spawn(async move {
        storage::s3_uploader::run_daily_maintenance(uploader_config, uploader_telemetry_tx).await
    });

    let signal_logger_task = tokio::spawn(async move {
        while let Ok(signal) = signal_rx.recv().await {
            info!(
                symbol = %signal.symbol,
                stale_state = signal.stale_state,
                best_bid = signal.best_bid,
                best_ask = signal.best_ask,
                spread_bps = signal.spread_bps,
                ingest_to_signal_ms = signal.ingest_to_signal_ms,
                "signal emitted"
            );
        }

        Ok(())
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutdown signal received");
        }
        join_result = collector_task => {
            flatten_join("collector", join_result)?;
            error!("collector task exited unexpectedly");
        }
        join_result = orderbook_task => {
            flatten_join("orderbook", join_result)?;
            error!("orderbook task exited unexpectedly");
        }
        join_result = merge_task => {
            flatten_join("stream merge", join_result)?;
            error!("stream merge task exited unexpectedly");
        }
        join_result = metrics_task => {
            flatten_join("metrics writer", join_result)?;
            error!("metrics writer task exited unexpectedly");
        }
        join_result = raw_task => {
            flatten_join("raw spooler", join_result)?;
            error!("raw spooler task exited unexpectedly");
        }
        join_result = archive_task => {
            flatten_join("orderbook archive spooler", join_result)?;
            error!("orderbook archive spooler task exited unexpectedly");
        }
        join_result = uploader_task => {
            flatten_join("uploader", join_result)?;
            error!("uploader task exited unexpectedly");
        }
        join_result = signal_logger_task => {
            flatten_join("signal logger", join_result)?;
            error!("signal logger task exited unexpectedly");
        }
        join_result = telemetry_task => {
            flatten_join("telemetry", join_result)?;
            error!("telemetry task exited unexpectedly");
        }
        join_result = health_task => {
            flatten_join("health", join_result)?;
            error!("health task exited unexpectedly");
        }
    }

    Ok(())
}

fn flatten_join(
    task_name: &str,
    join_result: Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match join_result {
        Ok(inner_result) => inner_result.with_context(|| format!("{} task failed", task_name)),
        Err(error) => Err(anyhow::anyhow!(error)).with_context(|| format!("{} task panicked", task_name)),
    }
}
