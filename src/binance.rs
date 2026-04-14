use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

use crate::clock::corrected_utc_ms;
use crate::config::AppConfig;
use crate::telemetry::TelemetryEvent;
use crate::types::{
    AggTrade, AggTradeEvent, BookTicker, BookTickerEvent, DepthEvent, DepthSnapshot, DepthUpdate,
};

#[derive(Debug, Deserialize)]
struct CombinedStreamEnvelope {
    data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct DepthSnapshotResponse {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    #[serde(rename = "bids")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "asks")]
    asks: Vec<[String; 2]>,
}

pub async fn run_collector(
    config: AppConfig,
    depth_tx: mpsc::Sender<DepthEvent>,
    book_ticker_tx: mpsc::Sender<BookTickerEvent>,
    agg_trade_tx: mpsc::Sender<AggTradeEvent>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let stream_symbol = config.symbol_stream_key();
    let stream_path = build_stream_path(
        stream_symbol.as_str(),
        config.enable_depth_stream,
        config.enable_book_ticker_stream,
        config.enable_agg_trade_stream,
    )?;
    let stream_url = format!(
        "{}/stream?streams={}",
        config.ws_base_url, stream_path
    );

    let mut reconnect_backoff = 1_u64;

    loop {
        info!(url = %stream_url, "connecting to Binance combined stream");
        match connect_async(stream_url.as_str()).await {
            Ok((ws_stream, _)) => {
                info!("websocket connected");
                reconnect_backoff = 1;

                let (_, mut read) = ws_stream.split();
                while let Some(message_result) = read.next().await {
                    let message = match message_result {
                        Ok(message) => message,
                        Err(error) => {
                            warn!(%error, "websocket read error; reconnecting");
                            break;
                        }
                    };

                    if !message.is_text() {
                        continue;
                    }

                    let text = match message.to_text() {
                        Ok(text) => text,
                        Err(error) => {
                            warn!(%error, "received non-utf8 websocket text frame");
                            continue;
                        }
                    };

                    let envelope: CombinedStreamEnvelope = match serde_json::from_str(text) {
                        Ok(value) => value,
                        Err(error) => {
                            warn!(%error, payload = text, "failed to parse websocket envelope");
                            send_telemetry(&telemetry_tx, TelemetryEvent::ParserError).await;
                            continue;
                        }
                    };

                    let Some(event_type) = envelope
                        .data
                        .get("e")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                    else {
                        continue;
                    };

                    if event_type == "depthUpdate" {
                        if let Err(error) = forward_depth_event(&envelope.data, &depth_tx).await {
                            warn!(%error, "failed to forward depth event");
                            send_telemetry(&telemetry_tx, TelemetryEvent::ParserError).await;
                        }
                        continue;
                    }

                    if event_type == "bookTicker" {
                        if let Err(error) =
                            forward_book_ticker_event(&envelope.data, &book_ticker_tx).await
                        {
                            warn!(%error, "failed to forward bookTicker event");
                            send_telemetry(&telemetry_tx, TelemetryEvent::ParserError).await;
                        }
                        continue;
                    }

                    if event_type == "aggTrade" {
                        if let Err(error) =
                            forward_agg_trade_event(&envelope.data, &agg_trade_tx).await
                        {
                            warn!(%error, "failed to forward aggTrade event");
                            send_telemetry(&telemetry_tx, TelemetryEvent::ParserError).await;
                        }
                    }
                }
            }
            Err(error) => {
                warn!(%error, "failed to connect websocket; retrying");
            }
        }

        let wait_secs = reconnect_backoff.min(30);
        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
        reconnect_backoff = reconnect_backoff.saturating_mul(2).min(30);
    }
}

fn build_stream_path(
    stream_symbol: &str,
    enable_depth_stream: bool,
    enable_book_ticker_stream: bool,
    enable_agg_trade_stream: bool,
) -> Result<String> {
    let mut parts = Vec::new();

    if enable_depth_stream {
        parts.push(format!("{}@depth@100ms", stream_symbol));
    }

    if enable_book_ticker_stream {
        parts.push(format!("{}@bookTicker", stream_symbol));
    }

    if enable_agg_trade_stream {
        parts.push(format!("{}@aggTrade", stream_symbol));
    }

    if parts.is_empty() {
        return Err(anyhow::anyhow!(
            "no Binance streams enabled; enable at least one stream"
        ));
    }

    Ok(parts.join("/"))
}

async fn send_telemetry(telemetry_tx: &mpsc::Sender<TelemetryEvent>, event: TelemetryEvent) {
    let _ = telemetry_tx.send(event).await;
}

fn aligned_recv_ts_ms(event_ts_ms: i64) -> i64 {
    corrected_utc_ms().max(event_ts_ms)
}

async fn forward_depth_event(data: &serde_json::Value, tx: &mpsc::Sender<DepthEvent>) -> Result<()> {
    let payload: DepthUpdate = serde_json::from_value(data.clone())
        .context("failed to decode depth update payload")?;

    if payload.event_type != "depthUpdate" {
        return Ok(());
    }

    let event = DepthEvent {
        recv_ts_ms: aligned_recv_ts_ms(payload.event_time_ms),
        payload,
        recv_instant: Instant::now(),
    };

    tx.send(event)
        .await
        .context("depth channel closed while sending event")
}

async fn forward_book_ticker_event(
    data: &serde_json::Value,
    tx: &mpsc::Sender<BookTickerEvent>,
) -> Result<()> {
    let payload: BookTicker =
        serde_json::from_value(data.clone()).context("failed to decode bookTicker payload")?;

    if payload.event_type != "bookTicker" {
        return Ok(());
    }

    let event = BookTickerEvent {
        recv_ts_ms: aligned_recv_ts_ms(payload.event_time_ms),
        payload,
        raw_json: data.to_string(),
    };

    tx.send(event)
        .await
        .context("bookTicker channel closed while sending event")
}

async fn forward_agg_trade_event(
    data: &serde_json::Value,
    tx: &mpsc::Sender<AggTradeEvent>,
) -> Result<()> {
    let payload: AggTrade =
        serde_json::from_value(data.clone()).context("failed to decode aggTrade payload")?;

    if payload.event_type != "aggTrade" {
        return Ok(());
    }

    let event = AggTradeEvent {
        recv_ts_ms: aligned_recv_ts_ms(payload.event_time_ms),
        payload,
        recv_instant: Instant::now(),
        raw_json: data.to_string(),
    };

    tx.send(event)
        .await
        .context("aggTrade channel closed while sending event")
}

pub async fn fetch_depth_snapshot(config: &AppConfig, http_client: &Client) -> Result<DepthSnapshot> {
    let endpoint = format!(
        "{}/dapi/v1/depth?symbol={}&limit={}",
        config.rest_base_url, config.symbol, config.depth_limit
    );

    let response = http_client
        .get(endpoint)
        .send()
        .await
        .context("snapshot request failed")?
        .error_for_status()
        .context("snapshot response was not successful")?
        .json::<DepthSnapshotResponse>()
        .await
        .context("failed to parse snapshot response")?;

    let bids = parse_levels(response.bids)?;
    let asks = parse_levels(response.asks)?;

    Ok(DepthSnapshot {
        symbol: config.symbol.clone(),
        last_update_id: response.last_update_id,
        bids,
        asks,
    })
}

fn parse_levels(levels: Vec<[String; 2]>) -> Result<Vec<(f64, f64)>> {
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let price: f64 = level[0]
            .parse()
            .with_context(|| format!("invalid price level value {}", level[0]))?;
        let qty: f64 = level[1]
            .parse()
            .with_context(|| format!("invalid quantity level value {}", level[1]))?;

        if price <= 0.0 || qty < 0.0 {
            error!(price, qty, "invalid snapshot level values");
            continue;
        }

        parsed.push((price, qty));
    }
    Ok(parsed)
}
