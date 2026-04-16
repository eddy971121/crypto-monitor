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
    AggTradeEvent, BookTickerEvent, DepthEvent, DepthSnapshot, NormalizedAggTrade,
    NormalizedBookTicker, NormalizedDepthUpdate,
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

#[derive(Debug, Deserialize)]
struct BinanceDepthUpdate {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "T")]
    transaction_time_ms: Option<i64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "ps")]
    pair: Option<String>,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    prev_final_update_id: u64,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "T")]
    transaction_time_ms: Option<i64>,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "ps")]
    pair: Option<String>,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "B")]
    best_bid_qty: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "A")]
    best_ask_qty: String,
}

#[derive(Debug, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "ps")]
    pair: Option<String>,
    #[serde(rename = "a")]
    aggregate_trade_id: u64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
    #[serde(rename = "f")]
    first_trade_id: u64,
    #[serde(rename = "l")]
    last_trade_id: u64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
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

fn normalize_depth_update(raw: BinanceDepthUpdate) -> NormalizedDepthUpdate {
    NormalizedDepthUpdate {
        event_type: raw.event_type,
        event_time_ms: raw.event_time_ms,
        transaction_time_ms: raw.transaction_time_ms,
        symbol: raw.symbol,
        pair: raw.pair,
        first_update_id: raw.first_update_id,
        final_update_id: raw.final_update_id,
        prev_final_update_id: raw.prev_final_update_id,
        bids: raw.bids,
        asks: raw.asks,
    }
}

fn normalize_book_ticker(raw: BinanceBookTicker) -> NormalizedBookTicker {
    NormalizedBookTicker {
        event_type: raw.event_type,
        update_id: raw.update_id,
        event_time_ms: raw.event_time_ms,
        transaction_time_ms: raw.transaction_time_ms,
        symbol: raw.symbol,
        pair: raw.pair,
        best_bid_price: raw.best_bid_price,
        best_bid_qty: raw.best_bid_qty,
        best_ask_price: raw.best_ask_price,
        best_ask_qty: raw.best_ask_qty,
    }
}

fn normalize_agg_trade(raw: BinanceAggTrade) -> NormalizedAggTrade {
    NormalizedAggTrade {
        event_type: raw.event_type,
        event_time_ms: raw.event_time_ms,
        trade_time_ms: raw.trade_time_ms,
        symbol: raw.symbol,
        pair: raw.pair,
        aggregate_trade_id: raw.aggregate_trade_id,
        price: raw.price,
        quantity: raw.quantity,
        first_trade_id: raw.first_trade_id,
        last_trade_id: raw.last_trade_id,
        buyer_is_maker: raw.buyer_is_maker,
    }
}

async fn forward_depth_event(data: &serde_json::Value, tx: &mpsc::Sender<DepthEvent>) -> Result<()> {
    let raw_payload: BinanceDepthUpdate = serde_json::from_value(data.clone())
        .context("failed to decode depth update payload")?;
    let payload = normalize_depth_update(raw_payload);

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
    let raw_payload: BinanceBookTicker =
        serde_json::from_value(data.clone()).context("failed to decode bookTicker payload")?;
    let payload = normalize_book_ticker(raw_payload);

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
    let raw_payload: BinanceAggTrade =
        serde_json::from_value(data.clone()).context("failed to decode aggTrade payload")?;
    let payload = normalize_agg_trade(raw_payload);

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
    let rest_base = config.rest_base_url.trim_end_matches('/');
    let depth_path = config.depth_snapshot_path.trim_start_matches('/');
    let endpoint = format!(
        "{}/{}?symbol={}&limit={}",
        rest_base, depth_path, config.symbol, config.depth_limit
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        normalize_agg_trade, normalize_book_ticker, normalize_depth_update, BinanceAggTrade,
        BinanceBookTicker, BinanceDepthUpdate,
    };

    #[test]
    fn normalizes_coin_m_wire_payloads() {
        let depth_raw: BinanceDepthUpdate = serde_json::from_value(json!({
            "e": "depthUpdate",
            "E": 1_710_000_000_000i64,
            "T": 1_710_000_000_001i64,
            "s": "BTCUSD_PERP",
            "ps": "BTCUSD",
            "U": 100u64,
            "u": 101u64,
            "pu": 99u64,
            "b": [["64000.1", "5"], ["64000.0", "2"]],
            "a": [["64000.2", "4"]]
        }))
        .expect("coin-m depth payload should decode");
        let depth = normalize_depth_update(depth_raw);
        assert_eq!(depth.symbol, "BTCUSD_PERP");
        assert_eq!(depth.pair.as_deref(), Some("BTCUSD"));
        assert_eq!(depth.transaction_time_ms, Some(1_710_000_000_001i64));
        assert_eq!(depth.first_update_id, 100u64);

        let ticker_raw: BinanceBookTicker = serde_json::from_value(json!({
            "e": "bookTicker",
            "u": 700u64,
            "E": 1_710_000_000_010i64,
            "T": 1_710_000_000_011i64,
            "s": "BTCUSD_PERP",
            "ps": "BTCUSD",
            "b": "64000.1",
            "B": "3",
            "a": "64000.2",
            "A": "6"
        }))
        .expect("coin-m bookTicker payload should decode");
        let ticker = normalize_book_ticker(ticker_raw);
        assert_eq!(ticker.symbol, "BTCUSD_PERP");
        assert_eq!(ticker.pair.as_deref(), Some("BTCUSD"));
        assert_eq!(ticker.best_bid_price, "64000.1");
        assert_eq!(ticker.best_ask_qty, "6");

        let trade_raw: BinanceAggTrade = serde_json::from_value(json!({
            "e": "aggTrade",
            "E": 1_710_000_000_020i64,
            "T": 1_710_000_000_021i64,
            "s": "BTCUSD_PERP",
            "ps": "BTCUSD",
            "a": 42u64,
            "p": "64000.15",
            "q": "1",
            "f": 900u64,
            "l": 905u64,
            "m": true
        }))
        .expect("coin-m aggTrade payload should decode");
        let trade = normalize_agg_trade(trade_raw);
        assert_eq!(trade.symbol, "BTCUSD_PERP");
        assert_eq!(trade.pair.as_deref(), Some("BTCUSD"));
        assert_eq!(trade.aggregate_trade_id, 42u64);
        assert!(trade.buyer_is_maker);
    }

    #[test]
    fn normalizes_usd_m_wire_payloads_without_optional_fields() {
        let depth_raw: BinanceDepthUpdate = serde_json::from_value(json!({
            "e": "depthUpdate",
            "E": 1_710_100_000_000i64,
            "s": "BTCUSDT",
            "U": 200u64,
            "u": 202u64,
            "pu": 199u64,
            "b": [["64010.0", "0.25"]],
            "a": [["64010.1", "0.50"]]
        }))
        .expect("usd-m depth payload should decode");
        let depth = normalize_depth_update(depth_raw);
        assert_eq!(depth.symbol, "BTCUSDT");
        assert_eq!(depth.pair, None);
        assert_eq!(depth.transaction_time_ms, None);

        let ticker_raw: BinanceBookTicker = serde_json::from_value(json!({
            "e": "bookTicker",
            "u": 1_500u64,
            "E": 1_710_100_000_010i64,
            "s": "BTCUSDT",
            "b": "64010.0",
            "B": "0.10",
            "a": "64010.1",
            "A": "0.20"
        }))
        .expect("usd-m bookTicker payload should decode");
        let ticker = normalize_book_ticker(ticker_raw);
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert_eq!(ticker.pair, None);
        assert_eq!(ticker.transaction_time_ms, None);

        let trade_raw: BinanceAggTrade = serde_json::from_value(json!({
            "e": "aggTrade",
            "E": 1_710_100_000_020i64,
            "T": 1_710_100_000_021i64,
            "s": "BTCUSDT",
            "a": 77u64,
            "p": "64010.05",
            "q": "0.05",
            "f": 2_100u64,
            "l": 2_101u64,
            "m": false
        }))
        .expect("usd-m aggTrade payload should decode");
        let trade = normalize_agg_trade(trade_raw);
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.pair, None);
        assert_eq!(trade.quantity, "0.05");
        assert!(!trade.buyer_is_maker);
    }
}
