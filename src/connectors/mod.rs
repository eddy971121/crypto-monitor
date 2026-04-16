use anyhow::Result;
use futures_util::future::BoxFuture;
use reqwest::Client;
use tokio::sync::mpsc;

use crate::config::AppConfig;
use crate::telemetry::TelemetryEvent;
use crate::types::{AggTradeEvent, BookTickerEvent, DepthEvent, DepthSnapshot};

pub mod binance;

pub trait MarketConnector: Send + Sync {
	fn run_collector<'a>(
		&'a self,
		config: AppConfig,
		depth_tx: mpsc::Sender<DepthEvent>,
		book_ticker_tx: mpsc::Sender<BookTickerEvent>,
		agg_trade_tx: mpsc::Sender<AggTradeEvent>,
		telemetry_tx: mpsc::Sender<TelemetryEvent>,
	) -> BoxFuture<'a, Result<()>>;

	fn fetch_depth_snapshot<'a>(
		&'a self,
		config: &'a AppConfig,
		http_client: &'a Client,
	) -> BoxFuture<'a, Result<DepthSnapshot>>;
}

struct BinanceConnector;

impl MarketConnector for BinanceConnector {
	fn run_collector<'a>(
		&'a self,
		config: AppConfig,
		depth_tx: mpsc::Sender<DepthEvent>,
		book_ticker_tx: mpsc::Sender<BookTickerEvent>,
		agg_trade_tx: mpsc::Sender<AggTradeEvent>,
		telemetry_tx: mpsc::Sender<TelemetryEvent>,
	) -> BoxFuture<'a, Result<()>> {
		Box::pin(binance::run_collector(
			config,
			depth_tx,
			book_ticker_tx,
			agg_trade_tx,
			telemetry_tx,
		))
	}

	fn fetch_depth_snapshot<'a>(
		&'a self,
		config: &'a AppConfig,
		http_client: &'a Client,
	) -> BoxFuture<'a, Result<DepthSnapshot>> {
		Box::pin(binance::fetch_depth_snapshot(config, http_client))
	}
}

static BINANCE_CONNECTOR: BinanceConnector = BinanceConnector;

fn connector_for_exchange(exchange: &str) -> Result<&'static dyn MarketConnector> {
	match exchange {
		"binance" => Ok(&BINANCE_CONNECTOR),
		other => Err(anyhow::anyhow!(
			"unsupported connector exchange: {other}; currently supported: binance"
		)),
	}
}

pub async fn run_collector(
	config: AppConfig,
	depth_tx: mpsc::Sender<DepthEvent>,
	book_ticker_tx: mpsc::Sender<BookTickerEvent>,
	agg_trade_tx: mpsc::Sender<AggTradeEvent>,
	telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
	let connector = connector_for_exchange(config.exchange.as_str())?;
	connector
		.run_collector(config, depth_tx, book_ticker_tx, agg_trade_tx, telemetry_tx)
		.await
}

pub async fn fetch_depth_snapshot(config: &AppConfig, http_client: &Client) -> Result<DepthSnapshot> {
	let connector = connector_for_exchange(config.exchange.as_str())?;
	connector.fetch_depth_snapshot(config, http_client).await
}
