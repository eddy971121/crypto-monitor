use std::time::Duration;

use anyhow::Result;
use tracing::info;

use crate::config::AppConfig;

pub async fn run_health_reporter(config: AppConfig) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(config.health_report_interval_secs));

    loop {
        ticker.tick().await;

        info!(
            exchange = %config.exchange,
            market = %config.market,
            canonical_symbol = %config.canonical_symbol,
            exchange_symbol = %config.symbol,
            depth_stream_enabled = config.enable_depth_stream,
            book_ticker_stream_enabled = config.enable_book_ticker_stream,
            agg_trade_stream_enabled = config.enable_agg_trade_stream,
            cancel_heuristic = ?config.cancel_heuristic,
            metrics_backend = ?config.metrics_backend,
            "service health heartbeat"
        );
    }
}
