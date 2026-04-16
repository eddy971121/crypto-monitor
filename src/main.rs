mod clock;
mod connectors;
mod config;
mod health;
mod orderbook;
mod pipeline;
mod signal;
mod storage;
mod telemetry;
mod types;

use anyhow::Result;
use anyhow::Context;
use chrono::{Duration as ChronoDuration, NaiveDate, Utc};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy)]
enum Command {
    RunService,
    UploadOnce { target_date: NaiveDate },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env when present so local runs pick up configuration and AWS credentials.
    let _ = dotenvy::dotenv();

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls ring crypto provider"))?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("crypto_monitor=info,orderbook=info,binance=info")
            }),
        )
        .with_target(false)
        .compact()
        .init();

    let command = parse_command()?;
    let config = config::AppConfig::from_env()?;

    match command {
        Command::RunService => pipeline::run(config).await,
        Command::UploadOnce { target_date } => {
            storage::s3_uploader::run_upload_once(config, target_date).await
        }
    }
}

fn parse_command() -> Result<Command> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        return Ok(Command::RunService);
    };

    if command != "upload-once" {
        return Err(anyhow::anyhow!(
            "unknown command: {command}. supported commands: upload-once [--date YYYY-MM-DD | --today | --yesterday]"
        ));
    }

    let mut target_date = Utc::now().date_naive();

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--today" => {
                target_date = Utc::now().date_naive();
            }
            "--yesterday" => {
                target_date = Utc::now().date_naive() - ChronoDuration::days(1);
            }
            "--date" => {
                let value = args
                    .next()
                    .context("missing value for --date (expected YYYY-MM-DD)")?;
                target_date = NaiveDate::parse_from_str(value.as_str(), "%Y-%m-%d")
                    .with_context(|| {
                        format!("invalid --date value '{value}'; expected YYYY-MM-DD")
                    })?;
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unknown argument for upload-once: {other}; supported: --date YYYY-MM-DD, --today, --yesterday"
                ));
            }
        }
    }

    Ok(Command::UploadOnce { target_date })
}
