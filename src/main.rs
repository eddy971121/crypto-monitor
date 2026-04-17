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

#[derive(Debug, Clone)]
struct CliArgs {
    command: Command,
    config_path: Option<String>,
}

#[derive(Debug, Clone, Copy)]
enum LogFormat {
    Compact,
    Json,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli_args = parse_cli_args()?;

    load_environment(cli_args.config_path.as_deref())?;

    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls ring crypto provider"))?;

    init_tracing()?;

    let config = config::AppConfig::from_env()?;

    match cli_args.command {
        Command::RunService => pipeline::run(config).await,
        Command::UploadOnce { target_date } => {
            storage::s3_uploader::run_upload_once(config, target_date).await
        }
    }
}

fn load_environment(config_path: Option<&str>) -> Result<()> {
    match config_path {
        Some(path) => {
            dotenvy::from_path(path)
                .with_context(|| format!("failed to load --config env file at path '{path}'"))?;
        }
        None => {
            // Load .env when present so local runs pick up configuration and AWS credentials.
            let _ = dotenvy::dotenv();
        }
    }

    Ok(())
}

fn init_tracing() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("crypto_monitor=info,orderbook=info,binance=info")
    });
    let log_format = parse_log_format(&std::env::var("APP_LOG_FORMAT").unwrap_or_else(|_| "compact".to_string()))?;

    match log_format {
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_target(false)
                .compact()
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_target(false)
                .json()
                .init();
        }
    }

    Ok(())
}

fn parse_log_format(value: &str) -> Result<LogFormat> {
    match value.trim().to_ascii_lowercase().as_str() {
        "compact" | "text" => Ok(LogFormat::Compact),
        "json" => Ok(LogFormat::Json),
        other => Err(anyhow::anyhow!(
            "invalid APP_LOG_FORMAT value: {other}; expected compact or json"
        )),
    }
}

fn parse_cli_args() -> Result<CliArgs> {
    let mut args = std::env::args().skip(1);
    let mut config_path = None;
    let mut command = Command::RunService;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--config" => {
                let value = args
                    .next()
                    .context("missing value for --config (expected path to env file)")?;
                config_path = Some(value);
            }
            "upload-once" => {
                let target_date = parse_upload_once_target_date(&mut args)?;
                command = Command::UploadOnce { target_date };
                break;
            }
            other => {
                return Err(anyhow::anyhow!(
                    "unknown command or option: {other}. supported: [--config PATH] and upload-once [--date YYYY-MM-DD | --today | --yesterday]"
                ));
            }
        }
    }

    Ok(CliArgs {
        command,
        config_path,
    })
}

fn parse_upload_once_target_date(args: &mut impl Iterator<Item = String>) -> Result<NaiveDate> {
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

    Ok(target_date)
}
