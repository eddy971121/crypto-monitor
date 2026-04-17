# crypto-monitor

Low-latency market microstructure monitor for Binance spot and futures (COIN-M and USD-M) with:

- real-time depth, bookTicker, and aggTrade ingestion
- local orderbook reconstruction with sequence safety and resync
- signal metric emission (JSONL or ClickHouse)
- raw archival spooling to parquet parts (bookTicker, depth-delta, snapshot)
- daily S3 upload and retention maintenance

## Supported Runtime Profiles

Profile env files are provided in env/profiles for:

- Binance COIN-M BTCUSD_PERP
- Binance COIN-M ETHUSD_PERP
- Binance USD-M BTCUSDT
- Binance USD-M ETHUSDT
- Binance SPOT BTCUSD
- Binance SPOT ETHUSD

## Quick Start

1. Copy .env.example to .env and update values for your environment.
2. Build:

```bash
cargo build
```

3. Run a single instance:

```bash
cargo run
```

Run with an explicit env profile file:

```bash
cargo run -- --config env/profiles/binance-usdm-btcusdt.env
```

4. Run all predefined profiles from env/profiles:

```bash
scripts/start_4pairs.sh --release
```

Check status:

```bash
scripts/status_4pairs.sh
```

Stop latest run:

```bash
scripts/stop_4pairs.sh
```

## Upload Once

Run one-shot archival upload for a specific date:

```bash
cargo run -- upload-once --date 2026-04-16
```

Or use:

```bash
cargo run -- upload-once --today
cargo run -- upload-once --yesterday
```

## Operations Docs

- docs/RUNBOOK.md
- docs/AWS_EC2_DEPLOYMENT.md
- docs/AWS_CLOUDWATCH_ALARMS.md
- docs/MIGRATION_JSONL_TO_CLICKHOUSE.md
- docs/IMPLEMENTATION_MILESTONE_CHECKLIST_MULTI_MARKET.md
- docs/EXCHANGE_CONNECTOR_ONBOARDING_TEMPLATE.md
- docs/PAIR_ONBOARDING_TEMPLATE.md

## EC2 Bootstrap

Bootstrap systemd unit installation and profile enable/start on EC2:

```bash
scripts/install_ec2_systemd.sh
```

Select a subset of profiles:

```bash
scripts/install_ec2_systemd.sh --profiles binance-usdm-btcusdt,binance-usdm-ethusdt
```

## Notes

- Runtime credentials and environment variables are loaded from .env when present.
- Raw data and runtime logs are intentionally excluded from git tracking via .gitignore.
