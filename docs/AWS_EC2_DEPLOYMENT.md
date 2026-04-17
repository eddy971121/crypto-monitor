# AWS EC2 Deployment Guide

This guide operationalizes the EC2 hardening phases for `crypto-monitor`.

## Phase 0: Readiness Gates

Define explicit acceptance criteria before production cutover:

- reconnect MTTR target under transient network failures
- watchdog restart target for process hangs
- telemetry continuity target (no prolonged heartbeat gaps)
- soak gate pass criteria (`loss_rate_pass=true`, `p99_latency_pass=true`)

## Phase 1: EC2 and IAM Baseline

1. Launch EC2 with IMDSv2 required.
2. Attach an instance profile (IAM role) with least-privilege S3 access for `APP_S3_BUCKET`/`APP_S3_PREFIX`.
3. Ensure outbound network access to Binance WS/REST endpoints and S3.
4. Install and configure chrony against Amazon Time Sync Service:
   - source: `169.254.169.123`
   - verify with `chronyc sources -v`
5. Size disk for local spool retention (`APP_RAW_SPOOL_DIR`, `APP_METRICS_DIR`).

## Phase 2: systemd Supervision

Template unit file is provided at `deploy/systemd/crypto-monitor@.service`.

Fast-path bootstrap (installs unit, copies env files, enables+starts instances):

```bash
scripts/install_ec2_systemd.sh
```

Example enabling only selected profiles:

```bash
scripts/install_ec2_systemd.sh --profiles binance-usdm-btcusdt,binance-usdm-ethusdt
```

Install and start:

```bash
sudo install -D -m 0644 deploy/systemd/crypto-monitor@.service /etc/systemd/system/crypto-monitor@.service
sudo mkdir -p /etc/crypto-monitor /var/lib/crypto-monitor
sudo cp env/profiles/*.env /etc/crypto-monitor/
sudo systemctl daemon-reload
sudo systemctl enable --now crypto-monitor@binance-usdm-btcusdt
```

Manage per-symbol instances:

```bash
sudo systemctl status crypto-monitor@binance-usdm-btcusdt
sudo systemctl restart crypto-monitor@binance-usdm-btcusdt
sudo systemctl stop crypto-monitor@binance-usdm-btcusdt
```

Watchdog behavior:

- Unit sets `WatchdogSec=30s`.
- Service emits `READY=1` and periodic `WATCHDOG=1` when `APP_SYSTEMD_NOTIFY_ENABLED=true`.
- systemd restarts the service if watchdog pings stop.

## Phase 3: Connection Hardening

Use these runtime controls:

- `APP_WS_READ_IDLE_TIMEOUT_SECS`: dead-man timeout for WS frame silence
- `APP_WS_RECONNECT_BACKOFF_MAX_SECS`: reconnect backoff cap
- `APP_WS_RECONNECT_JITTER_BPS`: jitter in basis points (1000 = 10%)

Telemetry counters to monitor reconnect quality:

- `ws_connect_attempts`
- `ws_connected_sessions`
- `ws_connect_failures`
- `ws_read_errors`
- `ws_idle_timeouts`
- `ws_reconnect_scheduled`

## Phase 4: Logging and Alarms

Set `APP_LOG_FORMAT=json` for CloudWatch-friendly parsing.

Detailed AWS CLI metric filter/alarm recipes are in `docs/AWS_CLOUDWATCH_ALARMS.md`.

Recommended alarms:

- heartbeat absence (`service health heartbeat` missing for N intervals)
- reconnect storm (`ws_reconnect_scheduled` rate spike)
- upload failures (`s3_upload_failures` > 0)
- sustained SLO breaches (`loss_rate`, `p99_latency_ms`)

## Phase 5: Soak and Performance Validation

Before 24h soak, profile task scheduling under load (`tokio-console`) to confirm reader task is not starved.

Run soak gate:

```bash
scripts/run_soak_gate.sh --duration-minutes 60 --release
scripts/get_soak_status.sh --as-json
```

Increase soak to 24h once 60-minute stability is achieved.
