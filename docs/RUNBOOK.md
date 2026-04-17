# Operations Runbook

## Startup
1. Set environment variables (.env style) for symbol, stream toggles, storage, and optional ClickHouse/S3.
	- For CloudWatch parsing on EC2, set APP_LOG_FORMAT=json.
2. Run cargo check.
3. Start service with cargo run.
4. Confirm logs show:
- websocket connected
- orderbook snapshot bootstrap
- signal emitted
- runtime telemetry
- service health heartbeat

## Multi-Profile Startup (All Profiles)
Use the predefined profile files under env/profiles for:
- Binance COIN-M BTCUSD_PERP
- Binance COIN-M ETHUSD_PERP
- Binance USD-M BTCUSDT
- Binance USD-M ETHUSDT
- Binance SPOT BTCUSD
- Binance SPOT ETHUSD

Operational note:
- The scripts below discover all *.env profile files under env/profiles automatically.

Start all profiles:

```bash
scripts/start_4pairs.sh --release
```

Operational note:
- `start_4pairs.sh` performs a single upfront `cargo build` and then launches the compiled binary per profile. This avoids multi-process cargo lock contention during startup and improves soak readiness time.

Check running/stopped status:

```bash
scripts/status_4pairs.sh
```

Stop the latest run:

```bash
scripts/stop_4pairs.sh
```

Stop a specific run label:

```bash
scripts/stop_4pairs.sh --run-label 20260416-120000
```

## systemd Startup (EC2)
Use template unit `deploy/systemd/crypto-monitor@.service` to run one process per profile.

Fast-path helper:

```bash
scripts/install_ec2_systemd.sh
```

Install template and profile env files:

```bash
sudo install -D -m 0644 deploy/systemd/crypto-monitor@.service /etc/systemd/system/crypto-monitor@.service
sudo mkdir -p /etc/crypto-monitor /var/lib/crypto-monitor
sudo cp env/profiles/*.env /etc/crypto-monitor/
sudo systemctl daemon-reload
```

Start one profile instance (example):

```bash
sudo systemctl enable --now crypto-monitor@binance-usdm-btcusdt
```

Service controls:

```bash
sudo systemctl status crypto-monitor@binance-usdm-btcusdt
sudo systemctl restart crypto-monitor@binance-usdm-btcusdt
sudo systemctl stop crypto-monitor@binance-usdm-btcusdt
```

Watchdog notes:
- Unit uses `Type=notify` and `WatchdogSec=30s`.
- Runtime sends READY and WATCHDOG signals when APP_SYSTEMD_NOTIFY_ENABLED=true.
- If watchdog pings stop, systemd restarts the process.

## Shutdown
1. Send Ctrl+C.
2. Confirm graceful shutdown log line appears.

## Connection Liveness Controls
Tune websocket dead-man and reconnect behavior with:
- APP_WS_READ_IDLE_TIMEOUT_SECS: reconnect if no websocket frame is received within this timeout.
- APP_WS_RECONNECT_BACKOFF_MAX_SECS: reconnect backoff cap.
- APP_WS_RECONNECT_JITTER_BPS: reconnect jitter in bps (1000 = +/-10%).

Watch runtime telemetry for:
- ws_connect_attempts
- ws_connected_sessions
- ws_connect_failures
- ws_read_errors
- ws_idle_timeouts
- ws_reconnect_scheduled

CloudWatch metric filter and alarm examples are documented in `docs/AWS_CLOUDWATCH_ALARMS.md`.

## Stale-State Interpretation
- stale_state=true means local orderbook is in degraded confidence mode.
- Expected causes: sequence gap, bridge miss, apply failure.
- Signals continue emitting with stale_state=true until resync is healthy.

## Recovery Procedure
1. Check telemetry logs for:
- sequence_gap_events
- resync_started
- resync_completed
- parser_errors
2. If resync does not complete:
- verify internet and Binance endpoint reachability
- verify APP_SYMBOL and stream settings
- restart service

## S3 Upload Failures
1. Check telemetry for s3_upload_failures.
2. Verify APP_S3_BUCKET and AWS credentials.
3. Verify local parquet files still exist under APP_RAW_SPOOL_DIR.
4. For immediate verification, run one-shot upload for a target date:

```powershell
cargo run -- upload-once --today
cargo run -- upload-once --yesterday
cargo run -- upload-once --date 2026-04-14
```

For all profile environments with report output:

```bash
scripts/verify_s3_upload_4pairs.sh --date 2026-04-16 --release
```

Outputs:
- logs/s3-upload-check/<run_label>/s3-upload-report.json
- logs/s3-upload-check/<run_label>/s3-upload-report.md
- logs/s3-upload-check/<run_label>/<profile>.log

Optional no-op acceptance mode (passes profiles even when no new uploads are needed):

```bash
scripts/verify_s3_upload_4pairs.sh --date 2026-04-16 --allow-noop
```

5. Otherwise, restart service or wait until next daily cycle.

## Raw Parquet Spool Recovery
Local spool layout under APP_RAW_SPOOL_DIR:
- APP_SYMBOL_LOWERCASE refers to the normalized stream symbol (for example, BTCUSD_PERP -> btcusd_perp).
- exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/bookticker_manifest.json
- exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/depth-delta_manifest.json
- exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/snapshot_manifest.json
- bookTicker parts: bookticker_<chunk_start_ts_ms>_<APP_EXCHANGE>_<APP_SYMBOL_LOWERCASE>_part-000001.parquet
- Depth delta parts: depth-delta_<chunk_start_ts_ms>_<APP_EXCHANGE>_<APP_SYMBOL_LOWERCASE>_part-000001.parquet
- Snapshot parts: snapshot_<chunk_start_ts_ms>_<APP_EXCHANGE>_<APP_SYMBOL_LOWERCASE>_part-000001.parquet

S3 object layout:
- <APP_S3_PREFIX>/exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<bookticker_part_file_name>
- <APP_S3_PREFIX>/exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<depth_delta_part_file_name>
- <APP_S3_PREFIX>/exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<snapshot_part_file_name>
- <APP_S3_PREFIX>/exchange=<APP_EXCHANGE>/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/metrics/metrics-YYYY-MM-DD.jsonl

Local metrics JSONL naming:
- New pattern: metrics-<APP_EXCHANGE>-<APP_MARKET>-<APP_SYMBOL_LOWERCASE>-YYYY-MM-DD.jsonl
- Legacy pattern: metrics-YYYY-MM-DD.jsonl (still accepted by uploader fallback)

Runtime behavior:
- Writers flush bounded in-memory chunks directly to parquet part temp files and atomically rename to final part names.
- Snapshot writer records REST bootstrap/resync snapshots and periodic local-memory keyframes.
- On date rotation/startup, spoolers sweep for orphaned finalized parquet parts that are missing from manifests and append them back into the upload queue.
- Uploader marks a part as uploaded in manifest before local deletion to support restart-safe progress.
- If manifest is missing but part files exist, uploader rebuilds manifest from discovered part files.
- If manifest exists but orphaned finalized parquet parts are discovered, uploader reconciles the manifest before upload.
- If manifest references a missing part file, uploader fails the daily cycle to avoid silent data loss.

Manual recovery procedure:
1. Stop service.
2. Inspect APP_RAW_SPOOL_DIR for the target date.
3. If part files exist but manifest is missing/corrupt, remove only the manifest file and restart service to trigger manifest rebuild.
4. If manifest references a non-existent part file, restore that part from backup or remove inconsistent manifest and re-spool for that date.
5. Resume service and monitor telemetry for s3_upload_successes and s3_upload_failures.

Archive integrity check across all profiles:

```bash
scripts/verify_archive_integrity_4pairs.sh --date 2026-04-16
```

Outputs:
- logs/archive-check/<run_label>/archive-integrity-report.json
- logs/archive-check/<run_label>/archive-integrity-report.md

Optional stricter check (requires APP_S3_BUCKET configured in effective profile env):

```bash
scripts/verify_archive_integrity_4pairs.sh --date 2026-04-16 --require-s3-config
```

Note:
- This script validates local partition/manifests/metrics integrity and S3 configuration presence.
- For remote object verification, run upload-once for the target date and confirm s3_upload_successes telemetry.

Chunk tuning:
- APP_RAW_PARQUET_CHUNK_ROWS controls rows per parquet part.
- Lower values reduce peak in-memory buffer usage but increase part/file counts and upload calls.
- Higher values reduce file churn but increase memory pressure per active day buffer.
- APP_EXECUTION_MATCH_LOOKAHEAD_MS adds a jitter-tolerant look-ahead window before final depth execution/cancel attribution.
- APP_TRADE_ALIGNMENT_MAX_LAG_MS bounds how long depth waits for aggTrade alignment before forced-open classification.
- APP_REORDER_RESYNC_MAX_LAG_MS is the hard reorder-buffer lag circuit breaker; exceeding it forces snapshot resync.
- APP_SNAPSHOT_DUMP_INTERVAL_SECS controls periodic local-memory snapshot keyframes (0 disables periodic keyframes).

## Metrics Backend Failures
### JSONL mode
- Check APP_METRICS_DIR write permissions.
- Check disk space.

### ClickHouse mode
- Check APP_CLICKHOUSE_URL reachability.
- Check database/table names.
- Check metrics_write_errors in telemetry.
- Startup runs a ClickHouse schema contract check against system.columns and fails fast on missing/type-mismatched fields.
- Query deduplicated metrics from <db>.signal_metrics_vw for dashboards and ad hoc analysis.
- Avoid SELECT ... FINAL on large windows; reserve FINAL for targeted small-scan diagnostics.
- If needed, rollback to APP_METRICS_BACKEND=jsonl.

## SLO Monitoring
Watch runtime telemetry logs for:
- p99_latency_ms (target < 60 ms)
- loss_rate (target <= 0.0001)

If threshold breach persists:
1. Check parser_errors and sequence_gap_events.
2. Check local resource pressure (CPU, disk, network).
3. Reduce optional load or increase channel capacity.

## Soak Validation Procedure
Run a full reliability gate with:

```bash
scripts/run_soak_gate.sh --duration-minutes 1440 --release
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_soak_gate.ps1 -DurationMinutes 1440 -Release
```

Outputs are written under logs/soak/<run_label>/:
- stdout.log
- stderr.log
- gate-report.json
- gate-report.md

Gate expectations:
- overall_pass=true in gate-report.json
- loss_rate_pass=true
- p99_latency_pass=true

Exit behavior:
- exit code 0 means gate pass
- exit code 2 means gate fail
- exit code 130 means user interrupted the run with Ctrl+C (report is still generated)

Manual stop:
- Press Ctrl+C in the soak script terminal.
- The script will stop the child cargo process, keep generated logs, and still write gate-report.json and gate-report.md.
- In reports, stop_reason will be ctrl_c_requested.

Alternative stop from another terminal:

```bash
scripts/stop_soak_run.sh
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/stop_soak_run.ps1
```

To stop a specific run label:

```bash
scripts/stop_soak_run.sh --run-label 20260413-193755
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/stop_soak_run.ps1 -RunLabel 20260413-193755
```

For a short dry-run before the full soak:

```bash
scripts/run_soak_gate.sh --duration-minutes 15
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_soak_gate.ps1 -DurationMinutes 15
```

Check live soak progress while a run is still active:

```bash
scripts/get_soak_status.sh
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1
```

To inspect a specific run label:

```bash
scripts/get_soak_status.sh --run-label 20260413-192655
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1 -RunLabel 20260413-192655
```

For structured output (includes state_cargo_pid and state_stop_reason):

```bash
scripts/get_soak_status.sh --run-label 20260413-193755 --as-json
```

or on Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1 -RunLabel 20260413-193755 -AsJson
```

## Multi-Process Failure Recovery
Common failure scenarios and actions during 4-profile runs:
- One profile exits while others remain running:
1. Check profile log under logs/multi/<run_label>/<profile>.log.
2. Stop all four with scripts/stop_4pairs.sh --run-label <run_label>.
3. Restart all four with scripts/start_4pairs.sh to preserve synchronized operational state.
- Repeated sequence_gap_events or resync loops on one symbol:
1. Confirm snapshot endpoint and stream endpoint match market type for that profile.
2. Verify APP_SYMBOL/APP_MARKET profile pairing and restart the full run.
3. If persistent, reduce load and capture logs for connector-level investigation.
- Gate report missing telemetry lines:
1. Confirm each profile log has runtime telemetry entries.
2. Ensure run duration is long enough to capture periodic telemetry windows.
3. Re-run soak gate and validate with scripts/get_soak_status.sh --as-json.
- Stop command does not fully terminate run:
1. Run scripts/stop_soak_run.sh for graceful + forced stop.
2. Verify scripts/status_4pairs.sh --run-label <multi_run_label> reports running=0.
3. If still active, capture PID list from logs/multi/<run_label>/pids.tsv and escalate.

## Handover Notes
- Raw archival includes bookTicker, accepted diff-depth deltas, snapshots, and metrics files.
- Depth and orderbook continuity are enforced by sequence rules.
- Metrics backend is config-switchable between jsonl and clickhouse.
