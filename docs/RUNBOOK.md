# Operations Runbook

## Startup
1. Set environment variables (.env style) for symbol, stream toggles, storage, and optional ClickHouse/S3.
2. Run cargo check.
3. Start service with cargo run.
4. Confirm logs show:
- websocket connected
- orderbook snapshot bootstrap
- signal emitted
- runtime telemetry
- service health heartbeat

## Shutdown
1. Send Ctrl+C.
2. Confirm graceful shutdown log line appears.

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

5. Otherwise, restart service or wait until next daily cycle.

## Raw Parquet Spool Recovery
Local spool layout under APP_RAW_SPOOL_DIR:
- APP_SYMBOL_LOWERCASE refers to the normalized stream symbol (for example, BTCUSD_PERP -> btcusd_perp).
- exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/bookticker_manifest.json
- exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/depth-delta_manifest.json
- exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/snapshot_manifest.json
- bookTicker parts: bookticker_<chunk_start_ts_ms>_binance_<APP_SYMBOL_LOWERCASE>_part-000001.parquet
- Depth delta parts: depth-delta_<chunk_start_ts_ms>_binance_<APP_SYMBOL_LOWERCASE>_part-000001.parquet
- Snapshot parts: snapshot_<chunk_start_ts_ms>_binance_<APP_SYMBOL_LOWERCASE>_part-000001.parquet

S3 object layout:
- <APP_S3_PREFIX>/exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<bookticker_part_file_name>
- <APP_S3_PREFIX>/exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<depth_delta_part_file_name>
- <APP_S3_PREFIX>/exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/<snapshot_part_file_name>
- <APP_S3_PREFIX>/exchange=binance/symbol=<APP_SYMBOL_LOWERCASE>/date=YYYY-MM-DD/metrics/metrics-YYYY-MM-DD.jsonl

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

```powershell
powershell -ExecutionPolicy Bypass -File scripts/stop_soak_run.ps1
```

To stop a specific run label:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/stop_soak_run.ps1 -RunLabel 20260413-193755
```

For a short dry-run before the full soak:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_soak_gate.ps1 -DurationMinutes 15
```

Check live soak progress while a run is still active:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1
```

To inspect a specific run label:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1 -RunLabel 20260413-192655
```

For structured output (includes state_cargo_pid and state_stop_reason):

```powershell
powershell -ExecutionPolicy Bypass -File scripts/get_soak_status.ps1 -RunLabel 20260413-193755 -AsJson
```

## Handover Notes
- Raw archival includes bookTicker, accepted diff-depth deltas, snapshots, and metrics files.
- Depth and orderbook continuity are enforced by sequence rules.
- Metrics backend is config-switchable between jsonl and clickhouse.
