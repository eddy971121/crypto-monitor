# Migration From JSONL Metrics To ClickHouse

## Purpose
Move metrics persistence from local JSONL files to ClickHouse while preserving runtime signal behavior and low-latency ingest path.

## Preconditions
- ClickHouse server reachable from local runtime.
- Application environment set with:
  - APP_METRICS_BACKEND=clickhouse
  - APP_CLICKHOUSE_URL
  - APP_CLICKHOUSE_DATABASE
  - APP_CLICKHOUSE_TABLE
  - APP_CLICKHOUSE_BATCH_SIZE
  - APP_CLICKHOUSE_FLUSH_MS

## Current Runtime Guarantees
- Metrics writer runs off the low-latency path in a dedicated async task.
- Writes are batched by size and flush interval.
- ClickHouse inserts use binary row encoding via the Rust clickhouse client.
- Event-level dedup cache avoids duplicate inserts during process lifetime.
- Table engine uses ReplacingMergeTree(recv_ts_ms) with ORDER BY (symbol, event_id).
- Startup preflight validates schema contract from system.columns and fails fast on missing/type-mismatched columns.
- A read-safe dedup view <db>.signal_metrics_vw is created for consumer queries.

## Migration Steps
1. Keep APP_METRICS_BACKEND=jsonl and run service baseline for at least 1 hour.
2. Confirm ingest and telemetry are stable in logs.
3. Start local ClickHouse and validate connectivity to APP_CLICKHOUSE_URL.
4. Switch APP_METRICS_BACKEND to clickhouse.
5. Restart service and confirm auto schema bootstrap succeeds.
6. Verify rows are being inserted:
   - SELECT count() FROM <db>.<table>
   - SELECT symbol, max(event_ts_ms) FROM <db>.<table> GROUP BY symbol
7. Verify deduplicated read path:
  - SELECT count() FROM <db>.signal_metrics_vw
  - SELECT symbol, max(event_ts_ms) FROM <db>.signal_metrics_vw GROUP BY symbol
8. Monitor telemetry for metrics_write_errors and latency regressions.
9. Keep JSONL backend config available for rollback.

## Rollback
1. Set APP_METRICS_BACKEND=jsonl.
2. Restart service.
3. Confirm metrics files are being written under APP_METRICS_DIR.

## Notes
- Existing tables created before event_id schema changes may require migration.
- Safe path: create a new table name via APP_CLICKHOUSE_TABLE and cut over.
- Prefer querying signal_metrics_vw over the base table to avoid duplicate reads from asynchronous ReplacingMergeTree merges.
- Long-term retention in ClickHouse should be controlled with TTL policy in production.
