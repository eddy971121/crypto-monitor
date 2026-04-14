# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog.

## [Unreleased]

### Added
- Runtime scaffold for multi-task crypto monitor service.
- Binance COIN-M collector for depthUpdate (100ms) and bookTicker streams.
- REST snapshot bootstrap for orderbook initialization and resync.
- Local L2 orderbook engine with sequence continuity checks.
- Stale-state behavior and automatic resync on sequence gaps.
- In-process signal channel and signal logging.
- 5% per-side near-book metric computation.
- Ingest-to-signal latency measurement field in emitted metrics.
- Local metrics timeseries writer (daily JSONL files).
- Raw bookTicker spooler (daily JSONL files).
- Daily raw conversion pipeline from JSONL to Parquet.
- Daily S3 uploader with object size verification and post-upload local cleanup.
- Local retention pruning for metrics files.
- Environment template in .env.example.
- Planning document at docs/PLAN_AND_MILESTONES.md.
- Runtime telemetry aggregator for p99 ingest-to-signal latency and loss-rate monitoring.
- SLO warning logs for p99 latency and depth-gap loss-rate threshold breaches.
- Runtime counters for sequence gaps, resync cycles, parser errors, write errors, and S3 upload outcomes.
- Configurable metrics backend with jsonl and clickhouse modes.
- ClickHouse schema bootstrap and JSONEachRow insert path for signal metrics timeseries.
- Per-stream toggles for depth and bookTicker subscriptions.
- Startup canonical instrument validation for Binance COIN-M mapping.
- Health reporter module with periodic runtime heartbeat logs.
- Batched ClickHouse metric inserts with configurable batch size and flush interval.
- Event-level dedup cache and ReplacingMergeTree-based idempotency strategy for ClickHouse writes.
- Migration guide for switching metrics backend from JSONL to ClickHouse.
- Pair onboarding template for adding new symbols without core refactor.
- Operations runbook covering startup, stale-state, recovery, and upload/backend failures.
- Fault-injection unit tests for missed bridge, sequence gap, out-of-order, and crossed-book mismatch paths.
- Soak gate harness script at scripts/run_soak_gate.ps1 with JSON and Markdown pass/fail reports.
- M7 release gate checklist document at docs/RELEASE_GATE_CHECKLIST.md.
- Short soak dry-run validation of gate-report generation workflow.
- Soak gate script now supports Ctrl+C interruption with graceful child stop and report finalization.
- Added scripts/get_soak_status.ps1 for live soak telemetry and report-state inspection.
- get_soak_status now surfaces run-state metadata (cargo pid and running/stop reason).
- Soak runner now resolves the real rustup cargo binary to avoid detached shim process behavior.
- Soak runner now stops full process trees to avoid lingering cargo/crypto-monitor processes.
- Added scripts/stop_soak_run.ps1 to stop active soak runs by run label.
- Added in-process NTP slew correction for recv_ts_ms with configurable polling, smoothing, and bounded slew step.
- Added event-based L1 OFI and top-5 M-OFI metrics from local orderbook updates.
- Added depth feature gating against AggTrade watermark with configurable lag budget and forced-open quality flag.
- Added APP_TRADE_ALIGNMENT_MAX_LAG_MS runtime config with validation and a default of 250 ms.
- Added trade_alignment_forced_open to emitted SignalMetric payloads and ClickHouse schema.
- Added telemetry counter coverage for trade_alignment_forced_open fallback releases.
- Added one-shot uploader CLI command (`cargo run -- upload-once --today|--yesterday|--date YYYY-MM-DD`) for immediate archival verification.
- Added raw depth-delta parquet spooling for accepted, synchronized diff-depth updates.
- Added raw snapshot parquet spooling for REST bootstrap/resync snapshots and periodic local-memory keyframe snapshots.
- Added daily S3 archival of metrics JSONL files alongside raw parquet datasets.
- Added APP_SNAPSHOT_DUMP_INTERVAL_SECS configuration for periodic local orderbook keyframes.
- Added regression test coverage to assert snapshot parquet schema fields use parallel numeric arrays (`bid_prices`, `bid_sizes`, `ask_prices`, `ask_sizes`).

### Changed
- Upgraded runtime dependencies to support websocket collection, parquet conversion, and S3 upload.
- Startup now installs rustls ring CryptoProvider explicitly to avoid runtime TLS provider panics.
- Changed same-timestamp reorder behavior to process AggTrade before Depth events.
- Changed top-5 M-OFI computation to price-aligned matching to avoid index-shift phantom flow.
- Changed CVD window aggregation to rolling running sums for constant-time updates.
- Raw bookTicker spooling now writes chunked Parquet parts directly with atomic temp-file rename and manifest-based recovery.
- Daily uploader now also archives depth-delta parts, snapshot parts, and metrics files.
- Initial depth sync now drops stale updates when `u <= lastUpdateId` and bridges from `lastUpdateId + 1`.
- Raw and orderbook archival spool layout now uses hive-style partitioning by `exchange=<exchange>/symbol=<stream_symbol>/date=<YYYY-MM-DD>`.
- Raw/depth/snapshot part naming now includes chunk start timestamp plus exchange and symbol in the filename for better time-range targeting.
- Dataset manifests now live in partition directories and track part paths relative to raw spool root for restart-safe replay and upload continuity.
- Daily uploader now mirrors partitioned manifest-relative paths into S3 object keys, with legacy flat-name compatibility retained for historical files.
- Snapshot parquet payload schema now writes parallel level arrays (`bid_prices`, `bid_sizes`, `ask_prices`, `ask_sizes`) instead of nested level objects to prevent `[object Object]` corruption in downstream viewers.

### Notes
- Metrics persistence backend supports both JSONL and ClickHouse.
- M7 remains open until a full 24h soak report confirms p99 and loss-rate gates.
