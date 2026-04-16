# Implementation Milestone and Checklist (Multi-Market + Connector Refactor)

Last updated (UTC): 2026-04-16

Use this document to track implementation progress for the recent COIN-M/USD-M expansion and connector normalization work.

## Status Legend
- [ ] Not started
- [x] Completed
- [~] In progress

## Milestone Overview
- [x] Phase 1 complete: Runtime market and instrument configuration
- [x] Phase 2 complete: Contract-semantic notional behavior in core metrics
- [x] Phase 3 complete: Dynamic identity propagation to storage and archival
- [x] Phase 4 complete: Four-profile operational launcher workflow
- [x] Phase 5 complete: Connector module boundary and Binance wire normalization
- [x] Phase 6 complete: Connector trait abstraction and alias cleanup
- [~] Phase 7 in progress: Multi-process soak and ops hardening

## Phase 1: Runtime Market and Instrument Configuration
Milestone:
- Service selects exchange/market/symbol/contract semantics at runtime rather than hardcoding Binance COIN-M BTC.

Checklist:
- [x] Add `APP_EXCHANGE`, `APP_MARKET`, `APP_MARKET_NAME`, `APP_CONTRACT_TYPE`, `APP_CONTRACT_SIZE`, and `APP_DEPTH_SNAPSHOT_PATH` support in config.
- [x] Support Binance COIN-M symbols: `BTCUSD_PERP`, `ETHUSD_PERP`.
- [x] Support Binance USD-M symbols: `BTCUSDT`, `ETHUSDT`.
- [x] Add market-aware defaults for websocket base URL, REST base URL, and depth snapshot endpoint path.
- [x] Add validation for unsupported exchange/market/symbol combinations.

Exit criteria:
- [x] Runtime configuration can represent all four target instruments.
- [x] Invalid market/symbol combinations fail fast with clear config errors.

## Phase 2: Contract-Semantic Notional Behavior
Milestone:
- Core notional math reflects instrument contract type (`Inverse` vs `Linear`) instead of fixed COIN-M assumptions.

Checklist:
- [x] Introduce `ContractType` with quote/base conversion helpers.
- [x] Remove hardcoded COIN-M notional constant from orderbook notional calculations.
- [x] Wire contract type through orderbook initialization path.
- [x] Add test coverage for linear notional behavior.

Exit criteria:
- [x] Bid/ask notional band calculations are contract-aware.
- [x] Unit tests cover both inverse and linear cases.

## Phase 3: Dynamic Identity in Metrics and Storage
Milestone:
- Output identity (`exchange`, `market`, symbol partitioning) is dynamic and config-driven.

Checklist:
- [x] Replace hardcoded `binance` and `coin-m-futures` metric identity fields with config-driven values.
- [x] Replace hardcoded event ID prefix with dynamic identity formatter.
- [x] Use dynamic exchange partition in raw spool paths/manifests.
- [x] Use dynamic exchange partition in orderbook archive paths/manifests.
- [x] Use dynamic exchange partition in S3 upload object keys.
- [x] Update runbook partitioning examples to use `<APP_EXCHANGE>` placeholders.

Exit criteria:
- [x] Archive and S3 layouts no longer assume a fixed exchange string.
- [x] Signal metric identity reflects runtime exchange/market values.

## Phase 4: Multi-Profile Operational Workflow
Milestone:
- Operators can run and control all four target instrument processes with one command path.

Checklist:
- [x] Add env profile files for each target instrument.
- [x] Add startup script to launch all profile instances and persist PID/log metadata.
- [x] Add status script to inspect active run state.
- [x] Add stop script to terminate latest or specific run labels.
- [x] Add runbook section documenting multi-profile startup/status/stop flow.

Exit criteria:
- [x] Four profile files exist and are executable through scripts.
- [x] Scripts pass shell syntax checks.

## Phase 5: Connector Boundary and Binance Wire Normalization
Milestone:
- Binance wire formats are isolated inside a connector adapter and translated to normalized internal events before entering core processing.

Checklist:
- [x] Define normalized internal event models in core types.
- [x] Add Binance-specific raw wire structs in connector adapter.
- [x] Add explicit translation functions from Binance wire events to normalized events.
- [x] Move Binance implementation under `src/connectors/binance.rs`.
- [x] Add connector dispatch entrypoints under `src/connectors/mod.rs`.
- [x] Update pipeline/orderbook call sites to use connector dispatch APIs.
- [x] Remove root-level Binance implementation dependency from runtime wiring.

Exit criteria:
- [x] Core event consumers no longer deserialize Binance wire fields directly.
- [x] Connector namespace is the primary ingestion/snapshot boundary.

## Phase 6: Connector Trait Abstraction and Alias Cleanup
Milestone:
- Prepare for additional exchanges with a stable connector interface and explicit normalized domain types.

Checklist:
- [x] Introduce a connector trait/interface for stream collection and snapshot fetch.
- [x] Replace temporary type aliases with explicit normalized event names across the core pipeline.
- [x] Add adapter-level fixture tests for Binance COIN-M and USD-M payload variants.
- [x] Define onboarding template for adding a second exchange connector.

Exit criteria:
- [x] Adding a new exchange requires implementing connector interface only.
- [x] Core modules no longer reference exchange-specific adapter types.

## Phase 7: Multi-Process Soak and Operational Hardening
Milestone:
- Verify that four concurrent instrument instances meet reliability and latency expectations in operational conditions.

Checklist:
- [~] Run 4-process soak with profile launcher scripts (Linux soak gate script added; 5-minute release run label `20260416-071121` captured telemetry across all four profiles; full-duration soak window still pending).
- [~] Validate loss-rate and p99 latency targets under concurrent load (release soak captured stable telemetry but still fails current SLO thresholds).
- [x] Verify S3 archival integrity for all four symbols and both market types (local/manifests check passed for run label `20260416-064044`; remote S3 upload verification passed for run label `20260416-064452`).
- [x] Capture runbook notes for failure recovery in multi-process mode.

Exit criteria:
- [ ] Soak pass report confirms no sustained SLO regression.
- [x] Operational runbook covers common multi-process failure scenarios.

## Progress Notes
- Date (UTC): 2026-04-16
- Current phase: Phase 7
- Build status: `cargo build` passed
- Test status: `cargo test` passed (38 passed, 0 failed)
- Latest soak run: `scripts/run_soak_gate.sh --duration-minutes 5 --release` with run label `20260416-071121`; gate report captured 120 telemetry lines and exited with code `2` because SLO checks failed (`loss_rate_pass=false`, `p99_latency_pass=false`, observed loss_rate ~0.00035-0.00038 and p99_latency_ms ~312-384).
- Latest archive check: local integrity `scripts/verify_archive_integrity_4pairs.sh --date 2026-04-16` (run label `20260416-064044`) and remote upload verification `scripts/verify_s3_upload_4pairs.sh --date 2026-04-16 --release` (run label `20260416-064452`) both reported `overall_pass=true`.
- Blockers: Phase 7 SLO targets are not yet met under concurrent load (especially p99 latency versus current <=60ms threshold).
- Next action: run longer release-mode soak (`scripts/run_soak_gate.sh --duration-minutes 60 --release` minimum), then tune data path and/or reassess threshold targets with recorded evidence.
- Owner: engineering
