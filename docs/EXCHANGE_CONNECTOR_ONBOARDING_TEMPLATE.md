# Exchange Connector Onboarding Template

## Goal
Add a new exchange connector by implementing the connector interface and adapter-specific normalization only, without changing core orderbook logic.

## Scope
Use this template when adding support for a new exchange (for example, OKX, Bybit, Deribit).

## Preconditions
- Confirm required stream types exist on the exchange: depth, bookTicker (or equivalent), aggTrade (or equivalent).
- Confirm snapshot REST endpoint supports bootstrap with update IDs.
- Confirm symbol naming and market segmentation (coin-margined vs linear) for target instruments.
- Confirm time fields and sequence fields needed for reorder and bridge validation.

## Implementation Checklist
- [ ] Add `src/connectors/<exchange>.rs` module.
- [ ] Implement exchange wire payload structs in adapter.
- [ ] Implement normalization from exchange payloads to internal normalized types:
  - [ ] `NormalizedDepthUpdate`
  - [ ] `NormalizedBookTicker`
  - [ ] `NormalizedAggTrade`
- [ ] Implement adapter stream collector function with parser error telemetry.
- [ ] Implement adapter snapshot fetch function and level parsing.
- [ ] Register connector in `src/connectors/mod.rs` exchange dispatch.
- [ ] Extend config validation to allow `<exchange>` and market/symbol matrix.
- [ ] Add adapter fixture tests for at least one instrument per supported market type.

## Adapter Contract
Your connector must satisfy the `MarketConnector` interface:
- `run_collector(...) -> Result<()>`
- `fetch_depth_snapshot(...) -> Result<DepthSnapshot>`

Core modules should continue to consume only normalized internal types and must not parse exchange wire fields directly.

## Required Validation
1. `cargo build`
2. `cargo test`
3. Run one profile per market type and verify:
   - websocket connects and receives depth/trade events
   - snapshot bootstrap and sequence bridge succeed
   - signal metrics emit with correct exchange/market identity
4. Verify raw spool and archive partitions include `exchange=<exchange>` and correct symbol/date layout.

## Exit Criteria
- New exchange works through connector registration only.
- No core logic changes are required outside config validation and connector dispatch.
- Adapter tests cover payload variants and optional field behavior.
