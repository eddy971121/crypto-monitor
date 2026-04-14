# Pair Onboarding Template

## Goal
Add a new Binance COIN-M pair using configuration and validation checks, without refactoring core runtime.

## Pre-Change Checklist
- Confirm exchange and market type are in scope.
- Confirm stream availability for depth and bookTicker.
- Confirm symbol naming conventions for canonical, exchange, and stream identifiers.
- Confirm storage partition requirements for S3 key layout.

## Configuration Template
Use this template when extending symbol mapping in config validation:

```text
Canonical Symbol: <EXAMPLE_PERP>
Exchange Symbol: <EXAMPLE_PERP>
Stream Symbol: <example_perp>
```

Environment variables remain unchanged unless you switch active symbol:

```text
APP_SYMBOL=<EXAMPLE_PERP>
APP_ENABLE_DEPTH_STREAM=true
APP_ENABLE_BOOK_TICKER_STREAM=true
```

## Required Code Touchpoints
1. Instrument mapping resolver in config validation.
2. Any symbol-based filtering in orderbook and raw spooler.
3. S3 object key path (symbol partition).

## Validation Steps
1. Run cargo check.
2. Run service with APP_SYMBOL set to target pair.
3. Verify websocket connection and event flow.
4. Verify snapshot bootstrap and sequence bridge.
5. Verify signal output includes target symbol.
6. Verify daily raw file name and S3 key partition use target symbol.

## Exit Criteria
- Service starts with new APP_SYMBOL without code refactor.
- Orderbook updates and signals flow for new symbol.
- Raw archival and metrics persistence show correct symbol partitioning.
