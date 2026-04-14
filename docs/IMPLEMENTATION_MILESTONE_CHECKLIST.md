# Implementation Milestone and Checklist

Last updated (UTC): 2026-04-13

Use this document to track implementation progress across all planned phases.

## Status Legend
- [ ] Not started
- [x] Completed

## Milestone Overview
- [ ] Phase 1 complete: AggTrade feed and event model
- [ ] Phase 2 complete: Unified chronological processor
- [ ] Phase 3 complete: Synthetic FIFO level state
- [ ] Phase 4 complete: Trade/depth matching logic
- [ ] Phase 5 complete: Metrics
- [ ] Phase 6 complete: Validation and hardening

## Phase 1: AggTrade Feed and Event Model
Milestone:
- AggTrade events flow end-to-end from Binance feed into internal processing.

Checklist:
- [x] Add AggTrade and AggTradeEvent types in src/types.rs.
- [x] Extend collector to subscribe and forward aggtrade events in src/binance.rs.
- [x] Add aggtrade channel wiring in src/pipeline.rs.

Exit criteria:
- [ ] AggTrade events are observed in runtime telemetry/logs.
- [ ] Build and smoke checks pass for the new event path.

## Phase 2: Unified Chronological Processor
Milestone:
- Depth and AggTrade are processed by one ordered engine loop.

Checklist:
- [x] Introduce a unified event enum (Depth, AggTrade) consumed by a single engine loop.
- [x] Add reorder buffer keyed by exchange timestamp with 10-20ms hold window.
- [x] Implement reorder buffer with BTreeMap<u64, UnifiedEvent> keyed by exchange timestamp.
- [x] Flush strictly using moving watermark_ts = max_received_ts - 20ms.
- [x] Add bounded-buffer safeguards so temporary stream stalls/drops cannot cause unbounded growth.
- [x] Emit telemetry for late/out-of-order corrections.

Exit criteria:
- [ ] Events are processed in chronological order within configured hold window.
- [ ] Late/out-of-order correction metrics are populated and visible.

## Phase 3: Synthetic FIFO Level State
Milestone:
- Per-level state uses FIFO queue chunks instead of a single quantity value, with bounded near-touch scope.

Checklist:
- [x] Replace per-level qty with total_qty.
- [x] Add queue: VecDeque<VolumeChunk { size, added_at_exchange_ts }>.
- [x] Add configurable cancellation heuristic enum in src/types.rs for A/B testing:

```rust
pub enum CancelHeuristic {
	Lifo,
	ProRata,
}
```

- [x] Default cancellation heuristic to LIFO (spoof-focused), while keeping ProRata available by config.
- [x] Document rationale: ProRata is a safer benign-flow assumption, but LIFO is the default to isolate adversarial wall-layering and pull behavior.
- [x] Implement queue delta handler: increase => push_back(new chunk).
- [x] Implement queue delta handler: execution decrease => consume from front.
- [x] Implement queue delta handler: cancel decrease => consume from back (LIFO default, configurable).
- [x] Restrict queue tracking to top 10-20 levels (or levels within 1%-2% of mid-price).
- [x] For deeper levels, fall back to legacy price->total_qty logic.

Exit criteria:
- [ ] Queue math is consistent for mixed increase/execution/cancel flows.
- [ ] Level state remains non-negative and internally consistent.
- [ ] Queue memory and p99 latency stay within target under depth-bound tracking.

## Phase 4: Trade/Depth Matching Logic
Milestone:
- Depth drops are attributed between executions and cancels using nearby aggtrades.

Checklist:
- [x] Maintain short-lived aggtrade buckets by (price, side, ts window).
- [x] Enforce max trade/cancel correlation window of 50-100ms for causal matching.
- [x] When depth drop occurs at level, consume matched executed volume first, remainder treated as cancel.
- [x] Support partial matches and multi-level impacts.

Exit criteria:
- [ ] Matching behavior works for single-level and multi-level depletion scenarios.
- [ ] Residual unmatched volume is classified as cancel.

## Phase 5: Metrics
Milestone:
- New microstructure and spoofing metrics are emitted and persisted.

Checklist:
- [x] Add VWAA metrics for L1 and Top5 (bid/ask and optionally imbalance).
- [x] Add CVD windows [5s, 1m, 5m] (optionally add tick window such as last 100 trades).
- [x] Use 5s (or micro tick window) specifically to correlate immediate opposite-side sweeping before wall cancellation.
- [x] Add spoof trigger outputs (flag + score + reason code).
- [x] Parameterize spoof thresholds from rolling stats (not static constants):
- [x] Minimum wall size > (mu + 2 * sigma) of resting volume at that level over last 60s, or >= 3x average BBO size.
- [x] Minimum cancellation ratio > 80% of the wall size.
- [x] Extend SignalMetric schema in src/types.rs.
- [x] Extend metric writers in src/storage/metrics_writer.rs.

Exit criteria:
- [ ] Metrics appear in output sinks with expected fields.
- [ ] Backward compatibility and parsing expectations are validated.

## Phase 6: Validation and Hardening
Milestone:
- Core behaviors are covered by deterministic tests and latency impact is acceptable.

Checklist:
- [x] Unit tests for queue math and age math.
- [x] Deterministic tests for out-of-order depth/trade arrival.
- [x] Scenario tests for spoof sequence trigger.
- [ ] Check p99 impact against existing latency target.

Exit criteria:
- [ ] Test suite passes consistently.
- [ ] p99 latency remains within target or approved threshold.

## Progress Notes
- Date (UTC): 2026-04-13
- Current phase: Phase 6
- Blockers: None
- Next action: run soak/p99 latency impact validation for hardening gate
- Active soak run: 20260413-230703 (release, 24h)
- Early telemetry snapshot: total_depth_events=181, loss_rate=0.00552, p99_latency_ms=136
- Owner: engineering
