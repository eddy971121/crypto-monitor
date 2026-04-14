use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ordered_float::OrderedFloat;
use reqwest::Client;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::binance::fetch_depth_snapshot;
use crate::config::AppConfig;
use crate::telemetry::TelemetryEvent;
use crate::types::{
    CancelHeuristic, DepthEvent, DepthSnapshot, DepthUpdate, OrderbookSnapshotEvent,
    SignalMetric, UnifiedEvent, VolumeChunk, now_utc_ms,
};

const OFI_PRICE_EPSILON: f64 = 1e-9;
const LEVEL_QTY_EPSILON: f64 = 1e-9;
const MOFI_SIGN_EPSILON: f64 = 1e-6;
const COIN_M_CONTRACT_NOTIONAL_USD: f64 = 100.0;
const MOFI_DEPTH_LEVELS: usize = 5;
const REORDER_HOLD_WINDOW_MS: i64 = 20;
const REORDER_MAX_BUFFERED_EVENTS: usize = 50_000;
const QUEUE_TRACK_LEVELS_PER_SIDE: usize = 20;
const QUEUE_TRACK_BAND_PCT: f64 = 0.02;
const AGGTRADE_MATCH_WINDOW_MS: i64 = 100;
const SPOOF_THRESHOLD_WINDOW_MS: i64 = 60_000;
const SPOOF_BBO_MULTIPLIER: f64 = 3.0;
const SPOOF_CANCEL_RATIO_THRESHOLD: f64 = 0.80;
const SPOOF_LEVEL_SAMPLE_DEPTH: usize = 50;
const SNAPSHOT_SOURCE_REST_BOOTSTRAP: &str = "rest_bootstrap";
const SNAPSHOT_SOURCE_REST_RESYNC: &str = "rest_resync";
const SNAPSHOT_SOURCE_LOCAL_KEYFRAME: &str = "local_keyframe";

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum QueueDecreaseKind {
    Execution,
    Cancel,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone)]
struct AggTradeBucket {
    window_start_ms: i64,
    remaining_qty: f64,
}

#[derive(Debug, Clone)]
struct AggTradeMatcher {
    window_ms: i64,
    buckets: BTreeMap<(BookSide, OrderedFloat<f64>), VecDeque<AggTradeBucket>>,
}

impl AggTradeMatcher {
    fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            buckets: BTreeMap::new(),
        }
    }

    fn record_trade(&mut self, side: BookSide, price: f64, qty: f64, event_ts_ms: i64) {
        if qty <= LEVEL_QTY_EPSILON || price <= 0.0 {
            return;
        }

        self.prune_old(event_ts_ms);

        let window_start_ms = event_ts_ms - (event_ts_ms.rem_euclid(self.window_ms));
        let key = (side, OrderedFloat(price));
        let buckets = self.buckets.entry(key).or_default();

        if let Some(last_bucket) = buckets.back_mut() {
            if last_bucket.window_start_ms == window_start_ms {
                last_bucket.remaining_qty += qty;
                return;
            }
        }

        buckets.push_back(AggTradeBucket {
            window_start_ms,
            remaining_qty: qty,
        });
    }

    fn consume_matched(
        &mut self,
        side: BookSide,
        price: f64,
        requested_qty: f64,
        event_ts_ms: i64,
    ) -> f64 {
        if requested_qty <= LEVEL_QTY_EPSILON || price <= 0.0 {
            return 0.0;
        }

        self.prune_old(event_ts_ms);

        let key = (side, OrderedFloat(price));
        let Some(buckets) = self.buckets.get_mut(&key) else {
            return 0.0;
        };

        let mut remaining = requested_qty;
        for bucket in buckets.iter_mut() {
            if remaining <= LEVEL_QTY_EPSILON {
                break;
            }

            let take = remaining.min(bucket.remaining_qty);
            bucket.remaining_qty -= take;
            remaining -= take;
        }

        buckets.retain(|bucket| bucket.remaining_qty > LEVEL_QTY_EPSILON);
        requested_qty - remaining
    }

    fn prune_old(&mut self, reference_ts_ms: i64) {
        let min_window_start = reference_ts_ms.saturating_sub(self.window_ms);
        self.buckets.retain(|_, buckets| {
            buckets.retain(|bucket| bucket.window_start_ms >= min_window_start);
            !buckets.is_empty()
        });
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct DecreaseBreakdown {
    execution_qty: f64,
    cancel_qty: f64,
}

type DecreasePlan = BTreeMap<OrderedFloat<f64>, DecreaseBreakdown>;

#[derive(Debug, Clone, Default)]
struct LevelState {
    total_qty: f64,
    queue: VecDeque<VolumeChunk>,
}

impl LevelState {
    fn from_single_chunk(size: f64, exchange_ts_ms: i64) -> Self {
        let mut state = Self::default();
        state.push_chunk(size, exchange_ts_ms);
        state
    }

    fn push_chunk(&mut self, size: f64, exchange_ts_ms: i64) {
        if size <= LEVEL_QTY_EPSILON {
            return;
        }

        self.queue.push_back(VolumeChunk {
            size,
            added_at_exchange_ts: exchange_ts_ms,
        });
        self.total_qty += size;
    }

    fn apply_increase(&mut self, size: f64, exchange_ts_ms: i64) {
        self.push_chunk(size, exchange_ts_ms);
    }

    fn apply_decrease(
        &mut self,
        size: f64,
        kind: QueueDecreaseKind,
        cancel_heuristic: CancelHeuristic,
    ) {
        if size <= LEVEL_QTY_EPSILON || self.total_qty <= LEVEL_QTY_EPSILON {
            return;
        }

        match kind {
            QueueDecreaseKind::Execution => self.consume_front(size),
            QueueDecreaseKind::Cancel => match cancel_heuristic {
                CancelHeuristic::Lifo => self.consume_back(size),
                CancelHeuristic::ProRata => self.consume_pro_rata(size),
            },
        }

        self.recompute_total();
    }

    fn consume_front(&mut self, mut remaining: f64) {
        while remaining > LEVEL_QTY_EPSILON {
            let Some(front) = self.queue.front_mut() else {
                break;
            };

            if front.size <= remaining + LEVEL_QTY_EPSILON {
                remaining -= front.size;
                self.queue.pop_front();
            } else {
                front.size -= remaining;
                remaining = 0.0;
            }
        }
    }

    fn consume_back(&mut self, mut remaining: f64) {
        while remaining > LEVEL_QTY_EPSILON {
            let Some(back) = self.queue.back_mut() else {
                break;
            };

            if back.size <= remaining + LEVEL_QTY_EPSILON {
                remaining -= back.size;
                self.queue.pop_back();
            } else {
                back.size -= remaining;
                remaining = 0.0;
            }
        }
    }

    fn consume_pro_rata(&mut self, remaining: f64) {
        if remaining <= LEVEL_QTY_EPSILON || self.total_qty <= LEVEL_QTY_EPSILON {
            return;
        }

        if remaining >= self.total_qty - LEVEL_QTY_EPSILON {
            self.queue.clear();
            self.total_qty = 0.0;
            return;
        }

        let target_total = self.total_qty - remaining;
        let scale = if self.total_qty > 0.0 {
            target_total / self.total_qty
        } else {
            0.0
        };

        for chunk in &mut self.queue {
            chunk.size *= scale;
        }
    }

    fn recompute_total(&mut self) {
        self.queue.retain(|chunk| chunk.size > LEVEL_QTY_EPSILON);
        self.total_qty = self.queue.iter().map(|chunk| chunk.size).sum::<f64>();
        if self.total_qty <= LEVEL_QTY_EPSILON {
            self.queue.clear();
            self.total_qty = 0.0;
        }
    }

    fn collapse_to_single_chunk(&mut self) {
        self.recompute_total();
        if self.total_qty <= LEVEL_QTY_EPSILON {
            return;
        }

        if self.queue.len() == 1 {
            return;
        }

        self.queue.clear();
        self.queue.push_back(VolumeChunk {
            size: self.total_qty,
            added_at_exchange_ts: 0,
        });
    }

    fn weighted_average_age_ms(&self, event_ts_ms: i64) -> f64 {
        if self.total_qty <= LEVEL_QTY_EPSILON {
            return 0.0;
        }

        let weighted_age_sum = self
            .queue
            .iter()
            .map(|chunk| {
                let age_ms = if chunk.added_at_exchange_ts > 0 {
                    event_ts_ms.saturating_sub(chunk.added_at_exchange_ts).max(0)
                } else {
                    0
                };
                chunk.size * age_ms as f64
            })
            .sum::<f64>();

        weighted_age_sum / self.total_qty
    }
}

#[derive(Debug, Clone, Default)]
struct TopBookLevels {
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    bids: BTreeMap<OrderedFloat<f64>, LevelState>,
    asks: BTreeMap<OrderedFloat<f64>, LevelState>,
    cancel_heuristic: CancelHeuristic,
}

impl LocalOrderBook {
    pub fn from_snapshot(snapshot: &DepthSnapshot, cancel_heuristic: CancelHeuristic) -> Self {
        let mut book = Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            cancel_heuristic,
        };
        book.load_snapshot(snapshot);
        book
    }

    pub fn load_snapshot(&mut self, snapshot: &DepthSnapshot) {
        self.bids.clear();
        self.asks.clear();

        for (price, qty) in &snapshot.bids {
            if *price > 0.0 && *qty > 0.0 {
                self.bids
                    .insert(OrderedFloat(*price), LevelState::from_single_chunk(*qty, 0));
            }
        }

        for (price, qty) in &snapshot.asks {
            if *price > 0.0 && *qty > 0.0 {
                self.asks
                    .insert(OrderedFloat(*price), LevelState::from_single_chunk(*qty, 0));
            }
        }
    }

    #[allow(dead_code)]
    pub fn apply_depth_update(&mut self, update: &DepthUpdate) -> Result<()> {
        let empty_bid_plan = DecreasePlan::new();
        let empty_ask_plan = DecreasePlan::new();
        self.apply_depth_update_with_plans(update, &empty_bid_plan, &empty_ask_plan)
    }

    fn apply_depth_update_with_plans(
        &mut self,
        update: &DepthUpdate,
        bid_decrease_plan: &DecreasePlan,
        ask_decrease_plan: &DecreasePlan,
    ) -> Result<()> {
        apply_side(
            &mut self.bids,
            &update.bids,
            update.event_time_ms,
            self.cancel_heuristic,
            bid_decrease_plan,
        )?;
        apply_side(
            &mut self.asks,
            &update.asks,
            update.event_time_ms,
            self.cancel_heuristic,
            ask_decrease_plan,
        )?;

        self.rebalance_queue_tracking_scope();

        if let (Some((best_bid, _)), Some((best_ask, _))) = (self.best_bid(), self.best_ask()) {
            if best_bid >= best_ask {
                return Err(anyhow!(
                    "crossed book detected after update: best_bid={} best_ask={}",
                    best_bid,
                    best_ask
                ));
            }
        }

        Ok(())
    }

    fn build_decrease_plans(
        &self,
        update: &DepthUpdate,
        trade_matcher: &mut AggTradeMatcher,
    ) -> Result<(DecreasePlan, DecreasePlan)> {
        let bid_decrease_plan = build_side_decrease_plan(
            &self.bids,
            &update.bids,
            BookSide::Bid,
            update.event_time_ms,
            trade_matcher,
        )?;

        let ask_decrease_plan = build_side_decrease_plan(
            &self.asks,
            &update.asks,
            BookSide::Ask,
            update.event_time_ms,
            trade_matcher,
        )?;

        Ok((bid_decrease_plan, ask_decrease_plan))
    }

    fn rebalance_queue_tracking_scope(&mut self) {
        let (Some((best_bid, _)), Some((best_ask, _))) = (self.best_bid(), self.best_ask()) else {
            return;
        };

        let mid = (best_bid + best_ask) / 2.0;
        if mid <= 0.0 {
            return;
        }

        let bid_floor = mid * (1.0 - QUEUE_TRACK_BAND_PCT);
        let ask_ceiling = mid * (1.0 + QUEUE_TRACK_BAND_PCT);

        for (rank, (price, level_state)) in self.bids.iter_mut().rev().enumerate() {
            let track_by_rank = rank < QUEUE_TRACK_LEVELS_PER_SIDE;
            let track_by_band = price.into_inner() >= bid_floor;
            if !(track_by_rank || track_by_band) {
                level_state.collapse_to_single_chunk();
            }
        }

        for (rank, (price, level_state)) in self.asks.iter_mut().enumerate() {
            let track_by_rank = rank < QUEUE_TRACK_LEVELS_PER_SIDE;
            let track_by_band = price.into_inner() <= ask_ceiling;
            if !(track_by_rank || track_by_band) {
                level_state.collapse_to_single_chunk();
            }
        }
    }

    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
    }

    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks
            .iter()
            .next()
            .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
    }

    pub fn bid_notional_in_band(&self, min_price: f64) -> f64 {
        let mut total = 0.0;
        for (price, level_state) in self.bids.iter().rev() {
            let price = price.into_inner();
            if price < min_price {
                break;
            }
            total += COIN_M_CONTRACT_NOTIONAL_USD * level_state.total_qty;
        }
        total
    }

    pub fn ask_notional_in_band(&self, max_price: f64) -> f64 {
        let mut total = 0.0;
        for (price, level_state) in &self.asks {
            let price = price.into_inner();
            if price > max_price {
                break;
            }
            total += COIN_M_CONTRACT_NOTIONAL_USD * level_state.total_qty;
        }
        total
    }

    fn top_levels(&self, depth: usize) -> TopBookLevels {
        TopBookLevels {
            bids: self
                .bids
                .iter()
                .rev()
                .take(depth)
                .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
                .collect(),
            asks: self
                .asks
                .iter()
                .take(depth)
                .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
                .collect(),
        }
    }

    fn full_levels(&self) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
            .collect::<Vec<_>>();
        let asks = self
            .asks
            .iter()
            .map(|(price, level_state)| (price.into_inner(), level_state.total_qty))
            .collect::<Vec<_>>();

        (bids, asks)
    }

    fn vwaa_l1(&self, event_ts_ms: i64) -> (f64, f64) {
        let bid = self
            .bids
            .iter()
            .next_back()
            .map(|(_, level)| level.weighted_average_age_ms(event_ts_ms))
            .unwrap_or(0.0);
        let ask = self
            .asks
            .iter()
            .next()
            .map(|(_, level)| level.weighted_average_age_ms(event_ts_ms))
            .unwrap_or(0.0);

        (bid, ask)
    }

    fn vwaa_top_n(&self, event_ts_ms: i64, depth: usize) -> (f64, f64) {
        let (bid_weighted_sum, bid_total_qty) = self
            .bids
            .iter()
            .rev()
            .take(depth)
            .fold((0.0, 0.0), |(sum, qty), (_, level)| {
                (
                    sum + (level.weighted_average_age_ms(event_ts_ms) * level.total_qty),
                    qty + level.total_qty,
                )
            });

        let (ask_weighted_sum, ask_total_qty) = self
            .asks
            .iter()
            .take(depth)
            .fold((0.0, 0.0), |(sum, qty), (_, level)| {
                (
                    sum + (level.weighted_average_age_ms(event_ts_ms) * level.total_qty),
                    qty + level.total_qty,
                )
            });

        let bid = if bid_total_qty > LEVEL_QTY_EPSILON {
            bid_weighted_sum / bid_total_qty
        } else {
            0.0
        };
        let ask = if ask_total_qty > LEVEL_QTY_EPSILON {
            ask_weighted_sum / ask_total_qty
        } else {
            0.0
        };

        (bid, ask)
    }

    fn signed_imbalance(lhs: f64, rhs: f64) -> f64 {
        let denom = lhs + rhs;
        if denom.abs() <= LEVEL_QTY_EPSILON {
            0.0
        } else {
            (lhs - rhs) / denom
        }
    }
}

#[derive(Debug, Clone)]
enum ResyncReason {
    InitialBridgeApplyFailed { error: String },
    MissedBridgeEvent,
    SequenceGap,
    DepthApplyFailed { error: String },
}

#[derive(Debug, Clone)]
enum ProcessOutcome {
    Ignore,
    EmitMetric {
        stale_state: bool,
        spoof_signal: SpoofSignal,
    },
    Resync { reason: ResyncReason },
}

#[derive(Debug, Clone)]
struct SpoofSignal {
    flag: bool,
    score: f64,
    reason_code: String,
}

impl Default for SpoofSignal {
    fn default() -> Self {
        Self {
            flag: false,
            score: 0.0,
            reason_code: "none".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct RollingWindowSum {
    window_ms: i64,
    entries: VecDeque<(i64, f64)>,
    running_sum: f64,
}

impl RollingWindowSum {
    fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            entries: VecDeque::new(),
            running_sum: 0.0,
        }
    }

    fn record(&mut self, event_ts_ms: i64, signed_qty: f64) {
        if signed_qty.abs() <= LEVEL_QTY_EPSILON {
            return;
        }

        self.entries.push_back((event_ts_ms, signed_qty));
        self.running_sum += signed_qty;
        self.prune(event_ts_ms);
    }

    fn value(&mut self, now_ts_ms: i64) -> f64 {
        self.prune(now_ts_ms);
        self.running_sum
    }

    fn prune(&mut self, reference_ts_ms: i64) {
        let lower_bound = reference_ts_ms.saturating_sub(self.window_ms);

        while let Some((ts, value)) = self.entries.front().copied() {
            if ts >= lower_bound {
                break;
            }

            self.running_sum -= value;
            self.entries.pop_front();
        }

        if self.running_sum.abs() <= LEVEL_QTY_EPSILON {
            self.running_sum = 0.0;
        }
    }
}

#[derive(Debug, Clone)]
struct CvdTracker {
    window_5s: RollingWindowSum,
    window_1m: RollingWindowSum,
    window_5m: RollingWindowSum,
}

impl CvdTracker {
    fn new() -> Self {
        Self {
            window_5s: RollingWindowSum::new(5_000),
            window_1m: RollingWindowSum::new(60_000),
            window_5m: RollingWindowSum::new(300_000),
        }
    }

    fn record(&mut self, event_ts_ms: i64, signed_qty: f64) {
        if signed_qty.abs() <= LEVEL_QTY_EPSILON {
            return;
        }

        self.window_5s.record(event_ts_ms, signed_qty);
        self.window_1m.record(event_ts_ms, signed_qty);
        self.window_5m.record(event_ts_ms, signed_qty);
    }

    fn values(&mut self, now_ts_ms: i64) -> (f64, f64, f64) {
        let cvd_5s = self.window_5s.value(now_ts_ms);
        let cvd_1m = self.window_1m.value(now_ts_ms);
        let cvd_5m = self.window_5m.value(now_ts_ms);
        (cvd_5s, cvd_1m, cvd_5m)
    }
}

#[derive(Debug, Clone)]
struct SpoofThresholdTracker {
    window_ms: i64,
    bid_level_samples: BTreeMap<OrderedFloat<f64>, VecDeque<(i64, f64)>>,
    ask_level_samples: BTreeMap<OrderedFloat<f64>, VecDeque<(i64, f64)>>,
    bbo_samples: VecDeque<(i64, f64)>,
}

impl SpoofThresholdTracker {
    fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            bid_level_samples: BTreeMap::new(),
            ask_level_samples: BTreeMap::new(),
            bbo_samples: VecDeque::new(),
        }
    }

    fn observe_book(&mut self, orderbook: &LocalOrderBook, event_ts_ms: i64) {
        self.prune(event_ts_ms);

        for (price, level_state) in orderbook.bids.iter().rev().take(SPOOF_LEVEL_SAMPLE_DEPTH) {
            self.bid_level_samples
                .entry(*price)
                .or_default()
                .push_back((event_ts_ms, level_state.total_qty));
        }

        for (price, level_state) in orderbook.asks.iter().take(SPOOF_LEVEL_SAMPLE_DEPTH) {
            self.ask_level_samples
                .entry(*price)
                .or_default()
                .push_back((event_ts_ms, level_state.total_qty));
        }

        if let (Some((_, best_bid_qty)), Some((_, best_ask_qty))) =
            (orderbook.best_bid(), orderbook.best_ask())
        {
            self.bbo_samples
                .push_back((event_ts_ms, (best_bid_qty + best_ask_qty) / 2.0));
        }

        self.prune(event_ts_ms);
    }

    fn level_threshold(&self, side: BookSide, price: f64) -> Option<f64> {
        let key = OrderedFloat(price);
        let samples = match side {
            BookSide::Bid => self.bid_level_samples.get(&key),
            BookSide::Ask => self.ask_level_samples.get(&key),
        }?;

        Self::threshold_from_samples(samples)
    }

    fn average_bbo_size(&self) -> Option<f64> {
        Self::average_samples(&self.bbo_samples)
    }

    fn prune(&mut self, reference_ts_ms: i64) {
        let lower_bound = reference_ts_ms.saturating_sub(self.window_ms);

        Self::prune_level_samples(&mut self.bid_level_samples, lower_bound);
        Self::prune_level_samples(&mut self.ask_level_samples, lower_bound);

        while let Some((ts, _)) = self.bbo_samples.front() {
            if *ts >= lower_bound {
                break;
            }
            self.bbo_samples.pop_front();
        }
    }

    fn threshold_from_samples(samples: &VecDeque<(i64, f64)>) -> Option<f64> {
        let (mean, std_dev) = Self::mean_std(samples)?;
        Some(mean + (2.0 * std_dev))
    }

    fn average_samples(samples: &VecDeque<(i64, f64)>) -> Option<f64> {
        let mut count = 0.0;
        let mut sum = 0.0;

        for (_, value) in samples {
            if *value <= LEVEL_QTY_EPSILON {
                continue;
            }
            count += 1.0;
            sum += *value;
        }

        if count <= 0.0 {
            return None;
        }

        Some(sum / count)
    }

    fn mean_std(samples: &VecDeque<(i64, f64)>) -> Option<(f64, f64)> {
        let mut count = 0.0;
        let mut sum = 0.0;

        for (_, value) in samples {
            if *value <= LEVEL_QTY_EPSILON {
                continue;
            }

            count += 1.0;
            sum += *value;
        }

        if count < 2.0 {
            return None;
        }

        let mean = sum / count;
        let mut variance_sum = 0.0;

        for (_, value) in samples {
            if *value <= LEVEL_QTY_EPSILON {
                continue;
            }

            let delta = *value - mean;
            variance_sum += delta * delta;
        }

        let variance = (variance_sum / count).max(0.0);
        Some((mean, variance.sqrt()))
    }

    fn prune_level_samples(
        samples_by_level: &mut BTreeMap<OrderedFloat<f64>, VecDeque<(i64, f64)>>,
        lower_bound: i64,
    ) {
        samples_by_level.retain(|_, samples| {
            while let Some((ts, _)) = samples.front() {
                if *ts >= lower_bound {
                    break;
                }
                samples.pop_front();
            }

            !samples.is_empty()
        });
    }
}

#[derive(Debug, Clone)]
struct EngineState {
    orderbook: LocalOrderBook,
    trade_matcher: AggTradeMatcher,
    cvd_tracker: CvdTracker,
    spoof_threshold_tracker: SpoofThresholdTracker,
    last_update_id: u64,
    is_synced: bool,
}

impl EngineState {
    fn new(snapshot: &DepthSnapshot, cancel_heuristic: CancelHeuristic) -> Self {
        Self {
            orderbook: LocalOrderBook::from_snapshot(snapshot, cancel_heuristic),
            trade_matcher: AggTradeMatcher::new(AGGTRADE_MATCH_WINDOW_MS),
            cvd_tracker: CvdTracker::new(),
            spoof_threshold_tracker: SpoofThresholdTracker::new(SPOOF_THRESHOLD_WINDOW_MS),
            last_update_id: snapshot.last_update_id,
            is_synced: false,
        }
    }

    fn replace_snapshot(&mut self, snapshot: &DepthSnapshot) {
        self.orderbook.load_snapshot(snapshot);
        self.last_update_id = snapshot.last_update_id;
        self.is_synced = false;
    }

    fn process_depth_event(&mut self, event: &DepthEvent, cvd_5s: f64) -> ProcessOutcome {
        let (bid_decrease_plan, ask_decrease_plan) =
            match self
                .orderbook
                .build_decrease_plans(&event.payload, &mut self.trade_matcher)
            {
                Ok(value) => value,
                Err(error) => {
                    return ProcessOutcome::Resync {
                        reason: ResyncReason::DepthApplyFailed {
                            error: error.to_string(),
                        },
                    }
                }
            };

        let spoof_signal = self.derive_spoof_signal(event, &bid_decrease_plan, &ask_decrease_plan, cvd_5s);

        if !self.is_synced {
            if event.payload.final_update_id <= self.last_update_id {
                return ProcessOutcome::Ignore;
            }

            let bridge_target = self.last_update_id.saturating_add(1);

            if event.payload.first_update_id <= bridge_target
                && event.payload.final_update_id >= bridge_target
            {
                if let Err(error) = self.orderbook.apply_depth_update_with_plans(
                    &event.payload,
                    &bid_decrease_plan,
                    &ask_decrease_plan,
                ) {
                    return ProcessOutcome::Resync {
                        reason: ResyncReason::InitialBridgeApplyFailed {
                            error: error.to_string(),
                        },
                    };
                }

                self.last_update_id = event.payload.final_update_id;
                self.is_synced = true;
                self.spoof_threshold_tracker
                    .observe_book(&self.orderbook, event.payload.event_time_ms);

                return ProcessOutcome::EmitMetric {
                    stale_state: false,
                    spoof_signal,
                };
            }

            if event.payload.first_update_id > bridge_target {
                return ProcessOutcome::Resync {
                    reason: ResyncReason::MissedBridgeEvent,
                };
            }

            return ProcessOutcome::Ignore;
        }

        if event.payload.final_update_id <= self.last_update_id {
            return ProcessOutcome::Ignore;
        }

        if event.payload.prev_final_update_id != self.last_update_id {
            return ProcessOutcome::Resync {
                reason: ResyncReason::SequenceGap,
            };
        }

        if let Err(error) = self.orderbook.apply_depth_update_with_plans(
            &event.payload,
            &bid_decrease_plan,
            &ask_decrease_plan,
        ) {
            return ProcessOutcome::Resync {
                reason: ResyncReason::DepthApplyFailed {
                    error: error.to_string(),
                },
            };
        }

        self.last_update_id = event.payload.final_update_id;
        self.spoof_threshold_tracker
            .observe_book(&self.orderbook, event.payload.event_time_ms);

        ProcessOutcome::EmitMetric {
            stale_state: false,
            spoof_signal,
        }
    }

    fn process_agg_trade_event(&mut self, event: &crate::types::AggTradeEvent) {
        let Ok(price) = event.payload.price.parse::<f64>() else {
            return;
        };
        let Ok(qty) = event.payload.quantity.parse::<f64>() else {
            return;
        };

        let (depleted_side, signed_qty) = if event.payload.buyer_is_maker {
            (BookSide::Bid, -qty)
        } else {
            (BookSide::Ask, qty)
        };

        self.trade_matcher
            .record_trade(depleted_side, price, qty, event.payload.event_time_ms);
        self.cvd_tracker.record(event.payload.event_time_ms, signed_qty);
    }

    fn derive_spoof_signal(
        &mut self,
        event: &DepthEvent,
        bid_decrease_plan: &DecreasePlan,
        ask_decrease_plan: &DecreasePlan,
        cvd_5s: f64,
    ) -> SpoofSignal {
        let event_ts_ms = event.payload.event_time_ms;
        self.spoof_threshold_tracker.prune(event_ts_ms);

        let avg_bbo_size = self
            .spoof_threshold_tracker
            .average_bbo_size()
            .unwrap_or(0.0);

        let bid_signal = evaluate_side_spoof_signal(
            BookSide::Bid,
            &self.orderbook.bids,
            bid_decrease_plan,
            &self.spoof_threshold_tracker,
            avg_bbo_size,
            cvd_5s,
        );

        let ask_signal = evaluate_side_spoof_signal(
            BookSide::Ask,
            &self.orderbook.asks,
            ask_decrease_plan,
            &self.spoof_threshold_tracker,
            avg_bbo_size,
            cvd_5s,
        );

        if ask_signal.score > bid_signal.score {
            ask_signal
        } else {
            bid_signal
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ReorderArrival {
    out_of_order: bool,
    late: bool,
}

#[derive(Debug, Clone)]
struct ReadyUnifiedEvent {
    event: UnifiedEvent,
    trade_alignment_forced_open: bool,
}

#[derive(Debug, Clone)]
struct ReorderBuffer {
    hold_window_ms: i64,
    max_buffered_events: usize,
    trade_alignment_max_lag_ms: i64,
    execution_match_lookahead_ms: i64,
    max_received_ts_ms: i64,
    max_emitted_aggtrade_ts_ms: i64,
    buffered_events: usize,
    by_exchange_ts: BTreeMap<u64, Vec<UnifiedEvent>>,
}

impl ReorderBuffer {
    fn new(
        hold_window_ms: i64,
        max_buffered_events: usize,
        trade_alignment_max_lag_ms: i64,
        execution_match_lookahead_ms: i64,
    ) -> Self {
        Self {
            hold_window_ms,
            max_buffered_events,
            trade_alignment_max_lag_ms,
            execution_match_lookahead_ms,
            max_received_ts_ms: i64::MIN,
            max_emitted_aggtrade_ts_ms: i64::MIN,
            buffered_events: 0,
            by_exchange_ts: BTreeMap::new(),
        }
    }

    fn push(&mut self, event: UnifiedEvent) -> ReorderArrival {
        let exchange_ts_ms = event.exchange_ts_ms().max(0);
        let mut arrival = ReorderArrival::default();

        if self.max_received_ts_ms != i64::MIN {
            if exchange_ts_ms < self.max_received_ts_ms {
                arrival.out_of_order = true;
            }

            let watermark_ts = self.max_received_ts_ms.saturating_sub(self.hold_window_ms);
            if exchange_ts_ms < watermark_ts {
                arrival.late = true;
            }
        }

        if exchange_ts_ms > self.max_received_ts_ms {
            self.max_received_ts_ms = exchange_ts_ms;
        }

        let exchange_ts_key = exchange_ts_ms as u64;
        self.by_exchange_ts
            .entry(exchange_ts_key)
            .or_default()
            .push(event);
        self.buffered_events = self.buffered_events.saturating_add(1);

        arrival
    }

    fn is_over_capacity(&self) -> bool {
        self.buffered_events > self.max_buffered_events
    }

    fn buffered_lag_ms(&self) -> Option<i64> {
        let oldest_ts_ms = self.by_exchange_ts.keys().next().copied()?;
        Some(
            self.max_received_ts_ms
                .saturating_sub(i64::try_from(oldest_ts_ms).unwrap_or(i64::MAX)),
        )
    }

    fn reset(&mut self) -> usize {
        let dropped_events = self.buffered_events;
        self.by_exchange_ts.clear();
        self.buffered_events = 0;
        self.max_received_ts_ms = i64::MIN;
        self.max_emitted_aggtrade_ts_ms = i64::MIN;
        dropped_events
    }

    fn event_priority(event: &UnifiedEvent) -> u8 {
        match event {
            UnifiedEvent::AggTrade(_) => 0,
            UnifiedEvent::Depth(_) => 1,
        }
    }

    fn prioritize_same_timestamp_events(events: &mut Vec<UnifiedEvent>) {
        // Within the same exchange timestamp, consume trades before depth so
        // CVD snapshots include contemporaneous trade prints when possible.
        events.sort_by_key(Self::event_priority);
    }

    fn classify_depth_release(&self, depth_ts_ms: i64, force_release_depth: bool) -> Option<bool> {
        let required_trade_watermark =
            depth_ts_ms.saturating_add(self.execution_match_lookahead_ms);

        if self.max_emitted_aggtrade_ts_ms >= required_trade_watermark {
            return Some(false);
        }

        let observed_skew_ms = self
            .max_received_ts_ms
            .saturating_sub(required_trade_watermark);
        if force_release_depth || observed_skew_ms >= self.trade_alignment_max_lag_ms {
            return Some(true);
        }

        None
    }

    fn record_emitted_aggtrade_ts(&mut self, event_ts_ms: i64) {
        if event_ts_ms > self.max_emitted_aggtrade_ts_ms {
            self.max_emitted_aggtrade_ts_ms = event_ts_ms;
        }
    }

    fn drain_ready(&mut self) -> Vec<ReadyUnifiedEvent> {
        if self.max_received_ts_ms == i64::MIN {
            return Vec::new();
        }

        let watermark_ts = self
            .max_received_ts_ms
            .saturating_sub(self.hold_window_ms)
            .max(0) as u64;

        self.drain_up_to(watermark_ts, false)
    }

    fn drain_all(&mut self) -> Vec<ReadyUnifiedEvent> {
        self.drain_up_to(u64::MAX, true)
    }

    fn drain_up_to(
        &mut self,
        max_exchange_ts: u64,
        force_release_depth: bool,
    ) -> Vec<ReadyUnifiedEvent> {
        let ready_keys = self
            .by_exchange_ts
            .keys()
            .copied()
            .take_while(|ts| *ts <= max_exchange_ts)
            .collect::<Vec<_>>();

        let mut drained = Vec::new();
        for key in ready_keys {
            if let Some(mut events) = self.by_exchange_ts.remove(&key) {
                self.buffered_events = self.buffered_events.saturating_sub(events.len());
                Self::prioritize_same_timestamp_events(&mut events);

                let mut retained = Vec::new();

                for event in events {
                    match &event {
                        UnifiedEvent::AggTrade(_) => {
                            self.record_emitted_aggtrade_ts(event.exchange_ts_ms());
                            drained.push(ReadyUnifiedEvent {
                                event,
                                trade_alignment_forced_open: false,
                            });
                        }
                        UnifiedEvent::Depth(depth_event) => {
                            let depth_ts_ms = depth_event.payload.event_time_ms.max(0);
                            let Some(forced_open) =
                                self.classify_depth_release(depth_ts_ms, force_release_depth)
                            else {
                                retained.push(event);
                                continue;
                            };

                            drained.push(ReadyUnifiedEvent {
                                event,
                                trade_alignment_forced_open: forced_open,
                            });
                        }
                    }
                }

                if !retained.is_empty() {
                    self.buffered_events = self.buffered_events.saturating_add(retained.len());
                    self.by_exchange_ts.insert(key, retained);
                }
            }
        }

        drained
    }
}

pub async fn run_engine(
    config: AppConfig,
    mut unified_rx: mpsc::Receiver<UnifiedEvent>,
    metrics_tx: mpsc::Sender<SignalMetric>,
    signal_tx: broadcast::Sender<SignalMetric>,
    raw_depth_tx: mpsc::Sender<DepthEvent>,
    snapshot_tx: mpsc::Sender<OrderbookSnapshotEvent>,
    telemetry_tx: mpsc::Sender<TelemetryEvent>,
) -> Result<()> {
    let http_client = Client::new();
    let snapshot = fetch_snapshot_with_retry(&config, &http_client).await?;
    let mut engine = EngineState::new(&snapshot, config.cancel_heuristic);

    let bootstrap_snapshot_ts_ms = now_utc_ms();
    emit_snapshot_event(
        &snapshot_tx,
        build_snapshot_event_from_snapshot(
            &snapshot,
            bootstrap_snapshot_ts_ms,
            SNAPSHOT_SOURCE_REST_BOOTSTRAP,
        ),
    )
    .await;

    let snapshot_dump_interval_ms = snapshot_dump_interval_ms(&config);
    let mut next_snapshot_dump_ts_ms =
        snapshot_dump_interval_ms.map(|interval| bootstrap_snapshot_ts_ms.saturating_add(interval));

    info!(
        symbol = %snapshot.symbol,
        last_update_id = snapshot.last_update_id,
        "orderbook engine bootstrapped from REST snapshot"
    );

    let mut reorder_buffer = ReorderBuffer::new(
        REORDER_HOLD_WINDOW_MS,
        REORDER_MAX_BUFFERED_EVENTS,
        config.trade_alignment_max_lag_ms,
        config.execution_match_lookahead_ms,
    );

    while let Some(event) = unified_rx.recv().await {
        if event.symbol() != config.symbol {
            continue;
        }

        let arrival = reorder_buffer.push(event);

        if arrival.out_of_order {
            send_telemetry(&telemetry_tx, TelemetryEvent::OutOfOrderCorrection).await;
        }
        if arrival.late {
            send_telemetry(&telemetry_tx, TelemetryEvent::LateEventCorrection).await;
        }

        for ready_event in reorder_buffer.drain_ready() {
            process_unified_event(
                ready_event,
                &mut engine,
                &config,
                &http_client,
                &metrics_tx,
                &signal_tx,
                &raw_depth_tx,
                &snapshot_tx,
                &telemetry_tx,
                snapshot_dump_interval_ms,
                &mut next_snapshot_dump_ts_ms,
            )
            .await?;
        }

        let buffer_lag_ms = reorder_buffer.buffered_lag_ms().unwrap_or(0);
        let capacity_exceeded = reorder_buffer.is_over_capacity();
        let lag_exceeded = buffer_lag_ms > config.reorder_resync_max_buffer_lag_ms;

        if capacity_exceeded || lag_exceeded {
            send_telemetry(&telemetry_tx, TelemetryEvent::ReorderBufferGuardTriggered).await;
            let dropped_events = reorder_buffer.reset();

            warn!(
                dropped_events,
                buffered_lag_ms = buffer_lag_ms,
                max_allowed_lag_ms = config.reorder_resync_max_buffer_lag_ms,
                capacity_exceeded,
                lag_exceeded,
                "reorder buffer circuit breaker triggered; forcing snapshot resync"
            );

            send_telemetry(&telemetry_tx, TelemetryEvent::ResyncTriggered).await;

            let snapshot = fetch_snapshot_with_retry(&config, &http_client).await?;
            let resync_snapshot_ts_ms = now_utc_ms();
            emit_snapshot_event(
                &snapshot_tx,
                build_snapshot_event_from_snapshot(
                    &snapshot,
                    resync_snapshot_ts_ms,
                    SNAPSHOT_SOURCE_REST_RESYNC,
                ),
            )
            .await;

            next_snapshot_dump_ts_ms =
                snapshot_dump_interval_ms.map(|interval| resync_snapshot_ts_ms.saturating_add(interval));

            engine.replace_snapshot(&snapshot);
            send_telemetry(&telemetry_tx, TelemetryEvent::ResyncCompleted).await;
        }
    }

    for pending_event in reorder_buffer.drain_all() {
        process_unified_event(
            pending_event,
            &mut engine,
            &config,
            &http_client,
            &metrics_tx,
            &signal_tx,
            &raw_depth_tx,
            &snapshot_tx,
            &telemetry_tx,
            snapshot_dump_interval_ms,
            &mut next_snapshot_dump_ts_ms,
        )
        .await?;
    }

    Ok(())
}

async fn process_unified_event(
    ready_event: ReadyUnifiedEvent,
    engine: &mut EngineState,
    config: &AppConfig,
    http_client: &Client,
    metrics_tx: &mpsc::Sender<SignalMetric>,
    signal_tx: &broadcast::Sender<SignalMetric>,
    raw_depth_tx: &mpsc::Sender<DepthEvent>,
    snapshot_tx: &mpsc::Sender<OrderbookSnapshotEvent>,
    telemetry_tx: &mpsc::Sender<TelemetryEvent>,
    snapshot_dump_interval_ms: Option<i64>,
    next_snapshot_dump_ts_ms: &mut Option<i64>,
) -> Result<()> {
    let trade_alignment_forced_open = ready_event.trade_alignment_forced_open;

    match ready_event.event {
        UnifiedEvent::Depth(event) => {
            let previous_levels = engine.orderbook.top_levels(MOFI_DEPTH_LEVELS);
            let (cvd_5s, cvd_1m, cvd_5m) = engine.cvd_tracker.values(event.payload.event_time_ms);

            send_telemetry(telemetry_tx, TelemetryEvent::DepthEventReceived).await;
            if trade_alignment_forced_open {
                send_telemetry(telemetry_tx, TelemetryEvent::TradeAlignmentForcedOpen).await;
            }

            match engine.process_depth_event(&event, cvd_5s) {
                ProcessOutcome::Ignore => {}
                ProcessOutcome::EmitMetric {
                    stale_state,
                    spoof_signal,
                } => {
                    emit_metric(
                        &engine.orderbook,
                        &event,
                        &previous_levels,
                        stale_state,
                        trade_alignment_forced_open,
                        spoof_signal,
                        cvd_5s,
                        cvd_1m,
                        cvd_5m,
                        metrics_tx,
                        signal_tx,
                        telemetry_tx,
                        config.symbol.as_str(),
                    )
                    .await;

                    if let Err(error) = raw_depth_tx.send(event.clone()).await {
                        warn!(%error, "failed to forward accepted depth delta to archive spool");
                    }

                    maybe_emit_local_keyframe_snapshot(
                        engine,
                        config.symbol.as_str(),
                        event.payload.event_time_ms,
                        snapshot_dump_interval_ms,
                        next_snapshot_dump_ts_ms,
                        snapshot_tx,
                    )
                    .await;
                }
                ProcessOutcome::Resync { reason } => {
                    match reason {
                        ResyncReason::InitialBridgeApplyFailed { error } => {
                            warn!(%error, "failed to apply initial bridge event; forcing resync");
                        }
                        ResyncReason::MissedBridgeEvent => {
                            warn!(
                                first_update_id = event.payload.first_update_id,
                                last_update_id = engine.last_update_id,
                                "missed bridge event while syncing; forcing resync"
                            );
                        }
                        ResyncReason::SequenceGap => {
                            warn!(
                                prev_final_update_id = event.payload.prev_final_update_id,
                                expected_prev = engine.last_update_id,
                                "depth sequence gap detected; entering stale mode"
                            );
                        }
                        ResyncReason::DepthApplyFailed { error } => {
                            warn!(%error, "failed to apply depth update; entering stale mode and resync");
                        }
                    }

                    send_telemetry(telemetry_tx, TelemetryEvent::SequenceGapDetected).await;
                    send_telemetry(telemetry_tx, TelemetryEvent::ResyncTriggered).await;

                    emit_metric(
                        &engine.orderbook,
                        &event,
                        &previous_levels,
                        true,
                        trade_alignment_forced_open,
                        SpoofSignal::default(),
                        cvd_5s,
                        cvd_1m,
                        cvd_5m,
                        metrics_tx,
                        signal_tx,
                        telemetry_tx,
                        config.symbol.as_str(),
                    )
                    .await;

                    let snapshot = fetch_snapshot_with_retry(config, http_client).await?;
                    let resync_snapshot_ts_ms = now_utc_ms();
                    emit_snapshot_event(
                        snapshot_tx,
                        build_snapshot_event_from_snapshot(
                            &snapshot,
                            resync_snapshot_ts_ms,
                            SNAPSHOT_SOURCE_REST_RESYNC,
                        ),
                    )
                    .await;

                    *next_snapshot_dump_ts_ms = snapshot_dump_interval_ms
                        .map(|interval| resync_snapshot_ts_ms.saturating_add(interval));

                    engine.replace_snapshot(&snapshot);
                    send_telemetry(telemetry_tx, TelemetryEvent::ResyncCompleted).await;
                }
            }
        }
        UnifiedEvent::AggTrade(event) => {
            send_telemetry(telemetry_tx, TelemetryEvent::AggTradeEventReceived).await;
            engine.process_agg_trade_event(&event);
        }
    }

    Ok(())
}

fn snapshot_dump_interval_ms(config: &AppConfig) -> Option<i64> {
    if config.snapshot_dump_interval_secs == 0 {
        return None;
    }

    let seconds = config
        .snapshot_dump_interval_secs
        .min((i64::MAX / 1_000) as u64) as i64;
    Some(seconds.saturating_mul(1_000))
}

fn build_snapshot_event_from_snapshot(
    snapshot: &DepthSnapshot,
    snapshot_ts_ms: i64,
    source: &str,
) -> OrderbookSnapshotEvent {
    OrderbookSnapshotEvent {
        schema_version: 1,
        exchange: "binance".to_string(),
        market: "coin-m-futures".to_string(),
        symbol: snapshot.symbol.clone(),
        snapshot_ts_ms,
        last_update_id: snapshot.last_update_id,
        snapshot_source: source.to_string(),
        bids: snapshot.bids.clone(),
        asks: snapshot.asks.clone(),
    }
}

fn build_snapshot_event_from_local_orderbook(
    orderbook: &LocalOrderBook,
    symbol: &str,
    last_update_id: u64,
    snapshot_ts_ms: i64,
) -> OrderbookSnapshotEvent {
    let (bids, asks) = orderbook.full_levels();
    OrderbookSnapshotEvent {
        schema_version: 1,
        exchange: "binance".to_string(),
        market: "coin-m-futures".to_string(),
        symbol: symbol.to_string(),
        snapshot_ts_ms,
        last_update_id,
        snapshot_source: SNAPSHOT_SOURCE_LOCAL_KEYFRAME.to_string(),
        bids,
        asks,
    }
}

async fn emit_snapshot_event(
    snapshot_tx: &mpsc::Sender<OrderbookSnapshotEvent>,
    snapshot_event: OrderbookSnapshotEvent,
) {
    if let Err(error) = snapshot_tx.send(snapshot_event).await {
        warn!(%error, "failed to forward orderbook snapshot to archive spool");
    }
}

async fn maybe_emit_local_keyframe_snapshot(
    engine: &EngineState,
    symbol: &str,
    event_ts_ms: i64,
    snapshot_dump_interval_ms: Option<i64>,
    next_snapshot_dump_ts_ms: &mut Option<i64>,
    snapshot_tx: &mpsc::Sender<OrderbookSnapshotEvent>,
) {
    let Some(interval_ms) = snapshot_dump_interval_ms else {
        return;
    };

    let Some(next_snapshot_ts_ms) = *next_snapshot_dump_ts_ms else {
        return;
    };

    if event_ts_ms < next_snapshot_ts_ms {
        return;
    }

    emit_snapshot_event(
        snapshot_tx,
        build_snapshot_event_from_local_orderbook(
            &engine.orderbook,
            symbol,
            engine.last_update_id,
            event_ts_ms,
        ),
    )
    .await;

    let mut next_target = next_snapshot_ts_ms;
    while event_ts_ms >= next_target {
        next_target = next_target.saturating_add(interval_ms);
    }
    *next_snapshot_dump_ts_ms = Some(next_target);
}

fn apply_side(
    side: &mut BTreeMap<OrderedFloat<f64>, LevelState>,
    levels: &[[String; 2]],
    exchange_ts_ms: i64,
    cancel_heuristic: CancelHeuristic,
    decrease_plan: &DecreasePlan,
) -> Result<()> {
    for level in levels {
        let price = level[0]
            .parse::<f64>()
            .with_context(|| format!("invalid price in depth update: {}", level[0]))?;
        let qty = level[1]
            .parse::<f64>()
            .with_context(|| format!("invalid quantity in depth update: {}", level[1]))?;

        if price <= 0.0 {
            continue;
        }

        if qty <= 0.0 {
            side.remove(&OrderedFloat(price));
        } else {
            let key = OrderedFloat(price);
            match side.get_mut(&key) {
                Some(level_state) => {
                    if qty > level_state.total_qty + LEVEL_QTY_EPSILON {
                        level_state.apply_increase(qty - level_state.total_qty, exchange_ts_ms);
                    } else if qty + LEVEL_QTY_EPSILON < level_state.total_qty {
                        let drop_qty = level_state.total_qty - qty;
                        let breakdown = decrease_plan.get(&key).copied().unwrap_or_default();

                        let execution_qty = breakdown.execution_qty.min(drop_qty).max(0.0);
                        let cancel_qty = breakdown
                            .cancel_qty
                            .min((drop_qty - execution_qty).max(0.0))
                            .max(0.0);
                        let residual_cancel = (drop_qty - execution_qty - cancel_qty).max(0.0);

                        if execution_qty > LEVEL_QTY_EPSILON {
                            level_state.apply_decrease(
                                execution_qty,
                                QueueDecreaseKind::Execution,
                                cancel_heuristic,
                            );
                        }

                        let total_cancel = cancel_qty + residual_cancel;
                        if total_cancel > LEVEL_QTY_EPSILON {
                            level_state.apply_decrease(
                                total_cancel,
                                QueueDecreaseKind::Cancel,
                                cancel_heuristic,
                            );
                        }
                    }

                    if level_state.total_qty <= LEVEL_QTY_EPSILON {
                        side.remove(&key);
                    }
                }
                None => {
                    side.insert(key, LevelState::from_single_chunk(qty, exchange_ts_ms));
                }
            }
        }
    }

    Ok(())
}

fn build_side_decrease_plan(
    side: &BTreeMap<OrderedFloat<f64>, LevelState>,
    levels: &[[String; 2]],
    depleted_side: BookSide,
    event_ts_ms: i64,
    trade_matcher: &mut AggTradeMatcher,
) -> Result<DecreasePlan> {
    let mut plan = DecreasePlan::new();

    for level in levels {
        let price = level[0]
            .parse::<f64>()
            .with_context(|| format!("invalid price in depth update: {}", level[0]))?;
        let qty = level[1]
            .parse::<f64>()
            .with_context(|| format!("invalid quantity in depth update: {}", level[1]))?;

        if price <= 0.0 {
            continue;
        }

        let key = OrderedFloat(price);
        let previous_qty = side.get(&key).map(|state| state.total_qty).unwrap_or(0.0);
        if qty + LEVEL_QTY_EPSILON >= previous_qty {
            continue;
        }

        let drop_qty = (previous_qty - qty).max(0.0);
        let execution_qty = trade_matcher.consume_matched(depleted_side, price, drop_qty, event_ts_ms);
        let cancel_qty = (drop_qty - execution_qty).max(0.0);

        plan.insert(
            key,
            DecreaseBreakdown {
                execution_qty,
                cancel_qty,
            },
        );
    }

    Ok(plan)
}

fn evaluate_side_spoof_signal(
    side: BookSide,
    side_levels: &BTreeMap<OrderedFloat<f64>, LevelState>,
    decrease_plan: &DecreasePlan,
    threshold_tracker: &SpoofThresholdTracker,
    avg_bbo_size: f64,
    cvd_5s: f64,
) -> SpoofSignal {
    let mut best_signal = SpoofSignal::default();

    if decrease_plan.is_empty() {
        return best_signal;
    }

    let bbo_threshold = if avg_bbo_size > LEVEL_QTY_EPSILON {
        SPOOF_BBO_MULTIPLIER * avg_bbo_size
    } else {
        f64::INFINITY
    };

    for (price, breakdown) in decrease_plan {
        let cancel_qty = breakdown.cancel_qty.max(0.0);
        if cancel_qty <= LEVEL_QTY_EPSILON {
            continue;
        }

        let Some(level_state) = side_levels.get(price) else {
            continue;
        };

        let wall_qty = level_state.total_qty;
        if wall_qty <= LEVEL_QTY_EPSILON {
            continue;
        }

        let cancel_ratio = (cancel_qty / wall_qty).clamp(0.0, 1.0);
        if cancel_ratio + LEVEL_QTY_EPSILON < SPOOF_CANCEL_RATIO_THRESHOLD {
            continue;
        }

        let dynamic_threshold = threshold_tracker
            .level_threshold(side, price.into_inner())
            .unwrap_or(f64::INFINITY);

        let threshold = dynamic_threshold.min(bbo_threshold);
        if !threshold.is_finite() || wall_qty + LEVEL_QTY_EPSILON < threshold {
            continue;
        }

        let opposite_side_sweep = match side {
            BookSide::Bid => cvd_5s > LEVEL_QTY_EPSILON,
            BookSide::Ask => cvd_5s < -LEVEL_QTY_EPSILON,
        };

        if !opposite_side_sweep {
            continue;
        }

        let wall_component = ((wall_qty / threshold) - 1.0).clamp(0.0, 2.0) / 2.0;
        let cancel_component = ((cancel_ratio - SPOOF_CANCEL_RATIO_THRESHOLD)
            / (1.0 - SPOOF_CANCEL_RATIO_THRESHOLD))
            .clamp(0.0, 1.0);
        let cvd_component = (cvd_5s.abs() / (1.0 + avg_bbo_size)).clamp(0.0, 1.0);

        let score = (10.0 * ((0.45 * wall_component) + (0.45 * cancel_component) + (0.10 * cvd_component)))
            .min(10.0);

        if score > best_signal.score {
            let side_reason = match side {
                BookSide::Bid => "bid_wall_cancel_after_buy_sweep",
                BookSide::Ask => "ask_wall_cancel_after_sell_sweep",
            };

            let threshold_reason = if dynamic_threshold <= bbo_threshold {
                "mu2sigma"
            } else {
                "3x_bbo"
            };

            best_signal = SpoofSignal {
                flag: true,
                score,
                reason_code: format!("{}_{}", side_reason, threshold_reason),
            };
        }
    }

    best_signal
}

fn align_spoof_signal_with_mofi(spoof_signal: SpoofSignal, m_ofi_top5: f64) -> SpoofSignal {
    if !spoof_signal.flag || m_ofi_top5.abs() <= MOFI_SIGN_EPSILON {
        return spoof_signal;
    }

    let reason = spoof_signal.reason_code.as_str();

    if reason.starts_with("ask_wall_cancel_after_sell_sweep") && m_ofi_top5 < 0.0 {
        return SpoofSignal::default();
    }

    if reason.starts_with("bid_wall_cancel_after_buy_sweep") && m_ofi_top5 > 0.0 {
        return SpoofSignal::default();
    }

    spoof_signal
}

fn build_signal_metric(
    orderbook: &LocalOrderBook,
    event: &DepthEvent,
    ofi_l1: f64,
    m_ofi_top5: f64,
    stale_state: bool,
    trade_alignment_forced_open: bool,
    spoof_signal: SpoofSignal,
    cvd_5s: f64,
    cvd_1m: f64,
    cvd_5m: f64,
    symbol: &str,
) -> Option<SignalMetric> {
    let (best_bid, best_bid_qty) = orderbook.best_bid()?;
    let (best_ask, best_ask_qty) = orderbook.best_ask()?;
    if best_bid <= 0.0 || best_ask <= 0.0 {
        return None;
    }

    let mid_price = (best_bid + best_ask) / 2.0;
    if mid_price <= 0.0 {
        return None;
    }

    let spread_bps = ((best_ask - best_bid) / mid_price) * 10_000.0;

    let bid_band_min_price = best_bid * 0.95;
    let ask_band_max_price = best_ask * 1.05;

    let bid_notional_5pct = orderbook.bid_notional_in_band(bid_band_min_price);
    let ask_notional_5pct = orderbook.ask_notional_in_band(ask_band_max_price);

    let total_notional = bid_notional_5pct + ask_notional_5pct;
    let imbalance_5pct = if total_notional > 0.0 {
        (bid_notional_5pct - ask_notional_5pct) / total_notional
    } else {
        0.0
    };

    let microprice_denominator = best_bid_qty + best_ask_qty;
    let microprice = if microprice_denominator > 0.0 {
        ((best_ask * best_bid_qty) + (best_bid * best_ask_qty)) / microprice_denominator
    } else {
        mid_price
    };

    let (vwaa_l1_bid_ms, vwaa_l1_ask_ms) = orderbook.vwaa_l1(event.payload.event_time_ms);
    let (vwaa_top5_bid_ms, vwaa_top5_ask_ms) =
        orderbook.vwaa_top_n(event.payload.event_time_ms, MOFI_DEPTH_LEVELS);
    let vwaa_l1_imbalance = LocalOrderBook::signed_imbalance(vwaa_l1_bid_ms, vwaa_l1_ask_ms);
    let vwaa_top5_imbalance =
        LocalOrderBook::signed_imbalance(vwaa_top5_bid_ms, vwaa_top5_ask_ms);
    let spoof_signal = align_spoof_signal_with_mofi(spoof_signal, m_ofi_top5);

    Some(SignalMetric {
        schema_version: 1,
        exchange: "binance".to_string(),
        market: "coin-m-futures".to_string(),
        symbol: symbol.to_string(),
        event_id: format!("binance:coin-m-futures:{}:{}", symbol, event.payload.final_update_id),
        event_ts_ms: event.payload.event_time_ms,
        recv_ts_ms: event.recv_ts_ms,
        stale_state,
        trade_alignment_forced_open,
        best_bid,
        best_ask,
        mid_price,
        spread_bps,
        bid_notional_5pct,
        ask_notional_5pct,
        imbalance_5pct,
        microprice,
        ofi_l1,
        m_ofi_top5,
        vwaa_l1_bid_ms,
        vwaa_l1_ask_ms,
        vwaa_l1_imbalance,
        vwaa_top5_bid_ms,
        vwaa_top5_ask_ms,
        vwaa_top5_imbalance,
        cvd_5s,
        cvd_1m,
        cvd_5m,
        spoof_flag: spoof_signal.flag,
        spoof_score: spoof_signal.score,
        spoof_reason_code: spoof_signal.reason_code,
        depth_update_u: event.payload.final_update_id,
        depth_update_pu: event.payload.prev_final_update_id,
        ingest_to_signal_ms: event.recv_instant.elapsed().as_millis() as u64,
    })
}

async fn emit_metric(
    orderbook: &LocalOrderBook,
    event: &DepthEvent,
    previous_levels: &TopBookLevels,
    stale_state: bool,
    trade_alignment_forced_open: bool,
    spoof_signal: SpoofSignal,
    cvd_5s: f64,
    cvd_1m: f64,
    cvd_5m: f64,
    metrics_tx: &mpsc::Sender<SignalMetric>,
    signal_tx: &broadcast::Sender<SignalMetric>,
    telemetry_tx: &mpsc::Sender<TelemetryEvent>,
    symbol: &str,
) {
    let current_levels = orderbook.top_levels(MOFI_DEPTH_LEVELS);
    let ofi_l1 = compute_l1_ofi(previous_levels, &current_levels);
    let m_ofi_top5 = compute_m_ofi(previous_levels, &current_levels, MOFI_DEPTH_LEVELS);

    let Some(metric) = build_signal_metric(
        orderbook,
        event,
        ofi_l1,
        m_ofi_top5,
        stale_state,
        trade_alignment_forced_open,
        spoof_signal,
        cvd_5s,
        cvd_1m,
        cvd_5m,
        symbol,
    ) else {
        return;
    };
    let ingest_to_signal_ms = metric.ingest_to_signal_ms;

    if let Err(error) = metrics_tx.send(metric.clone()).await {
        warn!(%error, "metrics channel closed while publishing metric");
    }

    if signal_tx.send(metric).is_err() {
        debug!("no in-process signal listeners are currently attached");
    }

    send_telemetry(
        telemetry_tx,
        TelemetryEvent::SignalEmitted {
            ingest_to_signal_ms,
            stale_state,
        },
    )
    .await;
}

async fn send_telemetry(telemetry_tx: &mpsc::Sender<TelemetryEvent>, event: TelemetryEvent) {
    let _ = telemetry_tx.send(event).await;
}

fn compute_l1_ofi(previous: &TopBookLevels, current: &TopBookLevels) -> f64 {
    let previous_bid = previous.bids.first().copied();
    let current_bid = current.bids.first().copied();
    let previous_ask = previous.asks.first().copied();
    let current_ask = current.asks.first().copied();

    let delta_bid = compute_bid_flow_delta(previous_bid, current_bid);
    let delta_ask = compute_ask_flow_delta(previous_ask, current_ask);
    delta_bid - delta_ask
}

fn compute_m_ofi(previous: &TopBookLevels, current: &TopBookLevels, depth: usize) -> f64 {
    let previous_bid_levels = previous.bids.iter().take(depth).copied().collect::<Vec<_>>();
    let current_bid_levels = current.bids.iter().take(depth).copied().collect::<Vec<_>>();
    let previous_ask_levels = previous.asks.iter().take(depth).copied().collect::<Vec<_>>();
    let current_ask_levels = current.asks.iter().take(depth).copied().collect::<Vec<_>>();

    let delta_bid = compute_price_aligned_flow_delta(&previous_bid_levels, &current_bid_levels);
    let delta_ask = compute_price_aligned_flow_delta(&previous_ask_levels, &current_ask_levels);

    delta_bid - delta_ask
}

fn compute_price_aligned_flow_delta(previous_levels: &[(f64, f64)], current_levels: &[(f64, f64)]) -> f64 {
    let mut previous_by_price = BTreeMap::new();
    let mut current_by_price = BTreeMap::new();

    for (price, qty) in previous_levels {
        if *qty > LEVEL_QTY_EPSILON {
            previous_by_price.insert(OrderedFloat(*price), *qty);
        }
    }

    for (price, qty) in current_levels {
        if *qty > LEVEL_QTY_EPSILON {
            current_by_price.insert(OrderedFloat(*price), *qty);
        }
    }

    let mut total = 0.0;

    for (price, current_qty) in &current_by_price {
        let previous_qty = previous_by_price.get(price).copied().unwrap_or(0.0);
        total += *current_qty - previous_qty;
    }

    for (price, previous_qty) in &previous_by_price {
        if !current_by_price.contains_key(price) {
            total -= *previous_qty;
        }
    }

    total
}

fn compute_bid_flow_delta(previous: Option<(f64, f64)>, current: Option<(f64, f64)>) -> f64 {
    match (previous, current) {
        (None, None) => 0.0,
        (None, Some((_, current_qty))) => current_qty,
        (Some((_, previous_qty)), None) => -previous_qty,
        (Some((previous_price, previous_qty)), Some((current_price, current_qty))) => {
            if current_price > previous_price + OFI_PRICE_EPSILON {
                current_qty
            } else if (current_price - previous_price).abs() <= OFI_PRICE_EPSILON {
                current_qty - previous_qty
            } else {
                -previous_qty
            }
        }
    }
}

fn compute_ask_flow_delta(previous: Option<(f64, f64)>, current: Option<(f64, f64)>) -> f64 {
    match (previous, current) {
        (None, None) => 0.0,
        (None, Some((_, current_qty))) => current_qty,
        (Some((_, previous_qty)), None) => -previous_qty,
        (Some((previous_price, previous_qty)), Some((current_price, current_qty))) => {
            if current_price < previous_price - OFI_PRICE_EPSILON {
                current_qty
            } else if (current_price - previous_price).abs() <= OFI_PRICE_EPSILON {
                current_qty - previous_qty
            } else {
                -previous_qty
            }
        }
    }
}

async fn fetch_snapshot_with_retry(config: &AppConfig, http_client: &Client) -> Result<DepthSnapshot> {
    let mut attempts = 0_u8;

    loop {
        attempts = attempts.saturating_add(1);

        match fetch_depth_snapshot(config, http_client).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) if attempts < 5 => {
                warn!(attempts, %error, "snapshot fetch failed; retrying shortly");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => {
                return Err(error).context("snapshot fetch exhausted retry budget");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::types::{AggTrade, AggTradeEvent, CancelHeuristic, DepthUpdate, UnifiedEvent};

    use super::{
        AggTradeMatcher, BookSide, compute_l1_ofi, compute_m_ofi, DepthEvent, DepthSnapshot,
        CvdTracker, EngineState, LevelState, LocalOrderBook, ProcessOutcome, QueueDecreaseKind,
        ReorderBuffer, ResyncReason, SpoofSignal, TopBookLevels, align_spoof_signal_with_mofi,
    };

    fn test_snapshot(last_update_id: u64) -> DepthSnapshot {
        DepthSnapshot {
            symbol: "BTCUSD_PERP".to_string(),
            last_update_id,
            bids: vec![(100.0, 2.0), (99.5, 1.0)],
            asks: vec![(101.0, 2.0), (101.5, 1.0)],
        }
    }

    fn test_event(
        first_update_id: u64,
        final_update_id: u64,
        prev_final_update_id: u64,
        bids: Vec<[&str; 2]>,
        asks: Vec<[&str; 2]>,
    ) -> DepthEvent {
        DepthEvent {
            payload: DepthUpdate {
                event_type: "depthUpdate".to_string(),
                event_time_ms: final_update_id as i64,
                transaction_time_ms: Some(final_update_id as i64),
                symbol: "BTCUSD_PERP".to_string(),
                pair: Some("BTCUSD".to_string()),
                first_update_id,
                final_update_id,
                prev_final_update_id,
                bids: bids
                    .into_iter()
                    .map(|[price, qty]| [price.to_string(), qty.to_string()])
                    .collect(),
                asks: asks
                    .into_iter()
                    .map(|[price, qty]| [price.to_string(), qty.to_string()])
                    .collect(),
            },
            recv_ts_ms: 1,
            recv_instant: Instant::now(),
        }
    }

    fn test_agg_trade_event(event_ts_ms: i64) -> AggTradeEvent {
        AggTradeEvent {
            payload: AggTrade {
                event_type: "aggTrade".to_string(),
                event_time_ms: event_ts_ms,
                trade_time_ms: event_ts_ms,
                symbol: "BTCUSD_PERP".to_string(),
                pair: Some("BTCUSD".to_string()),
                aggregate_trade_id: event_ts_ms as u64,
                price: "100.0".to_string(),
                quantity: "1.0".to_string(),
                first_trade_id: event_ts_ms as u64,
                last_trade_id: event_ts_ms as u64,
                buyer_is_maker: true,
            },
            recv_ts_ms: event_ts_ms,
            recv_instant: Instant::now(),
            raw_json: "{}".to_string(),
        }
    }

    fn sync_engine(state: &mut EngineState) {
        let bridge = test_event(100, 101, 99, vec![["100.0", "2.5"]], vec![]);
        let outcome = state.process_depth_event(&bridge, 0.0);
        assert!(matches!(
            outcome,
            ProcessOutcome::EmitMetric {
                stale_state: false,
                ..
            }
        ));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);
    }

    #[test]
    fn fault_injection_missed_bridge_requires_resync() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        let event = test_event(105, 106, 104, vec![["100.0", "1.0"]], vec![]);

        let outcome = state.process_depth_event(&event, 0.0);

        assert!(matches!(
            outcome,
            ProcessOutcome::Resync {
                reason: ResyncReason::MissedBridgeEvent
            }
        ));
        assert!(!state.is_synced);
        assert_eq!(state.last_update_id, 100);
    }

    #[test]
    fn fault_injection_out_of_order_update_is_ignored() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        sync_engine(&mut state);

        let out_of_order = test_event(100, 101, 100, vec![["99.0", "1.2"]], vec![]);
        let outcome = state.process_depth_event(&out_of_order, 0.0);

        assert!(matches!(outcome, ProcessOutcome::Ignore));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);
    }

    #[test]
    fn fault_injection_disconnect_gap_triggers_resync() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        sync_engine(&mut state);

        let gap_event = test_event(102, 103, 99, vec![["100.0", "3.0"]], vec![]);
        let outcome = state.process_depth_event(&gap_event, 0.0);

        assert!(matches!(
            outcome,
            ProcessOutcome::Resync {
                reason: ResyncReason::SequenceGap
            }
        ));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);
    }

    #[test]
    fn fault_injection_snapshot_mismatch_triggers_resync() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        sync_engine(&mut state);

        let crossed_book_event = test_event(102, 102, 101, vec![["102.5", "1.0"]], vec![]);
        let outcome = state.process_depth_event(&crossed_book_event, 0.0);

        assert!(matches!(
            outcome,
            ProcessOutcome::Resync {
                reason: ResyncReason::DepthApplyFailed { .. }
            }
        ));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);
    }

    #[test]
    fn replace_snapshot_resets_sync_state() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        sync_engine(&mut state);

        let replacement = DepthSnapshot {
            symbol: "BTCUSD_PERP".to_string(),
            last_update_id: 250,
            bids: vec![(200.0, 1.0)],
            asks: vec![(201.0, 1.0)],
        };

        state.replace_snapshot(&replacement);

        assert!(!state.is_synced);
        assert_eq!(state.last_update_id, 250);
        assert_eq!(state.orderbook.best_bid().map(|(price, _)| price), Some(200.0));
        assert_eq!(state.orderbook.best_ask().map(|(price, _)| price), Some(201.0));
    }

    #[test]
    fn l1_ofi_matches_piecewise_definition() {
        let previous = TopBookLevels {
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 3.0)],
        };
        let current = TopBookLevels {
            bids: vec![(100.5, 5.0)],
            asks: vec![(101.5, 1.0)],
        };

        // Bid improved: +Qb(t)=+5.0. Ask worsened: -Qa(t-1)=-3.0. OFI=5 - (-3)=8.
        let ofi = compute_l1_ofi(&previous, &current);
        assert_eq!(ofi, 8.0);
    }

    #[test]
    fn m_ofi_top_levels_aggregates_per_level_flows() {
        let previous = TopBookLevels {
            bids: vec![(100.0, 2.0), (99.5, 4.0)],
            asks: vec![(101.0, 3.0), (101.5, 2.0)],
        };
        let current = TopBookLevels {
            bids: vec![(100.0, 5.0), (99.0, 1.0)],
            asks: vec![(101.0, 1.0), (102.0, 2.0)],
        };

        // Price-aligned bid deltas: 100:+3, 99:+1, 99.5:-4 => 0
        // Price-aligned ask deltas: 101:-2, 102:+2, 101.5:-2 => -2
        // Total M-OFI over top 2 levels = 0 - (-2) = +2
        let m_ofi = compute_m_ofi(&previous, &current, 2);
        assert_eq!(m_ofi, 2.0);
    }

    #[test]
    fn m_ofi_price_alignment_avoids_index_shift_phantom_flow() {
        let previous = TopBookLevels {
            bids: vec![(100.0, 5.0), (99.0, 10.0), (98.0, 10.0)],
            asks: vec![(101.0, 4.0), (102.0, 4.0), (103.0, 4.0)],
        };
        let current = TopBookLevels {
            bids: vec![(101.0, 2.0), (100.0, 5.0), (99.0, 10.0), (98.0, 10.0)],
            asks: vec![(101.0, 4.0), (102.0, 4.0), (103.0, 4.0)],
        };

        // A new bid at 101 enters while existing price levels keep their size.
        // Price-aligned M-OFI should only register the +2 addition.
        let m_ofi = compute_m_ofi(&previous, &current, 4);
        assert_eq!(m_ofi, 2.0);
    }

    #[test]
    fn cvd_tracker_maintains_running_window_sums() {
        let mut tracker = CvdTracker::new();
        tracker.record(1_000, 2.0);
        tracker.record(2_000, -1.0);
        tracker.record(58_000, 3.0);
        tracker.record(62_000, -2.0);

        let (cvd_5s, cvd_1m, cvd_5m) = tracker.values(62_000);
        assert!((cvd_5s - 1.0).abs() < 1e-9);
        assert!(cvd_1m.abs() < 1e-9);
        assert!((cvd_5m - 2.0).abs() < 1e-9);
    }

    #[test]
    fn execution_decrease_consumes_from_front() {
        let mut level = LevelState::from_single_chunk(2.0, 10);
        level.push_chunk(3.0, 20);

        level.apply_decrease(2.5, QueueDecreaseKind::Execution, CancelHeuristic::Lifo);

        assert_eq!(level.queue.len(), 1);
        assert!((level.queue[0].size - 2.5).abs() < 1e-9);
        assert_eq!(level.queue[0].added_at_exchange_ts, 20);
        assert!((level.total_qty - 2.5).abs() < 1e-9);
    }

    #[test]
    fn cancel_decrease_lifo_consumes_from_back() {
        let mut level = LevelState::from_single_chunk(2.0, 10);
        level.push_chunk(3.0, 20);

        level.apply_decrease(2.5, QueueDecreaseKind::Cancel, CancelHeuristic::Lifo);

        assert_eq!(level.queue.len(), 2);
        assert!((level.queue[0].size - 2.0).abs() < 1e-9);
        assert!((level.queue[1].size - 0.5).abs() < 1e-9);
        assert!((level.total_qty - 2.5).abs() < 1e-9);
    }

    #[test]
    fn cancel_decrease_pro_rata_scales_queue() {
        let mut level = LevelState::from_single_chunk(2.0, 10);
        level.push_chunk(3.0, 20);

        level.apply_decrease(1.0, QueueDecreaseKind::Cancel, CancelHeuristic::ProRata);

        assert_eq!(level.queue.len(), 2);
        assert!((level.queue[0].size - 1.6).abs() < 1e-9);
        assert!((level.queue[1].size - 2.4).abs() < 1e-9);
        assert!((level.total_qty - 4.0).abs() < 1e-9);
    }

    #[test]
    fn deep_levels_fallback_to_single_chunk_queue() {
        let bids = (0..25)
            .map(|idx| (100.0 - idx as f64, 1.0))
            .collect::<Vec<_>>();
        let asks = (0..25)
            .map(|idx| (101.0 + idx as f64, 1.0))
            .collect::<Vec<_>>();
        let snapshot = DepthSnapshot {
            symbol: "BTCUSD_PERP".to_string(),
            last_update_id: 100,
            bids,
            asks,
        };

        let mut book = LocalOrderBook::from_snapshot(&snapshot, CancelHeuristic::Lifo);
        let update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 123,
            transaction_time_ms: Some(123),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 101,
            final_update_id: 101,
            prev_final_update_id: 100,
            bids: vec![
                ["95.0".to_string(), "3.0".to_string()],
                ["76.0".to_string(), "3.0".to_string()],
            ],
            asks: Vec::new(),
        };

        book.apply_depth_update(&update).expect("depth update should apply");

        let near_level = book.bids.get(&ordered_float::OrderedFloat(95.0)).expect("near bid level present");
        let deep_level = book.bids.get(&ordered_float::OrderedFloat(76.0)).expect("deep bid level present");

        assert!(near_level.queue.len() >= 2);
        assert_eq!(deep_level.queue.len(), 1);
    }

    #[test]
    fn matched_execution_is_consumed_before_cancel() {
        let snapshot = DepthSnapshot {
            symbol: "BTCUSD_PERP".to_string(),
            last_update_id: 100,
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 2.0)],
        };

        let mut book = LocalOrderBook::from_snapshot(&snapshot, CancelHeuristic::Lifo);
        let increase_update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 1000,
            transaction_time_ms: Some(1000),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 101,
            final_update_id: 101,
            prev_final_update_id: 100,
            bids: vec![["100.0".to_string(), "5.0".to_string()]],
            asks: Vec::new(),
        };
        book.apply_depth_update(&increase_update).expect("increase update should apply");

        let mut matcher = AggTradeMatcher::new(100);
        matcher.record_trade(BookSide::Bid, 100.0, 2.0, 1040);

        let decrease_update = DepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 1050,
            transaction_time_ms: Some(1050),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 102,
            final_update_id: 102,
            prev_final_update_id: 101,
            bids: vec![["100.0".to_string(), "3.0".to_string()]],
            asks: Vec::new(),
        };

        let (bid_plan, ask_plan) = book
            .build_decrease_plans(&decrease_update, &mut matcher)
            .expect("decrease plan should build");
        let breakdown = bid_plan
            .get(&ordered_float::OrderedFloat(100.0))
            .expect("breakdown should exist");
        assert!((breakdown.execution_qty - 2.0).abs() < 1e-9);

        book.apply_depth_update_with_plans(&decrease_update, &bid_plan, &ask_plan)
            .expect("decrease update should apply");

        let level = book
            .bids
            .get(&ordered_float::OrderedFloat(100.0))
            .expect("bid level should remain");
        assert_eq!(level.queue.len(), 1);
        assert!((level.queue[0].size - 3.0).abs() < 1e-9);
    }

    #[test]
    fn weighted_age_math_is_volume_weighted() {
        let mut level = LevelState::from_single_chunk(2.0, 1_000);
        level.push_chunk(3.0, 2_000);

        let vwaa_ms = level.weighted_average_age_ms(4_000);
        assert!((vwaa_ms - 2_400.0).abs() < 1e-9);
    }

    #[test]
    fn coin_m_notional_uses_contract_face_value_units() {
        let snapshot = DepthSnapshot {
            symbol: "BTCUSD_PERP".to_string(),
            last_update_id: 1,
            bids: vec![(100.0, 2.0), (99.0, 3.0)],
            asks: vec![(101.0, 1.0), (102.0, 4.0)],
        };

        let book = LocalOrderBook::from_snapshot(&snapshot, CancelHeuristic::Lifo);

        assert!((book.bid_notional_in_band(95.0) - 500.0).abs() < 1e-9);
        assert!((book.ask_notional_in_band(200.0) - 500.0).abs() < 1e-9);
    }

    #[test]
    fn spoof_signal_is_suppressed_when_mofi_sign_conflicts() {
        let ask_cancel_signal = SpoofSignal {
            flag: true,
            score: 7.0,
            reason_code: "ask_wall_cancel_after_sell_sweep_mu2sigma".to_string(),
        };

        let suppressed = align_spoof_signal_with_mofi(ask_cancel_signal, -25.0);
        assert!(!suppressed.flag);
        assert_eq!(suppressed.score, 0.0);
        assert_eq!(suppressed.reason_code, "none");

        let bid_cancel_signal = SpoofSignal {
            flag: true,
            score: 5.0,
            reason_code: "bid_wall_cancel_after_buy_sweep_3x_bbo".to_string(),
        };

        let kept = align_spoof_signal_with_mofi(bid_cancel_signal.clone(), -10.0);
        assert!(kept.flag);
        assert_eq!(kept.reason_code, bid_cancel_signal.reason_code);
    }

    #[test]
    fn spoof_sequence_trigger_fires_on_wall_pull_with_opposite_sweep() {
        let mut state = EngineState::new(&test_snapshot(100), CancelHeuristic::Lifo);
        sync_engine(&mut state);

        for update_id in 102..=107 {
            let baseline = test_event(
                update_id,
                update_id,
                update_id.saturating_sub(1),
                vec![["100.0", "2.0"]],
                vec![],
            );

            let outcome = state.process_depth_event(&baseline, 0.0);
            assert!(matches!(outcome, ProcessOutcome::EmitMetric { .. }));
        }

        let wall_add = test_event(108, 108, 107, vec![["100.0", "12.0"]], vec![]);
        let wall_add_outcome = state.process_depth_event(&wall_add, 0.0);
        assert!(matches!(
            wall_add_outcome,
            ProcessOutcome::EmitMetric {
                stale_state: false,
                ..
            }
        ));

        let wall_pull = test_event(109, 109, 108, vec![["100.0", "1.0"]], vec![]);
        let wall_pull_outcome = state.process_depth_event(&wall_pull, 5.0);

        match wall_pull_outcome {
            ProcessOutcome::EmitMetric {
                stale_state: false,
                spoof_signal,
            } => {
                assert!(spoof_signal.flag);
                assert!(spoof_signal.score > 0.0);
                assert!(
                    spoof_signal
                        .reason_code
                        .starts_with("bid_wall_cancel_after_buy_sweep")
                );
            }
            other => panic!("expected emitted metric, got {:?}", other),
        }
    }

    #[test]
    fn reorder_buffer_orders_out_of_order_unified_events() {
        let mut reorder = ReorderBuffer::new(20, 1_000, 20, 0);

        let arrival_a = reorder.push(UnifiedEvent::Depth(test_event(120, 120, 119, vec![], vec![])));
        assert!(!arrival_a.out_of_order);

        let arrival_b =
            reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(90)));
        assert!(arrival_b.out_of_order);

        let ready = reorder.drain_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].event.exchange_ts_ms(), 90);
        assert!(!ready[0].trade_alignment_forced_open);

        let _ = reorder.push(UnifiedEvent::Depth(test_event(140, 140, 139, vec![], vec![])));
        let ready_2 = reorder.drain_ready();
        assert_eq!(ready_2.len(), 1);
        assert_eq!(ready_2[0].event.exchange_ts_ms(), 120);
        assert!(matches!(ready_2[0].event, UnifiedEvent::Depth(_)));
        assert!(ready_2[0].trade_alignment_forced_open);
    }

    #[test]
    fn reorder_buffer_prioritizes_trade_before_depth_at_same_timestamp() {
        let mut reorder = ReorderBuffer::new(20, 1_000, 100, 0);

        let _ = reorder.push(UnifiedEvent::Depth(test_event(120, 120, 119, vec![], vec![])));
        let _ = reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(120)));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(141, 141, 140, vec![], vec![])));

        let ready = reorder.drain_ready();
        assert_eq!(ready.len(), 2);
        assert!(matches!(ready[0].event, UnifiedEvent::AggTrade(_)));
        assert!(matches!(ready[1].event, UnifiedEvent::Depth(_)));
        assert!(!ready[1].trade_alignment_forced_open);
    }

    #[test]
    fn reorder_buffer_holds_depth_until_aggtrade_watermark_catches_up() {
        let mut reorder = ReorderBuffer::new(20, 1_000, 100, 0);

        let _ = reorder.push(UnifiedEvent::Depth(test_event(120, 120, 119, vec![], vec![])));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(141, 141, 140, vec![], vec![])));

        let ready = reorder.drain_ready();
        assert!(ready.is_empty());

        let _ = reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(120)));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(160, 160, 159, vec![], vec![])));

        let ready_after_trade = reorder.drain_ready();
        assert_eq!(ready_after_trade.len(), 2);
        assert!(matches!(ready_after_trade[0].event, UnifiedEvent::AggTrade(_)));
        assert!(matches!(ready_after_trade[1].event, UnifiedEvent::Depth(_)));
        assert!(!ready_after_trade[1].trade_alignment_forced_open);
    }

    #[test]
    fn reorder_buffer_waits_for_execution_lookahead_watermark() {
        let mut reorder = ReorderBuffer::new(20, 1_000, 100, 25);

        let _ = reorder.push(UnifiedEvent::Depth(test_event(120, 120, 119, vec![], vec![])));
        let _ = reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(120)));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(141, 141, 140, vec![], vec![])));

        let first_ready = reorder.drain_ready();
        assert_eq!(first_ready.len(), 1);
        assert!(matches!(first_ready[0].event, UnifiedEvent::AggTrade(_)));

        let _ = reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(145)));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(200, 200, 199, vec![], vec![])));

        let second_ready = reorder.drain_ready();
        assert!(second_ready.iter().any(|event| matches!(event.event, UnifiedEvent::AggTrade(_))));
        assert!(!second_ready.iter().any(|event| matches!(event.event, UnifiedEvent::Depth(_))));

        let third_ready = reorder.drain_ready();
        assert_eq!(third_ready.len(), 1);
        assert!(matches!(third_ready[0].event, UnifiedEvent::Depth(_)));
        assert!(!third_ready[0].trade_alignment_forced_open);
    }

    #[test]
    fn reorder_buffer_forces_open_depth_when_trade_alignment_lag_exceeded() {
        let mut reorder = ReorderBuffer::new(20, 1_000, 30, 0);

        let _ = reorder.push(UnifiedEvent::AggTrade(test_agg_trade_event(110)));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(120, 120, 119, vec![], vec![])));
        let _ = reorder.push(UnifiedEvent::Depth(test_event(200, 200, 199, vec![], vec![])));

        let ready = reorder.drain_ready();
        assert_eq!(ready.len(), 2);
        assert!(matches!(ready[0].event, UnifiedEvent::AggTrade(_)));
        assert!(matches!(ready[1].event, UnifiedEvent::Depth(_)));
        assert!(ready[1].trade_alignment_forced_open);
    }
}
