const OFI_PRICE_EPSILON: f64 = 1e-9;
const LEVEL_QTY_EPSILON: f64 = 1e-9;
const MOFI_SIGN_EPSILON: f64 = 1e-6;
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
    contract_type: ContractType,
}

impl LocalOrderBook {
    pub fn from_snapshot(
        snapshot: &DepthSnapshot,
        cancel_heuristic: CancelHeuristic,
        contract_type: ContractType,
    ) -> Self {
        let mut book = Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            cancel_heuristic,
            contract_type,
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
    pub fn apply_depth_update(&mut self, update: &NormalizedDepthUpdate) -> Result<()> {
        let empty_bid_plan = DecreasePlan::new();
        let empty_ask_plan = DecreasePlan::new();
        self.apply_depth_update_with_plans(update, &empty_bid_plan, &empty_ask_plan)
    }

    fn apply_depth_update_with_plans(
        &mut self,
        update: &NormalizedDepthUpdate,
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
        update: &NormalizedDepthUpdate,
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
            total += self
                .contract_type
                .quote_notional(price, level_state.total_qty);
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
            total += self
                .contract_type
                .quote_notional(price, level_state.total_qty);
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
    market_category: MarketCategory,
    last_update_id: u64,
    is_synced: bool,
}

impl EngineState {
    fn new(
        snapshot: &DepthSnapshot,
        cancel_heuristic: CancelHeuristic,
        contract_type: ContractType,
        market_category: MarketCategory,
    ) -> Self {
        Self {
            orderbook: LocalOrderBook::from_snapshot(snapshot, cancel_heuristic, contract_type),
            trade_matcher: AggTradeMatcher::new(AGGTRADE_MATCH_WINDOW_MS),
            cvd_tracker: CvdTracker::new(),
            spoof_threshold_tracker: SpoofThresholdTracker::new(SPOOF_THRESHOLD_WINDOW_MS),
            market_category,
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

        let bridge_target = self.last_update_id.saturating_add(1);

        if self.market_category.uses_prev_final_update_id() {
            if event.payload.prev_final_update_id != Some(self.last_update_id) {
                return ProcessOutcome::Resync {
                    reason: ResyncReason::SequenceGap,
                };
            }
        } else if event.payload.first_update_id > bridge_target {
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

