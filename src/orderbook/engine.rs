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
    let mut engine = EngineState::new(
        &snapshot,
        config.cancel_heuristic,
        config.contract_type,
        config.market_category,
    );

    let bootstrap_snapshot_ts_ms = now_utc_ms();
    emit_snapshot_event(
        &snapshot_tx,
        build_snapshot_event_from_snapshot(
            &snapshot,
            bootstrap_snapshot_ts_ms,
            SNAPSHOT_SOURCE_REST_BOOTSTRAP,
            &config,
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
                    &config,
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
                        config,
                        config.symbol.as_str(),
                    )
                    .await;

                    if let Err(error) = raw_depth_tx.send(event.clone()).await {
                        warn!(%error, "failed to forward accepted depth delta to archive spool");
                    }

                    maybe_emit_local_keyframe_snapshot(
                        engine,
                        config,
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
                        config,
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
                            config,
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
    config: &AppConfig,
) -> OrderbookSnapshotEvent {
    OrderbookSnapshotEvent {
        schema_version: 1,
        exchange: config.exchange.clone(),
        market: config.market.clone(),
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
    config: &AppConfig,
) -> OrderbookSnapshotEvent {
    let (bids, asks) = orderbook.full_levels();
    OrderbookSnapshotEvent {
        schema_version: 1,
        exchange: config.exchange.clone(),
        market: config.market.clone(),
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
    config: &AppConfig,
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
            config.symbol.as_str(),
            engine.last_update_id,
            event_ts_ms,
            config,
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
    config: &AppConfig,
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
        exchange: config.exchange.clone(),
        market: config.market.clone(),
        symbol: symbol.to_string(),
        event_id: config.depth_event_id(symbol, event.payload.final_update_id),
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
        depth_update_pu: event
            .payload
            .prev_final_update_id
            .unwrap_or_else(|| event.payload.first_update_id.saturating_sub(1)),
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
    config: &AppConfig,
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
        config,
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

