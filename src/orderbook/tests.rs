#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::config::{ContractType, MarketCategory};
    use crate::types::{
        AggTradeEvent, CancelHeuristic, NormalizedAggTrade, NormalizedDepthUpdate, UnifiedEvent,
    };

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
            payload: NormalizedDepthUpdate {
                event_type: "depthUpdate".to_string(),
                event_time_ms: final_update_id as i64,
                transaction_time_ms: Some(final_update_id as i64),
                symbol: "BTCUSD_PERP".to_string(),
                pair: Some("BTCUSD".to_string()),
                first_update_id,
                final_update_id,
                prev_final_update_id: Some(prev_final_update_id),
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

    fn test_spot_event(
        first_update_id: u64,
        final_update_id: u64,
        bids: Vec<[&str; 2]>,
        asks: Vec<[&str; 2]>,
    ) -> DepthEvent {
        DepthEvent {
            payload: NormalizedDepthUpdate {
                event_type: "depthUpdate".to_string(),
                event_time_ms: final_update_id as i64,
                transaction_time_ms: None,
                symbol: "BTCUSD".to_string(),
                pair: None,
                first_update_id,
                final_update_id,
                prev_final_update_id: None,
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
            payload: NormalizedAggTrade {
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
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
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
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
        sync_engine(&mut state);

        let out_of_order = test_event(100, 101, 100, vec![["99.0", "1.2"]], vec![]);
        let outcome = state.process_depth_event(&out_of_order, 0.0);

        assert!(matches!(outcome, ProcessOutcome::Ignore));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);
    }

    #[test]
    fn fault_injection_disconnect_gap_triggers_resync() {
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
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
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
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
    fn spot_sequence_allows_overlap_without_pu() {
        let snapshot = DepthSnapshot {
            symbol: "BTCUSD".to_string(),
            last_update_id: 100,
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 2.0)],
        };

        let mut state = EngineState::new(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Linear,
            MarketCategory::Spot,
        );

        let bridge = test_spot_event(101, 101, vec![["100.0", "2.5"]], vec![]);
        let bridge_outcome = state.process_depth_event(&bridge, 0.0);
        assert!(matches!(
            bridge_outcome,
            ProcessOutcome::EmitMetric {
                stale_state: false,
                ..
            }
        ));
        assert!(state.is_synced);
        assert_eq!(state.last_update_id, 101);

        let overlap = test_spot_event(101, 102, vec![["100.0", "2.2"]], vec![]);
        let overlap_outcome = state.process_depth_event(&overlap, 0.0);
        assert!(matches!(
            overlap_outcome,
            ProcessOutcome::EmitMetric {
                stale_state: false,
                ..
            }
        ));
        assert_eq!(state.last_update_id, 102);
    }

    #[test]
    fn spot_sequence_gap_triggers_resync_without_pu() {
        let snapshot = DepthSnapshot {
            symbol: "BTCUSD".to_string(),
            last_update_id: 100,
            bids: vec![(100.0, 2.0)],
            asks: vec![(101.0, 2.0)],
        };

        let mut state = EngineState::new(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Linear,
            MarketCategory::Spot,
        );

        let bridge = test_spot_event(101, 101, vec![["100.0", "2.5"]], vec![]);
        let bridge_outcome = state.process_depth_event(&bridge, 0.0);
        assert!(matches!(
            bridge_outcome,
            ProcessOutcome::EmitMetric {
                stale_state: false,
                ..
            }
        ));

        let gap = test_spot_event(103, 103, vec![["100.0", "2.3"]], vec![]);
        let gap_outcome = state.process_depth_event(&gap, 0.0);
        assert!(matches!(
            gap_outcome,
            ProcessOutcome::Resync {
                reason: ResyncReason::SequenceGap
            }
        ));
    }

    #[test]
    fn replace_snapshot_resets_sync_state() {
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
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

        let mut book = LocalOrderBook::from_snapshot(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
        );
        let update = NormalizedDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 123,
            transaction_time_ms: Some(123),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 101,
            final_update_id: 101,
            prev_final_update_id: Some(100),
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

        let mut book = LocalOrderBook::from_snapshot(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
        );
        let increase_update = NormalizedDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 1000,
            transaction_time_ms: Some(1000),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 101,
            final_update_id: 101,
            prev_final_update_id: Some(100),
            bids: vec![["100.0".to_string(), "5.0".to_string()]],
            asks: Vec::new(),
        };
        book.apply_depth_update(&increase_update).expect("increase update should apply");

        let mut matcher = AggTradeMatcher::new(100);
        matcher.record_trade(BookSide::Bid, 100.0, 2.0, 1040);

        let decrease_update = NormalizedDepthUpdate {
            event_type: "depthUpdate".to_string(),
            event_time_ms: 1050,
            transaction_time_ms: Some(1050),
            symbol: "BTCUSD_PERP".to_string(),
            pair: Some("BTCUSD".to_string()),
            first_update_id: 102,
            final_update_id: 102,
            prev_final_update_id: Some(101),
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

        let book = LocalOrderBook::from_snapshot(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
        );

        assert!((book.bid_notional_in_band(95.0) - 500.0).abs() < 1e-9);
        assert!((book.ask_notional_in_band(200.0) - 500.0).abs() < 1e-9);
    }

    #[test]
    fn linear_notional_uses_price_times_quantity() {
        let snapshot = DepthSnapshot {
            symbol: "BTCUSDT".to_string(),
            last_update_id: 1,
            bids: vec![(100.0, 2.0), (99.0, 3.0)],
            asks: vec![(101.0, 1.0), (102.0, 4.0)],
        };

        let book = LocalOrderBook::from_snapshot(
            &snapshot,
            CancelHeuristic::Lifo,
            ContractType::Linear,
        );

        assert!((book.bid_notional_in_band(95.0) - 497.0).abs() < 1e-9);
        assert!((book.ask_notional_in_band(200.0) - 509.0).abs() < 1e-9);
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
        let mut state = EngineState::new(
            &test_snapshot(100),
            CancelHeuristic::Lifo,
            ContractType::Inverse { contract_size: 100.0 },
            MarketCategory::CoinM,
        );
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
