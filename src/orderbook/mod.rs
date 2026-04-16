use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use ordered_float::OrderedFloat;
use reqwest::Client;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::connectors::fetch_depth_snapshot;
use crate::config::{AppConfig, ContractType};
use crate::telemetry::TelemetryEvent;
use crate::types::{
    CancelHeuristic, DepthEvent, DepthSnapshot, NormalizedDepthUpdate, OrderbookSnapshotEvent,
    SignalMetric, UnifiedEvent, VolumeChunk, now_utc_ms,
};

include!("core.rs");
include!("engine.rs");
include!("tests.rs");
