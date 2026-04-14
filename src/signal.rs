use tokio::sync::broadcast;

use crate::types::SignalMetric;

pub type SignalSender = broadcast::Sender<SignalMetric>;
pub type SignalReceiver = broadcast::Receiver<SignalMetric>;

pub fn channel(capacity: usize) -> (SignalSender, SignalReceiver) {
    broadcast::channel(capacity)
}
