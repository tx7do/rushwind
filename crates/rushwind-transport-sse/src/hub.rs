//! The in-process notification hub: senders publish (userId, json);
//! every SSE connection subscribed filters to its own stream.

use tokio::sync::broadcast;

/// The in-process notification hub: senders publish (userId, json);
/// every SSE connection subscribed filters to its own stream.
#[derive(Clone)]
pub struct Hub {
    tx: broadcast::Sender<(u32, String)>,
}

impl Default for Hub {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }
}

impl Hub {
    /// TryPublish (non-blocking; a slow/full buffer drops for that
    /// receiver only).
    pub fn publish(&self, user_id: u32, payload: String) {
        let _ = self.tx.send((user_id, payload));
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<(u32, String)> {
        self.tx.subscribe()
    }
}
