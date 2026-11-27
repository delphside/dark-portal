//! The item fetch rate: no two requests start closer than the rate allows.

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::collect::Clock;
use crate::settings::collection::ITEM_FETCH_RATE_PER_SECOND;

/// The least time between the starts of two item requests: one second over
/// the item fetch rate.
pub fn interval() -> Duration {
    Duration::from_secs(1) / ITEM_FETCH_RATE_PER_SECOND
}

/// Spaces requests by an interval, by the injected clock.
#[derive(Debug)]
pub struct Pacer {
    interval: Duration,
    /// The earliest the next request may start.
    next: Option<DateTime<Utc>>,
}

impl Pacer {
    /// A pacer at the item fetch rate.
    pub fn new() -> Pacer {
        Pacer::every(interval())
    }

    /// A pacer that spaces requests by `interval`.
    pub fn every(interval: Duration) -> Pacer {
        Pacer {
            interval,
            next: None,
        }
    }

    /// Waits until a request may start, and counts it as started. The first
    /// request is not delayed.
    pub fn wait(&mut self, clock: &dyn Clock) {
        let now = clock.now();
        if let Some(next) = self.next
            && now < next
        {
            clock.sleep((next - now).to_std().unwrap_or_default());
        }
        let started = clock.now();
        self.next = Some(
            started + chrono::Duration::from_std(self.interval).unwrap_or(chrono::Duration::MAX),
        );
    }
}

impl Default for Pacer {
    fn default() -> Pacer {
        Pacer::new()
    }
}
