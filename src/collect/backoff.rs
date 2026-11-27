//! How long a run waits after a blip: the one place the backoff and
//! `Retry-After` rules are.

use std::time::Duration;

use crate::settings::collection::{RETRY_BACKOFF_INITIAL, RETRY_BACKOFF_MAX};

/// The least a `Retry-After` is waited, so a `0` is not a hot loop. An
/// implementation guard, not a setting: Blizzard's own figure is the wait.
const MIN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// The wait after each blip: the backoff setting, doubling to its maximum, or
/// Blizzard's `Retry-After` where it gave one.
#[derive(Debug)]
pub(crate) struct Backoff {
    next: Duration,
}

impl Backoff {
    /// At the first wait.
    pub(crate) fn new() -> Backoff {
        Backoff {
            next: RETRY_BACKOFF_INITIAL,
        }
    }

    /// Back to the first wait: Blizzard answered.
    pub(crate) fn reset(&mut self) {
        self.next = RETRY_BACKOFF_INITIAL;
    }

    /// The wait for this blip: `after` where Blizzard gave one (at least
    /// [`MIN_RETRY_AFTER`]), otherwise the backoff. The backoff doubles either
    /// way.
    pub(crate) fn wait(&mut self, after: Option<Duration>) -> Duration {
        let wait = after.map_or(self.next, |a| a.max(MIN_RETRY_AFTER));
        self.next = (self.next * 2).min(RETRY_BACKOFF_MAX);
        wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn the_wait_doubles_from_the_first_to_the_maximum() {
        let mut backoff = Backoff::new();
        let waits: Vec<_> = (0..6).map(|_| backoff.wait(None)).collect();
        assert_eq!(waits, [30, 60, 120, 240, 300, 300].map(secs));
    }

    #[test]
    fn a_retry_after_is_the_wait_and_a_zero_is_one_second() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.wait(Some(secs(7))), secs(7));
        assert_eq!(backoff.wait(Some(secs(0))), secs(1));
        assert_eq!(backoff.wait(Some(secs(3600))), secs(3600));
    }

    #[test]
    fn a_reset_starts_again_at_the_first_wait() {
        let mut backoff = Backoff::new();
        backoff.wait(None);
        backoff.wait(None);
        backoff.reset();
        assert_eq!(backoff.wait(None), secs(30));
    }
}
