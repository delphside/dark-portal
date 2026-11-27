//! Time, injected: a run asks a [`Clock`] for the time and for a wait, so a
//! test moves time on without sleeping for real minutes.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// The time and the wait a run uses.
pub trait Clock {
    /// The moment now.
    fn now(&self) -> DateTime<Utc>;

    /// Waits for `duration`.
    fn sleep(&self, duration: Duration);
}

/// The host's clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn sleep(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

#[cfg(test)]
pub use fake::FakeClock;

#[cfg(test)]
mod fake {
    use std::sync::Mutex;

    use super::*;

    /// A clock a test owns: waiting moves it on and takes no real time.
    #[derive(Debug)]
    pub struct FakeClock {
        now: Mutex<DateTime<Utc>>,
        slept: Mutex<Vec<Duration>>,
    }

    impl FakeClock {
        /// A clock at `rfc3339`.
        pub fn at(rfc3339: &str) -> FakeClock {
            FakeClock {
                now: Mutex::new(DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()),
                slept: Mutex::new(Vec::new()),
            }
        }

        /// Every wait asked for, in order.
        pub fn slept(&self) -> Vec<Duration> {
            self.slept.lock().unwrap().clone()
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }

        fn sleep(&self, duration: Duration) {
            self.slept.lock().unwrap().push(duration);
            *self.now.lock().unwrap() += duration;
        }
    }
}
