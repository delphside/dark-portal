//! The VM-wide lock every writing job takes
//! (5.1.1, `docs/5.1.1-architecture.md`): an exclusive `flock` on
//! `DP_LOCK_FILE`, waited for up to the lock wait.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::time::Duration;

use super::clock::Clock;
use super::failure::{Failure, io_failure};
use crate::error::{ErrorType, Report};

/// How often a waiting job tries the lock again. An implementation detail:
/// the wait itself is the lock wait setting.
const RETRY_EVERY: Duration = Duration::from_secs(1);

/// The lock, held until dropped.
#[derive(Debug)]
pub struct Lock {
    _file: File,
}

/// Takes the lock, waiting up to `wait`; a lock still held then is a
/// *technical, blip*, exit `75`.
pub fn acquire(path: &Path, wait: Duration, clock: &dyn Clock) -> Result<Lock, Failure> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .map_err(|e| io_failure(&e, "opening the lock file"))?;
    let deadline = clock.now() + wait;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(Lock { _file: file }),
            Err(TryLockError::WouldBlock) => {
                if clock.now() >= deadline {
                    return Err(Failure::Stop(Box::new(Report::fatal(
                        ErrorType::TechnicalBlip,
                        "LOCK_WAIT_EXPIRED",
                        "Another job held the lock past the lock wait; the scheduler tries again.",
                    ))));
                }
                clock.sleep(RETRY_EVERY);
            }
            Err(TryLockError::Error(e)) => return Err(io_failure(&e, "taking the lock")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::clock::FakeClock;
    use crate::error::{ExitStatus, exit_status};

    fn temp(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("dp-lock-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("dp.lock")
    }

    #[test]
    fn a_free_lock_is_taken_and_released_on_drop() {
        let path = temp("free");
        let clock = FakeClock::at("2026-11-26T11:40:00Z");
        let held = acquire(&path, Duration::from_secs(600), &clock).unwrap();
        assert!(clock.slept().is_empty());
        drop(held);
        assert!(acquire(&path, Duration::from_secs(0), &clock).is_ok());
    }

    #[test]
    fn a_lock_held_past_the_wait_is_a_blip_exit_75() {
        let path = temp("held");
        let clock = FakeClock::at("2026-11-26T11:40:00Z");
        let _held = acquire(&path, Duration::from_secs(600), &clock).unwrap();
        let Err(Failure::Stop(report)) = acquire(&path, Duration::from_secs(600), &clock) else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
        assert_eq!(exit_status(&report), ExitStatus::Blip);
        let waited: Duration = clock.slept().iter().sum();
        assert_eq!(waited, Duration::from_secs(600));
    }

    #[test]
    fn an_unusable_lock_path_is_fundamental() {
        let clock = FakeClock::at("2026-11-26T11:40:00Z");
        let Err(Failure::Stop(report)) = acquire(
            Path::new("/nonexistent-dir/dp.lock"),
            Duration::ZERO,
            &clock,
        ) else {
            panic!("expected a stop");
        };
        assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    }
}
