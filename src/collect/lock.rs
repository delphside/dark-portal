//! The VM-wide lock every writing job takes
//! (5.1.1, `docs/5.1.1-architecture.md`): an exclusive `flock` on
//! `DP_LOCK_FILE`, waited for up to the lock wait.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
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
    /// The lock file it is on.
    path: PathBuf,
}

/// How a step that writes under the lock comes by it: it takes it itself, or
/// its caller (`dp hourly`) holds it already and lends it. A second handle on
/// the lock file blocks while the first is held, so a step never takes what
/// its caller holds.
#[derive(Debug, Clone, Copy)]
pub enum Holding<'a> {
    /// The step takes the lock when it writes, and releases it after.
    Take,
    /// The caller holds this lock for the whole step.
    Held(&'a Lock),
}

/// The lock, as a step has it from [`Holding::proof`]: its own, released on
/// drop, or its caller's.
#[derive(Debug)]
pub enum Proof<'a> {
    /// Taken by the step.
    Taken(Lock),
    /// Lent by the caller.
    Lent(&'a Lock),
}

impl<'a> Holding<'a> {
    /// The lock, taken now (waiting up to `wait`) or the one lent. A lent lock
    /// must be on `path`, the configured lock file: another file's lock proves
    /// nothing about this one, and is an *unexpected* failure.
    pub fn proof(
        self,
        path: &Path,
        wait: Duration,
        clock: &dyn Clock,
    ) -> Result<Proof<'a>, Failure> {
        match self {
            Holding::Take => acquire(path, wait, clock).map(Proof::Taken),
            Holding::Held(lock) if lock.path == path => Ok(Proof::Lent(lock)),
            Holding::Held(_) => Err(Failure::Stop(Box::new(Report::fatal(
                ErrorType::Unexpected,
                "LOCK_NOT_THE_LOCK_FILE",
                "The lock lent to the step is on another file than the lock file.",
            )))),
        }
    }
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
            Ok(()) => {
                return Ok(Lock {
                    _file: file,
                    path: path.to_owned(),
                });
            }
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
    fn a_lent_lock_is_not_taken_again_and_a_taken_one_is_released_on_drop() {
        let path = temp("lent");
        let clock = FakeClock::at("2026-11-26T11:40:00Z");
        let held = acquire(&path, Duration::ZERO, &clock).unwrap();
        // Lent: no second handle is opened, so nothing waits.
        let lent = Holding::Held(&held)
            .proof(&path, Duration::from_secs(600), &clock)
            .unwrap();
        assert!(matches!(lent, Proof::Lent(_)));
        assert!(clock.slept().is_empty());
        // Take, while the lock is held, waits out the wait.
        assert!(
            Holding::Take
                .proof(&path, Duration::from_secs(3), &clock)
                .is_err()
        );
        drop(held);
        let taken = Holding::Take.proof(&path, Duration::ZERO, &clock).unwrap();
        assert!(matches!(taken, Proof::Taken(_)));
        assert!(acquire(&path, Duration::ZERO, &clock).is_err());
        drop(taken);
        assert!(acquire(&path, Duration::ZERO, &clock).is_ok());
    }

    #[test]
    fn a_lock_lent_for_another_file_is_refused() {
        let path = temp("lent-right");
        let other = temp("lent-wrong").with_file_name("other.lock");
        let clock = FakeClock::at("2026-11-26T11:40:00Z");
        let held = acquire(&path, Duration::ZERO, &clock).unwrap();
        let Err(Failure::Stop(report)) = Holding::Held(&held).proof(&other, Duration::ZERO, &clock)
        else {
            panic!("expected a stop");
        };
        assert_eq!(report.reason, "LOCK_NOT_THE_LOCK_FILE");
        assert_eq!(exit_status(&report), ExitStatus::Unexpected);
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
