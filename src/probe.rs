//! `dp probe`: once a day, asks Blizzard when the commodities snapshot was last
//! refreshed and warns where that is newer than the hour before's snapshot, so
//! a move to refreshing more often than hourly is seen
//! (5.2.1 *When Blizzard's response changes*, `docs/5.2.1-collection.md`).
//!
//! The hourly poll cannot see it: it runs at :35 and always meets the :31
//! refresh. The probe runs at a minute well away from it, reads bronze and
//! asks Blizzard for the header alone. It writes no bronze, takes no lock and
//! no cached token (it takes a token of its own, so it never races the hourly
//! run for the cache file), and never fails: whatever it meets is a warning.

use chrono::{DateTime, Utc};
use duckdb::Connection;

use crate::collect::blizzard::{Blizzard, Probed};
use crate::collect::failure::Failure;
use crate::collect::snapshot::last_modified_of;
use crate::collect::{Clock, Endpoints, bronze_dir, window};
use crate::config::Config;
use crate::error::{Report, warned};
use crate::types::Hour;

/// Runs `dp probe`. The report is `Ok`, or a warning: never a failure.
pub fn run(config: &Config, clock: &dyn Clock, endpoints: Endpoints) -> Report {
    run_with(config, clock, &Blizzard::new(endpoints))
}

pub(crate) fn run_with(config: &Config, clock: &dyn Clock, blizzard: &Blizzard) -> Report {
    let report = match probe(config, clock, blizzard) {
        Ok(report) => report,
        Err(failure) => failed(failure),
    };
    for warning in &report.warnings {
        warning.log();
    }
    report
}

/// What the probe met, as a warning: it never fails a run.
fn failed(failure: Failure) -> Report {
    let (reason, message) = match &failure {
        Failure::Stop(report) | Failure::Retry { report, .. } => {
            (report.reason.clone(), report.message.clone())
        }
        Failure::Refused => (
            "TOKEN_REFUSED".to_owned(),
            "Blizzard refused the access token.".to_owned(),
        ),
        Failure::Gone => ("FILE_GONE".to_owned(), "A file was removed.".to_owned()),
    };
    let warning = Report::warning(
        "PROBE_FAILED",
        "The refresh probe could not ask Blizzard or read bronze; it is tried again tomorrow.",
    )
    .with_metadata("cause", reason)
    .with_metadata("cause_message", message);
    warned(Report::ok(), vec![warning])
}

fn probe(config: &Config, clock: &dyn Clock, blizzard: &Blizzard) -> Result<Report, Failure> {
    let now = clock.now();
    let previous = Hour(Hour::containing(now).0 - 1);
    let file = bronze_dir(&config.data_dir).join(window::file_name(previous));
    let hour_label = window::file_name(previous);
    let report = Report::ok().with_metadata("hour", hour_label.clone());

    let connection = Connection::open_in_memory()
        .map_err(|e| crate::collect::snapshot::duckdb_failure(&e.to_string()))?;
    let Some(collected) = last_modified_of(&connection, &file) else {
        return Ok(report.with_metadata("result", "no snapshot of the hour before to compare"));
    };
    let token = blizzard.token(
        &config.blizzard_client_id,
        &config.blizzard_client_secret,
        now,
    )?;
    let report = report.with_metadata("collected_last_modified", collected.to_rfc3339());
    match blizzard.snapshot_probe(&token, collected)? {
        Probed::Modified(latest) if latest > collected => Ok(warned(
            report
                .with_metadata("latest_last_modified", latest.to_rfc3339())
                .with_metadata("result", "refreshed since"),
            vec![refreshed_more_often(&hour_label, collected, latest)],
        )),
        Probed::Modified(latest) => Ok(report
            .with_metadata("latest_last_modified", latest.to_rfc3339())
            .with_metadata("result", "not newer")),
        Probed::NotModified => Ok(report.with_metadata("result", "not newer")),
    }
}

/// Blizzard's latest snapshot is newer than the hour before's.
fn refreshed_more_often(
    hour_label: &str,
    collected: DateTime<Utc>,
    latest: DateTime<Utc>,
) -> Report {
    Report::warning(
        "REFRESH_MORE_OFTEN",
        "Blizzard's snapshot was refreshed since the hour before's was; it is refreshed more often than hourly.",
    )
    .with_metadata("hour", hour_label)
    .with_metadata("collected_last_modified", collected.to_rfc3339())
    .with_metadata("latest_last_modified", latest.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect::clock::FakeClock;
    use crate::collect::lock;
    use crate::collect::test_support::{SECRET, Stub, TOKEN, http_date};
    use crate::error::{ExitStatus, Severity, exit_status};
    use crate::transform::test_support::{World, l};
    use std::time::Duration;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, ResponseTemplate};

    /// The probe at 12:05 of the day the test world's hours are on, after the
    /// hour before's snapshot, last modified :31:40, was collected.
    const AT_1205: &str = "2026-11-20T12:05:00Z";
    const COLLECTED: &str = "2026-11-20T11:31:40Z";

    /// A transform world with the stub Blizzard's client secret.
    fn empty_world(name: &str) -> World {
        let mut w = World::new(&format!("probe-{name}"));
        w.config.blizzard_client_secret = crate::config::Secret::new(SECRET);
        w
    }

    /// A world whose bronze holds hour 11, and a stub Blizzard with a token.
    fn world(name: &str) -> (World, Stub) {
        let w = empty_world(name);
        w.snapshot_modified(11, &[l(1, 100, 1)], Some(COLLECTED));
        let stub = Stub::new();
        stub.token_ok(5);
        (w, stub)
    }

    fn mount(stub: &Stub, template: ResponseTemplate) {
        stub.mount(
            Mock::given(method("GET"))
                .and(path("/data/wow/auctions/commodities"))
                .and(query_param("namespace", "dynamic-eu"))
                .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
                // The conditional request, with the time collected.
                .and(IfModifiedSince(http_date(COLLECTED)))
                .respond_with(template),
        );
    }

    /// The request's `If-Modified-Since`, whole: wiremock's own header
    /// matcher splits a date at its comma.
    struct IfModifiedSince(String);

    impl wiremock::Match for IfModifiedSince {
        fn matches(&self, request: &wiremock::Request) -> bool {
            let got = request
                .headers
                .get("if-modified-since")
                .map(|value| value.to_str().unwrap_or_default().to_owned());
            got.as_deref() == Some(self.0.as_str())
        }
    }

    fn modified(at: &str) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("last-modified", http_date(at).as_str())
            .set_body_string(r#"{"auctions":[]}"#)
    }

    fn probe_at(w: &World, stub: &Stub) -> Report {
        let clock = FakeClock::at(AT_1205);
        run(&w.config, &clock, stub.endpoints())
    }

    #[test]
    fn nothing_newer_is_a_run_with_no_warning() {
        let (w, stub) = world("not-modified");
        mount(&stub, ResponseTemplate::new(304));

        let report = probe_at(&w, &stub);
        assert_eq!(report.outcome, Severity::Ok, "{report:?}");
        assert_eq!(report.metadata["result"], "not newer");
        assert!(report.warnings.is_empty());
        assert_eq!(stub.requests("GET"), 1);
    }

    #[test]
    fn a_server_that_ignores_the_condition_and_gives_the_same_time_is_not_newer() {
        let (w, stub) = world("same");
        mount(&stub, modified(COLLECTED));
        let report = probe_at(&w, &stub);
        assert_eq!(report.outcome, Severity::Ok, "{report:?}");
        assert_eq!(report.metadata["result"], "not newer");
    }

    #[test]
    fn a_snapshot_refreshed_since_warns_refresh_more_often_with_both_times() {
        let (w, stub) = world("newer");
        mount(&stub, modified("2026-11-20T12:01:10Z"));

        let report = probe_at(&w, &stub);

        assert_eq!(exit_status(&report), ExitStatus::Completed);
        assert_eq!(report.reason, "REFRESH_MORE_OFTEN");
        let [warning] = report.warnings.as_slice() else {
            panic!("one warning: {report:?}");
        };
        assert_eq!(
            warning.metadata["collected_last_modified"],
            "2026-11-20T11:31:40+00:00"
        );
        assert_eq!(
            warning.metadata["latest_last_modified"],
            "2026-11-20T12:01:10+00:00"
        );
        assert_eq!(warning.metadata["hour"], "2026-11-20T11.parquet");
    }

    #[test]
    fn one_second_newer_is_newer() {
        let (w, stub) = world("one-second");
        mount(&stub, modified("2026-11-20T11:31:41Z"));
        assert_eq!(probe_at(&w, &stub).reason, "REFRESH_MORE_OFTEN");
    }

    #[test]
    fn no_snapshot_of_the_hour_before_is_nothing_to_compare_and_asks_blizzard_nothing() {
        let w = empty_world("no-file");
        let stub = Stub::new();
        stub.token_ok(5);

        let report = probe_at(&w, &stub);

        assert_eq!(report.outcome, Severity::Ok, "{report:?}");
        assert_eq!(stub.requests("GET") + stub.requests("POST"), 0);
        assert!(report.metadata["result"].starts_with("no snapshot"));
    }

    #[test]
    fn a_bronze_file_that_cannot_be_read_is_nothing_to_compare_not_a_failure() {
        let (w, stub) = world("garbage");
        std::fs::write(
            crate::transform::bronze::path(
                &w.config.data_dir,
                crate::transform::test_support::h(11),
            ),
            b"not parquet",
        )
        .unwrap();
        let report = probe_at(&w, &stub);
        assert_eq!(exit_status(&report), ExitStatus::Completed);
        assert_eq!(stub.requests("GET"), 0);
    }

    #[test]
    fn whatever_it_meets_is_a_warning_and_never_a_failure() {
        for (name, template, cause) in [
            (
                "server-error",
                ResponseTemplate::new(500),
                "BLIZZARD_UNEXPECTED_STATUS",
            ),
            ("busy", ResponseTemplate::new(503), "BLIZZARD_BUSY"),
            ("refused", ResponseTemplate::new(403), "BLIZZARD_REFUSED"),
            (
                "no-header",
                ResponseTemplate::new(200).set_body_string("{}"),
                "LAST_MODIFIED_UNUSABLE",
            ),
        ] {
            let (w, stub) = world(name);
            mount(&stub, template);
            let report = probe_at(&w, &stub);

            assert_eq!(
                exit_status(&report),
                ExitStatus::Completed,
                "{name}: {report:?}"
            );
            assert_eq!(report.reason, "PROBE_FAILED", "{name}");
            assert_eq!(report.warnings[0].metadata["cause"], cause, "{name}");
        }
    }

    #[test]
    fn a_refused_token_is_a_warning_too() {
        let w = empty_world("token");
        w.snapshot_modified(11, &[l(1, 100, 1)], Some(COLLECTED));
        let stub = Stub::new();
        stub.token_status(401);
        let report = probe_at(&w, &stub);
        assert_eq!(exit_status(&report), ExitStatus::Completed, "{report:?}");
        assert_eq!(report.reason, "PROBE_FAILED");
        assert_eq!(stub.requests("GET"), 0);
    }

    #[test]
    fn blizzard_not_answering_is_a_warning() {
        let (w, stub) = world("slow");
        mount(
            &stub,
            modified("2026-11-20T12:01:10Z").set_delay(Duration::from_millis(600)),
        );
        let clock = FakeClock::at(AT_1205);
        let blizzard = Blizzard::with_timeouts(
            stub.endpoints(),
            crate::collect::blizzard::Timeouts {
                snapshot: Duration::from_millis(100),
                other: Duration::from_millis(100),
            },
        );
        let report = run_with(&w.config, &clock, &blizzard);
        assert_eq!(report.reason, "PROBE_FAILED");
        assert_eq!(report.warnings[0].metadata["cause"], "BLIZZARD_TIMEOUT");
        assert_eq!(exit_status(&report), ExitStatus::Completed);
    }

    #[test]
    fn the_probe_writes_no_bronze_takes_no_lock_waits_for_none_and_caches_no_token() {
        let (w, stub) = world("quiet");
        mount(&stub, modified("2026-11-20T12:01:10Z"));
        // Another job holds the lock throughout.
        let held_by = FakeClock::at(AT_1205);
        let _held = lock::acquire(&w.config.lock_file, Duration::ZERO, &held_by).unwrap();
        let before = bronze_files(&w);
        let clock = FakeClock::at(AT_1205);

        let report = run(&w.config, &clock, stub.endpoints());

        assert_eq!(report.reason, "REFRESH_MORE_OFTEN");
        assert!(clock.slept().is_empty(), "it waits for nothing");
        assert_eq!(bronze_files(&w), before, "no bronze file written");
        assert!(!w.config.data_dir.join("state/blizzard-token.json").exists());
    }

    fn bronze_files(w: &World) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(bronze_dir(&w.config.data_dir))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }
}
