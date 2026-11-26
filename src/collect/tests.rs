//! `dp collect` against a stubbed Blizzard, with time injected.

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use duckdb::Connection;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

use super::blizzard::{Blizzard, Timeouts};
use super::clock::FakeClock;
use super::test_support::{SECRET, Stub, TOKEN, http_date};
use super::*;
use crate::config::Secret;
use crate::error::{ExitStatus, exit_status};

const BODY: &str = r#"{"_links":{},"auctions":[
    {"id":1,"item":{"id":10},"quantity":5,"unit_price":123,"time_left":"SHORT"},
    {"id":2,"item":{"id":10},"quantity":7,"unit_price":130,"time_left":"LONG"},
    {"id":3,"item":{"id":20},"quantity":1,"unit_price":9000,"time_left":"VERY_LONG"}]}"#;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

impl Stub {
    /// A snapshot answer, `times` times, to a request bearing `bearer`.
    fn snapshot(&self, bearer: &str, last_modified: &str, times: u64) {
        self.mount(
            snapshot_mock(bearer)
                .respond_with(ok(last_modified, BODY))
                .up_to_n_times(times),
        );
    }

    fn snapshot_answer(&self, template: ResponseTemplate, times: u64) {
        self.snapshot_answer_for(TOKEN, template, times);
    }

    fn snapshot_answer_for(&self, bearer: &str, template: ResponseTemplate, times: u64) {
        self.mount(
            snapshot_mock(bearer)
                .respond_with(template)
                .up_to_n_times(times),
        );
    }
}

fn snapshot_mock(bearer: &str) -> wiremock::MockBuilder {
    Mock::given(method("GET"))
        .and(path("/data/wow/auctions/commodities"))
        .and(query_param("namespace", "dynamic-eu"))
        .and(header("authorization", format!("Bearer {bearer}").as_str()))
}

fn ok(last_modified_rfc3339: &str, body: &str) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("last-modified", http_date(last_modified_rfc3339).as_str())
        .set_body_string(body)
}

struct World {
    stub: Stub,
    clock: FakeClock,
    config: Config,
}

impl World {
    /// A run started at `start`, with an empty data folder.
    fn at(name: &str, start: &str) -> World {
        let dir = std::env::temp_dir().join(format!("dp-collect-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        World {
            stub: Stub::new(),
            clock: FakeClock::at(start),
            config: Config {
                env: "test".into(),
                data_dir: dir.join("data"),
                blizzard_client_id: "client-id".into(),
                blizzard_client_secret: Secret::new(SECRET),
                bucket: None,
                lock_file: dir.join("dp.lock"),
                adhoc_memory_limit: "4GB".into(),
                adhoc_threads: 1,
            },
        }
    }

    fn go(&self) -> Report {
        run(&self.config, &self.clock, self.stub.endpoints())
    }

    fn bronze(&self, file: &str) -> PathBuf {
        bronze_dir(&self.config.data_dir).join(file)
    }

    /// The temporary files in bronze's folder.
    fn temporaries(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(bronze_dir(&self.config.data_dir))
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.ends_with(".tmp"))
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    fn token_cache(&self) -> PathBuf {
        self.config.data_dir.join("state/blizzard-token.json")
    }

    /// Puts a token in the cache, expiring at `expires_at`.
    fn cache_token(&self, value: &str, expires_at: &str) {
        test_support::cache_token(&self.config.data_dir, value, expires_at);
    }
}

fn exit(report: &Report) -> u8 {
    exit_status(report).code()
}

fn rows(file: &Path) -> i64 {
    Connection::open_in_memory()
        .unwrap()
        .query_row(
            &format!("SELECT count(*) FROM read_parquet('{}')", file.display()),
            [],
            |r| r.get(0),
        )
        .unwrap()
}

const AT_1135: &str = "2026-11-26T11:35:00Z";
const LM_11: &str = "2026-11-26T11:31:40Z";
const LM_10: &str = "2026-11-26T10:31:40Z";
const LM_12: &str = "2026-11-26T12:31:40Z";
const LM_13: &str = "2026-11-26T13:31:40Z";

// Step 1 and the current hour

#[test]
fn the_hour_is_collected_into_its_file_and_exits_0() {
    let w = World::at("happy", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    let file = w.bronze("2026-11-26T11.parquet");
    assert_eq!(rows(&file), 3);
    assert!(w.temporaries().is_empty());
    assert!(w.clock.slept().is_empty());
    assert_eq!(report.outcome, crate::error::Severity::Ok);
}

#[test]
fn a_run_delayed_to_11_10_takes_hour_10() {
    let w = World::at("delayed", "2026-11-26T11:10:00Z");
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 5);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(rows(&w.bronze("2026-11-26T10.parquet")), 3);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn a_run_in_a_collected_hour_exits_0_and_asks_blizzard_nothing() {
    let w = World::at("rerun", AT_1135);
    fs::create_dir_all(bronze_dir(&w.config.data_dir)).unwrap();
    fs::write(w.bronze("2026-11-26T11.parquet"), "already here").unwrap();

    let report = w.go();

    assert_eq!(exit(&report), 0);
    assert_eq!(report.metadata["result"], "already collected");
    assert_eq!(w.stub.requests("GET") + w.stub.requests("POST"), 0);
    assert_eq!(
        fs::read(w.bronze("2026-11-26T11.parquet")).unwrap(),
        b"already here"
    );
}

#[test]
fn the_temporary_file_is_named_for_the_run() {
    assert_eq!(
        temporary_name("2026-11-26T11.parquet", 4242, "00ff00ff00ff00ff"),
        "2026-11-26T11.parquet.4242-00ff00ff00ff00ff.tmp"
    );
}

#[test]
fn the_random_suffix_is_16_hex_characters_and_differs_between_calls() {
    let (a, b) = (random_suffix().unwrap(), random_suffix().unwrap());
    for suffix in [&a, &b] {
        assert_eq!(suffix.len(), 16);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }
    assert_ne!(a, b);
}

#[test]
fn another_runs_half_written_file_is_never_renamed_into_place() {
    // Run B is part-way through its own temporary file of the same hour when
    // run A (this one) finishes: A must publish its own file, and clear B's
    // once the hour is in place.
    let w = World::at("interleave", AT_1135);
    // Same pid, as another container's PID 1 would have; only the suffix differs.
    let other = format!(
        "2026-11-26T11.parquet.{}-0123456789abcdef.tmp",
        std::process::id()
    );
    fs::create_dir_all(bronze_dir(&w.config.data_dir)).unwrap();
    fs::write(w.bronze(&other), "run B, half written").unwrap();
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);

    assert_eq!(rows(&w.bronze("2026-11-26T11.parquet")), 3);
    assert!(w.temporaries().is_empty(), "{:?}", w.temporaries());
}

#[test]
fn clearing_stale_files_touches_only_the_hours_other_temporaries() {
    let w = World::at("clear", AT_1135);
    let dir = bronze_dir(&w.config.data_dir);
    fs::create_dir_all(&dir).unwrap();
    for name in [
        "2026-11-26T11.parquet.1.tmp",
        "2026-11-26T11.parquet.2.tmp",
        "2026-11-26T10.parquet.1.tmp",
        "2026-11-26T11.parquet",
        "2026-11-26T11.parquet.notes",
    ] {
        fs::write(dir.join(name), "x").unwrap();
    }

    clear_stale(
        &dir,
        "2026-11-26T11.parquet",
        &dir.join("2026-11-26T11.parquet.2.tmp"),
    );

    assert_eq!(
        w.temporaries(),
        ["2026-11-26T10.parquet.1.tmp", "2026-11-26T11.parquet.2.tmp"]
    );
    assert!(dir.join("2026-11-26T11.parquet").exists());
    assert!(dir.join("2026-11-26T11.parquet.notes").exists());
}

#[test]
fn a_stale_temporary_file_from_a_killed_run_is_ignored_and_cleared() {
    let w = World::at("stale", AT_1135);
    fs::create_dir_all(bronze_dir(&w.config.data_dir)).unwrap();
    fs::write(w.bronze("2026-11-26T11.parquet.1.tmp"), "half a file").unwrap();
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(rows(&w.bronze("2026-11-26T11.parquet")), 3);
    assert!(w.temporaries().is_empty());
}

#[test]
fn a_run_that_finds_the_hour_collected_at_the_lock_drops_its_own_file_only() {
    // Run A finished while this run was parsing: this run's file goes, the
    // hour's stays, and another run's temporary file is left to its owner.
    let w = World::at("raced", AT_1135);
    let dir = bronze_dir(&w.config.data_dir);
    fs::create_dir_all(&dir).unwrap();
    w.stub.token_ok(5);
    // The other run finishes while this one is being answered: the responder
    // puts the hour's file in place before the body reaches this run.
    let final_file = w.bronze("2026-11-26T11.parquet");
    fs::write(w.bronze("2026-11-26T11.parquet.1.tmp"), "run B's").unwrap();
    let published = final_file.clone();
    w.stub.mount(
        snapshot_mock(TOKEN)
            .respond_with(move |_: &wiremock::Request| {
                fs::write(&published, "run A's file").unwrap();
                ok(LM_11, BODY)
            })
            .up_to_n_times(5),
    );

    let report = w.go();

    assert_eq!(report.metadata["result"], "already collected");
    assert_eq!(fs::read(&final_file).unwrap(), b"run A's file");
    assert_eq!(w.temporaries(), ["2026-11-26T11.parquet.1.tmp"]);
}

// The token

#[test]
fn the_token_is_cached_mode_0600_and_basic_auth_carries_the_client() {
    let w = World::at("cache", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);

    let mode = fs::metadata(w.token_cache()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    assert!(fs::read_to_string(w.token_cache()).unwrap().contains(TOKEN));
    assert_eq!(w.stub.requests("POST"), 1);
}

#[test]
fn a_cached_token_with_more_than_the_margin_left_is_reused() {
    let w = World::at("reuse", AT_1135);
    w.cache_token("tok-cached", "2026-11-26T12:36:00Z"); // 1 h 1 min left
    w.stub.snapshot("tok-cached", LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.stub.requests("POST"), 0);
}

#[test]
fn a_cached_token_with_the_margin_or_less_left_is_replaced() {
    let w = World::at("margin", AT_1135);
    w.cache_token("tok-cached", "2026-11-26T12:35:00Z"); // exactly 1 h left
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.stub.requests("POST"), 1);
    assert!(fs::read_to_string(w.token_cache()).unwrap().contains(TOKEN));
}

#[test]
fn a_cached_token_that_is_refused_is_replaced_once() {
    let w = World::at("refused-cached", AT_1135);
    w.cache_token("tok-revoked", "2026-11-27T11:00:00Z");
    w.stub
        .snapshot_answer_for("tok-revoked", ResponseTemplate::new(401), 5);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.stub.requests("POST"), 1);
    assert_eq!(w.stub.requests("GET"), 2);
}

#[test]
fn a_new_token_that_is_refused_is_a_fundamental_exit_2() {
    let w = World::at("refused-new", AT_1135);
    w.stub.token_status(401);

    let report = w.go();

    assert_eq!(report.reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(report.metadata["http_status"], "401");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    assert_eq!(w.stub.requests("GET"), 0);
}

#[test]
fn a_fresh_token_that_the_snapshot_refuses_is_exit_2_and_not_asked_for_again() {
    let w = World::at("refused-fresh", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(401), 5);

    let report = w.go();

    assert_eq!(report.reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(exit(&report), 2);
    assert_eq!(w.stub.requests("POST"), 1);
}

#[test]
fn a_damaged_token_cache_is_replaced() {
    let w = World::at("damaged", AT_1135);
    fs::create_dir_all(w.token_cache().parent().unwrap()).unwrap();
    fs::write(w.token_cache(), "{broken").unwrap();
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert!(fs::read_to_string(w.token_cache()).unwrap().contains(TOKEN));
}

#[derive(Clone)]
struct Logged(Arc<Mutex<Vec<u8>>>);

impl io::Write for Logged {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn neither_the_token_nor_the_secret_reaches_the_log_or_a_report() {
    let log = Logged(Arc::default());
    let writer = log.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::TRACE)
        .finish();

    let (good, bad) = tracing::subscriber::with_default(subscriber, || {
        let good = World::at("secrets-good", AT_1135);
        good.stub.token_ok(5);
        good.stub.snapshot(TOKEN, LM_11, 5);
        let good_report = good.go();
        good_report.log();
        let bad = World::at("secrets-bad", AT_1135);
        bad.stub.token_status(401);
        let bad_report = bad.go();
        bad_report.log();
        (format!("{good_report:?}"), format!("{bad_report:?}"))
    });

    let text = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert!(text.contains("took a new Blizzard access token"), "{text}");
    for seen in [&text, &good, &bad] {
        assert!(!seen.contains(TOKEN));
        assert!(!seen.contains(SECRET));
        assert!(!seen.contains(&STANDARD.encode(format!("client-id:{SECRET}"))));
    }
}

// last-modified

#[test]
fn a_snapshot_from_before_the_hour_is_polled_every_poll_interval_until_it_refreshes() {
    let w = World::at("poll", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 2);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.clock.slept(), [secs(120), secs(120)]);
    assert_eq!(rows(&w.bronze("2026-11-26T11.parquet")), 3);
}

#[test]
fn a_snapshot_still_old_when_the_window_closes_is_a_gap_exit_75() {
    let w = World::at("stuck", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 1000);

    let report = w.go();

    assert_eq!(report.reason, "RETRY_WINDOW_CLOSED");
    assert_eq!(
        report.cause.as_ref().unwrap().reason,
        "SNAPSHOT_NOT_REFRESHED"
    );
    assert_eq!(exit_status(&report), ExitStatus::Blip);
    assert!(w.clock.slept().iter().all(|s| *s == secs(120)));
    // Polling stopped before :25 past the next hour, 12:25.
    let closes = chrono::DateTime::parse_from_rfc3339("2026-11-26T12:25:00Z").unwrap();
    assert!(w.clock.now() < closes);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn a_snapshot_of_the_next_hour_is_a_gap_exit_75_and_is_not_written() {
    let w = World::at("moved-on", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_12, 5);

    let report = w.go();

    assert_eq!(report.reason, "HOUR_IS_A_GAP");
    assert_eq!(exit_status(&report), ExitStatus::Blip);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
    assert!(w.clock.slept().is_empty());
}

#[test]
fn a_snapshot_two_hours_ahead_means_a_wrong_clock_exit_2() {
    let w = World::at("clock", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_13, 5);

    let report = w.go();

    assert_eq!(report.reason, "CLOCK_WRONG");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

#[test]
fn the_hour_edges_of_last_modified_are_exact() {
    // 11:00:00 is in hour 11, collected; 10:59:59 is still hour 10.
    let w = World::at("edge-in", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, "2026-11-26T11:00:00Z", 5);
    assert_eq!(exit(&w.go()), 0);

    let w = World::at("edge-out", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, "2026-11-26T10:59:59Z", 1);
    w.stub.snapshot(TOKEN, "2026-11-26T11:00:00Z", 5);
    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.clock.slept(), [secs(120)]);

    let w = World::at("edge-next", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, "2026-11-26T12:00:00Z", 5);
    assert_eq!(exit(&w.go()), 75);
}

// Blips and backoff

#[test]
fn a_blip_is_retried_with_backoff_doubling_from_30_seconds() {
    let w = World::at("backoff", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(503), 3);
    w.stub.snapshot(TOKEN, LM_11, 5);

    let report = w.go();

    assert_eq!(exit(&report), 0);
    assert_eq!(w.clock.slept(), [secs(30), secs(60), secs(120)]);
    // A retry that cleared the blip is a warning, with its count.
    assert_eq!(report.outcome, crate::error::Severity::Warning);
    assert_eq!(report.reason, "COLLECTED_AFTER_RETRIES");
    assert_eq!(report.metadata["retries"], "3");
}

#[test]
fn the_backoff_stops_doubling_at_5_minutes() {
    let w = World::at("cap", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(502), 1000);

    let report = w.go();

    assert_eq!(exit(&report), 75);
    let slept = w.clock.slept();
    assert_eq!(
        slept[..6],
        [
            secs(30),
            secs(60),
            secs(120),
            secs(240),
            secs(300),
            secs(300)
        ]
    );
    assert!(slept.iter().all(|s| *s <= secs(300)));
}

#[test]
fn a_retry_after_replaces_the_backoff() {
    let w = World::at("retry-after", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(429).insert_header("retry-after", "7"),
        1,
    );
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.clock.slept(), [secs(7)]);
}

#[test]
fn a_retry_after_of_zero_or_a_date_is_not_a_hot_loop() {
    let w = World::at("retry-after-odd", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(503).insert_header("retry-after", "0"),
        1,
    );
    w.stub.snapshot_answer(
        ResponseTemplate::new(503).insert_header("retry-after", "Thu, 26 Nov 2026 11:40:00 GMT"),
        1,
    );
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    // 0 is waited as a second; a date is not read, so the backoff applies,
    // which had doubled once.
    assert_eq!(w.clock.slept(), [secs(1), secs(60)]);
}

#[test]
fn each_blip_status_that_never_clears_ends_at_75_when_the_window_closes() {
    for status in [429, 502, 503, 504] {
        let w = World::at(&format!("blip-{status}"), AT_1135);
        w.stub.token_ok(5);
        w.stub.snapshot_answer(ResponseTemplate::new(status), 1000);

        let report = w.go();

        assert_eq!(report.reason, "RETRY_WINDOW_CLOSED", "{status}");
        let cause = report.cause.as_ref().unwrap();
        assert_eq!(cause.reason, "BLIZZARD_BUSY");
        assert_eq!(cause.metadata["http_status"], status.to_string());
        assert_eq!(exit_status(&report), ExitStatus::Blip, "{status}");
    }
}

#[test]
fn a_retry_that_would_end_after_the_window_closes_stops_at_once() {
    // At 12:24 the window closes in a minute; a Retry-After of 2 minutes
    // would end outside it.
    let w = World::at("late", "2026-11-26T12:24:00Z");
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(503).insert_header("retry-after", "120"),
        1000,
    );

    let report = w.go();

    assert_eq!(exit(&report), 75);
    assert!(w.clock.slept().is_empty());
    assert_eq!(w.stub.requests("GET"), 1);
}

#[test]
fn a_timeout_is_a_blip_retried_and_the_snapshot_timeout_applies() {
    let w = World::at("timeout", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(200)
            .insert_header("last-modified", http_date(LM_11).as_str())
            .set_body_string(BODY)
            .set_delay(Duration::from_secs(3)),
        1,
    );
    w.stub.snapshot(TOKEN, LM_11, 5);
    let blizzard = Blizzard::with_timeouts(
        w.stub.endpoints(),
        Timeouts {
            snapshot: Duration::from_millis(300),
            other: Duration::from_secs(10),
        },
    );

    let report = run_with(&w.config, &w.clock, blizzard);

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(w.clock.slept(), [secs(30)]);
}

#[test]
fn an_unreachable_blizzard_is_a_blip_and_ends_at_75() {
    let w = World::at("unreachable", AT_1135);
    let endpoints = Endpoints {
        oauth: "http://127.0.0.1:1".into(),
        api: "http://127.0.0.1:1".into(),
    };

    let report = run(&w.config, &w.clock, endpoints);

    assert_eq!(report.reason, "RETRY_WINDOW_CLOSED");
    assert_eq!(
        report.cause.as_ref().unwrap().reason,
        "BLIZZARD_UNREACHABLE"
    );
    assert_eq!(exit(&report), 75);
}

#[test]
fn a_token_endpoint_blip_is_retried_too() {
    let w = World::at("token-blip", AT_1135);
    w.stub.mount(
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1),
    );
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.clock.slept(), [secs(30)]);
}

// Fundamental and unexpected

#[test]
fn a_403_or_404_is_fundamental_exit_2() {
    for status in [403, 404] {
        let w = World::at(&format!("fund-{status}"), AT_1135);
        w.stub.token_ok(5);
        w.stub.snapshot_answer(ResponseTemplate::new(status), 5);

        let report = w.go();

        assert_eq!(report.reason, "BLIZZARD_REFUSED", "{status}");
        assert_eq!(report.metadata["http_status"], status.to_string());
        assert_eq!(exit_status(&report), ExitStatus::Fundamental, "{status}");
        assert_eq!(w.stub.requests("GET"), 1, "not retried");
    }
}

#[test]
fn a_400_a_500_or_another_status_is_unexpected_exit_70() {
    for status in [400, 500, 418] {
        let w = World::at(&format!("unexp-{status}"), AT_1135);
        w.stub.token_ok(5);
        w.stub.snapshot_answer(ResponseTemplate::new(status), 5);

        let report = w.go();

        assert_eq!(report.reason, "BLIZZARD_UNEXPECTED_STATUS", "{status}");
        assert_eq!(report.metadata["http_status"], status.to_string());
        assert_eq!(exit_status(&report), ExitStatus::Unexpected, "{status}");
    }
}

#[test]
fn a_token_endpoint_400_is_unexpected_exit_70() {
    let w = World::at("token-400", AT_1135);
    w.stub.token_status(400);
    assert_eq!(exit(&w.go()), 70);
}

#[test]
fn a_snapshot_without_last_modified_is_unexpected_exit_70() {
    let w = World::at("no-lm", AT_1135);
    w.stub.token_ok(5);
    w.stub
        .snapshot_answer(ResponseTemplate::new(200).set_body_string(BODY), 5);

    let report = w.go();

    assert_eq!(report.reason, "LAST_MODIFIED_UNUSABLE");
    assert_eq!(exit(&report), 70);
}

#[test]
fn a_body_the_parser_cannot_use_is_unexpected_exit_70_and_leaves_no_file() {
    let w = World::at("bad-body", AT_1135);
    w.stub.token_ok(5);
    w.stub
        .mount(snapshot_mock(TOKEN).respond_with(ok(LM_11, r#"{"auctions":[{"id":"x"}]}"#)));

    let report = w.go();

    assert_eq!(report.reason, "SNAPSHOT_BODY_UNUSABLE");
    assert_eq!(exit(&report), 70);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn a_data_folder_that_cannot_be_written_is_fundamental_exit_2() {
    let w = World::at("no-disk", AT_1135);
    fs::create_dir_all(w.config.data_dir.parent().unwrap()).unwrap();
    fs::write(&w.config.data_dir, "a file where the folder should be").unwrap();

    let report = w.go();

    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

// The lock

#[test]
fn a_lock_held_past_the_lock_wait_is_exit_75_and_the_file_is_not_renamed() {
    let w = World::at("locked", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);
    let held = File::create(&w.config.lock_file).unwrap();
    held.lock().unwrap();

    let report = w.go();

    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(exit(&report), 75);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
    let waited: Duration = w.clock.slept().iter().sum();
    assert_eq!(waited, LOCK_WAIT);
}

#[test]
fn the_lock_is_free_again_once_the_run_ends() {
    let w = World::at("unlocked", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);
    assert_eq!(exit(&w.go()), 0);

    let other = File::open(&w.config.lock_file).unwrap();
    assert!(other.try_lock().is_ok());
}

// A winner clears this run's temporary file

/// Plays the winner between this run's write and its lock: the hour is
/// published (or not) and every temporary file of the hour is cleared.
fn winner(w: &World, publish: bool) -> impl Fn() + use<> {
    let dir = bronze_dir(&w.config.data_dir);
    move || {
        for entry in fs::read_dir(&dir).unwrap().flatten() {
            if entry.file_name().to_string_lossy().ends_with(".tmp") {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        if publish {
            fs::write(dir.join("2026-11-26T11.parquet"), "the winner's").unwrap();
        }
    }
}

#[test]
fn a_run_whose_file_a_winner_cleared_exits_0_as_already_collected() {
    let w = World::at("cleared", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);
    let hook = winner(&w, true);

    let blizzard = Blizzard::new(w.stub.endpoints());
    let report = run_hooked(&w.config, &w.clock, blizzard, &hook);

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(report.metadata["result"], "already collected");
    assert_eq!(
        fs::read(w.bronze("2026-11-26T11.parquet")).unwrap(),
        b"the winner's"
    );
}

#[test]
fn a_file_that_vanished_with_no_hour_in_place_is_fundamental_exit_2() {
    let w = World::at("vanished", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);
    let hook = winner(&w, false);

    let blizzard = Blizzard::new(w.stub.endpoints());
    let report = run_hooked(&w.config, &w.clock, blizzard, &hook);

    assert_eq!(report.reason, "TEMPORARY_FILE_GONE");
    assert_eq!(exit(&report), 2);
}
