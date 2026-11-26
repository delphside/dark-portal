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
        crate::test_support::listening();
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
                bucket_access: None,
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
fn snapshot_lands_in_its_hours_bronze_file_by_rename() {
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
fn run_inside_the_previous_hours_retry_window_collects_that_hour() {
    let w = World::at("delayed", "2026-11-26T11:10:00Z");
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 5);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(rows(&w.bronze("2026-11-26T10.parquet")), 3);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn collect_in_a_collected_hour_exits_0_without_a_request() {
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
fn cached_token_is_reused_with_more_than_the_margin_left() {
    let w = World::at("reuse", AT_1135);
    w.cache_token("tok-cached", "2026-11-26T12:36:00Z"); // 1 h 1 min left
    w.stub.snapshot("tok-cached", LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.stub.requests("POST"), 0);
}

#[test]
fn token_is_renewed_at_or_inside_the_margin() {
    let w = World::at("margin", AT_1135);
    w.cache_token("tok-cached", "2026-11-26T12:35:00Z"); // exactly 1 h left
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.stub.requests("POST"), 1);
    assert!(fs::read_to_string(w.token_cache()).unwrap().contains(TOKEN));
}

#[test]
fn cached_token_refused_once_takes_a_new_token() {
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
fn refused_client_secret_exits_2() {
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
    crate::test_support::listening();
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
fn stale_snapshot_is_polled_every_2_minutes_until_it_refreshes() {
    let w = World::at("poll", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 2);
    w.stub.snapshot(TOKEN, LM_11, 5);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.clock.slept(), [secs(120), secs(120)]);
    assert_eq!(rows(&w.bronze("2026-11-26T11.parquet")), 3);
}

#[test]
fn snapshot_unrefreshed_by_25_past_exits_75_and_writes_nothing() {
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
fn last_modified_in_the_hour_after_the_current_one_exits_75_as_a_gap() {
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
fn last_modified_two_or_more_hours_ahead_exits_2() {
    let w = World::at("clock", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_13, 5);

    let report = w.go();

    assert_eq!(report.reason, "CLOCK_WRONG");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

#[test]
fn snapshot_hour_is_the_utc_hour_starting_of_last_modified() {
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
fn blip_cleared_by_retry_exits_0_with_a_warning() {
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
fn retry_backoff_doubles_from_30_seconds_to_5_minutes() {
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
fn retry_after_is_honoured() {
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
fn timeout_429_502_503_504_until_25_past_exits_75() {
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

    // A request that times out every time is a blip as well.
    let w = World::at("blip-timeout", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(200).set_delay(Duration::from_secs(2)),
        1000,
    );
    let blizzard = Blizzard::with_timeouts(
        w.stub.endpoints(),
        Timeouts {
            snapshot: Duration::from_millis(50),
            other: Duration::from_secs(10),
        },
    );
    let report = run_with(&w.config, &w.clock, blizzard);
    assert_eq!(report.reason, "RETRY_WINDOW_CLOSED");
    assert_eq!(exit_status(&report), ExitStatus::Blip);
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
fn unreachable_blizzard_is_a_blip() {
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

#[test]
fn snapshot_request_times_out_at_60_seconds_others_at_10() {
    assert_eq!(Timeouts::SETTINGS.snapshot, secs(60));
    assert_eq!(Timeouts::SETTINGS.other, secs(10));
    // `Blizzard::new` gives its agents those settings, the snapshot's the 60 s.
    let held = Blizzard::new(World::at("timeouts-held", AT_1135).stub.endpoints()).timeouts_held();
    assert_eq!(held, (Some(secs(60)), Some(secs(10))));
    // Each timeout is the one of its own requests: the token request is cut at
    // `other` (a first answer 1 s late is a blip, retried), the snapshot at
    // `snapshot` (an answer 0.6 s late, past `other`, is read).
    let w = World::at("timeouts", AT_1135);
    w.stub.mount(
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(1)))
            .up_to_n_times(1),
    );
    w.stub.token_ok(5);
    w.stub.snapshot_answer(
        ResponseTemplate::new(200)
            .insert_header("last-modified", http_date(LM_11).as_str())
            .set_body_string(BODY)
            .set_delay(Duration::from_millis(600)),
        5,
    );
    let blizzard = Blizzard::with_timeouts(
        w.stub.endpoints(),
        Timeouts {
            snapshot: Duration::from_secs(5),
            other: Duration::from_millis(300),
        },
    );

    let report = run_with(&w.config, &w.clock, blizzard);

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(
        w.clock.slept(),
        [secs(30)],
        "only the token request timed out"
    );
    assert_eq!(w.stub.requests("GET"), 1);
}

#[test]
fn new_token_refused_after_a_cached_one_exits_2() {
    // C2, X3: the cached token is refused, a new one is taken, and that one is
    // refused too: the client secret, exit 2, and no third token.
    let w = World::at("refused-both", AT_1135);
    w.cache_token("tok-revoked", "2026-11-27T11:00:00Z");
    w.stub
        .snapshot_answer_for("tok-revoked", ResponseTemplate::new(401), 5);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(401), 5);

    let report = w.go();

    assert_eq!(report.reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
    assert_eq!(exit(&report), 2);
    assert_eq!(w.stub.requests("POST"), 1, "a new token is taken once");
    assert_eq!(
        w.stub.requests("GET"),
        2,
        "the cached token, then the new one"
    );
}

/// A clock that lets go of the lock it was given once a wait has taken it
/// past `after`.
struct ReleasesOnWait {
    inner: FakeClock,
    after: DateTime<Utc>,
    held: Mutex<Option<lock::Lock>>,
}

impl Clock for ReleasesOnWait {
    fn now(&self) -> DateTime<Utc> {
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) {
        self.inner.sleep(duration);
        if self.inner.now() > self.after {
            self.held.lock().unwrap().take();
        }
    }
}

#[test]
fn current_hour_is_fixed_when_collect_starts() {
    // C0: started at 11:24:58 the current hour is 10 (hour 11 begins at 11:25).
    // Another run holds the lock when the file is written and lets go only
    // after the lock wait has taken the clock past 11:25, still inside hour
    // 10's retry window (which closes at 12:25).
    // The file is still hour 10's, which a run that read the hour again after
    // the wait would not write.
    let w = World::at("fixed", "2026-11-26T11:24:58Z");
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 5);
    let inner = FakeClock::at("2026-11-26T11:24:58Z");
    let held = lock::acquire(&w.config.lock_file, Duration::ZERO, &inner).unwrap();
    let clock = ReleasesOnWait {
        inner,
        after: "2026-11-26T11:25:00Z".parse().unwrap(),
        held: Mutex::new(Some(held)),
    };

    let report = run_with(&w.config, &clock, Blizzard::new(w.stub.endpoints()));

    assert_eq!(exit(&report), 0, "{report:?}");
    let slept = clock.inner.slept();
    assert!(!slept.is_empty(), "it waited for the lock");
    assert!(
        clock.now() > "2026-11-26T11:25:00Z".parse::<DateTime<Utc>>().unwrap(),
        "the wait took the clock past 11:25"
    );
    assert!(w.bronze("2026-11-26T10.parquet").exists());
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

// Fundamental and unexpected

#[test]
fn snapshot_403_or_404_exits_2() {
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

/// A run whose snapshot request is answered `status` every time.
fn answered(name: &str, status: u16) -> (World, Report) {
    let w = World::at(&format!("{name}-{status}"), AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(status), 5);
    let report = w.go();
    (w, report)
}

fn assert_unexpected_status(report: &Report, status: u16) {
    assert_eq!(report.reason, "BLIZZARD_UNEXPECTED_STATUS", "{status}");
    assert_eq!(report.metadata["http_status"], status.to_string());
    assert_eq!(exit_status(report), ExitStatus::Unexpected, "{status}");
}

#[test]
fn snapshot_400_or_an_unlisted_status_exits_70() {
    for status in [400, 418] {
        let (_, report) = answered("unexp", status);
        assert_unexpected_status(&report, status);
    }
}

#[test]
fn status_408_is_unexpected_and_not_a_blip() {
    let (w, report) = answered("unexp", 408);
    assert_unexpected_status(&report, 408);
    assert_eq!(w.stub.requests("GET"), 1, "not retried");
    assert!(w.clock.slept().is_empty(), "no backoff");
}

#[test]
fn blizzard_500_exits_70_and_writes_nothing() {
    let (w, report) = answered("unexp", 500);
    assert_unexpected_status(&report, 500);
    assert_eq!(exit(&report), 70);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
    assert!(w.temporaries().is_empty());
}

#[test]
fn a_token_endpoint_400_is_unexpected_exit_70() {
    let w = World::at("token-400", AT_1135);
    w.stub.token_status(400);
    assert_eq!(exit(&w.go()), 70);
}

#[test]
fn missing_last_modified_exits_70() {
    let w = World::at("no-lm", AT_1135);
    w.stub.token_ok(5);
    w.stub
        .snapshot_answer(ResponseTemplate::new(200).set_body_string(BODY), 5);

    let report = w.go();

    assert_eq!(report.reason, "LAST_MODIFIED_UNUSABLE");
    assert_eq!(exit(&report), 70);
}

#[test]
fn unusable_body_exits_70_and_leaves_no_bronze_file() {
    let w = World::at("bad-body", AT_1135);
    w.stub.token_ok(5);
    w.stub
        .mount(snapshot_mock(TOKEN).respond_with(ok(LM_11, r#"{"auctions":[{"id":"#)));

    let report = w.go();

    assert_eq!(report.reason, "SNAPSHOT_BODY_UNUSABLE");
    assert_eq!(exit(&report), 70);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn unwritable_data_folder_file_exits_2() {
    let w = World::at("no-disk", AT_1135);
    fs::create_dir_all(w.config.data_dir.parent().unwrap()).unwrap();
    fs::write(&w.config.data_dir, "a file where the folder should be").unwrap();

    let report = w.go();

    assert_eq!(report.reason, "LOCAL_IO_FAILED");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

// The lock

#[test]
fn writing_subcommand_exits_75_when_the_lock_wait_runs_out() {
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
fn collect_polls_without_the_lock_and_takes_it_only_to_rename() {
    // H1: another run holds the lock throughout. Collect still polls (two
    // waits of the poll interval for the stale snapshot), parses and writes its
    // file, and waits for the lock only at the rename, where the wait runs out.
    let w = World::at("polls-unlocked", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_10, 2);
    w.stub.snapshot(TOKEN, LM_11, 5);
    let held = File::create(&w.config.lock_file).unwrap();
    held.lock().unwrap();

    let report = w.go();

    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(exit(&report), 75);
    let slept = w.clock.slept();
    assert_eq!(
        slept[..2],
        [secs(120), secs(120)],
        "polled with the lock held"
    );
    assert_eq!(slept[2..].iter().sum::<Duration>(), LOCK_WAIT);
    assert_eq!(w.stub.requests("GET"), 3, "the snapshot was fetched whole");
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
    // X6, O37: nothing is left, its own temporary file included.
    assert!(w.temporaries().is_empty(), "{:?}", w.temporaries());
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

// DuckDB out of memory writing the bronze file (X4, D28.9)

#[test]
fn bronze_write_failure_in_collect_is_unexpected_exit_70_out_of_memory_included() {
    let w = World::at("oom", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot(TOKEN, LM_11, 5);

    let report = crate::lake::starved::under(crate::lake::starved::TINY, || w.go());

    assert_eq!(report.reason, "BRONZE_WRITE_FAILED", "{report:?}");
    assert_eq!(report.error_type, Some(ErrorType::Unexpected));
    assert_eq!(exit(&report), 70);
    assert!(!w.bronze("2026-11-26T11.parquet").exists());
    assert!(w.temporaries().is_empty(), "{:?}", w.temporaries());
}

// When Blizzard's response changes (5.2.1)

/// A listing as Blizzard gives it.
fn listing(id: u64) -> String {
    format!(r#"{{"id":{id},"item":{{"id":10}},"quantity":5,"unit_price":123,"time_left":"LONG"}}"#)
}

/// `listing` with the unknown fields `buyout` and `item.bonus`.
fn listing_with_new_fields(id: u64) -> String {
    listing(id)
        .replace(r#""id":10"#, r#""id":10,"bonus":[1]"#)
        .replace(r#""LONG""#, r#""LONG","buyout":5"#)
}

/// A snapshot of `listings`.
fn snapshot_of(listings: &[String]) -> String {
    format!(r#"{{"auctions":[{}]}}"#, listings.join(","))
}

/// Runs a collect in a world of its own, the stub answering `body` last
/// modified at `last_modified`.
fn collecting(name: &str, last_modified: &str, body: &str) -> (World, Report) {
    let w = World::at(name, AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ok(last_modified, body), 5);
    let report = w.go();
    (w, report)
}

fn warnings_of(report: &Report) -> Vec<&str> {
    report.warnings.iter().map(|w| w.reason.as_str()).collect()
}

#[test]
fn unknown_listing_field_is_logged_by_name() {
    // The log is the report's warning, `SNAPSHOT_FIELD_NEW`, once for the run.
    let body = snapshot_of(&[listing_with_new_fields(1), listing_with_new_fields(2)]);
    let (outcome, log) = crate::test_support::logging(|| collecting("field-new", LM_11, &body));
    let (w, report) = outcome;

    assert_eq!(exit(&report), 0, "{report:?}");
    assert!(w.bronze("2026-11-26T11.parquet").exists());
    assert_eq!(rows(&w.bronze("2026-11-26T11.parquet")), 2);
    assert_eq!(report.reason, "SNAPSHOT_FIELD_NEW");
    let [warning] = report.warnings.as_slice() else {
        panic!("one warning: {report:?}");
    };
    assert_eq!(warning.metadata["fields"], "buyout,item.bonus");
    assert_eq!(warning.metadata["hour"], "2026-11-26T11.parquet");
    assert_eq!(log.matches("SNAPSHOT_FIELD_NEW").count(), 1, "{log}");
    assert!(log.contains("WARN"), "{log}");
}

#[test]
fn a_new_field_beside_auctions_is_named_with_its_path_and_links_are_known() {
    // D38.9, G47
    let body = format!(
        r#"{{"_links":{{}},"version":3,"auctions":[{}]}}"#,
        listing(1)
    );
    let (w, report) = collecting("field-top-level", LM_11, &body);

    assert_eq!(exit(&report), 0, "{report:?}");
    assert!(w.bronze("2026-11-26T11.parquet").exists());
    assert_eq!(warnings_of(&report), ["SNAPSHOT_FIELD_NEW"]);
    assert_eq!(report.warnings[0].metadata["fields"], "$.version");
}

#[test]
fn an_unknown_fields_long_text_is_ignored_and_the_hour_collected() {
    // D38.8, G47: the 32-byte bound is for text `dp` keeps.
    let long = "T".repeat(500);
    let listing = listing(1).replace(r#""LONG""#, &format!(r#""LONG","note":"{long}""#));
    let (w, report) = collecting("field-long-text", LM_11, &snapshot_of(&[listing]));

    assert_eq!(exit(&report), 0, "{report:?}");
    assert!(w.bronze("2026-11-26T11.parquet").exists());
    assert_eq!(report.warnings[0].metadata["fields"], "note");
}

#[test]
fn a_change_that_lasts_repeats_its_warning_every_hour() {
    // D38.7, G50: each run reports what it sees; deduplicating is #2's.
    for (name, at, modified, hour) in [
        (
            "lasting-11",
            AT_1135,
            "2026-11-26T11:50:00Z",
            "2026-11-26T11",
        ),
        (
            "lasting-12",
            "2026-11-26T12:35:00Z",
            "2026-11-26T12:50:00Z",
            "2026-11-26T12",
        ),
    ] {
        let w = World::at(name, at);
        w.stub.token_ok(5);
        let body = snapshot_of(&[listing_with_new_fields(1)]);
        w.stub.snapshot_answer(ok(modified, &body), 5);
        let report = w.go();
        assert_eq!(
            warnings_of(&report),
            ["REFRESH_TIME_MOVED", "SNAPSHOT_FIELD_NEW"],
            "{hour}"
        );
        assert_eq!(
            report.warnings[0].metadata["hour"],
            format!("{hour}.parquet")
        );
    }
}

#[test]
fn collect_in_a_collected_hour_has_no_refresh_time_and_raises_no_warning() {
    // D44.10, G56: nothing was read, so there is no refresh time to report.
    let w = World::at("rerun-no-refresh", AT_1135);
    fs::create_dir_all(bronze_dir(&w.config.data_dir)).unwrap();
    fs::write(w.bronze("2026-11-26T11.parquet"), "already here").unwrap();

    let report = w.go();

    assert_eq!(exit(&report), 0);
    assert!(report.warnings.is_empty());
    assert!(!report.metadata.contains_key("refresh_past_hour"));
}

#[test]
fn a_run_that_fails_reports_no_refresh_time() {
    // G56: a failure before the snapshot is read carries none.
    let w = World::at("failed-no-refresh", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(500), 5);
    let report = w.go();
    assert_eq!(exit(&report), 70);
    assert!(!report.metadata.contains_key("refresh_past_hour"));
}

#[test]
fn a_decimal_or_minus_zero_in_a_listing_makes_the_hour_a_gap() {
    // D43.7, G45
    for (name, field, json) in [
        ("fraction-quantity", "quantity", "2.0"),
        ("exponent-price", "unit_price", "1e3"),
        ("minus-zero-quantity", "quantity", "-0"),
    ] {
        let second = listing(2).replace(
            &format!(
                r#""{field}":{}"#,
                if field == "quantity" { "5" } else { "123" }
            ),
            &format!(r#""{field}":{json}"#),
        );
        let (w, report) = collecting(name, LM_11, &snapshot_of(&[listing(1), second]));
        assert_eq!(report.reason, "SNAPSHOT_VALUE_UNEXPECTED", "{name}");
        assert_eq!(report.metadata["field"], field, "{name}");
        assert_eq!(exit(&report), 70, "{name}");
        assert!(!w.bronze("2026-11-26T11.parquet").exists(), "{name}");
    }
}

#[test]
fn an_id_or_item_id_of_zero_is_collected() {
    // D43.8, G45
    let body = snapshot_of(&[listing(0).replace(r#""id":10"#, r#""id":0"#)]);
    let (w, report) = collecting("zero-ids", LM_11, &body);
    assert_eq!(exit(&report), 0, "{report:?}");
    assert!(w.bronze("2026-11-26T11.parquet").exists());
}

#[test]
fn a_body_that_is_json_but_not_an_object_is_a_shape_change_at_the_root() {
    // D42.8 against D42.9, G46: valid JSON is `$`; not JSON, or cut off, is
    // unusable.
    for (name, body, reason, field) in [
        ("root-array", "[1]", "SNAPSHOT_SHAPE_CHANGED", Some("$")),
        ("root-number", "5", "SNAPSHOT_SHAPE_CHANGED", Some("$")),
        ("not-json", "<html>", "SNAPSHOT_BODY_UNUSABLE", None),
        (
            "cut-off",
            r#"{"auctions":[{"id":1,"item":{"id":1},"quan"#,
            "SNAPSHOT_BODY_UNUSABLE",
            None,
        ),
    ] {
        let (w, report) = collecting(name, LM_11, body);
        assert_eq!(report.reason, reason, "{name}");
        assert_eq!(exit(&report), 70, "{name}");
        if let Some(field) = field {
            assert_eq!(report.metadata["field"], field, "{name}");
            assert_eq!(report.metadata["listings"], "0", "{name}");
        }
        assert!(!w.bronze("2026-11-26T11.parquet").exists(), "{name}");
        assert!(w.temporaries().is_empty(), "{name}");
    }
}

#[test]
fn text_beyond_the_longest_where_a_number_is_needed_is_a_shape_change() {
    // D41.6
    let second = listing(2).replace(
        r#""quantity":5"#,
        &format!(r#""quantity":"{}""#, "Z".repeat(40)),
    );
    let (_, report) = collecting(
        "long-text-number",
        LM_11,
        &snapshot_of(&[listing(1), second]),
    );
    assert_eq!(report.reason, "SNAPSHOT_SHAPE_CHANGED");
    assert_eq!(report.metadata["field"], "quantity");
}

#[test]
fn known_fields_only_give_a_report_with_no_warning() {
    let (_, report) = collecting("fields-known", LM_11, BODY);
    assert_eq!(report.outcome, crate::error::Severity::Ok);
    assert!(report.warnings.is_empty());
    assert_eq!(report.metadata["refresh_past_hour"], "31:40");
}

#[test]
fn time_left_not_a_band_is_collected_as_given() {
    let body = snapshot_of(&[
        listing(1),
        listing(2).replace("LONG", "FOREVER"),
        listing(3).replace("LONG", "very_long"),
    ]);
    let (w, report) = collecting("time-left-new", LM_11, &body);

    assert_eq!(exit(&report), 0, "{report:?}");
    assert!(report.warnings.is_empty(), "the transform warns of it");
    let values: Vec<String> = {
        let connection = Connection::open_in_memory().unwrap();
        let mut statement = connection
            .prepare(&format!(
                "SELECT time_left FROM read_parquet('{}') ORDER BY id",
                w.bronze("2026-11-26T11.parquet").display()
            ))
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert_eq!(values, ["LONG", "FOREVER", "very_long"]);
}

#[test]
fn snapshot_missing_a_needed_field_is_a_gap_with_its_own_code_exit_70() {
    for (name, missing, field) in [
        ("shape-quantity", "quantity", "quantity"),
        ("shape-time-left", "time_left", "time_left"),
    ] {
        let second = listing(2).replace(&format!(r#""{missing}":"#), r#""renamed":"#);
        let body = snapshot_of(&[listing(1), second]);
        let (w, report) = collecting(name, LM_11, &body);

        assert_eq!(report.reason, "SNAPSHOT_SHAPE_CHANGED", "{report:?}");
        assert_eq!(exit(&report), 70);
        assert_eq!(report.metadata["field"], field);
        assert_eq!(report.metadata["listings"], "1");
        assert!(
            !w.bronze("2026-11-26T11.parquet").exists(),
            "a partial hour is a gap"
        );
        assert!(w.temporaries().is_empty());
    }
}

#[test]
fn snapshot_structure_changed_is_a_gap_naming_the_path() {
    for (name, body, path) in [
        (
            "structure-item-array",
            r#"{"auctions":[{"id":1,"item":[{"id":10}],"quantity":5,"unit_price":1,"time_left":"LONG"}]}"#,
            "item",
        ),
        (
            "structure-item-id-moved",
            r#"{"auctions":[{"id":1,"item":{"item_id":10},"quantity":5,"unit_price":1,"time_left":"LONG"}]}"#,
            "item.id",
        ),
        (
            "structure-auctions-renamed",
            r#"{"listings":[]}"#,
            "auctions",
        ),
    ] {
        let (w, report) = collecting(name, LM_11, body);
        assert_eq!(
            report.reason, "SNAPSHOT_SHAPE_CHANGED",
            "{name}: {report:?}"
        );
        assert_eq!(report.metadata["field"], path, "{name}");
        assert_eq!(exit(&report), 70);
        assert!(!w.bronze("2026-11-26T11.parquet").exists());
    }
}

#[test]
fn snapshot_value_the_column_cannot_hold_is_a_gap_with_the_value_exit_70() {
    for (name, field, json, shown) in [
        ("value-negative", "quantity", "-1", "-1"),
        ("value-decimal", "unit_price", "1.5", "1.5"),
        ("value-range", "quantity", "4294967296", "4294967296"),
        ("value-zero-price", "unit_price", "0", "0"),
        ("value-zero-quantity", "quantity", "0", "0"),
    ] {
        let second = listing(2).replace(
            &format!(
                r#""{field}":{}"#,
                if field == "quantity" { "5" } else { "123" }
            ),
            &format!(r#""{field}":{json}"#),
        );
        let body = snapshot_of(&[listing(1), second]);
        let (w, report) = collecting(name, LM_11, &body);

        assert_eq!(
            report.reason, "SNAPSHOT_VALUE_UNEXPECTED",
            "{name}: {report:?}"
        );
        assert_eq!(exit(&report), 70, "{name}");
        assert_eq!(report.metadata["field"], field, "{name}");
        assert_eq!(report.metadata["value"], shown, "{name}");
        assert_eq!(report.metadata["listings"], "1", "{name}");
        assert!(!w.bronze("2026-11-26T11.parquet").exists(), "{name}");
    }
}

#[test]
fn text_beyond_the_longest_is_a_gap_and_the_longest_is_kept() {
    let kept = "X".repeat(crate::settings::collection::TEXT_MAX_LENGTH);
    let body = snapshot_of(&[listing(1).replace("LONG", &kept)]);
    let (_, report) = collecting("text-longest", LM_11, &body);
    assert_eq!(exit(&report), 0, "{report:?}");

    let over = format!("{kept}X");
    let body = snapshot_of(&[listing(1).replace("LONG", &over)]);
    let (_, report) = collecting("text-over", LM_11, &body);
    assert_eq!(report.reason, "SNAPSHOT_VALUE_UNEXPECTED");
    assert_eq!(report.metadata["field"], "time_left");
    assert_eq!(report.metadata["value"], over);
}

#[test]
fn refresh_outside_the_expected_window_warns_with_the_time_past_the_hour() {
    // From :20:00 to :45:00 both included is as expected.
    for (name, at, past, moved) in [
        ("refresh-early-edge", "2026-11-26T11:20:00Z", "20:00", false),
        ("refresh-late-edge", "2026-11-26T11:45:00Z", "45:00", false),
        ("refresh-usual", LM_11, "31:40", false),
        ("refresh-early", "2026-11-26T11:19:59Z", "19:59", true),
        ("refresh-late", "2026-11-26T11:45:01Z", "45:01", true),
        ("refresh-on-the-hour", "2026-11-26T11:00:00Z", "00:00", true),
        ("refresh-at-05-03", "2026-11-26T11:05:03Z", "05:03", true),
        (
            "refresh-at-the-last-second",
            "2026-11-26T11:59:59Z",
            "59:59",
            true,
        ),
    ] {
        let (w, report) = collecting(name, at, BODY);
        assert_eq!(exit(&report), 0, "{name}: {report:?}");
        assert!(w.bronze("2026-11-26T11.parquet").exists(), "{name}");
        assert_eq!(report.metadata["refresh_past_hour"], past, "{name}");
        if moved {
            assert_eq!(report.reason, "REFRESH_TIME_MOVED", "{name}");
            let warning = &report.warnings[0];
            assert_eq!(warning.metadata["refresh_past_hour"], past);
            assert_eq!(warning.metadata["window_start"], "20:00");
            assert_eq!(warning.metadata["window_end"], "45:00");
        } else {
            assert!(report.warnings.is_empty(), "{name}: {report:?}");
        }
    }
}

#[test]
fn every_warning_of_a_run_is_listed_on_its_report() {
    let w = World::at("warnings-listed", AT_1135);
    w.stub.token_ok(5);
    w.stub.snapshot_answer(ResponseTemplate::new(503), 1);
    let body = snapshot_of(&[listing_with_new_fields(1)]);
    w.stub.snapshot_answer(ok("2026-11-26T11:50:00Z", &body), 1);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(
        warnings_of(&report),
        [
            "COLLECTED_AFTER_RETRIES",
            "REFRESH_TIME_MOVED",
            "SNAPSHOT_FIELD_NEW"
        ]
    );
    assert_eq!(report.reason, "COLLECTED_AFTER_RETRIES");
    assert_eq!(
        report.metadata["warnings"],
        "COLLECTED_AFTER_RETRIES,REFRESH_TIME_MOVED,SNAPSHOT_FIELD_NEW"
    );
}
