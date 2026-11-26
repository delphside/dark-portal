//! `dp items` against a stubbed Blizzard, with time injected.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use duckdb::Connection;
use wiremock::matchers::{header, method, path, path_regex, query_param};
use wiremock::{Mock, MockBuilder, ResponseTemplate};

use super::pacer::{Pacer, interval};
use super::store::{Responses, ids_to_fetch, items_dir};
use super::*;
use crate::collect::blizzard::Timeouts;
use crate::collect::clock::FakeClock;
use crate::collect::snapshot::{SnapshotMeta, write_parquet};
use crate::collect::test_support::{SECRET, Stub, TOKEN, cache_token};
use crate::config::Secret;
use crate::error::{ExitStatus, exit_status};
use crate::lake::scratch_dir;
use crate::test_support::logging;

const START: &str = "2026-11-26T11:36:00Z";
const FILE: &str = "2026-11-26T113600.parquet";

fn at(rfc3339: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
}

fn exit(report: &Report) -> u8 {
    exit_status(report).code()
}

/// An item's body as Blizzard sends it, with whitespace the parser would not
/// keep.
fn body(id: u32) -> String {
    format!(
        r#"{{"id": {id},  "name":"Flask {id}", "item_class":{{"id":0}},"item_subclass":{{"id":3}},
        "sell_price":100,"purchase_price":500,"preview_item":{{"a":[1, 2]}}}}"#
    )
}

fn item_mock(id: u32, bearer: &str) -> MockBuilder {
    Mock::given(method("GET"))
        .and(path(format!("/data/wow/item/{id}")))
        .and(query_param("namespace", "static-eu"))
        .and(query_param("locale", "en_GB"))
        .and(header("authorization", format!("Bearer {bearer}").as_str()))
}

impl Stub {
    fn item_ok(&self, id: u32) {
        self.mount(
            item_mock(id, TOKEN).respond_with(ResponseTemplate::new(200).set_body_string(body(id))),
        );
    }

    fn item_answer(&self, id: u32, template: ResponseTemplate, times: u64) {
        self.mount(
            item_mock(id, TOKEN)
                .respond_with(template)
                .up_to_n_times(times),
        );
    }

    fn item_requests(&self, id: u32) -> usize {
        let wanted = format!("/data/wow/item/{id}");
        self.runtime
            .block_on(self.server.received_requests())
            .unwrap()
            .iter()
            .filter(|r| r.url.path() == wanted)
            .count()
    }
}

struct World {
    stub: Stub,
    clock: FakeClock,
    config: Config,
    /// The item file a run started at `start` writes.
    file_name: String,
}

impl World {
    fn at(name: &str, start: &str) -> World {
        crate::test_support::listening();
        let dir = std::env::temp_dir().join(format!("dp-items-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        World {
            stub: Stub::new(),
            clock: FakeClock::at(start),
            file_name: format!("{}.parquet", at(start).format("%Y-%m-%dT%H%M%S")),
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

    /// Runs with a client whose timeouts are `timeout`.
    fn go_with_timeout(&self, timeout: Duration) -> Report {
        let blizzard = Blizzard::with_timeouts(
            self.stub.endpoints(),
            Timeouts {
                snapshot: timeout,
                other: timeout,
            },
        );
        let ids = ids_to_fetch(&self.config.data_dir).unwrap();
        fetch(&self.config, &self.clock, &blizzard, &ids, Holding::Take)
    }

    /// A snapshot file with a listing of each item, written by the collector's
    /// own writer.
    fn snapshot(&self, hour: &str, items: &[u32]) {
        let dir = self.config.data_dir.join("bronze/commodities/eu");
        fs::create_dir_all(&dir).unwrap();
        let listings: Vec<String> = items
            .iter()
            .enumerate()
            .map(|(n, item)| {
                format!(
                    r#"{{"id":{},"item":{{"id":{item}}},"quantity":1,"unit_price":5,"time_left":"LONG"}}"#,
                    n + 1
                )
            })
            .collect();
        let json = format!(r#"{{"auctions":[{}]}}"#, listings.join(","));
        let meta = SnapshotMeta {
            hour: at("2026-11-26T10:00:00Z"),
            last_modified: at("2026-11-26T10:31:40Z"),
            fetched_at: at("2026-11-26T10:35:00Z"),
        };
        write_parquet(
            json.as_bytes(),
            &meta,
            &dir.join(format!("{hour}.parquet")),
            &scratch_dir(&self.config.data_dir),
        )
        .unwrap();
    }

    /// An item file from an earlier run, holding these responses.
    fn earlier_items(&self, file: &str, rows: &[(u32, u16)]) {
        let dir = items_dir(&self.config.data_dir);
        fs::create_dir_all(&dir).unwrap();
        let mut responses = Responses::new(&scratch_dir(&self.config.data_dir)).unwrap();
        for &(item, status) in rows {
            let body = (status == 200).then(|| body(item));
            responses
                .push(
                    ItemId(item),
                    at("2026-11-26T09:36:00Z"),
                    status,
                    body.as_deref(),
                )
                .unwrap();
        }
        responses.write(&dir.join(file)).unwrap();
    }

    fn item_files(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(items_dir(&self.config.data_dir))
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    }

    fn file(&self) -> PathBuf {
        items_dir(&self.config.data_dir).join(&self.file_name)
    }
}

/// `(item, status, body, fetched_at in seconds)` of each row, in item order.
type Row = (u32, u16, Option<String>, i64);

fn read_rows(file: &Path) -> Vec<Row> {
    let db = Connection::open_in_memory().unwrap();
    let mut statement = db
        .prepare(&format!(
            "SELECT item, status, CAST(body AS VARCHAR), fetched_at \
             FROM read_parquet('{}') ORDER BY item",
            file.display()
        ))
        .unwrap();
    statement
        .query_map([], |r| {
            // A TIMESTAMPTZ read as its microseconds, so no ICU extension is
            // needed.
            let at = match r.get::<_, duckdb::types::Value>(3)? {
                duckdb::types::Value::Timestamp(duckdb::types::TimeUnit::Microsecond, us) => {
                    us / 1_000_000
                }
                other => panic!("not a timestamp: {other:?}"),
            };
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, at))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn columns(file: &Path) -> Vec<(String, String)> {
    let db = Connection::open_in_memory().unwrap();
    let mut statement = db
        .prepare(&format!(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{}'))",
            file.display()
        ))
        .unwrap();
    statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The waits a run asked for that are a pause and not the fetch rate's.
fn waits(w: &World) -> Vec<Duration> {
    w.clock
        .slept()
        .into_iter()
        .filter(|d| *d >= Duration::from_secs(1))
        .collect()
}

// Which items, and the file

#[test]
fn items_new_to_bronze_are_fetched_raw_once() {
    let w = World::at("which", START);
    w.snapshot("2026-11-26T10", &[10, 20, 30]);
    w.snapshot("2026-11-26T11", &[30, 40]);
    w.earlier_items("2026-11-26T093600.parquet", &[(20, 200), (30, 404)]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_ok(40);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(report.metadata["fetched"], "2");
    assert_eq!(w.stub.item_requests(10), 1);
    assert_eq!(w.stub.item_requests(40), 1);
    assert_eq!(w.stub.item_requests(20) + w.stub.item_requests(30), 0);
    assert_eq!(w.item_files(), ["2026-11-26T093600.parquet", FILE]);
    let rows = read_rows(&w.file());
    assert_eq!(
        rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        [10, 40],
        "one file, one row per item fetched"
    );
}

#[test]
fn the_file_read_back_has_the_columns_types_and_each_body_unchanged() {
    let w = World::at("readback", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_answer(20, ResponseTemplate::new(404), 5);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    let expected = [
        ("item", "UINTEGER"),
        ("fetched_at", "TIMESTAMP WITH TIME ZONE"),
        ("status", "USMALLINT"),
        ("body", "JSON"),
    ]
    .map(|(a, b)| (a.to_owned(), b.to_owned()));
    assert_eq!(columns(&w.file()), expected);
    let fetched_at = at(START).timestamp();
    let rows = read_rows(&w.file());
    // Inverse: what Blizzard sent is what was kept, byte for byte, and a 404
    // has no body.
    assert_eq!(rows[0], (10, 200, Some(body(10)), fetched_at));
    assert_eq!(rows[1], (20, 404, None, fetched_at));
}

#[test]
fn the_file_is_named_for_the_runs_start_in_utc() {
    let w = World::at("name", "2026-01-02T03:04:05Z");
    w.snapshot("2026-01-02T02", &[10]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);

    assert_eq!(exit(&w.go()), 0);

    assert_eq!(w.item_files(), ["2026-01-02T030405.parquet"]);
}

#[test]
fn item_whose_latest_response_is_404_is_not_asked_again_hourly() {
    let w = World::at("rerun", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_answer(20, ResponseTemplate::new(404), 5);
    assert_eq!(exit(&w.go()), 0);
    let before = (w.stub.requests("GET"), w.stub.requests("POST"));
    w.clock.sleep(Duration::from_secs(3600));

    let report = w.go();

    assert_eq!(exit(&report), 0);
    assert_eq!(report.metadata["result"], "no new items");
    assert_eq!((w.stub.requests("GET"), w.stub.requests("POST")), before);
    assert_eq!(
        w.item_files(),
        [FILE],
        "a 404 is a response: it is not asked again"
    );
}

#[test]
fn no_snapshot_files_is_nothing_to_fetch() {
    let w = World::at("none", START);

    let report = w.go();

    assert_eq!(exit(&report), 0);
    assert!(w.item_files().is_empty());
    assert_eq!(w.stub.requests("GET") + w.stub.requests("POST"), 0);
}

#[test]
fn the_ids_are_distinct_ascending_and_ignore_temporary_files() {
    let w = World::at("ids", START);
    w.snapshot("2026-11-26T10", &[30, 10, 30]);
    w.snapshot("2026-11-26T11", &[20, 10]);
    let dir = w.config.data_dir.join("bronze/commodities/eu");
    fs::write(dir.join("2026-11-26T12.parquet.1-aa.tmp"), "half a file").unwrap();

    let ids = ids_to_fetch(&w.config.data_dir).unwrap();

    assert_eq!(ids, [ItemId(10), ItemId(20), ItemId(30)]);
}

#[test]
fn the_largest_item_id_is_fetched_and_kept() {
    let w = World::at("max", START);
    w.snapshot("2026-11-26T11", &[u32::MAX]);
    w.stub.token_ok(5);
    w.stub.item_ok(u32::MAX);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(read_rows(&w.file())[0].0, u32::MAX);
}

#[test]
fn items_run_alone_with_unreadable_bronze_stops_before_its_first_request() {
    let w = World::at("corrupt", START);
    let dir = w.config.data_dir.join("bronze/commodities/eu");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("2026-11-26T11.parquet"), "not parquet").unwrap();

    let report = w.go();

    assert_eq!(report.reason, "BRONZE_READ_FAILED");
    assert_eq!(exit_status(&report), ExitStatus::Unexpected);
    assert_eq!(exit(&report), 70);
    assert_eq!(
        w.stub.requests("GET") + w.stub.requests("POST"),
        0,
        "stopped before its first request"
    );
}

#[test]
fn the_request_is_the_item_url_with_the_cached_token() {
    let w = World::at("request", START);
    w.snapshot("2026-11-26T11", &[10]);
    cache_token(&w.config.data_dir, "tok-cached", "2026-11-27T11:36:00Z");
    w.stub.mount(
        item_mock(10, "tok-cached")
            .respond_with(ResponseTemplate::new(200).set_body_string(body(10))),
    );

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(w.stub.requests("POST"), 0);
    assert_eq!(w.stub.item_requests(10), 1);
}

#[test]
fn a_stale_file_a_killed_run_left_does_not_stop_the_next_run() {
    let w = World::at("stale", START);
    w.snapshot("2026-11-26T11", &[10]);
    fs::create_dir_all(items_dir(&w.config.data_dir)).unwrap();
    fs::write(
        items_dir(&w.config.data_dir).join(format!("{FILE}.1-aa.tmp")),
        "half a file",
    )
    .unwrap();
    w.stub.token_ok(5);
    w.stub.item_ok(10);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(read_rows(&w.file()).len(), 1);
}

#[test]
fn no_temporary_file_is_left_beside_the_item_file() {
    let w = World::at("tidy", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);

    assert_eq!(exit(&w.go()), 0);
    assert_eq!(w.item_files(), [FILE]);
}

// The fetch rate

#[test]
fn the_interval_is_one_second_over_the_item_fetch_rate() {
    assert_eq!(interval(), Duration::from_millis(20));
}

#[test]
fn the_first_request_is_not_delayed_and_none_comes_sooner_than_the_interval() {
    let clock = FakeClock::at(START);
    let mut pacer = Pacer::new();
    let mut starts = Vec::new();
    for _ in 0..10 {
        pacer.wait(&clock);
        starts.push(clock.now());
    }
    assert_eq!(starts[0], at(START));
    for pair in starts.windows(2) {
        assert!(
            pair[1] - pair[0] >= chrono::Duration::milliseconds(20),
            "{pair:?}"
        );
    }
}

#[test]
fn a_request_after_a_long_gap_is_not_delayed_again() {
    let clock = FakeClock::at(START);
    let mut pacer = Pacer::every(Duration::from_millis(20));
    pacer.wait(&clock);
    clock.sleep(Duration::from_secs(5));
    let slept = clock.slept().len();
    pacer.wait(&clock);
    assert_eq!(clock.slept().len(), slept);
}

#[test]
fn n_items_and_the_token_request_take_at_least_one_interval_each_but_the_first() {
    let w = World::at("rate", START);
    let ids: Vec<u32> = (1..=25).collect();
    w.snapshot("2026-11-26T11", &ids);
    w.stub.token_ok(5);
    for id in &ids {
        w.stub.item_ok(*id);
    }

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    let taken = w.clock.now() - at(START);
    assert!(taken >= chrono::Duration::milliseconds(25 * 20), "{taken}");
    // The token request is a request too: 26 in all, 25 spacings.
    assert_eq!(w.clock.slept().len(), 25);
    assert_eq!(read_rows(&w.file()).len(), 25);
}

#[test]
fn item_fetch_is_paced_at_50_a_second() {
    let w = World::at("many", START);
    let ids: Vec<u32> = (1..=2_000).collect();
    w.snapshot("2026-11-26T11", &ids);
    w.stub.token_ok(5);
    w.stub.mount(
        Mock::given(method("GET"))
            .and(path_regex(r"^/data/wow/item/\d+$"))
            .respond_with(|r: &wiremock::Request| {
                let id: u32 = r.url.path().rsplit('/').next().unwrap().parse().unwrap();
                ResponseTemplate::new(200).set_body_string(body(id))
            }),
    );

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(read_rows(&w.file()).len(), 2_000);
    let taken = w.clock.now() - at(START);
    assert!(
        taken >= chrono::Duration::milliseconds(2_000 * 20),
        "{taken}"
    );
}

// When it fails

#[test]
fn a_blip_is_retried_with_backoff_and_the_item_is_kept() {
    let w = World::at("blip", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    for status in [503, 502] {
        w.stub.item_answer(10, ResponseTemplate::new(status), 1);
    }
    w.stub.item_ok(10);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    // 30 s, then doubled: the backoff is the snapshot's.
    let waits: Vec<Duration> = w
        .clock
        .slept()
        .into_iter()
        .filter(|d| *d >= Duration::from_secs(1))
        .collect();
    assert_eq!(waits, [30, 60].map(Duration::from_secs));
    assert_eq!(read_rows(&w.file())[0].1, 200);
}

#[test]
fn a_retry_after_is_waited_in_place_of_the_backoff() {
    let w = World::at("retry-after", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_answer(
        10,
        ResponseTemplate::new(429).insert_header("retry-after", "7"),
        1,
    );
    w.stub.item_ok(10);

    assert_eq!(exit(&w.go()), 0);

    assert!(w.clock.slept().contains(&Duration::from_secs(7)));
    assert!(!w.clock.slept().contains(&Duration::from_secs(30)));
}

#[test]
fn a_retry_after_of_zero_is_not_a_hot_loop() {
    let w = World::at("retry-zero", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_answer(
        10,
        ResponseTemplate::new(429).insert_header("retry-after", "0"),
        1,
    );
    w.stub.item_ok(10);

    assert_eq!(exit(&w.go()), 0);
    assert!(w.clock.slept().contains(&Duration::from_secs(1)));
}

#[test]
fn item_is_asked_at_most_three_times_in_a_run() {
    for status in [429, 502, 503, 504] {
        let w = World::at(&format!("lasting-{status}"), START);
        w.snapshot("2026-11-26T11", &[10]);
        w.stub.token_ok(5);
        w.stub.item_answer(10, ResponseTemplate::new(status), 100);

        let report = w.go();

        assert_eq!(exit(&report), 3, "{status}: {report:?}");
        assert_eq!(report.errors.len(), 1);
        let error = &report.errors[0];
        assert_eq!(error.error_type, Some(ErrorType::TechnicalBlip));
        assert_eq!(error.metadata["item"], "10");
        assert_eq!(error.metadata["http_status"], status.to_string());
        assert_eq!(error.metadata["attempts"], "3");
        assert_eq!(w.stub.item_requests(10), 3);
        assert!(w.item_files().is_empty(), "nothing was fetched, so no file");
        // Paused after the first two; after the third nothing is left to ask.
        assert_eq!(waits(&w), [30, 60].map(Duration::from_secs));
    }
}

#[test]
fn a_timeout_is_a_blip() {
    let w = World::at("timeout", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_answer(
        10,
        ResponseTemplate::new(200)
            .set_body_string(body(10))
            .set_delay(Duration::from_millis(1_500)),
        100,
    );

    let report = w.go_with_timeout(Duration::from_millis(200));

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors[0].reason, "BLIZZARD_TIMEOUT");
    assert_eq!(report.errors[0].error_type, Some(ErrorType::TechnicalBlip));
}

#[test]
fn an_unreachable_blizzard_is_a_blip() {
    let mut w = World::at("unreachable", START);
    w.snapshot("2026-11-26T11", &[10]);
    cache_token(&w.config.data_dir, "tok", "2026-11-27T11:36:00Z");
    // Nothing listens on port 1.
    let endpoints = Endpoints {
        oauth: "http://127.0.0.1:1".into(),
        api: "http://127.0.0.1:1".into(),
    };
    w.config.env = "test".into();

    let report = run(&w.config, &w.clock, endpoints);

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors[0].reason, "BLIZZARD_UNREACHABLE");
    assert_eq!(report.errors[0].error_type, Some(ErrorType::TechnicalBlip));
}

#[test]
fn a_failing_low_id_does_not_starve_the_higher_ones_and_the_backoff_resets_on_success() {
    let w = World::at("no-starve", START);
    w.snapshot("2026-11-26T11", &[10, 20, 30]);
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(503), 100);
    w.stub.item_ok(20);
    w.stub.item_ok(30);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].metadata["item"], "10");
    assert_eq!(w.stub.item_requests(10), 3);
    assert_eq!(
        read_rows(&w.file()).iter().map(|r| r.0).collect::<Vec<_>>(),
        [20, 30]
    );
    // The first failure paused 30 s; the successes reset the backoff, so the
    // second pause is 30 s again and not 60 s.
    assert_eq!(waits(&w), [30, 30].map(Duration::from_secs));
}

#[test]
fn item_blip_pauses_the_whole_fetch_and_sends_the_item_to_the_back() {
    let w = World::at("pause-all", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(503), 1);
    w.stub.item_ok(10);
    w.stub.item_ok(20);

    assert_eq!(exit(&w.go()), 0);

    // 10 failed, so 20 was asked only after the pause, then 10 again at the end.
    assert_eq!(waits(&w), [Duration::from_secs(30)]);
    assert_eq!(read_rows(&w.file()).len(), 2);
}

#[test]
fn a_retry_after_past_the_end_of_the_fetch_sends_no_further_requests() {
    let w = World::at("long-retry-after", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_answer(
        10,
        ResponseTemplate::new(429).insert_header("retry-after", "3600"),
        100,
    );
    w.stub.item_ok(20);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(w.stub.requests("GET"), 1);
    assert!(waits(&w).is_empty());
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].reason, "ITEMS_LEFT_FOR_NEXT_RUN");
    assert_eq!(report.errors[0].metadata["items"], "2");
}

#[test]
fn the_fetch_ends_the_lock_wait_before_the_next_hourly_run() {
    assert_eq!(
        fetch_end(at("2026-11-26T11:36:00Z")),
        at("2026-11-26T12:25:00Z")
    );
    assert_eq!(
        fetch_end(at("2026-11-26T11:00:00Z")),
        at("2026-11-26T11:25:00Z")
    );
}

#[test]
fn item_fetch_starts_no_request_that_could_end_after_25_past() {
    // The last moment a request can still time out before the fetch ends is
    // 12:24:50: the pacer's 20 ms spacing runs the clock out after three
    // requests, the token's the first.
    let w = World::at("out-of-time", "2026-11-26T12:24:49.950Z");
    w.snapshot("2026-11-26T12", &[1, 2, 3, 4, 5]);
    w.stub.token_ok(5);
    for id in 1..=5 {
        w.stub.item_ok(id);
    }

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].reason, "ITEMS_LEFT_FOR_NEXT_RUN");
    assert_eq!(report.errors[0].metadata["items"], "3");
    assert_eq!(
        w.stub.item_requests(3) + w.stub.item_requests(4) + w.stub.item_requests(5),
        0
    );
    assert_eq!(read_rows(&w.file()).len(), 2, "what was fetched is kept");
}

#[test]
fn no_token_request_starts_past_the_end_of_the_fetch() {
    let w = World::at("token-late", "2026-11-26T12:24:55Z");
    w.snapshot("2026-11-26T12", &[10]);
    w.stub.token_ok(5);

    let report = w.go();

    assert_eq!(exit(&report), 3);
    assert_eq!(w.stub.requests("POST") + w.stub.requests("GET"), 0);
    assert_eq!(report.errors[0].metadata["items"], "1");
}

#[test]
fn a_run_with_no_time_left_fetches_nothing_and_says_so() {
    let w = World::at("late", "2026-11-26T12:34:55Z");
    w.snapshot("2026-11-26T12", &[10]);

    let report = w.go();

    assert_eq!(exit(&report), 3);
    assert_eq!(w.stub.requests("GET") + w.stub.requests("POST"), 0);
    assert_eq!(report.errors[0].metadata["items"], "1");
}

#[test]
fn the_next_hourly_start_is_the_timer_minute_ahead() {
    let next = |t: &str| next_hourly_start(at(t));
    assert_eq!(next("2026-11-26T11:00:00Z"), at("2026-11-26T11:35:00Z"));
    assert_eq!(next("2026-11-26T11:34:59Z"), at("2026-11-26T11:35:00Z"));
    assert_eq!(next("2026-11-26T11:35:00Z"), at("2026-11-26T12:35:00Z"));
    assert_eq!(next("2026-11-26T11:36:00Z"), at("2026-11-26T12:35:00Z"));
    assert_eq!(next("2026-11-26T23:50:00Z"), at("2026-11-27T00:35:00Z"));
}

#[test]
fn item_lookup_401_takes_a_new_token_once_and_asks_again() {
    let w = World::at("401", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    cache_token(&w.config.data_dir, "tok-stale", "2026-11-27T11:36:00Z");
    w.stub
        .mount(item_mock(10, "tok-stale").respond_with(ResponseTemplate::new(401)));
    w.stub
        .mount(item_mock(20, "tok-stale").respond_with(ResponseTemplate::new(401)));
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_ok(20);

    let report = w.go();

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(w.stub.requests("POST"), 1, "a new token is taken once");
    assert_eq!(read_rows(&w.file()).len(), 2);
}

#[test]
fn a_new_token_that_is_refused_too_is_the_client_secret_and_the_items_wait() {
    let w = World::at("401-new", START);
    w.snapshot("2026-11-26T11", &[10, 20, 30]);
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(401), 100);

    let ((report, text), _) = (logging(|| w.go()), ());

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.error_type, Some(ErrorType::TechnicalFundamental));
    assert_eq!(report.errors[0].reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(report.errors[0].metadata["item"], "10");
    assert_eq!(report.errors[1].metadata["items"], "2");
    assert_eq!(w.stub.requests("POST"), 1);
    assert_eq!(
        w.stub.item_requests(10),
        1,
        "a token taken this run is not replaced"
    );
    assert_eq!(w.stub.item_requests(20) + w.stub.item_requests(30), 0);
    assert!(w.item_files().is_empty());
    assert!(!text.contains(TOKEN) && !text.contains(SECRET), "{text}");
}

#[test]
fn a_token_endpoint_that_refuses_the_secret_leaves_every_item_for_the_next_run() {
    let w = World::at("token-401", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_status(401);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors[0].reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(
        report.errors[0].error_type,
        Some(ErrorType::TechnicalFundamental)
    );
    assert_eq!(report.errors[1].metadata["items"], "1");
    assert_eq!(w.stub.requests("GET"), 0);
}

#[test]
fn item_lookup_refused_new_token_exits_3_and_is_fetched_next_run() {
    let w = World::at("401-both", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    cache_token(&w.config.data_dir, "tok-stale", "2026-11-27T11:36:00Z");
    w.stub
        .mount(item_mock(10, "tok-stale").respond_with(ResponseTemplate::new(401)));
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(401), 100);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(w.stub.requests("POST"), 1, "a new token is taken once");
    assert_eq!(
        w.stub.item_requests(10),
        2,
        "the cached token, then the new one"
    );
    assert_eq!(report.errors[0].reason, "CLIENT_SECRET_REFUSED");
    assert_eq!(
        report.errors[0].error_type,
        Some(ErrorType::TechnicalFundamental)
    );
    assert_eq!(report.errors[0].metadata["http_status"], "401");
    assert_eq!(report.errors[0].metadata["item"], "10");
    assert_eq!(report.errors[1].metadata["items"], "1");
    assert_eq!(w.stub.item_requests(20), 0);
    assert!(w.item_files().is_empty());
}

#[test]
fn item_404_is_recorded_with_its_status_and_logged_as_a_warning() {
    let w = World::at("404", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_answer(20, ResponseTemplate::new(404), 100);

    let (report, text) = logging(|| w.go());

    assert_eq!(exit(&report), 0, "{report:?}");
    assert_eq!(report.outcome, crate::error::Severity::Warning);
    assert_eq!(report.metadata["not_found"], "1");
    assert_eq!(w.stub.item_requests(20), 1, "a 404 is not retried");
    assert_eq!(
        read_rows(&w.file())[1],
        (20, 404, None, at(START).timestamp())
    );
    assert!(text.contains("WARN") && text.contains("item=20"), "{text}");
}

#[test]
fn a_404_beside_a_failure_is_still_written_and_the_step_exits_3() {
    let w = World::at("404-and-fail", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(404), 100);
    w.stub.item_answer(20, ResponseTemplate::new(500), 100);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(read_rows(&w.file()).len(), 1);
    assert_eq!(report.errors.len(), 1);
}

#[test]
fn item_lookup_400_403_408_500_or_unreadable_is_fetched_next_run() {
    for (status, error_type) in [
        (400, ErrorType::Unexpected),
        (403, ErrorType::TechnicalFundamental),
        (408, ErrorType::Unexpected),
        (500, ErrorType::Unexpected),
        (418, ErrorType::Unexpected),
    ] {
        let w = World::at(&format!("status-{status}"), START);
        w.snapshot("2026-11-26T11", &[10, 20, 30]);
        w.stub.token_ok(5);
        w.stub.item_answer(10, ResponseTemplate::new(status), 100);
        w.stub.item_ok(20);
        w.stub.item_answer(30, ResponseTemplate::new(status), 100);

        let report = w.go();

        assert_eq!(exit(&report), 3, "{status}: {report:?}");
        assert_eq!(report.errors.len(), 2, "{status}");
        for (error, item) in report.errors.iter().zip(["10", "30"]) {
            assert_eq!(error.error_type, Some(error_type), "{status}");
            assert_eq!(error.metadata["item"], item);
            assert_eq!(error.metadata["http_status"], status.to_string());
        }
        assert_eq!(w.stub.item_requests(10), 1, "{status}: not retried");
        assert!(w.clock.slept().iter().all(|d| *d < Duration::from_secs(1)));
        // The item that answered is kept; the others wait.
        assert_eq!(
            read_rows(&w.file()).iter().map(|r| r.0).collect::<Vec<_>>(),
            [20]
        );
    }
}

#[test]
fn failed_item_lookup_exits_3_and_is_retried_next_run() {
    // C6, X5: a lookup that failed gives exit 3 and no response; the item has
    // none, so the next run asks for it again, and for no other.
    let w = World::at("retried", START);
    w.snapshot("2026-11-26T11", &[10, 20]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_answer(20, ResponseTemplate::new(500), 1);
    w.stub.item_ok(20);

    let first = w.go();

    assert_eq!(exit(&first), 3, "{first:?}");
    assert_eq!(w.item_files().len(), 1);
    assert_eq!(w.stub.item_requests(20), 1);
    w.clock.sleep(Duration::from_secs(3600));

    let second = w.go();

    assert_eq!(exit(&second), 0, "{second:?}");
    assert_eq!(w.stub.item_requests(20), 2, "asked again");
    assert_eq!(
        w.stub.item_requests(10),
        1,
        "the item with a response is not"
    );
    assert_eq!(w.item_files().len(), 2);
}

#[test]
fn a_body_that_cannot_be_read_waits_for_the_next_run() {
    for unreadable in ["", "not json", "[1,2]", "null", r#"{"id":"#] {
        let w = World::at("unreadable", START);
        w.snapshot("2026-11-26T11", &[10]);
        w.stub.token_ok(5);
        w.stub.item_answer(
            10,
            ResponseTemplate::new(200).set_body_string(unreadable),
            100,
        );

        let report = w.go();

        assert_eq!(exit(&report), 3, "{unreadable:?}: {report:?}");
        assert_eq!(report.errors[0].reason, "ITEM_BODY_UNUSABLE");
        assert_eq!(report.errors[0].metadata["http_status"], "200");
        assert_eq!(report.errors[0].error_type, Some(ErrorType::Unexpected));
        assert!(w.item_files().is_empty());
    }
}

#[test]
fn every_error_is_logged_with_its_type() {
    let w = World::at("logged", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_answer(10, ResponseTemplate::new(500), 100);

    let (report, text) = logging(|| {
        let report = w.go();
        report.log();
        report
    });

    assert_eq!(exit(&report), 3);
    assert!(text.contains("BLIZZARD_UNEXPECTED_STATUS"), "{text}");
    assert!(text.contains("error.type=\"unexpected\""), "{text}");
}

// Writing

#[test]
fn a_lock_held_past_the_wait_leaves_the_items_for_the_next_run_and_no_file() {
    let w = World::at("locked", START);
    w.snapshot("2026-11-26T11", &[10]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    let holder = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&w.config.lock_file)
        .unwrap();
    holder.lock().unwrap();

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors[0].reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(report.errors[1].metadata["items"], "1");
    assert!(w.item_files().is_empty(), "{:?}", w.item_files());
    // The lock wait is the lock wait here, and ends before the next run.
    assert!(w.clock.now() <= at("2026-11-26T12:35:00Z"));
    drop(holder);
    // The next run fetches the item again.
    assert_eq!(exit(&w.go()), 0);
    let files = w.item_files();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(
        read_rows(&items_dir(&w.config.data_dir).join(&files[0])).len(),
        1
    );
}

#[test]
fn a_fetch_that_runs_to_its_end_still_waits_for_the_lock_before_the_next_hourly_run() {
    // Started so the whole fetch is left: the lock wait is the full lock wait,
    // and it ends before the next hourly run starts.
    let w = World::at("locked-late", "2026-11-26T12:24:30Z");
    w.snapshot("2026-11-26T12", &[10]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    let holder = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&w.config.lock_file)
        .unwrap();
    holder.lock().unwrap();

    let report = w.go();

    assert_eq!(exit(&report), 3);
    assert_eq!(report.errors[0].reason, "LOCK_WAIT_EXPIRED");
    assert!(
        w.clock.now() >= at("2026-11-26T12:34:30Z"),
        "{}",
        w.clock.now()
    );
    assert!(
        w.clock.now() <= at("2026-11-26T12:35:00Z"),
        "{}",
        w.clock.now()
    );
}

#[test]
fn a_file_of_the_same_name_is_not_overwritten() {
    let w = World::at("clash", START);
    w.snapshot("2026-11-26T11", &[10, 99]);
    w.earlier_items(FILE, &[(99, 200)]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);

    let report = w.go();

    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.errors[0].reason, "ITEM_FILE_EXISTS");
    assert_eq!(
        read_rows(&w.file()).iter().map(|r| r.0).collect::<Vec<_>>(),
        [99]
    );
    assert_eq!(w.item_files(), [FILE], "no temporary file is left");
}

#[test]
fn a_temporary_file_that_is_already_there_is_refused_not_shared() {
    let dir = std::env::temp_dir().join(format!("dp-items-clash-tmp-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let temporary = dir.join("x.tmp");
    fs::write(&temporary, "another run's").unwrap();
    let responses = Responses::new(&dir).unwrap();

    let Err(Failure::Stop(report)) = responses.write(&temporary) else {
        panic!("expected a stop");
    };

    assert_eq!(report.reason, "LOCAL_IO_FAILED");
    assert_eq!(fs::read(&temporary).unwrap(), b"another run's");
}

#[test]
fn an_empty_set_of_responses_is_a_file_with_no_rows_and_the_columns() {
    let dir = std::env::temp_dir().join(format!("dp-items-empty-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join("e.parquet");
    Responses::new(&dir).unwrap().write(&file).unwrap();
    assert_eq!(columns(&file).len(), 4);
    assert!(read_rows(&file).is_empty());
}

#[test]
fn a_snapshots_folder_that_cannot_be_read_is_fundamental_not_empty() {
    let w = World::at("unreadable-folder", START);
    let eu = w.config.data_dir.join("bronze/commodities/eu");
    fs::create_dir_all(eu.parent().unwrap()).unwrap();
    // A file where the folder should be cannot be listed.
    fs::write(&eu, "not a folder").unwrap();

    let report = w.go();

    assert_eq!(report.reason, "LOCAL_IO_FAILED");
    assert_eq!(exit_status(&report), ExitStatus::Fundamental);
}

// DuckDB out of memory in the item files (X4, X5, D28.10)

/// A world with two items to fetch, both answering 200.
fn two_items(name: &str) -> World {
    let w = World::at(name, START);
    w.snapshot("2026-11-26T11", &[10, 11]);
    w.stub.token_ok(5);
    w.stub.item_ok(10);
    w.stub.item_ok(11);
    w
}

#[test]
fn items_failing_to_open_its_store_for_the_responses_exits_70() {
    // Before the first request: DuckDB cannot be set up to hold the responses (a
    // setting it refuses is a failure of its own, and not a full disk).
    let w = two_items("oom-store");
    let ids = ids_to_fetch(&w.config.data_dir).unwrap();
    let report = crate::lake::starved::under("not-a-size", || {
        fetch(
            &w.config,
            &w.clock,
            &Blizzard::new(w.stub.endpoints()),
            &ids,
            Holding::Take,
        )
    });
    assert_eq!(report.reason, "BRONZE_WRITE_FAILED", "{report:?}");
    assert_eq!(report.outcome, Severity::Fatal);
    assert_eq!(exit(&report), 70);
    assert_eq!(w.stub.requests("GET") + w.stub.requests("POST"), 0);
    assert!(w.item_files().is_empty());
}

#[test]
fn items_bronze_write_failure_after_the_first_request_is_a_carried_error_exit_3() {
    // The store opens, then DuckDB cannot keep a response: an error the step
    // carries on past, and the items it could not write are fetched next run.
    let w = two_items("oom-after");
    let ids = ids_to_fetch(&w.config.data_dir).unwrap();
    let report = crate::lake::starved::under(crate::lake::starved::TINY, || {
        fetch(
            &w.config,
            &w.clock,
            &Blizzard::new(w.stub.endpoints()),
            &ids,
            Holding::Take,
        )
    });
    assert_eq!(exit(&report), 3, "{report:?}");
    assert_eq!(report.outcome, Severity::Error);
    assert_eq!(report.errors[0].reason, "BRONZE_WRITE_FAILED", "{report:?}");
    assert!(w.stub.requests("GET") >= 1);
    assert!(w.item_files().is_empty());
    assert_eq!(
        ids_to_fetch(&w.config.data_dir).unwrap(),
        [ItemId(10), ItemId(11)],
        "the next run fetches them"
    );
}

#[test]
fn items_bronze_read_failure_before_the_first_request_exits_70() {
    // X4, D28.11, D14.14: DuckDB cannot read bronze's snapshot files, so the
    // step stops before its first request, unexpected, exit 70.
    let w = World::at("read-failure", START);
    let dir = w.config.data_dir.join("bronze/commodities/eu");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("2026-11-26T11.parquet"), "not parquet").unwrap();
    w.stub.token_ok(5);

    let report = w.go();

    assert_eq!(report.reason, "BRONZE_READ_FAILED", "{report:?}");
    assert_eq!(report.outcome, Severity::Fatal);
    assert_eq!(exit(&report), 70);
    assert_eq!(w.stub.requests("GET") + w.stub.requests("POST"), 0);
    assert!(w.item_files().is_empty());
}
