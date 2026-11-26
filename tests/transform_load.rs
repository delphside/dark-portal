//! The transform of one hour at the size of the real thing: a 300,000-listing
//! snapshot against a populated silver (5.4.1 *hourly processing bound*: 5
//! minutes for `transform` and `export` of one hour; *job DuckDB limits*: 1 GB
//! and one thread).
//!
//! Run by hand, and in a debug build it is several times slower than the
//! image's:
//!
//! ```text
//! cargo test --test transform_load -- --ignored --nocapture
//! ```
//!
//! Each hour has 300,000 listings; between hours 15,000 (5%) leave from the
//! low IDs, 15,000 are new, and a twentieth of those staying drop by a unit.
//! There are 400 items, so about 750 listings in each queue. The first
//! first `PRIMED` hours (or `DP_LOAD_PRIMED`) fill silver; the next is measured, its memory by sampling
//! the process's resident set while it runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::DateTime;
use dp::collect::SystemClock;
use dp::collect::lock;
use dp::collect::window::file_name;
use dp::config::{Config, Secret};
use dp::lake::{Backend, Lake};
use dp::transform::catch_up;
use dp::types::Hour;
use duckdb::Connection;

const LISTINGS: u64 = 300_000;
const CHURN: u64 = 15_000;
/// Hours that fill silver before the measured one, unless `DP_LOAD_PRIMED` says.
const PRIMED: u64 = 5;

fn primed() -> u64 {
    std::env::var("DP_LOAD_PRIMED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(PRIMED)
}

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix(field))
        .map(|v| v.trim().trim_end_matches(" kB").trim().parse().unwrap())
        .unwrap()
}

fn write_hour(config: &Config, hour: Hour, k: u64) {
    let dir = config.data_dir.join("bronze/commodities/eu");
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join(file_name(hour));
    let start = hour.start().unwrap().format("%Y-%m-%d %H:%M:%S+00");
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(&format!(
            "COPY (SELECT TIMESTAMPTZ '{start}' AS hour, TIMESTAMPTZ '{start}' AS last_modified, \
             TIMESTAMPTZ '{start}' AS fetched_at, \
             CAST(1000000000 + j AS UBIGINT) AS id, CAST(100 + j % 400 AS UINTEGER) AS item, \
             CAST(2 + (j * 13) % 200 - CASE WHEN j % 20 = 0 AND {k} >= \
                greatest(0, (j - {first}) // {CHURN}) + 1 THEN 1 ELSE 0 END AS UINTEGER) AS quantity, \
             CAST(10000 + (j * 7919) % 900000 AS UBIGINT) AS unit_price, 'LONG' AS time_left \
             FROM range({}, {}) t(j)) TO '{}' (FORMAT PARQUET);",
            k * CHURN,
            k * CHURN + LISTINGS,
            file.to_string_lossy(),
            first = LISTINGS - CHURN,
        ))
        .unwrap();
}

#[test]
#[ignore = "a measurement, run by hand"]
fn one_hour_of_300_000_listings_against_a_populated_silver() {
    let dir = std::env::temp_dir().join(format!("dp-load-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let config = Config {
        env: "load".to_owned(),
        data_dir: dir.join("data"),
        blizzard_client_id: "x".to_owned(),
        blizzard_client_secret: Secret::new("x"),
        bucket: None,
        lock_file: dir.join("dp.lock"),
        adhoc_memory_limit: "1GB".to_owned(),
        adhoc_threads: 1,
    };
    let first = Hour::containing(
        DateTime::parse_from_rfc3339("2026-11-20T00:00:00Z")
            .unwrap()
            .to_utc(),
    );
    let backend = Backend::PlainFile(dir.join("lake.duckdb"));
    let clock = SystemClock;
    let lake = Lake::open(&backend, &dir.join("tmp")).unwrap();
    lake.migrate(chrono::Utc::now()).unwrap();

    let held = lock::acquire(&config.lock_file, Duration::ZERO, &clock).unwrap();
    let primed = primed();
    for k in 0..primed {
        write_hour(&config, Hour(first.0 + k as i64), k);
        let started = Instant::now();
        let outcome = catch_up(&lake, &config.data_dir, &clock, &held);
        assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
        eprintln!(
            "priming hour {k}: {:.1} s; events {}, ended {}",
            started.elapsed().as_secs_f64(),
            outcome.hours[0].events,
            outcome.hours[0].ended
        );
    }

    write_hour(&config, Hour(first.0 + primed as i64), primed);
    let baseline = status_kib("VmRSS:");
    let peak = Arc::new(AtomicU64::new(baseline));
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let (peak, done) = (peak.clone(), done.clone());
        std::thread::spawn(move || {
            while !done.load(Ordering::Relaxed) {
                peak.fetch_max(status_kib("VmRSS:"), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let started = Instant::now();
    let outcome = catch_up(&lake, &config.data_dir, &clock, &held);
    let took = started.elapsed();
    done.store(true, Ordering::Relaxed);
    sampler.join().unwrap();
    assert!(outcome.failure.is_none(), "{:?}", outcome.failure);

    let summary = &outcome.hours[0];
    let live: i64 = lake
        .connection()
        .query_row("SELECT count(*) FROM silver.listing_hour", [], |r| r.get(0))
        .unwrap();
    eprintln!(
        "MEASURED hour {primed}: {:.1} s (bound {} s); events {}, ended {}, live {}, \
         history rows in silver {live}; RSS before {} MiB, sampled peak {} MiB, process peak {} MiB",
        took.as_secs_f64(),
        dp::settings::transform::HOURLY_PROCESSING_BOUND.as_secs(),
        summary.events,
        summary.ended,
        summary.live,
        baseline / 1024,
        peak.load(Ordering::Relaxed) / 1024,
        status_kib("VmHWM:") / 1024,
    );
    assert_eq!(summary.live, LISTINGS);
    assert!(took < dp::settings::transform::HOURLY_PROCESSING_BOUND);
    let _ = std::fs::remove_dir_all(&dir);
}
