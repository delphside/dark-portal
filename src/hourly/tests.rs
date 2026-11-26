//! `dp hourly`: the order, the exit and the lock, over stub steps; and the
//! real steps once, over one constructed hour.

use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, ResponseTemplate};

use super::*;
use crate::collect::clock::FakeClock;
use crate::collect::failure::Failure;
use crate::collect::test_support::{SECRET, Stub, TOKEN, http_date};
use crate::config::Secret;
use crate::error::ErrorType;
use crate::test_support::{files_holding, logging};
use crate::transform::test_support::{World, h, item_file, l};

fn fatal(error_type: ErrorType) -> Report {
    Report::fatal(error_type, "STEP_STOPPED", "The step stopped.")
}

fn error(error_type: ErrorType) -> Report {
    Report::error(error_type, "STEP_FAILED", "The step failed.")
}

fn code(report: &Report) -> u8 {
    exit_status(report).code()
}

/// Every `collect` result the documents name, with its exit: H0.
fn collects() -> Vec<(&'static str, Report, u8)> {
    vec![
        ("0", Report::ok(), 0),
        (
            "0 after retries",
            Report::warning("COLLECTED_AFTER_RETRIES", "x"),
            0,
        ),
        ("75", fatal(ErrorType::TechnicalBlip), 75),
        ("2", fatal(ErrorType::TechnicalFundamental), 2),
        ("70", fatal(ErrorType::Unexpected), 70),
    ]
}

/// The results of `items`, `transform` and `export`, and whether any failed.
fn laters() -> Vec<(&'static str, [Report; 3], bool)> {
    vec![
        (
            "all pass",
            [Report::ok(), Report::ok(), Report::ok()],
            false,
        ),
        (
            "a warning is no failure",
            [
                Report::warning("ITEMS_FETCHED_WITH_404S", "x"),
                Report::ok(),
                Report::ok(),
            ],
            false,
        ),
        (
            "items fail",
            [error(ErrorType::TechnicalBlip), Report::ok(), Report::ok()],
            true,
        ),
        (
            "transform stops",
            [Report::ok(), fatal(ErrorType::Unexpected), Report::ok()],
            true,
        ),
        (
            "export fails",
            [
                Report::ok(),
                Report::ok(),
                error(ErrorType::TechnicalFundamental),
            ],
            true,
        ),
        (
            "all fail",
            [
                error(ErrorType::TechnicalBlip),
                fatal(ErrorType::TechnicalFundamental),
                error(ErrorType::Unexpected),
            ],
            true,
        ),
    ]
}

/// Runs the stub steps; the order they were called in and the report.
fn run_stubs(w: &World, collect: Report, later: &[Report; 3]) -> (Vec<&'static str>, Report) {
    let order = RefCell::new(Vec::new());
    let steps = Steps {
        collect: &|| {
            order.borrow_mut().push("collect");
            (collect.clone(), None)
        },
        items: &|_| {
            order.borrow_mut().push("items");
            later[0].clone()
        },
        transform: &|_| {
            order.borrow_mut().push("transform");
            later[1].clone()
        },
        export: &|_| {
            order.borrow_mut().push("export");
            later[2].clone()
        },
    };
    let report = run_steps(&w.config, &w.clock, &steps);
    (order.into_inner(), report)
}

#[test]
fn hourly_lists_the_warnings_of_every_step_and_exits_0() {
    // Each step's warnings, those it lists and one that is itself a warning,
    // are the hourly run's, which is a warning that nothing failed.
    let w = World::new("hourly-warnings");
    let collected = crate::error::warned(
        Report::ok(),
        vec![
            Report::warning("SNAPSHOT_FIELD_NEW", "new"),
            Report::warning("REFRESH_TIME_MOVED", "moved"),
        ],
    );
    let transformed = Report::warning("TIME_LEFT_UNKNOWN", "odd");
    let (_, report) = run_stubs(&w, collected, &[Report::ok(), transformed, Report::ok()]);

    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.outcome, Severity::Warning);
    assert_eq!(report.reason, "SNAPSHOT_FIELD_NEW");
    let reasons: Vec<&str> = report.warnings.iter().map(|w| w.reason.as_str()).collect();
    assert_eq!(
        reasons,
        [
            "SNAPSHOT_FIELD_NEW",
            "REFRESH_TIME_MOVED",
            "TIME_LEFT_UNKNOWN"
        ]
    );
}

#[test]
fn hourly_with_a_failed_step_keeps_its_failure_and_still_lists_the_warnings() {
    let w = World::new("hourly-warnings-failed");
    let warned = Report::warning("TIME_LEFT_UNKNOWN", "odd");
    let (_, report) = run_stubs(
        &w,
        Report::ok(),
        &[error(ErrorType::TechnicalBlip), warned, Report::ok()],
    );

    assert_eq!(report.reason, "HOURLY_STEPS_FAILED");
    assert_eq!(code(&report), 3);
    assert_eq!(report.warnings.len(), 1);
    assert_eq!(report.warnings[0].reason, "TIME_LEFT_UNKNOWN");
}

#[test]
fn hourly_runs_collect_items_transform_export_in_order() {
    let w = World::new("hourly-order");
    let (order, report) = run_stubs(
        &w,
        Report::ok(),
        &[Report::ok(), Report::ok(), Report::ok()],
    );
    assert_eq!(order, ["collect", "items", "transform", "export"]);
    assert_eq!(report.outcome, Severity::Ok);
}

#[test]
fn the_report_has_the_time_of_each_step() {
    // Each step takes a different time of the clock, which is what the report
    // carries under the step's name, whatever the step returned.
    let w = World::new("hourly-timings");
    let sleeps = [
        ("collect", 3),
        ("items", 5),
        ("transform", 7),
        ("export", 11),
    ];
    let took = |step: &str| Duration::from_secs(sleeps.iter().find(|(s, _)| *s == step).unwrap().1);
    let steps = Steps {
        collect: &|| {
            w.clock.sleep(took("collect"));
            (Report::ok(), None)
        },
        items: &|_| {
            w.clock.sleep(took("items"));
            Report::ok()
        },
        transform: &|_| {
            w.clock.sleep(took("transform"));
            error(ErrorType::TechnicalBlip)
        },
        export: &|_| {
            w.clock.sleep(took("export"));
            Report::ok()
        },
    };
    let report = run_steps(&w.config, &w.clock, &steps);
    for (step, seconds) in sleeps {
        assert_eq!(
            report.metadata[&format!("{step}_seconds")],
            format!("{seconds}.0"),
            "{step}: {report:?}"
        );
    }
}

#[test]
fn hourly_runs_items_transform_and_export_after_any_collect_result() {
    // O50, D18.2 to D18.5.
    for (name, collect, _) in collects() {
        for (case, later, _) in laters() {
            let w = World::new("hourly-always");
            let (order, _) = run_stubs(&w, collect.clone(), &later);
            assert_eq!(
                order,
                ["collect", "items", "transform", "export"],
                "collect {name}, {case}"
            );
        }
    }
}

#[test]
fn hourly_exits_with_collects_status_where_it_is_not_0() {
    // O53, H0: whatever the later steps gave.
    for (name, collect, collect_exit) in collects() {
        if collect_exit == 0 {
            continue;
        }
        for (case, later, _) in laters() {
            let w = World::new("hourly-exit-collect");
            let (_, report) = run_stubs(&w, collect.clone(), &later);
            assert_eq!(
                code(&report),
                collect_exit,
                "collect {name}, {case}: {report:?}"
            );
        }
    }
}

#[test]
fn hourly_exits_3_where_collect_succeeded_and_a_later_step_failed() {
    // O53, H0: collect gave 0, or 0 with warnings: 3 where any later step
    // failed, else 0.
    for (name, collect, collect_exit) in collects() {
        if collect_exit != 0 {
            continue;
        }
        for (case, later, later_failed) in laters() {
            let w = World::new("hourly-exit-later");
            let (_, report) = run_stubs(&w, collect.clone(), &later);
            let expected = if later_failed { 3 } else { 0 };
            assert_eq!(
                code(&report),
                expected,
                "collect {name}, {case}: {report:?}"
            );
        }
    }
}

#[test]
fn collects_own_report_stands_and_carries_the_later_failures() {
    let w = World::new("hourly-carries");
    let (_, report) = run_stubs(
        &w,
        fatal(ErrorType::TechnicalBlip),
        &[
            error(ErrorType::TechnicalBlip),
            Report::ok(),
            error(ErrorType::Unexpected),
        ],
    );
    assert_eq!(report.reason, "STEP_STOPPED");
    assert_eq!(code(&report), 75);
    let steps: Vec<&str> = report
        .errors
        .iter()
        .map(|e| e.metadata["step"].as_str())
        .collect();
    assert_eq!(steps, ["items", "export"]);
    assert_eq!(report.errors[1].error_type, Some(ErrorType::Unexpected));
}

#[test]
fn a_later_failure_is_one_report_with_each_failed_step_as_its_error_whatever_its_type() {
    let w = World::new("hourly-errors");
    let (_, report) = run_stubs(
        &w,
        Report::ok(),
        &[
            Report::ok(),
            fatal(ErrorType::Unexpected),
            error(ErrorType::TechnicalFundamental),
        ],
    );
    assert_eq!(report.reason, "HOURLY_STEPS_FAILED");
    assert_eq!(report.outcome, Severity::Error);
    assert_eq!(code(&report), 3);
    assert_eq!(report.error_type, Some(ErrorType::TechnicalFundamental));
    assert_eq!(report.errors.len(), 2);
    // Each carried failure is an error the run went on past, with its type.
    assert!(report.errors.iter().all(|e| e.outcome == Severity::Error));
    assert_eq!(report.errors[0].error_type, Some(ErrorType::Unexpected));
    // Every step's result is in the run's report.
    assert_eq!(report.metadata["step_collect"], "OK");
    assert_eq!(report.metadata["step_transform"], "STEP_STOPPED");
    assert_eq!(report.metadata["step_export"], "STEP_FAILED");
}

// -------------------------------------------------------------------- lock

fn refused(w: &World) -> bool {
    matches!(
        lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock),
        Err(Failure::Stop(r)) if r.reason == "LOCK_WAIT_EXPIRED"
    )
}

#[test]
fn hourly_takes_the_lock_for_items_where_collect_took_none() {
    // O59, D16.7, X-c7: collect holds no lock across its work; where it took
    // none (step 1, or a failure first), hourly takes it for items and holds it
    // through transform and export.
    let w = World::new("hourly-held");
    let free_in_collect = Cell::new(false);
    let refused_in = RefCell::new(Vec::new());
    let steps = Steps {
        collect: &|| {
            free_in_collect
                .set(lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).is_ok());
            (Report::ok(), None)
        },
        items: &|_| {
            refused_in.borrow_mut().push(refused(&w));
            Report::ok()
        },
        transform: &|_| {
            refused_in.borrow_mut().push(refused(&w));
            Report::ok()
        },
        export: &|_| {
            refused_in.borrow_mut().push(refused(&w));
            Report::ok()
        },
    };
    let report = run_steps(&w.config, &w.clock, &steps);
    assert_eq!(report.outcome, Severity::Ok);
    assert!(free_in_collect.get());
    assert_eq!(*refused_in.borrow(), [true, true, true]);
    // Released at the end.
    assert!(lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).is_ok());
}

/// A clock that, each time it is asked for the time or a wait once the hour's
/// file is in bronze, tries the lock from a second handle: whether it was
/// free is a gap in the run's hold.
struct ProbingClock {
    inner: FakeClock,
    lock_file: PathBuf,
    bronze_file: PathBuf,
    probes: Cell<u32>,
    free: Cell<u32>,
}

impl ProbingClock {
    fn probe(&self) {
        if !self.bronze_file.exists() {
            return;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock_file)
            .unwrap();
        self.probes.set(self.probes.get() + 1);
        if file.try_lock().is_ok() {
            self.free.set(self.free.get() + 1);
        }
    }
}

impl Clock for ProbingClock {
    fn now(&self) -> DateTime<Utc> {
        self.probe();
        self.inner.now()
    }

    fn sleep(&self, duration: Duration) {
        self.probe();
        self.inner.sleep(duration);
    }
}

/// A world whose Blizzard is stubbed: it serves hour 11's snapshot, of one
/// listing of item 10, and item 10's data; the clock is at 11:40.
fn blizzard_world(name: &str) -> (World, Stub) {
    let mut w = World::new(name);
    w.config.blizzard_client_secret = Secret::new(SECRET);
    let stub = Stub::new();
    stub.token_ok(5);
    let bearer = format!("Bearer {TOKEN}");
    stub.mount(
        Mock::given(method("GET"))
            .and(path("/data/wow/auctions/commodities"))
            .and(query_param("namespace", "dynamic-eu"))
            .and(header("authorization", bearer.as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("last-modified", http_date("2026-11-26T11:31:40Z").as_str())
                    .set_body_string(
                        r#"{"auctions":[{"id":1,"item":{"id":10},"quantity":5,
                        "unit_price":123,"time_left":"SHORT"}]}"#,
                    ),
            ),
    );
    stub.mount(
        Mock::given(method("GET"))
            .and(path("/data/wow/item/10"))
            .and(query_param("namespace", "static-eu"))
            .and(header("authorization", bearer.as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"id":10,"name":"Flask","sell_price":5}"#),
            ),
    );
    (w, stub)
}

#[test]
fn hourly_holds_the_lock_from_the_rename_through_export() {
    // O59, D16.7, X-c7: the real collect, so the gap between its rename and the
    // later steps is the real one. The clock tries the lock at every time and
    // wait asked for once the hour is in bronze: none may find it free.
    let (w, stub) = blizzard_world("hourly-gap");
    let clock = ProbingClock {
        inner: FakeClock::at("2026-11-26T11:40:00Z"),
        lock_file: w.config.lock_file.clone(),
        bronze_file: crate::collect::bronze_dir(&w.config.data_dir).join("2026-11-26T11.parquet"),
        probes: Cell::new(0),
        free: Cell::new(0),
    };
    let report = run_with(&w.config, &clock, || stub.endpoints(), &w.backend());
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["step_export"], "OK");
    assert!(
        clock.probes.get() > 0,
        "the probe saw the run after the rename"
    );
    assert_eq!(clock.free.get(), 0, "the lock was free after the rename");
    // Released when the run ends.
    assert!(lock::acquire(&w.config.lock_file, Duration::ZERO, &clock).is_ok());
}

#[test]
fn items_is_lent_the_lock_and_takes_none_of_its_own_for_a_new_item() {
    // A new item makes items rename a file into place, which it may do only
    // under the lock hourly holds: taking another would wait out the lock wait
    // and fail.
    let (w, stub) = blizzard_world("hourly-items-lent");
    let report = run_with(&w.config, &w.clock, || stub.endpoints(), &w.backend());
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["step_items"], "OK");
    assert!(crate::items::has_parquet(&crate::items::items_dir(&w.config.data_dir)).unwrap());
    assert!(
        !w.clock.slept().contains(&Duration::from_secs(1)),
        "no wait for the lock: {:?}",
        w.clock.slept()
    );
}

#[test]
fn hourly_skips_later_steps_and_exits_75_where_the_lock_is_held_past_the_wait() {
    let w = World::new("hourly-lock-blip");
    let held = lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).unwrap();
    let (order, report) = run_stubs(
        &w,
        Report::ok(),
        &[Report::ok(), Report::ok(), Report::ok()],
    );
    assert_eq!(order, ["collect"]);
    assert_eq!(report.reason, "LOCK_WAIT_EXPIRED");
    assert_eq!(code(&report), 75);
    let waited: Duration = w.clock.slept().iter().sum();
    assert_eq!(waited, LOCK_WAIT);
    drop(held);
}

#[test]
fn hourly_exits_collects_status_where_the_lock_is_held_and_collect_failed() {
    let w = World::new("hourly-lock-collect-failed");
    let _held = lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).unwrap();
    let (order, report) = run_stubs(
        &w,
        fatal(ErrorType::TechnicalFundamental),
        &[Report::ok(), Report::ok(), Report::ok()],
    );
    assert_eq!(order, ["collect"]);
    assert_eq!(code(&report), 2);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].reason, "LOCK_WAIT_EXPIRED");
}

// ---------------------------------------------------------- the real steps

#[test]
fn the_real_steps_take_the_lock_once_and_run_one_hour_through() {
    // The collected hour is in bronze and the item has its response, so no
    // request is made. A step that took the lock its caller holds would wait
    // out the lock wait and fail: the run is whole only if the lock is taken
    // once.
    let w = World::new("hourly-real");
    w.snapshot(10, &[l(1, 100, 5)]);
    item_file(
        &w,
        "2026-11-20T093500",
        &[(
            1,
            "2026-11-20 09:35:00+00",
            200,
            Some(r#"{"name":"Flask","sell_price":5}"#),
        )],
    );
    let clock = FakeClock::at("2026-11-20T10:40:00Z");
    let report = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(
        clock.slept().is_empty(),
        "nothing waited: {:?}",
        clock.slept()
    );
    assert_eq!(report.metadata["step_collect"], "OK");

    // Transform committed the hour, export wrote it, and the lock is free.
    let lake = w.open();
    assert_eq!(
        crate::transform::high_water_mark(&lake).unwrap(),
        Some(h(10))
    );
    assert!(crate::export::hour_file(&w.config.data_dir, "item_hour", h(10)).exists());
    assert!(
        crate::export::export_dir(&w.config.data_dir)
            .join("profiles.sql")
            .exists()
    );
    assert!(lock::acquire(&w.config.lock_file, Duration::ZERO, &clock).is_ok());
}

#[test]
fn transform_and_export_of_an_hour_are_judged_together_against_5_minutes() {
    // H10, TT3: each step is inside the bound for the hour, the two are not.
    let w = World::new("hourly-bound-sum");
    w.snapshot(10, &[l(1, 100, 5)]);
    item_file(
        &w,
        "2026-11-20T093500",
        &[(
            1,
            "2026-11-20 09:35:00+00",
            200,
            Some(r#"{"name":"Flask","sell_price":5}"#),
        )],
    );
    let clock = FakeClock::at("2026-11-20T10:40:00Z");
    let transform_takes = Duration::from_secs(200);
    let export_file_takes = Duration::from_secs(100); // two files: 200 s
    assert!(transform_takes < crate::settings::transform::HOURLY_PROCESSING_BOUND);
    assert!(2 * export_file_takes < crate::settings::transform::HOURLY_PROCESSING_BOUND);
    let times = crate::bound::HourTimes::default();
    let report = run_hooked(
        &w.config,
        &clock,
        Endpoints::blizzard,
        &w.backend(),
        &|_, at| {
            if at == crate::transform::Step::Apply {
                clock.sleep(transform_takes);
            }
            Ok(())
        },
        &|lake, file, select| {
            clock.sleep(export_file_takes);
            crate::export::publish(lake, file, select)
        },
        &times,
    );
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["step_export"], "OK");
    assert_eq!(exit_status(&report), ExitStatus::Completed);
    assert_eq!(
        times.over_bound(),
        [(h(10), Duration::from_secs(400))],
        "the sum is over the bound"
    );
    // The export, the last step, judged it and counted the hour.
    assert_eq!(report.metadata["hours_over_bound"], "1");
}

#[test]
fn runs_own_work_counts_against_the_bound_shared_across_its_hours() {
    // H10, TT3: no hour's own step is slow; rewriting the files that are not
    // an hour's takes 301 s, which the hour carries.
    let w = World::new("hourly-bound-run-level");
    w.snapshot(10, &[l(1, 100, 5)]);
    item_file(
        &w,
        "2026-11-20T093500",
        &[(
            1,
            "2026-11-20 09:35:00+00",
            200,
            Some(r#"{"name":"Flask","sell_price":5}"#),
        )],
    );
    let clock = crate::transform::test_support::SlowOnceWritten::new(
        "2026-11-20T10:40:00Z",
        crate::export::export_dir(&w.config.data_dir).join("item_day.parquet"),
        Duration::from_secs(301),
    );
    let report = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(report.metadata["hours_over_bound"], "1");
}

#[test]
fn a_rerun_of_the_real_steps_does_nothing_and_exits_0() {
    let w = World::new("hourly-real-rerun");
    w.snapshot(10, &[l(1, 100, 5)]);
    item_file(
        &w,
        "2026-11-20T093500",
        &[(
            1,
            "2026-11-20 09:35:00+00",
            200,
            Some(r#"{"name":"Flask","sell_price":5}"#),
        )],
    );
    let clock = FakeClock::at("2026-11-20T10:40:00Z");
    let first = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&first), 0, "{first:?}");
    let second = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&second), 0, "{second:?}");
}

#[test]
fn failed_transform_hour_rolls_back_whole_and_hourly_exits_3() {
    // H0, H3: under `dp hourly` the transform fails in its hour: that hour is
    // rolled back whole, the export still runs, and the exit is 3. The next
    // run processes the hour.
    let w = World::new("hourly-failed-hour");
    w.snapshot(10, &[l(1, 100, 5)]);
    item_file(
        &w,
        "2026-11-20T093500",
        &[(
            1,
            "2026-11-20 09:35:00+00",
            200,
            Some(r#"{"name":"Flask","sell_price":5}"#),
        )],
    );
    let clock = FakeClock::at("2026-11-20T10:40:00Z");
    // The hours' tables: the reference table is rebuilt after the hours, not in one.
    let hours_dump = |lake: &crate::lake::Lake| {
        let text = crate::transform::test_support::dump(lake);
        let end = text
            .find("FROM reference.item t")
            .expect("the reference part");
        text[..end].to_owned()
    };
    let before = hours_dump(&w.open());
    let times = crate::bound::HourTimes::default();
    let report = run_hooked(
        &w.config,
        &clock,
        Endpoints::blizzard,
        &w.backend(),
        &|_, at| {
            if at == crate::transform::Step::Gold {
                Err(Failure::unexpected("INJECTED", "A failure mid-hour."))
            } else {
                Ok(())
            }
        },
        &crate::export::publish,
        &times,
    );
    assert_eq!(code(&report), 3, "{report:?}");
    assert_eq!(report.metadata["step_collect"], "OK");
    assert_eq!(report.metadata["step_export"], "OK", "export still ran");
    let lake = w.open();
    assert_eq!(crate::transform::high_water_mark(&lake).unwrap(), None);
    assert!(crate::transform::test_support::processed(&lake).is_empty());
    assert_eq!(
        hours_dump(&lake),
        before,
        "nothing of the hour is visible: silver, gold and meta as before"
    );
    drop(lake);

    let report = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&report), 0, "{report:?}");
    assert_eq!(
        crate::transform::high_water_mark(&w.open()).unwrap(),
        Some(h(10))
    );
}

#[test]
fn hourly_carries_an_out_of_memory_step_as_exit_3() {
    // X5, D28.8, H0: where the transform, or the export, runs out of memory
    // under `dp hourly`, that step fails with its fundamental error and the run
    // exits 3 (the exit it would have had alone is 2), the other steps having
    // run.
    for failing in ["transform", "export"] {
        let w = World::new(&format!("hourly-oom-{failing}"));
        w.snapshot(10, &[l(1, 100, 5)]);
        if failing == "transform" {
            // A lake already made, so the memory runs out in the hour.
            drop(w.open());
        }
        let limit = if failing == "transform" {
            crate::lake::starved::AFTER_OPENING
        } else {
            crate::lake::starved::TINY
        };
        let times = HourTimes::default();
        let ran = RefCell::new(Vec::new());
        let steps = Steps {
            collect: &|| (Report::ok(), None),
            items: &|_| {
                ran.borrow_mut().push("items");
                Report::ok()
            },
            transform: &|held| {
                ran.borrow_mut().push("transform");
                if failing == "transform" {
                    crate::lake::starved::under(limit, || {
                        crate::transform::run_locked(
                            &w.config,
                            &w.clock,
                            &w.backend(),
                            &|_, _| Ok(()),
                            held,
                            &times,
                        )
                    })
                } else {
                    Report::ok()
                }
            },
            export: &|held| {
                ran.borrow_mut().push("export");
                if failing == "export" {
                    crate::lake::starved::under(limit, || {
                        crate::export::run_locked(&w.config, &w.clock, &w.backend(), held, &times)
                    })
                } else {
                    Report::ok()
                }
            },
        };
        let report = run_steps(&w.config, &w.clock, &steps);
        assert_eq!(code(&report), 3, "{failing}: {report:#?}");
        assert_eq!(ran.into_inner(), ["items", "transform", "export"]);
        assert_eq!(report.errors.len(), 1, "{failing}: {report:#?}");
        assert_eq!(report.errors[0].reason, "OUT_OF_MEMORY", "{failing}");
        assert_eq!(report.errors[0].metadata["step"], failing);
        assert_eq!(
            report.errors[0].error_type,
            Some(ErrorType::TechnicalFundamental)
        );
        assert_eq!(report.metadata[&format!("step_{failing}")], "OUT_OF_MEMORY");
    }
}

#[test]
fn hourly_with_the_lock_held_past_the_wait_reports_only_collect_seconds_and_the_total() {
    // TM, D35.4: collect ran, took no lock, and the lock stayed held past the
    // wait: the steps after it never ran, so they have no key (never null or
    // 0), and the total is the whole run, the wait included.
    let w = World::new("hourly-lock-timings");
    let _held = lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).unwrap();
    let report = bound::totalled(&w.clock, || {
        run_stubs(
            &w,
            Report::ok(),
            &[Report::ok(), Report::ok(), Report::ok()],
        )
        .1
    });
    assert_eq!(code(&report), 75, "{report:#?}");
    for step in ["items", "transform", "export"] {
        assert!(
            !report.metadata.contains_key(&format!("{step}_seconds")),
            "{step}: {report:#?}"
        );
    }
    assert_eq!(report.metadata["collect_seconds"], "0.0");
    assert_eq!(
        report.metadata["total_seconds"],
        bound::seconds(LOCK_WAIT),
        "{report:#?}"
    );
    let keys: Vec<&str> = report
        .metadata
        .keys()
        .map(String::as_str)
        .filter(|key| key.ends_with("_seconds"))
        .collect();
    assert_eq!(keys, ["collect_seconds", "total_seconds"]);
}

#[test]
fn secret_never_appears_in_logs_bronze_or_export() {
    // SEC, O44: the client secret is read from its file and used to ask for a
    // token; after a whole hour (collect, items, transform, export) and a day's
    // export it is in no log line, no report and no file of bronze or the
    // export. The scan finds what is there (the item's name), so it can find
    // what should not be.
    let (w, stub) = blizzard_world("hourly-secret");
    let (report, log) = logging(|| {
        let report = run_with(&w.config, &w.clock, || stub.endpoints(), &w.backend());
        report.log();
        report
    });
    assert_eq!(code(&report), 0, "{report:?}");
    assert!(log.contains("took a new Blizzard access token"), "{log}");
    let data = &w.config.data_dir;
    assert!(!files_holding(&data.join("bronze"), "Flask").is_empty());
    assert!(!files_holding(&data.join("export"), "Flask").is_empty());
    for hidden in [SECRET, TOKEN] {
        assert!(!log.contains(hidden), "{hidden} in the log");
        assert!(!format!("{report:?}").contains(hidden));
        assert_eq!(
            files_holding(&data.join("bronze"), hidden),
            Vec::<PathBuf>::new()
        );
        assert_eq!(
            files_holding(&data.join("export"), hidden),
            Vec::<PathBuf>::new()
        );
    }
}
