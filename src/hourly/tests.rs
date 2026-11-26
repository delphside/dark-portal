//! `dp hourly`: the order, the exit and the lock, over stub steps; and the
//! real steps once, over one constructed hour.

use std::cell::{Cell, RefCell};
use std::time::Duration;

use super::*;
use crate::collect::clock::FakeClock;
use crate::collect::failure::Failure;
use crate::error::ErrorType;
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
            collect.clone()
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
fn hourly_exits_with_collects_status_where_it_is_not_0_else_3_where_a_later_step_failed_else_0() {
    // O53: every combination of collect's exit and the later steps' results.
    for (name, collect, collect_exit) in collects() {
        for (case, later, later_failed) in laters() {
            let w = World::new("hourly-exit");
            let (_, report) = run_stubs(&w, collect.clone(), &later);
            let expected = if collect_exit != 0 {
                collect_exit
            } else if later_failed {
                3
            } else {
                0
            };
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
fn a_second_taker_is_refused_through_every_later_step_but_not_collect() {
    // O59, D16.7: collect holds no lock of its own across its work; the lock
    // is taken after it and held through items, transform and export.
    let w = World::new("hourly-held");
    let free_in_collect = Cell::new(false);
    let refused_in = RefCell::new(Vec::new());
    let steps = Steps {
        collect: &|| {
            free_in_collect
                .set(lock::acquire(&w.config.lock_file, Duration::ZERO, &w.clock).is_ok());
            Report::ok()
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

#[test]
fn a_lock_held_past_the_wait_is_a_blip_where_collect_succeeded_and_nothing_after_runs() {
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
fn a_lock_held_past_the_wait_leaves_collects_exit_and_is_carried_as_an_error() {
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
    assert_eq!(report.outcome, Severity::Ok, "{report:?}");
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
    assert_eq!(
        run_with(&w.config, &clock, Endpoints::blizzard, &w.backend()).outcome,
        Severity::Ok
    );
    let second = run_with(&w.config, &clock, Endpoints::blizzard, &w.backend());
    assert_eq!(code(&second), 0, "{second:?}");
}
