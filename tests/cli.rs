//! The `dp` binary's command line: the exit status of what is typed wrongly
//! and of what is asked for (`docs/5.4.1-configuration.md`, 1.10's exit
//! statuses).

use std::process::{Command, Output};

fn dp(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dp"))
        .args(args)
        // The command line is refused before any setting is read.
        .env_clear()
        .output()
        .expect("the dp binary runs")
}

#[test]
fn bad_flag_exits_64() {
    for args in [&["--bogus"][..], &["hourly", "--bogus"], &["bogus"], &[]] {
        let output = dp(args);
        assert_eq!(output.status.code(), Some(64), "{args:?}");
        assert!(!output.stderr.is_empty(), "{args:?}: it says what is wrong");
    }
}

#[test]
fn help_and_version_are_answers_and_exit_0() {
    for args in [&["--help"][..], &["--version"], &["hourly", "--help"]] {
        assert_eq!(dp(args).status.code(), Some(0), "{args:?}");
    }
    let help = String::from_utf8(dp(&["--help"]).stdout).unwrap();
    assert!(help.contains("75"), "the help lists the exit statuses");
}
