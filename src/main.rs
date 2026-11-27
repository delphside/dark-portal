//! The `dp` binary: parses the command line, runs the job, and ends with the
//! exit status its report gives.

use std::process::ExitCode;

use clap::Parser;
use dp::cli::{self, Cli};
use dp::error::{ExitStatus, exit_status};

fn main() -> ExitCode {
    // Logs go to stderr, which systemd sends to the journal.
    // The journal stamps each line itself and stores escape codes as typed,
    // so no time and no colour.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .without_time()
        // Fixed at INFO, not read from the environment: `ureq` at TRACE logs
        // the wire, the Authorization header with the access token in it.
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(refusal) => {
            let _ = refusal.print();
            // Help and version are answers; a bad flag is a usage error, 64.
            return if refusal.use_stderr() {
                ExitStatus::Usage.into()
            } else {
                ExitStatus::Completed.into()
            };
        }
    };

    let report = cli::run(cli.command);
    report.log();
    exit_status(&report).into()
}
