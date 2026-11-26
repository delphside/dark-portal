//! The command line: one subcommand per job
//! (`docs/5.1.1-architecture.md`).

use clap::{Parser, Subcommand};

use crate::collect::{self, Endpoints, SystemClock};
use crate::config::Config;
use crate::error::Report;
use crate::{daily, export, hourly, items, transform};

/// The exit statuses, for `--help` (1.10, *What a script's exit status
/// means*).
const EXIT_STATUSES: &str = "Exit statuses:
  0   completed
  1   stopped on a business error
  2   stopped on a fundamental error, such as a missing setting or a full disk
  3   completed with errors, which the next run repeats
  64  usage error
  70  unexpected error
  75  temporary failure, such as a late snapshot or the lock held; retry";

/// `dp`: collects the auction house and keeps its history.
#[derive(Debug, Parser)]
#[command(name = "dp", version, about, after_help = EXIT_STATUSES)]
pub struct Cli {
    /// The job to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The jobs `dp` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// The hourly run: collect, items, transform and export, in that order.
    Hourly,
    /// Collect the current hour's snapshot into bronze.
    Collect,
    /// Fetch item data for item IDs new to bronze.
    Items,
    /// Process every bronze hour after the high-water mark.
    Transform,
    /// Write the export: every committed hour within the hourly keep that it lacks.
    Export,
    /// Close the day, push to the bucket, delete what is past its keep and
    /// retry items that answered 404.
    Daily,
}

impl Command {
    /// The name the command is typed as.
    pub fn name(self) -> &'static str {
        match self {
            Command::Hourly => "hourly",
            Command::Collect => "collect",
            Command::Items => "items",
            Command::Transform => "transform",
            Command::Export => "export",
            Command::Daily => "daily",
        }
    }
}

/// Runs a command. Each reads its settings first: a missing or malformed one
/// stops it as *fundamental*, exit `2`.
pub fn run(command: Command) -> Report {
    match Config::from_env() {
        Ok(config) => match command {
            Command::Hourly => hourly::run(&config, &SystemClock),
            Command::Collect => collect::run(&config, &SystemClock, Endpoints::blizzard()),
            Command::Items => items::run(&config, &SystemClock, Endpoints::blizzard()),
            Command::Transform => transform::run(&config, &SystemClock),
            Command::Export => export::run(&config, &SystemClock),
            Command::Daily => daily::run(&config, &SystemClock, Endpoints::blizzard()),
        },
        Err(report) => *report,
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    use crate::error::{ExitStatus, exit_status};

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn each_subcommand_parses_by_its_name() {
        for command in [
            Command::Hourly,
            Command::Collect,
            Command::Items,
            Command::Transform,
            Command::Export,
            Command::Daily,
        ] {
            let cli = Cli::try_parse_from(["dp", command.name()]).unwrap();
            assert_eq!(cli.command, command);
        }
    }

    #[test]
    fn an_unknown_subcommand_or_none_is_refused() {
        assert!(Cli::try_parse_from(["dp", "bogus"]).is_err());
        assert!(Cli::try_parse_from(["dp"]).is_err());
    }

    #[test]
    fn each_command_stops_on_a_missing_setting_as_fundamental() {
        // The test process has no DP_ settings, so the command stops at its
        // configuration.
        if std::env::var_os("DP_DATA_DIR").is_some() {
            return;
        }
        for command in [
            Command::Hourly,
            Command::Collect,
            Command::Items,
            Command::Transform,
            Command::Export,
            Command::Daily,
        ] {
            let report = run(command);
            assert_eq!(report.reason, "MISSING_SETTING", "{}", command.name());
            assert_eq!(
                exit_status(&report),
                ExitStatus::Fundamental,
                "{}",
                command.name()
            );
        }
    }
}
