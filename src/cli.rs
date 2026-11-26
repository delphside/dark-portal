//! The command line: one subcommand per job
//! (`docs/5.1.1-architecture.md`). `hourly`, `collect`, `items`, `transform` and `export`
//! are built; `daily` is a stub until its increment is.

use clap::{Parser, Subcommand};

use crate::collect::{self, Endpoints, SystemClock};
use crate::config::Config;
use crate::error::{ErrorType, Report};
use crate::{export, hourly, items, transform};

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

/// Runs a command. Every command but `daily` runs; `daily` is a stub for now:
/// it logs that it is not built yet and stops with an *unexpected* fatal, exit
/// `70`.
pub fn run(command: Command) -> Report {
    if command != Command::Daily {
        return match Config::from_env() {
            Ok(config) => match command {
                Command::Hourly => hourly::run(&config, &SystemClock),
                Command::Collect => collect::run(&config, &SystemClock, Endpoints::blizzard()),
                Command::Items => items::run(&config, &SystemClock, Endpoints::blizzard()),
                Command::Transform => transform::run(&config, &SystemClock),
                Command::Export => export::run(&config, &SystemClock),
                Command::Daily => unreachable!("daily is a stub"),
            },
            Err(report) => *report,
        };
    }
    Report::fatal(
        ErrorType::Unexpected,
        "NOT_BUILT",
        "This command is not built yet.",
    )
    .with_metadata("command", command.name())
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
    fn each_built_command_stops_on_a_missing_setting_as_fundamental() {
        // The test process has no DP_ settings, so the command stops at its
        // configuration, which only a built command reads.
        if std::env::var_os("DP_DATA_DIR").is_some() {
            return;
        }
        for command in [
            Command::Hourly,
            Command::Collect,
            Command::Items,
            Command::Transform,
            Command::Export,
        ] {
            let report = run(command);
            assert_ne!(report.reason, "NOT_BUILT", "{}", command.name());
            assert_eq!(
                exit_status(&report),
                ExitStatus::Fundamental,
                "{}",
                command.name()
            );
        }
    }

    #[test]
    fn a_stub_stops_unexpected_with_exit_70() {
        let report = run(Command::Daily);
        assert_eq!(report.reason, "NOT_BUILT");
        assert_eq!(exit_status(&report), ExitStatus::Unexpected);
        assert_eq!(exit_status(&report).code(), 70);
    }
}
