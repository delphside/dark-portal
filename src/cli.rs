//! The command line: one subcommand per job
//! ([5.1.1](../docs/5.1.1-architecture.md)). Each is a stub until its
//! increment is built.

use clap::{Parser, Subcommand};

use crate::error::{ErrorType, Report};

/// `dp`: collects the auction house and keeps its history.
#[derive(Debug, Parser)]
#[command(name = "dp", version, about)]
pub struct Cli {
    /// The job to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The jobs `dp` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// The hourly run: collect, then transform and export the hour.
    Hourly,
    /// Collect the current hour's snapshot into bronze.
    Collect,
    /// Fetch item data into bronze.
    Items,
    /// Transform an hour from bronze into silver and gold.
    Transform,
    /// Export an hour for the clients.
    Export,
    /// The daily run: close the day before.
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

/// Runs a command. Every command is a stub for now: it logs that it is not
/// built yet and stops with an *unexpected* fatal, exit `70`.
pub fn run(command: Command) -> Report {
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
    fn a_stub_stops_unexpected_with_exit_70() {
        let report = run(Command::Daily);
        assert_eq!(report.reason, "NOT_BUILT");
        assert_eq!(exit_status(&report), ExitStatus::Unexpected);
        assert_eq!(exit_status(&report).code(), 70);
    }
}
