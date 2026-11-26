//! The words the transform's SQL shares with Rust, made once from the types
//! (`docs/5.4.5-data-dictionary.md`), so that no value of a fixed-value type
//! is spelled in `sql/transform/`: the SQL says `kind_posted()`, and
//! [`EventKind::as_str`] is where `posted` is written.
//!
//! [`install`] makes, on the job's connection and in its temporary catalog:
//!
//! - a macro for each value: `kind_*`, `outcome_*`, `tier_*`, `rule_*` and
//!   `warning_*`, named for the value in lower case with `-` as `_`;
//! - `hour_number(hour)`, the hours since the epoch, since the build has no ICU;
//! - `duration(hours, text)` and `band(time_left, lower_minutes)`, the two
//!   fixed-value types a rule reads as data.

use crate::collect::failure::Failure;
use crate::collect::snapshot::quoted;
use crate::lake::Lake;
use crate::types::{Duration, EventKind, Outcome, Rule, Tier, TimeLeft};

/// The warnings an hour can log, each for a listing: the reason code is the
/// SQL's macro and the log's `error.reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HourWarning {
    /// EV-1: a new ID at or below the highest live ID.
    IdNotAboveHighest,
    /// EV-1: an ID that ended has reappeared.
    IdReappeared,
    /// EV-2: a quantity drop behind an intact listing.
    SaleBehindIntact,
    /// A live listing's quantity rose (G7: 5.2.2 is silent).
    QuantityRose,
    /// A live listing's price changed (G7: 5.2.2 is silent).
    PriceChanged,
}

impl HourWarning {
    /// Every warning.
    pub const ALL: &'static [HourWarning] = &[
        HourWarning::IdNotAboveHighest,
        HourWarning::IdReappeared,
        HourWarning::SaleBehindIntact,
        HourWarning::QuantityRose,
        HourWarning::PriceChanged,
    ];

    /// The reason code.
    pub fn code(self) -> &'static str {
        match self {
            HourWarning::IdNotAboveHighest => "ID_NOT_ABOVE_HIGHEST",
            HourWarning::IdReappeared => "ID_REAPPEARED",
            HourWarning::SaleBehindIntact => "SALE_BEHIND_INTACT",
            HourWarning::QuantityRose => "QUANTITY_ROSE",
            HourWarning::PriceChanged => "PRICE_CHANGED",
        }
    }

    /// The sentence the log carries.
    pub fn message(self) -> &'static str {
        match self {
            HourWarning::IdNotAboveHighest => {
                "A new listing's ID is at or below the highest live ID; it is posted all the same."
            }
            HourWarning::IdReappeared => {
                "A listing that ended is in the snapshot again; it is left out and its record stands."
            }
            HourWarning::SaleBehindIntact => {
                "A listing's quantity dropped behind an intact one; it is recorded as a sale."
            }
            HourWarning::QuantityRose => {
                "A live listing's quantity rose; it is kept as it is now, with no event."
            }
            HourWarning::PriceChanged => {
                "A live listing's price changed; it is kept as it is now, with no event."
            }
        }
    }

    /// The warning for a reason code.
    pub fn from_code(code: &str) -> Option<HourWarning> {
        HourWarning::ALL.iter().copied().find(|w| w.code() == code)
    }
}

fn slug(text: &str) -> String {
    text.to_lowercase().replace('-', "_")
}

fn macros(sql: &mut String, prefix: &str, values: impl Iterator<Item = &'static str>) {
    for value in values {
        sql.push_str(&format!(
            "CREATE OR REPLACE TEMP MACRO {prefix}_{}() AS {};\n",
            slug(value),
            quoted(value)
        ));
    }
}

/// The SQL that makes the vocabulary.
pub fn sql() -> String {
    let mut sql = String::new();
    macros(&mut sql, "kind", EventKind::ALL.iter().map(|v| v.as_str()));
    macros(&mut sql, "outcome", Outcome::ALL.iter().map(|v| v.as_str()));
    macros(&mut sql, "tier", Tier::ALL.iter().map(|v| v.as_str()));
    macros(&mut sql, "rule", Rule::ALL.iter().map(|v| v.as_str()));
    macros(
        &mut sql,
        "warning",
        HourWarning::ALL.iter().map(|v| v.code()),
    );
    // The build has no ICU extension, so a TIMESTAMPTZ has no arithmetic, no
    // `epoch` and no `date_diff`: an hour is compared as its number, the hours
    // since the epoch, which `epoch_ms` gives without it.
    sql.push_str(
        "CREATE OR REPLACE TEMP MACRO hour_number(x) AS CAST(epoch_ms(x) // 3600000 AS BIGINT);\n",
    );
    sql.push_str("CREATE OR REPLACE TEMP TABLE duration (hours INTEGER, text VARCHAR);\n");
    for duration in Duration::ALL {
        sql.push_str(&format!(
            "INSERT INTO duration VALUES ({}, {});\n",
            duration.hours(),
            quoted(duration.as_str())
        ));
    }
    sql.push_str("CREATE OR REPLACE TEMP TABLE band (time_left VARCHAR, lower_minutes INTEGER);\n");
    for band in TimeLeft::ALL {
        sql.push_str(&format!(
            "INSERT INTO band VALUES ({}, {});\n",
            quoted(band.as_str()),
            band.lower_bound_minutes()
        ));
    }
    sql
}

/// Makes the vocabulary on the lake's connection.
pub fn install(lake: &Lake) -> Result<(), Failure> {
    lake.execute(&sql(), "making the vocabulary")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_value_of_every_type_has_its_macro() {
        let text = sql();
        for kind in EventKind::ALL {
            assert!(text.contains(&format!("kind_{}()", slug(kind.as_str()))));
        }
        assert!(text.contains("rule_ev_1() AS 'EV-1'"));
        assert!(text.contains("kind_partly_sold() AS 'partly_sold'"));
        assert!(text.contains("INSERT INTO duration VALUES (48, '48')"));
        assert!(text.contains("INSERT INTO band VALUES ('MEDIUM', 30)"));
    }

    #[test]
    fn a_warning_is_found_by_its_code_and_by_no_other() {
        for warning in HourWarning::ALL {
            assert_eq!(HourWarning::from_code(warning.code()), Some(*warning));
        }
        assert_eq!(HourWarning::from_code("NOT_A_WARNING"), None);
    }

    #[test]
    fn the_transform_sql_spells_no_fixed_value() {
        let files = [
            include_str!("../../sql/transform/1-compare.sql"),
            include_str!("../../sql/transform/2-events.sql"),
            include_str!("../../sql/transform/3-records.sql"),
            include_str!("../../sql/transform/4-apply.sql"),
        ];
        let values = EventKind::ALL
            .iter()
            .map(|v| v.as_str())
            .chain(Tier::ALL.iter().map(|v| v.as_str()))
            .chain(Rule::ALL.iter().map(|v| v.as_str()))
            .chain(TimeLeft::ALL.iter().map(|v| v.as_str()))
            .chain(["eu-commodities"]);
        for file in files {
            let code: String = file
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            for value in values.clone() {
                assert!(
                    !code.contains(&format!("'{value}'")),
                    "{value} is spelled in SQL"
                );
            }
        }
    }
}
