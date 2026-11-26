//! The types Dark Portal's data is held in, as
//! `docs/5.4.5-data-dictionary.md` lists them.
//!
//! A type with a fixed set of values is held in storage as a `VARCHAR` holding
//! one of them, since DuckLake has no `ENUM`; [`as_str`](TimeLeft::as_str) and
//! [`FromStr`] are the only two places that spell those values, and `dp`
//! writes no other.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};

/// A fixed-value type was given a string that is not one of its values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    type_name: &'static str,
    value: String,
}

impl ParseError {
    /// The name of the type that refused the value.
    pub fn type_name(&self) -> &'static str {
        self.type_name
    }

    /// The value that was refused.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} is not a {}", self.value, self.type_name)
    }
}

impl std::error::Error for ParseError {}

/// Defines a fixed-value enum with its `VARCHAR` spelling: `as_str`,
/// `FromStr`, `Display` and `ALL`.
macro_rules! fixed_values {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $(
                #[doc = concat!("Stored as `", $text, "`.")]
                $variant,
            )+
        }

        impl $name {
            /// Every value, in declaration order.
            pub const ALL: &'static [$name] = &[$($name::$variant),+];

            /// The `VARCHAR` this value is stored as.
            pub fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text,)+
                }
            }
        }

        impl FromStr for $name {
            type Err = ParseError;

            fn from_str(s: &str) -> Result<Self, ParseError> {
                match s {
                    $($text => Ok($name::$variant),)+
                    _ => Err(ParseError {
                        type_name: stringify!($name),
                        value: s.to_owned(),
                    }),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

/// An amount of money: 1 gold is 100 silver, and 1 silver is 100 copper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Copper(pub u64);

/// Blizzard's item ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ItemId(pub u32);

/// Blizzard's auction ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AuctionId(pub u64);

/// The hour starting of a snapshot, as hours since the Unix epoch, UTC
/// (`docs/5.2.2-listing-lifecycle-and-events.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hour(pub i64);

/// Seconds in an hour: the one place the conversion between an hour's number
/// and a time is written.
pub const SECONDS_PER_HOUR: i64 = 3600;

impl Hour {
    /// The hour a moment falls in: the timestamp truncated to the hour.
    pub fn containing(moment: DateTime<Utc>) -> Hour {
        Hour(moment.timestamp().div_euclid(SECONDS_PER_HOUR))
    }

    /// The moment the hour starts, on the hour in UTC; `None` where it is
    /// outside the range a timestamp can hold.
    pub fn start(self) -> Option<DateTime<Utc>> {
        self.0
            .checked_mul(SECONDS_PER_HOUR)
            .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
    }
}

impl From<DateTime<Utc>> for Hour {
    fn from(moment: DateTime<Utc>) -> Hour {
        Hour::containing(moment)
    }
}

fixed_values! {
    /// The band a listing's remaining time is in: under 30 minutes, 30 minutes
    /// to 2 hours, 2 to 12 hours or 12 to 48 hours.
    TimeLeft {
        Short => "SHORT",
        Medium => "MEDIUM",
        Long => "LONG",
        VeryLong => "VERY_LONG",
    }
}

impl TimeLeft {
    /// The lower bound of the band, in minutes: 0, 30, 120 or 720
    /// (`docs/5.4.5-data-dictionary.md`, *Types*).
    pub fn lower_bound_minutes(self) -> u32 {
        match self {
            TimeLeft::Short => 0,
            TimeLeft::Medium => 30,
            TimeLeft::Long => 2 * 60,
            TimeLeft::VeryLong => 12 * 60,
        }
    }
}

fixed_values! {
    /// The hours a listing was posted for.
    Duration {
        H12 => "12",
        H24 => "24",
        H48 => "48",
    }
}

impl Duration {
    /// The duration in hours.
    pub fn hours(self) -> u32 {
        match self {
            Duration::H12 => 12,
            Duration::H24 => 24,
            Duration::H48 => 48,
        }
    }
}

fixed_values! {
    /// An event (`docs/5.2.2-listing-lifecycle-and-events.md`).
    EventKind {
        Posted => "posted",
        PartlySold => "partly_sold",
        Sold => "sold",
        Expired => "expired",
        Cancelled => "cancelled",
    }
}

fixed_values! {
    /// How a listing ended.
    Outcome {
        Sold => "sold",
        Expired => "expired",
        Cancelled => "cancelled",
    }
}

fixed_values! {
    /// How sure a fact is
    /// (`docs/5.2.2-listing-lifecycle-and-events.md`).
    Tier {
        Observed => "observed",
        Certain => "certain",
        Inferred => "inferred",
    }
}

fixed_values! {
    /// The rule that decided an event's kind.
    Rule {
        Ev1 => "EV-1",
        Ev2 => "EV-2",
        Ev3 => "EV-3",
        Ev5 => "EV-5",
        Ev6 => "EV-6",
    }
}

fixed_values! {
    /// The auction house a snapshot is of.
    Market {
        EuCommodities => "eu-commodities",
    }
}

/// One listing as a snapshot shows it: the listing columns of a bronze row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Listing {
    /// The listing.
    pub id: AuctionId,
    /// The commodity.
    pub item: ItemId,
    /// The price of one unit.
    pub unit_price: Copper,
    /// Units in the listing at that hour.
    pub quantity: u32,
    /// The listing's band at that hour.
    pub time_left: TimeLeft,
}

/// An `event` row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Event {
    /// The snapshot hour the event is found in.
    pub hour: Hour,
    /// The listing.
    pub id: AuctionId,
    /// The commodity.
    pub item: ItemId,
    /// What happened.
    pub kind: EventKind,
    /// Units the event moved.
    pub units: u32,
    /// The price of one unit.
    pub unit_price: Copper,
    /// How sure the event's kind is.
    pub tier: Tier,
    /// The rule that decided the kind.
    pub rule: Rule,
    /// True where EV-4's tie applies.
    pub tie: bool,
    /// Hours since the previous snapshot held: 1 unless one was missed.
    pub span: u16,
}

/// The queue around one listing at one hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueuePlace {
    /// The lowest unit price listed.
    pub lowest: Copper,
    /// The listing's place in its item's queue, the head being 1.
    pub position: u32,
    /// Units in listings ahead of it in the queue.
    pub units_ahead: u64,
    /// Units listed at its price or lower.
    pub units_at_or_below: u64,
}

/// A `listing_record` row: `posting` and `end` flatten to `posting_*` and
/// `end_*` columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListingRecord {
    /// The listing.
    pub id: AuctionId,
    /// The commodity.
    pub item: ItemId,
    /// The price of one unit.
    pub unit_price: Copper,
    /// Units in the listing when first seen.
    pub quantity: u32,
    /// The first snapshot showing the listing.
    pub first_seen: Hour,
    /// The latest snapshot showing the listing.
    pub last_seen: Hour,
    /// The hour the listing was posted, where known.
    pub posted: Option<Hour>,
    /// The listing's duration, where known.
    pub duration: Option<Duration>,
    /// The queue around the listing in the hour it was first seen, where
    /// `posted` is known.
    pub posting: Option<QueuePlace>,
    /// The hour the listing was first missing.
    pub ended: Hour,
    /// How the listing ended.
    pub outcome: Outcome,
    /// How sure the outcome is.
    pub outcome_tier: Tier,
    /// True where EV-4's tie applies.
    pub tie: bool,
    /// Units sold from the listing over its life.
    pub units_sold: u32,
    /// The queue around the listing in the last hour it was seen.
    pub end: QueuePlace,
    /// Units in listings with a higher ID at a lower price, still listed when
    /// it was last seen.
    pub end_units_undercut: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every value of `T` round-trips through its string, and its text is the
    /// one the dictionary lists.
    fn check<T>(expected: &[&str])
    where
        T: FromStr<Err = ParseError> + Copy + PartialEq + fmt::Debug,
        T: AllValues,
    {
        let all = T::all();
        assert_eq!(all.len(), expected.len());
        for (value, text) in all.iter().zip(expected) {
            assert_eq!(value.text(), *text);
            assert_eq!(text.parse::<T>().unwrap(), *value);
        }
        let refused = "bogus".parse::<T>().unwrap_err();
        assert_eq!(refused.value(), "bogus");
        assert!("".parse::<T>().is_err());
    }

    trait AllValues: Sized + 'static {
        fn all() -> &'static [Self];
        fn text(&self) -> &'static str;
    }

    macro_rules! all_values {
        ($($t:ident),+) => {$(
            impl AllValues for $t {
                fn all() -> &'static [Self] { $t::ALL }
                fn text(&self) -> &'static str { self.as_str() }
            }
        )+};
    }
    all_values!(TimeLeft, Duration, EventKind, Outcome, Tier, Rule, Market);

    #[test]
    fn time_left_round_trips() {
        check::<TimeLeft>(&["SHORT", "MEDIUM", "LONG", "VERY_LONG"]);
        assert_eq!(TimeLeft::VeryLong.as_str(), "VERY_LONG");
    }

    #[test]
    fn duration_round_trips() {
        check::<Duration>(&["12", "24", "48"]);
        assert_eq!(Duration::H12.as_str(), "12");
        assert_eq!(Duration::H48.hours(), 48);
    }

    #[test]
    fn event_kind_round_trips() {
        check::<EventKind>(&["posted", "partly_sold", "sold", "expired", "cancelled"]);
    }

    #[test]
    fn outcome_round_trips() {
        check::<Outcome>(&["sold", "expired", "cancelled"]);
    }

    #[test]
    fn tier_round_trips() {
        check::<Tier>(&["observed", "certain", "inferred"]);
    }

    #[test]
    fn rule_round_trips() {
        check::<Rule>(&["EV-1", "EV-2", "EV-3", "EV-5", "EV-6"]);
        assert_eq!(Rule::Ev1.as_str(), "EV-1");
    }

    #[test]
    fn market_round_trips() {
        check::<Market>(&["eu-commodities"]);
    }

    #[test]
    fn a_value_is_matched_exactly() {
        assert!("short".parse::<TimeLeft>().is_err());
        assert!("ev-1".parse::<Rule>().is_err());
        assert!(" 12".parse::<Duration>().is_err());
        assert!("sold ".parse::<Outcome>().is_err());
    }

    #[test]
    fn the_refusal_names_the_type_and_the_value() {
        let refused = "7".parse::<Duration>().unwrap_err();
        assert_eq!(refused.type_name(), "Duration");
        assert_eq!(refused.to_string(), "\"7\" is not a Duration");
    }

    #[test]
    fn time_left_orders_by_remaining_time() {
        assert!(TimeLeft::Short < TimeLeft::Medium);
        assert!(TimeLeft::Long < TimeLeft::VeryLong);
    }

    #[test]
    fn an_hour_is_a_timestamp_truncated_to_the_hour() {
        let moment = DateTime::from_timestamp(1_700_000_000 + 1234, 0).unwrap();
        let hour = Hour::containing(moment);
        assert_eq!(hour, Hour(1_700_001_234_i64.div_euclid(3600)));
        let start = hour.start().unwrap();
        assert_eq!(start.timestamp() % 3600, 0);
        assert!(start <= moment && moment - start < chrono::Duration::hours(1));
        assert_eq!(Hour::from(start), hour);
    }

    #[test]
    fn the_epoch_is_hour_zero_and_before_it_truncates_downwards() {
        assert_eq!(
            Hour::containing(DateTime::from_timestamp(0, 0).unwrap()),
            Hour(0)
        );
        assert_eq!(
            Hour::containing(DateTime::from_timestamp(-1, 0).unwrap()),
            Hour(-1)
        );
        assert_eq!(Hour(-1).start().unwrap().timestamp(), -3600);
    }

    #[test]
    fn an_hour_out_of_range_has_no_start() {
        assert_eq!(Hour(i64::MAX).start(), None);
    }
}
