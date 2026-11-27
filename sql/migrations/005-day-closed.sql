-- The days `dp daily` has decided (docs/5.4.2-storage-schema.md, `meta`), apart
-- from `gold.item_day`'s rows: a day in which no item had a row has none, so
-- its rows cannot say it was closed. Only what DuckLake supports: no key.

-- Key: day. A day is closed where it has a row here, written in the
-- transaction that closes it, for every day closed, one with no item rows
-- included.
CREATE TABLE meta.day_closed (
    day DATE NOT NULL,
    closed_at TIMESTAMPTZ NOT NULL
);

-- Key: day. A day that was never closed and has no hour left, every hour of it
-- being past the hourly keep, so it can never be closed: the row says `dp
-- daily` has warned of it, so it warns once.
CREATE TABLE meta.day_expired (
    day DATE NOT NULL,
    warned_at TIMESTAMPTZ NOT NULL
);

-- A lake that closed days before this migration holds those days in
-- `gold.item_day`: they are closed. A day with no row there was closed to no
-- rows, and is closed again once to record it.
INSERT INTO meta.day_closed
SELECT DISTINCT day, now() FROM gold.item_day;
