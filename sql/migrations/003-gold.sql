-- The gold tables (docs/5.4.2-storage-schema.md, `gold`), with the types of
-- docs/5.4.5-data-dictionary.md. Money is UBIGINT, an hour is TIMESTAMPTZ on
-- the hour in UTC, and a flow is null for the first hour collected.

CREATE SCHEMA gold;

-- Key: item, hour, unit_price. One row for each distinct price an item has
-- listed in the hour.
CREATE TABLE gold.price_level_hour (
    item UINTEGER NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    unit_price UBIGINT NOT NULL,
    units UINTEGER NOT NULL,
    listings UINTEGER NOT NULL
);

-- Key: item, hour. `lowest` is null where nothing is listed.
CREATE TABLE gold.item_hour (
    item UINTEGER NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    lowest UBIGINT,
    units_listed UBIGINT NOT NULL,
    listings UINTEGER NOT NULL,
    posted_units UBIGINT,
    sold_floor_units UBIGINT,
    sold_estimate_units UBIGINT,
    expired_units UBIGINT,
    cancelled_units UBIGINT,
    span USMALLINT
);

-- Key: item, day. Written by `dp daily`; made here so that the daily summary
-- view of sql/profiles.sql works before it has run.
CREATE TABLE gold.item_day (
    item UINTEGER NOT NULL,
    day DATE NOT NULL,
    lowest UBIGINT,
    median_fill_price DECIMAL(18, 2),
    default_n UINTEGER NOT NULL,
    posted_units UBIGINT,
    sold_floor_units UBIGINT,
    sold_estimate_units UBIGINT,
    hours_held UTINYINT NOT NULL
);
