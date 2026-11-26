-- The silver tables (docs/5.4.2-storage-schema.md, `silver`), with the types
-- of docs/5.4.5-data-dictionary.md: fixed-value types are VARCHAR holding one
-- of their values, since DuckLake has no ENUM; money is UBIGINT, an hour is
-- TIMESTAMPTZ on the hour in UTC.

CREATE SCHEMA silver;

-- Key: id.
CREATE TABLE silver.live_listing (
    id UBIGINT NOT NULL,
    item UINTEGER NOT NULL,
    unit_price UBIGINT NOT NULL,
    quantity UINTEGER NOT NULL,
    time_left VARCHAR NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    posted TIMESTAMPTZ,
    posting_lowest UBIGINT,
    posting_position UINTEGER,
    posting_units_ahead UBIGINT,
    posting_units_at_or_below UBIGINT
);

-- Key: id, hour.
CREATE TABLE silver.listing_hour (
    id UBIGINT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    quantity UINTEGER NOT NULL,
    time_left VARCHAR NOT NULL,
    position UINTEGER NOT NULL,
    units_ahead UBIGINT NOT NULL,
    units_at_or_below UBIGINT NOT NULL
);

-- Key: hour, id, kind. `span` is never null here: the first hour collected
-- writes no events.
CREATE TABLE silver.event (
    hour TIMESTAMPTZ NOT NULL,
    id UBIGINT NOT NULL,
    kind VARCHAR NOT NULL,
    item UINTEGER NOT NULL,
    units UINTEGER NOT NULL,
    unit_price UBIGINT NOT NULL,
    tier VARCHAR NOT NULL,
    rule VARCHAR NOT NULL,
    tie BOOLEAN NOT NULL,
    span USMALLINT NOT NULL
);

-- Key: id.
CREATE TABLE silver.listing_record (
    id UBIGINT NOT NULL,
    item UINTEGER NOT NULL,
    unit_price UBIGINT NOT NULL,
    quantity UINTEGER NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    posted TIMESTAMPTZ,
    duration VARCHAR,
    posting_lowest UBIGINT,
    posting_position UINTEGER,
    posting_units_ahead UBIGINT,
    posting_units_at_or_below UBIGINT,
    ended TIMESTAMPTZ NOT NULL,
    outcome VARCHAR NOT NULL,
    outcome_tier VARCHAR NOT NULL,
    tie BOOLEAN NOT NULL,
    units_sold UINTEGER NOT NULL,
    end_lowest UBIGINT NOT NULL,
    end_position UINTEGER NOT NULL,
    end_units_ahead UBIGINT NOT NULL,
    end_units_at_or_below UBIGINT NOT NULL,
    end_units_undercut UBIGINT NOT NULL
);
