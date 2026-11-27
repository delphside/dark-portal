-- The meta tables (docs/5.4.2-storage-schema.md, `meta`), and the record of
-- the migrations themselves. Only what DuckLake supports: no key, no ENUM, no
-- index; a table's key is the columns `dp` keeps unique by merging on them.

CREATE SCHEMA meta;

-- Key: version. Written by the migration runner, in the migration's own
-- transaction.
CREATE TABLE meta.migration (
    version UINTEGER NOT NULL,
    name VARCHAR NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
);

-- Key: market, hour. The latest hour of a market is its high-water mark.
CREATE TABLE meta.processed (
    market VARCHAR NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    bronze_file VARCHAR NOT NULL,
    span USMALLINT,
    committed_at TIMESTAMPTZ NOT NULL
);

-- Key: market, hour.
CREATE TABLE meta.gap (
    market VARCHAR NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    found_at TIMESTAMPTZ NOT NULL
);
