-- The reference table (docs/5.4.2-storage-schema.md, `reference`): rebuilt
-- from bronze's item files by `dp transform`. A column Blizzard's body lacks
-- is null, except the vendor sell price, which is then 0.

CREATE SCHEMA reference;

-- Key: item.
CREATE TABLE reference.item (
    item UINTEGER NOT NULL,
    name VARCHAR,
    class VARCHAR,
    subclass VARCHAR,
    vendor_sell_price UBIGINT NOT NULL,
    vendor_buy_price UBIGINT,
    fetched_at TIMESTAMPTZ NOT NULL
);
