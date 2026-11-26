-- Step 3 of an hour: the listing record of each listing that ended
-- (docs/5.2.2-listing-lifecycle-and-events.md, *The listing record*).
--
-- Reads:   ended, current_hour, silver.listing_hour (still holding the ended
--          listings' history).
-- Makes:   life, hour_record.

-- What the history says: the quantity when first seen, and the units that
-- left it by drops.
CREATE OR REPLACE TEMP TABLE life AS
SELECT
    id,
    arg_min(quantity, hour) AS first_quantity,
    coalesce(sum(greatest(CAST(previous AS BIGINT) - CAST(quantity AS BIGINT), 0)), 0) AS dropped
FROM (
    SELECT h.id, h.hour, h.quantity,
           lag(h.quantity) OVER (PARTITION BY h.id ORDER BY h.hour) AS previous
    FROM silver.listing_hour h
    WHERE h.id IN (SELECT id FROM ended)
)
GROUP BY id;

CREATE OR REPLACE TEMP TABLE hour_record AS
SELECT
    e.id, e.item, e.unit_price,
    coalesce(f.first_quantity, e.quantity) AS quantity,
    e.first_seen, e.last_seen, e.posted, e.duration,
    e.posting_lowest, e.posting_position, e.posting_units_ahead,
    e.posting_units_at_or_below,
    c.hour AS ended, e.outcome, tier_inferred() AS outcome_tier, e.tie,
    -- Units sold over its life: the drops, and all it still held if it sold.
    CAST(coalesce(f.dropped, 0)
         + CASE WHEN e.outcome = outcome_sold() THEN e.quantity ELSE 0 END AS UINTEGER)
        AS units_sold,
    e.end_lowest, e.end_position, e.end_units_ahead, e.end_units_at_or_below,
    e.end_units_undercut
FROM ended e
CROSS JOIN current_hour c
LEFT JOIN life f ON f.id = e.id;
