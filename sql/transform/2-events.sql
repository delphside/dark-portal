-- Step 2 of an hour: the events EV-1 to EV-6
-- (docs/5.2.2-listing-lifecycle-and-events.md, *The events*), and the
-- warnings they log.
--
-- Reads:   step 1's tables, current_hour (hour, hour_number, span), silver.live_listing,
--          silver.listing_record, the vocabulary (kind_*(), tier_*(),
--          rule_*(), outcome_*(), warning_*(), duration, band).
-- Makes:   arrivals, ended, hour_event, warning.

-- The IDs in C that are not live. An ID whose record is written has ended
-- before and reappears: it is left out (EV-1). The record lookup is limited
-- to IDs from the lowest arrival up, so the files of old records are skipped
-- by their statistics.
CREATE OR REPLACE TEMP TABLE arrivals AS
WITH new_listing AS (
    SELECT p.* FROM snapshot_place p
    ANTI JOIN silver.live_listing l ON l.id = p.id
),
seen_before AS (
    SELECT DISTINCT r.id FROM silver.listing_record r
    WHERE r.id >= (SELECT min(id) FROM new_listing)
      AND r.id IN (SELECT id FROM new_listing)
)
SELECT
    n.*,
    s.id IS NOT NULL AS reappeared,
    coalesce(n.id <= (SELECT max(id) FROM silver.live_listing), false) AS below_highest
FROM new_listing n
LEFT JOIN seen_before s ON s.id = n.id;

-- The gone listings, each decided: sold in the front (EV-3), otherwise unsold
-- (EV-4), and then expired (EV-5) or cancelled (EV-6).
CREATE OR REPLACE TEMP TABLE ended AS
WITH gone AS (
    SELECT
        j.*,
        j.intact_ahead = 0 AS in_front,
        -- EV-4's tie: every intact listing ahead has this price and was first
        -- seen in this listing's snapshot.
        (j.intact_ahead > 0
            AND j.intact_ahead_min_price = j.unit_price
            AND j.intact_ahead_min_first_seen = j.first_seen
            AND j.intact_ahead_max_first_seen = j.first_seen) AS tie_candidate
    FROM judged j
    WHERE j.mark = 'gone'
),
-- EV-5, posting hour known: the shortest duration whose posting hour plus the
-- duration falls in [C - span, C]. Hours are compared as hour numbers, since the
-- build has no ICU, and so no arithmetic on a TIMESTAMPTZ.
by_duration AS (
    SELECT g.id, min(d.hours) AS hours, arg_min(d.text, d.hours) AS text
    FROM gone g
    CROSS JOIN current_hour c
    JOIN duration d
      ON hour_number(g.posted) + d.hours BETWEEN c.hour_number - c.span AND c.hour_number
    WHERE NOT g.in_front AND g.posted IS NOT NULL
    GROUP BY g.id
),
-- Units in listings with a higher ID at a lower price, still listed when it
-- was last seen.
undercut AS (
    SELECT g.id, sum(o.quantity) AS units
    FROM gone g
    JOIN marked o ON o.item = g.item AND o.id > g.id AND o.unit_price < g.unit_price
    GROUP BY g.id
)
SELECT
    g.*,
    d.text AS duration,
    CASE
        WHEN g.in_front THEN outcome_sold()
        WHEN d.hours IS NOT NULL THEN outcome_expired()
        -- EV-5, posting hour unknown: the last band's lower bound is under
        -- span hours.
        WHEN g.posted IS NULL AND b.lower_minutes < c.span * 60 THEN outcome_expired()
        ELSE outcome_cancelled()
    END AS outcome,
    NOT g.in_front AND g.tie_candidate AS tie,
    CAST(coalesce(u.units, 0) AS UBIGINT) AS end_units_undercut
FROM gone g
CROSS JOIN current_hour c
LEFT JOIN by_duration d ON d.id = g.id
LEFT JOIN band b ON b.time_left = g.time_left
LEFT JOIN undercut u ON u.id = g.id;

-- The hour's events. The first hour collected has nothing to compare with, so
-- it writes none (`span` is null).
CREATE OR REPLACE TEMP TABLE hour_event AS
-- EV-1: a new ID is a posting, wherever its ID falls and however its time is
-- known.
SELECT c.hour AS hour, a.id, kind_posted() AS kind, a.item, a.quantity AS units,
       a.unit_price, tier_certain() AS tier, rule_ev_1() AS rule, false AS tie,
       CAST(c.span AS USMALLINT) AS span
FROM arrivals a CROSS JOIN current_hour c
WHERE NOT a.reappeared AND c.span IS NOT NULL
UNION ALL
-- EV-2: a quantity drop is a partial sale of the drop.
SELECT c.hour, j.id, kind_partly_sold(), j.item, j.quantity - j.new_quantity,
       j.new_unit_price, tier_certain(), rule_ev_2(), false, CAST(c.span AS USMALLINT)
FROM judged j CROSS JOIN current_hour c
WHERE j.mark = 'reduced'
UNION ALL
-- EV-3, EV-5, EV-6: each gone listing writes one event, of its outcome's kind.
SELECT c.hour, e.id,
       CASE e.outcome
           WHEN outcome_sold() THEN kind_sold()
           WHEN outcome_expired() THEN kind_expired()
           ELSE kind_cancelled()
       END,
       e.item, e.quantity, e.unit_price, tier_inferred(),
       CASE e.outcome
           WHEN outcome_sold() THEN rule_ev_3()
           WHEN outcome_expired() THEN rule_ev_5()
           ELSE rule_ev_6()
       END,
       e.tie, CAST(c.span AS USMALLINT)
FROM ended e CROSS JOIN current_hour c;

-- What is logged as a warning, one row for each listing at fault.
CREATE OR REPLACE TEMP TABLE warning (reason VARCHAR, id UBIGINT);
-- EV-1: an ID at or below the highest live ID is posted all the same.
INSERT INTO warning
SELECT warning_id_not_above_highest(), a.id FROM arrivals a CROSS JOIN current_hour c
WHERE a.below_highest AND NOT a.reappeared AND c.span IS NOT NULL;
-- EV-1: an ID that ended and reappears is left out.
INSERT INTO warning SELECT warning_id_reappeared(), id FROM arrivals WHERE reappeared;
-- EV-2: buyers take from the head, so a drop behind an intact listing is odd.
INSERT INTO warning
SELECT warning_sale_behind_intact(), id FROM judged
WHERE mark = 'reduced' AND intact_ahead > 0;
-- 5.2.2 is silent on a live listing whose quantity rises or price changes
-- (G7): it is kept, as it is now, and named here.
INSERT INTO warning SELECT warning_quantity_rose(), id FROM judged WHERE new_quantity > quantity;
INSERT INTO warning
SELECT warning_price_changed(), id FROM judged WHERE new_unit_price <> unit_price;
