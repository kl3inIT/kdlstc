-- =========================================================================
-- Vertical slice 1 — QL Gia: Silver-1, Silver-2 and Gold.
-- Run once. dbt will own the curated layer from slice 2 onward.
--
-- The source publishes SURVEY POINTS: several outlets per commodity per
-- district per period. The report needs one number per commodity per district
-- per period. Collapsing the first into the second is the whole point of the
-- Silver-2 -> Gold step, so the grain changes here and nowhere else.
-- =========================================================================

DROP TABLE IF EXISTS curated.fact_price;
DROP TABLE IF EXISTS staging.stg_qlgia__price_grain;
DROP TABLE IF EXISTS staging.stg_qlgia__price_typed;
DROP TABLE IF EXISTS staging.stg_qlgia__price;

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-1 — one table per source, verbatim.
--
-- Every column is text on purpose. This layer is not allowed to lose data by
-- casting: a price that arrived as "21.300" or a date in the wrong format
-- must still land here, otherwise the next layer has nothing to point at when
-- it explains why the row was rejected.
--
-- Column names are already warehouse vocabulary — renaming from the source's
-- camelCase payload happens on the way in, and only there.
--
-- Grain: one survey point.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE staging.stg_qlgia__price (
    run_id            text NOT NULL,
    source_row_id     text,              -- id on the source side, for tracing
    commodity_code    text,              -- source vocabulary, not yet mapped
    commodity_name    text,
    locality_code     text,              -- source vocabulary, not yet mapped
    outlet_code       text,              -- which shop/market was surveyed
    outlet_name       text,
    survey_period     text,
    survey_date       text,
    unit_of_measure   text,
    price             text,
    source_updated_at text,
    raw_path          text NOT NULL,     -- bronze object this row was parsed from
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stg_qlgia_price_run ON staging.stg_qlgia__price (run_id);

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-2 — typed, deduplicated, mapped, quality-flagged.
--
-- Still one row per SURVEY POINT: judging happens per observation, because
-- one bad outlet should not discard the four good ones beside it.
--
-- Failing rows are NOT dropped. They stay with is_valid = false and a
-- reject_reason, because the quality score, the quarantine table, the
-- rejected_points count on the fact, and the answer to "why is this district
-- missing" are all read off this table.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE staging.stg_qlgia__price_typed (
    run_id            text NOT NULL,
    -- warehouse codes, null when no mapping rule matched
    commodity_code    text,
    locality_code     text,
    survey_period     text,
    survey_date       date,
    outlet_code       text,
    unit_of_measure   text,
    price             numeric(18,2),
    source_updated_at timestamptz,
    -- source vocabulary kept alongside, so a rejected row is still readable
    src_item_code     text,
    src_area_code     text,
    source_row_id     text,
    raw_path          text NOT NULL,
    is_valid          boolean NOT NULL,
    reject_reason     text,
    created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stg_qlgia_typed_run   ON staging.stg_qlgia__price_typed (run_id);
CREATE INDEX ix_stg_qlgia_typed_valid ON staging.stg_qlgia__price_typed (run_id, is_valid);
CREATE INDEX ix_stg_qlgia_typed_grain ON staging.stg_qlgia__price_typed
    (commodity_code, locality_code, survey_period);

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-3 — the grain change, made explicit.
--
-- Several survey points collapse into one row per commodity x district x
-- period. This is a table rather than a subquery inside the publish step so
-- the collapse can be inspected and asserted before anything reaches Gold:
-- how many points went in, how many were thrown away first, and how wide the
-- spread was.
--
-- Groups whose points were ALL rejected appear here with survey_points = 0
-- and are held back from Gold. They are the honest answer to "why is this
-- district missing" — better than a zero nobody can explain.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE staging.stg_qlgia__price_grain (
    run_id           text NOT NULL,
    commodity_code   text NOT NULL,
    locality_code    text NOT NULL,
    survey_period    text NOT NULL,
    survey_date      date,
    unit_of_measure  text,
    avg_price        numeric(18,2),
    min_price        numeric(18,2),
    max_price        numeric(18,2),
    survey_points    int NOT NULL,
    rejected_points  int NOT NULL,
    raw_paths        text[] NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stg_qlgia_grain_run ON staging.stg_qlgia__price_grain (run_id);

-- ─────────────────────────────────────────────────────────────────────────
-- Gold — fact_price
--
-- Grain: one commodity, one district, one survey period. NOT one observation.
-- The rows below are aggregates over the valid survey points of that group.
--
-- avg_price is NON-ADDITIVE. Summing it across districts is meaningless, and
-- averaging the averages is wrong whenever the districts were surveyed at
-- different numbers of outlets. Any roll-up must weight by survey_points —
-- that column exists to make the correct calculation possible, not for
-- decoration.
--
-- rejected_points is kept beside it so a reader can see the average rests on
-- 4 of 5 observations rather than silently on all 5.
--
-- A group whose points were ALL rejected produces no row at all. The gap is
-- recorded in metadata.quality_exceptions instead of being filled with a zero.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE curated.fact_price (
    commodity_code   text NOT NULL REFERENCES refdata.commodity(commodity_code),
    locality_code    text NOT NULL REFERENCES refdata.locality(locality_code),
    survey_period    text NOT NULL,
    survey_date      date NOT NULL,
    unit_of_measure  text NOT NULL,

    avg_price        numeric(18,2) NOT NULL,   -- non-additive, weight by survey_points
    min_price        numeric(18,2) NOT NULL,
    max_price        numeric(18,2) NOT NULL,
    survey_points    int NOT NULL,             -- observations behind avg_price
    rejected_points  int NOT NULL DEFAULT 0,   -- observations thrown away first

    run_id           text NOT NULL,
    batch_id         text NOT NULL,
    raw_paths        text[] NOT NULL,          -- bronze objects behind this row
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (commodity_code, locality_code, survey_period)
);
CREATE INDEX ix_fact_price_period ON curated.fact_price (survey_period);

COMMENT ON COLUMN curated.fact_price.avg_price IS
  'Non-additive. Never SUM. Roll-up must be weighted by survey_points.';
COMMENT ON COLUMN curated.fact_price.survey_points IS
  'Weight for any roll-up above district level.';
