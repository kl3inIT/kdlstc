-- =========================================================================
-- Vertical slice 1 — QL Gia: Silver-1 landing table and Gold fact.
-- Run once. dbt will own the curated layer from slice 2 onward.
-- =========================================================================

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-1 — one table per source, verbatim.
--
-- Every column is text on purpose. This layer is not allowed to lose data by
-- casting: a price that arrived as "21.300" or a date in the wrong format
-- must still land here, otherwise the next layer has nothing to point at when
-- it explains why the row was rejected.
--
-- Column names are already warehouse vocabulary — renaming from the source's
-- camelCase payload happens on the way in, and that renaming is recorded in
-- the DAG so the lineage stays readable.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.stg_qlgia__price (
    run_id           text NOT NULL,
    source_row_id    text,               -- id on the source side, for tracing
    commodity_code   text,               -- source vocabulary, not yet mapped
    commodity_name   text,
    locality_code    text,               -- source vocabulary, not yet mapped
    survey_period    text,
    survey_date      text,
    unit_of_measure  text,
    price            text,
    source_updated_at text,
    raw_path         text NOT NULL,      -- bronze object this row was parsed from
    loaded_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_qlgia_price_run
    ON staging.stg_qlgia__price (run_id);

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-2 — typed, deduplicated, mapped, quality-flagged.
--
-- Exactly one row per business key per run. This is where the source's
-- vocabulary becomes the warehouse's, where text becomes numbers and dates,
-- and where every row gets a verdict.
--
-- Failing rows are NOT dropped here. They stay with is_valid = false and a
-- reject_reason, because the quality score, the quarantine table and the
-- answer to "why is this locality missing" are all read off this table.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.stg_qlgia__price_typed (
    run_id            text NOT NULL,
    -- warehouse codes, null when no mapping rule matched
    commodity_code    text,
    locality_code     text,
    survey_period     text,
    survey_date       date,
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
CREATE INDEX IF NOT EXISTS ix_stg_qlgia_typed_run
    ON staging.stg_qlgia__price_typed (run_id);
CREATE INDEX IF NOT EXISTS ix_stg_qlgia_typed_valid
    ON staging.stg_qlgia__price_typed (run_id, is_valid);

-- ─────────────────────────────────────────────────────────────────────────
-- Gold — fact_price
--
-- Grain: one commodity, one locality, one survey period.
-- The business key IS the grain, so writes are merge-upsert: re-running the
-- same batch any number of times produces the same table.
--
-- Codes here are warehouse codes (CMD*/LOC*), never source codes.
-- run_id + batch_id + raw_path make every cell on a report traceable back to
-- the immutable bytes in Bronze.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS curated.fact_price (
    commodity_code   text NOT NULL REFERENCES refdata.commodity(commodity_code),
    locality_code    text NOT NULL REFERENCES refdata.locality(locality_code),
    survey_period    text NOT NULL,
    survey_date      date NOT NULL,
    unit_of_measure  text NOT NULL,
    price            numeric(18,2) NOT NULL,
    run_id           text NOT NULL,
    batch_id         text NOT NULL,
    raw_path         text NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (commodity_code, locality_code, survey_period)
);
CREATE INDEX IF NOT EXISTS ix_fact_price_period
    ON curated.fact_price (survey_period);
