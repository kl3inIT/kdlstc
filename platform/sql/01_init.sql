-- =========================================================================
-- Hung Yen Finance Data Warehouse — schema bootstrap
--
-- Layers:
--   ingestion  run ledger + incremental cursors
--   staging    Silver-1, one table per source, verbatim, all text
--   refdata    shared master data (MDM)
--   metadata   mapping rules, rejection queue, quality exceptions, quarantine
--   curated    Gold, dim_* / fact_* / dm_*
--   audit      append-only trail
--
-- Idempotent: safe to re-run.
-- =========================================================================

DROP SCHEMA IF EXISTS ingestion CASCADE;
DROP SCHEMA IF EXISTS staging   CASCADE;
DROP SCHEMA IF EXISTS refdata   CASCADE;
DROP SCHEMA IF EXISTS metadata  CASCADE;
DROP SCHEMA IF EXISTS curated   CASCADE;
DROP SCHEMA IF EXISTS audit     CASCADE;

CREATE SCHEMA ingestion;
CREATE SCHEMA staging;
CREATE SCHEMA refdata;
CREATE SCHEMA metadata;
CREATE SCHEMA curated;
CREATE SCHEMA audit;

-- =========================================================================
-- ingestion — every batch gets a ticket, and the ticket has a state machine
-- =========================================================================
CREATE TABLE ingestion.sources (
    source_code     text PRIMARY KEY,
    description     text,
    ingest_method   text NOT NULL,        -- rest_api | sql | file
    load_mode       text NOT NULL,        -- incremental | full_period
    business_key    text,                 -- natural key of one source row
    cursor_column   text,                 -- column driving incremental pulls
    data_owner      text,
    sensitivity     text NOT NULL DEFAULT 'internal',
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- One row per ingestion attempt. Nothing reaches curated without a run_id,
-- so any published number can be traced back to the bytes it came from.
CREATE TABLE ingestion.runs (
    run_id          text PRIMARY KEY,     -- r_<ulid>
    source_code     text NOT NULL REFERENCES ingestion.sources(source_code),
    period          text NOT NULL,        -- logical period of the run
    status          text NOT NULL,        -- received | schema_blocked | parsed
                                          -- | mapped | quality_passed
                                          -- | quality_failed | published
    row_count       bigint,
    checksum_sha256 text,
    raw_path        text,                 -- bronze/<source>/<period>/<run_id>/
    quality_score   numeric(5,4),
    message         text,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);
CREATE INDEX ix_runs_source_period ON ingestion.runs (source_code, period);
CREATE INDEX ix_runs_status        ON ingestion.runs (status);

-- High-water mark per source for incremental loads.
CREATE TABLE ingestion.cursors (
    source_code   text PRIMARY KEY REFERENCES ingestion.sources(source_code),
    cursor_value  text NOT NULL,
    run_id        text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- =========================================================================
-- refdata — shared master data.
-- Never hard-deleted: rows are closed with valid_to so historical reports
-- keep resolving. superseded_by carries mergers (e.g. commune consolidation).
-- =========================================================================
CREATE TABLE refdata.commodity (
    commodity_code   text PRIMARY KEY,
    commodity_name   text NOT NULL,
    commodity_group  text,
    unit_of_measure  text NOT NULL,
    specification    text,
    valid_from       date NOT NULL DEFAULT '2020-01-01',
    valid_to         date NOT NULL DEFAULT '9999-12-31',
    version          int  NOT NULL DEFAULT 1,
    created_by       text NOT NULL DEFAULT 'seed',
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE refdata.locality (
    locality_code    text PRIMARY KEY,
    locality_name    text NOT NULL,
    admin_level      text NOT NULL,       -- province | district | commune
    parent_code      text,
    valid_from       date NOT NULL DEFAULT '2020-01-01',
    valid_to         date NOT NULL DEFAULT '9999-12-31',
    superseded_by    text,                -- set when merged into another unit
    version          int  NOT NULL DEFAULT 1,
    created_by       text NOT NULL DEFAULT 'seed',
    created_at       timestamptz NOT NULL DEFAULT now()
);

-- =========================================================================
-- metadata — how source vocabulary becomes warehouse vocabulary,
--            and what happened to the rows that did not make it
-- =========================================================================
CREATE TABLE metadata.mapping_rules (
    id              bigserial PRIMARY KEY,
    source_code     text NOT NULL,
    code_type       text NOT NULL,        -- commodity | locality
    source_value    text NOT NULL,
    standard_value  text NOT NULL,
    valid_from      date NOT NULL DEFAULT '2020-01-01',
    valid_to        date NOT NULL DEFAULT '9999-12-31',
    version         int  NOT NULL DEFAULT 1,
    created_by      text NOT NULL DEFAULT 'seed',
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_code, code_type, source_value, valid_from)
);

-- A source code with no mapping rule is a business question, not a bug.
-- It lands here with an owner and a due date instead of being silently dropped.
CREATE TABLE metadata.mapping_rejections (
    id            bigserial PRIMARY KEY,
    run_id        text NOT NULL,
    source_code   text NOT NULL,
    code_type     text NOT NULL,
    source_value  text NOT NULL,
    row_count     int  NOT NULL DEFAULT 1,
    reason        text NOT NULL,
    status        text NOT NULL DEFAULT 'pending',  -- pending | resolved | ignored
    assignee      text,
    due_date      date,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_mapping_rejections_status ON metadata.mapping_rejections (status);

-- One row per rule evaluated per run — the evidence behind quality_score.
CREATE TABLE metadata.quality_exceptions (
    id           bigserial PRIMARY KEY,
    run_id       text NOT NULL,
    table_name   text NOT NULL,
    rule_name    text NOT NULL,
    severity     text NOT NULL,           -- blocker | scoring
    failed_rows  bigint,
    pass_rate    numeric(6,4),
    details      jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_quality_exceptions_run ON metadata.quality_exceptions (run_id);

-- Rows rejected by a scoring rule. Kept verbatim so "why is Phu Cu missing
-- from the report" has an answer that points at a specific row.
CREATE TABLE metadata.quarantine_rows (
    id            bigserial PRIMARY KEY,
    run_id        text NOT NULL,
    source_table  text NOT NULL,
    rule_name     text NOT NULL,
    payload       jsonb NOT NULL,
    raw_path      text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_quarantine_run ON metadata.quarantine_rows (run_id);

-- =========================================================================
-- curated — the publication gate.
-- Data is written to fact/dim tables first, then declared visible here.
-- =========================================================================
CREATE TABLE curated.batch_summary (
    batch_id        text PRIMARY KEY,     -- b_<ulid>
    run_id          text NOT NULL,
    source_code     text NOT NULL,
    period          text NOT NULL,
    target_table    text NOT NULL,
    row_count       bigint,
    quality_score   numeric(5,4),
    publish_status  text NOT NULL DEFAULT 'pending',  -- pending | approved | rejected
    data_freshness  timestamptz,          -- newest source timestamp in the batch
    published_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_batch_summary_run ON curated.batch_summary (run_id);

-- =========================================================================
-- audit — append-only
-- =========================================================================
CREATE TABLE audit.audit_log (
    id            bigserial PRIMARY KEY,
    occurred_at   timestamptz NOT NULL DEFAULT now(),
    actor         text NOT NULL,
    action        text NOT NULL,
    object_ref    text,
    before_after  jsonb,
    trace_id      text
);
CREATE INDEX ix_audit_log_occurred ON audit.audit_log (occurred_at);
