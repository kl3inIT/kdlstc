-- =========================================================================
-- iMate warehouse — bootstrap for its OWN database (stc_imate).
--
-- This slice runs on a separate PostgreSQL instance (the in-namespace
-- stc-airflow-postgresql) in its own database, owned by its own role.
-- Nothing here is shared with qlgia or tabmis, which live in stc_dwh on
-- jmix-ha — that is the point: the POC can be built, broken and wiped
-- without a thought for the other flows.
--
-- Same layer skeleton as the main warehouse, trimmed to what this slice
-- uses. DESTRUCTIVE: like the main 01_init, run it only to rebuild;
-- 02/03 are additive and safe to re-run any time.
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

-- ── ingestion: the run ledger ────────────────────────────────────────────
CREATE TABLE ingestion.sources (
    source_code     text PRIMARY KEY,
    description     text,
    ingest_method   text NOT NULL,
    load_mode       text NOT NULL,
    business_key    text,
    cursor_column   text,
    data_owner      text,
    sensitivity     text NOT NULL DEFAULT 'internal',
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ingestion.runs (
    run_id          text PRIMARY KEY,
    source_code     text NOT NULL REFERENCES ingestion.sources(source_code),
    period          text NOT NULL,
    status          text NOT NULL,
    row_count       bigint,
    checksum_sha256 text,
    raw_path        text,
    quality_score   numeric(5,4),
    message         text,
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz
);
CREATE INDEX ix_runs_source_period ON ingestion.runs (source_code, period);
CREATE INDEX ix_runs_status        ON ingestion.runs (status);

CREATE TABLE ingestion.cursors (
    source_code   text PRIMARY KEY REFERENCES ingestion.sources(source_code),
    cursor_value  text NOT NULL,
    run_id        text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- ── metadata: mapping gaps and quality evidence ──────────────────────────
CREATE TABLE metadata.mapping_rejections (
    id            bigserial PRIMARY KEY,
    run_id        text NOT NULL,
    source_code   text NOT NULL,
    code_type     text NOT NULL,
    source_value  text NOT NULL,
    row_count     int  NOT NULL DEFAULT 1,
    reason        text NOT NULL,
    status        text NOT NULL DEFAULT 'pending',
    assignee      text,
    due_date      date,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_mapping_rejections_status ON metadata.mapping_rejections (status);

CREATE TABLE metadata.quality_exceptions (
    id           bigserial PRIMARY KEY,
    run_id       text NOT NULL,
    table_name   text NOT NULL,
    rule_name    text NOT NULL,
    severity     text NOT NULL,
    failed_rows  bigint,
    pass_rate    numeric(6,4),
    details      jsonb,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_quality_exceptions_run ON metadata.quality_exceptions (run_id);

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

-- ── curated: the publication ledger ──────────────────────────────────────
CREATE TABLE curated.batch_summary (
    batch_id        text PRIMARY KEY,
    run_id          text NOT NULL,
    source_code     text NOT NULL,
    period          text NOT NULL,
    target_table    text NOT NULL,
    row_count       bigint,
    quality_score   numeric(5,4),
    publish_status  text NOT NULL DEFAULT 'pending',
    data_freshness  timestamptz,
    published_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_batch_summary_run ON curated.batch_summary (run_id);

-- ── audit: append-only ───────────────────────────────────────────────────
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
