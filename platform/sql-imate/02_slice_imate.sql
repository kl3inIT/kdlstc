-- =========================================================================
-- iMate document slice — schema for the 7-step pipeline
--
-- Source: iMate REST API, tenant doit.phuyen (So Cong Thuong tinh Phu Yen).
-- Grain of the published fact is ONE DOCUMENT, because the report the users
-- asked for counts documents per day. Routings ride alongside at their own
-- grain (one routing) — they are ~16x more numerous and must never be joined
-- into the document count, or every document with 16 routings counts 16 times.
--
-- Additive and idempotent: this file never drops a schema. Run it as often
-- as you like; run 01_init.sql only when you mean to rebuild everything.
-- =========================================================================

INSERT INTO ingestion.sources
  (source_code, description, ingest_method, load_mode, business_key,
   cursor_column, data_owner, sensitivity)
VALUES
  ('imate', 'iMate — van ban den/di theo tenant', 'rest_api', 'incremental',
   'globalId', 'updatedAt', 'So Cong Thuong Phu Yen', 'internal')
ON CONFLICT (source_code) DO UPDATE
  SET description   = EXCLUDED.description,
      cursor_column = EXCLUDED.cursor_column;

-- =========================================================================
-- refdata — document kinds and issuing bodies
-- =========================================================================

-- Vietnamese document numbers encode the KIND, not just a serial. The
-- abbreviations are fixed by Nghi dinh 30/2020/ND-CP, Phu luc I. Holding them
-- as reference data rather than a regex alternation means an unrecognised
-- abbreviation becomes a row someone owns, not a silent miscount.
CREATE TABLE IF NOT EXISTS refdata.document_kind (
    kind_code    text PRIMARY KEY,
    kind_name    text NOT NULL,
    kind_group   text,                    -- quy_pham | hanh_chinh | ca_biet
    sort_order   int  NOT NULL DEFAULT 99,
    valid_from   date NOT NULL DEFAULT '2020-01-01',
    valid_to     date NOT NULL DEFAULT '9999-12-31',
    created_by   text NOT NULL DEFAULT 'seed',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- The body that issued the document. For an incoming document this is the
-- "don vi gui" the report filters on. Discovered from documentNo rather than
-- supplied by the API — /api/publishers needs a JWT we do not have.
CREATE TABLE IF NOT EXISTS refdata.issuing_body (
    body_code    text PRIMARY KEY,
    body_name    text NOT NULL,
    body_level   text,                    -- trung_uong | tinh | so_nganh
                                          -- | huyen_xa | dang_doan_the | khac
    is_confirmed boolean NOT NULL DEFAULT false,  -- false = name inferred, needs review
    valid_from   date NOT NULL DEFAULT '2020-01-01',
    valid_to     date NOT NULL DEFAULT '9999-12-31',
    created_by   text NOT NULL DEFAULT 'seed',
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Units and contacts as the API hands them over. contact_id is the join key
-- routings carry (with a leading '#'), so it is the natural key here.
CREATE TABLE IF NOT EXISTS refdata.imate_unit (
    unit_global_id text PRIMARY KEY,
    unit_id        text NOT NULL,
    unit_name      text NOT NULL,
    tenant_code    text NOT NULL,
    loaded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS refdata.imate_contact (
    contact_global_id text PRIMARY KEY,
    contact_id        text NOT NULL,
    display_name      text NOT NULL,
    unit_name         text,
    tenant_code       text NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_imate_contact_cid
    ON refdata.imate_contact (tenant_code, contact_id);

-- =========================================================================
-- ingestion — the work list is the hand-off between DAG 01 and DAG 02
--
-- With 6,141 documents a run is not one file, so the batch cannot live in
-- XCom. It has to be a table regardless of how the DAGs are split — which is
-- exactly why splitting them costs nothing here.
-- =========================================================================
CREATE TABLE IF NOT EXISTS ingestion.doc_worklist (
    global_id      text PRIMARY KEY,
    tenant_id      text NOT NULL,
    document_id    text,
    document_no    text,
    subject        text,
    uploaded_at    timestamptz,
    source_updated_at timestamptz NOT NULL,   -- the API's updatedAt: the cursor
    process_status text,

    -- state machine, mirrors ingestion.runs
    status         text NOT NULL DEFAULT 'discovered',
                   -- discovered | landed | staged | mapped | published | failed
    bronze_key     text,
    content_hash   text,                      -- sha256 of the detail payload
    discover_run   text,
    land_run       text,
    attempts       int  NOT NULL DEFAULT 0,
    last_error     text,
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_worklist_status  ON ingestion.doc_worklist (status);
CREATE INDEX IF NOT EXISTS ix_worklist_updated ON ingestion.doc_worklist (source_updated_at DESC);

-- =========================================================================
-- staging Silver-1 — verbatim, every column text
--
-- Nothing is cast, trimmed or mapped here. The point of this layer is that a
-- value which fails later can be quoted back to the source exactly as it
-- arrived, with the row it came from.
-- =========================================================================
CREATE TABLE IF NOT EXISTS staging.stg_imate__document (
    run_id        text NOT NULL,
    global_id     text NOT NULL,
    tenant_id     text,
    document_id   text,
    version       text,
    document_no   text,
    subject       text,
    uploaded_at   text,
    created_at    text,
    updated_at    text,
    process_status text,
    bronze_key    text,
    loaded_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, global_id)
);

CREATE TABLE IF NOT EXISTS staging.stg_imate__routing (
    run_id      text NOT NULL,
    global_id   text NOT NULL,
    seq         int  NOT NULL,          -- position within the document's array
    sender      text,
    receiver    text,
    action      text,
    role        text,
    received_at text,
    seen_at     text,
    acted_at    text,
    loaded_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, global_id, seq)
);

CREATE TABLE IF NOT EXISTS staging.stg_imate__receipt (
    run_id          text NOT NULL,
    global_id       text NOT NULL,
    seq             int  NOT NULL,
    role            text,
    document_type   text,
    document_status text,
    reached_at      text,
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, global_id, seq)
);

CREATE TABLE IF NOT EXISTS staging.stg_imate__attachment (
    run_id       text NOT NULL,
    global_id    text NOT NULL,
    seq          int  NOT NULL,
    name         text,
    raw_url      text,
    content_hash text,
    is_main      text,
    is_user_generated text,
    render_count text,
    has_scanned  text,
    loaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, global_id, seq)
);

-- =========================================================================
-- staging Silver-2 — typed, mapped and judged
--
-- Three separate verdicts, deliberately not collapsed into one "is_valid":
--   kind_code / body_code   what the mapping resolved to
--   parse_pattern           which rule fired, or none
--   defect_reason           why a row is unusable even though it parsed
-- A document nobody wrote a rule for is a mapping gap; a document with no
-- date is a defect. Averaging them into one number hides both.
-- =========================================================================
CREATE TABLE IF NOT EXISTS staging.stg_imate__document_typed (
    run_id          text NOT NULL,
    global_id       text NOT NULL,
    tenant_id       text,
    document_id     text,
    document_no     text,
    subject         text,
    uploaded_at     timestamptz,
    uploaded_date   date,
    source_updated_at timestamptz,
    process_status  text,

    serial_no       text,        -- the number part of documentNo
    issued_year     int,
    kind_code       text,        -- resolved against refdata.document_kind
    body_code       text,        -- resolved against refdata.issuing_body
    parse_pattern   text,        -- A | B | cong_van | none
    kind_mapped     boolean NOT NULL DEFAULT false,
    body_mapped     boolean NOT NULL DEFAULT false,

    routing_count   int NOT NULL DEFAULT 0,
    attachment_count int NOT NULL DEFAULT 0,
    receipt_type    text,        -- INCOMING | OUTGOING
    defect_reason   text,        -- NULL = usable
    loaded_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, global_id)
);
CREATE INDEX IF NOT EXISTS ix_imate_typed_defect
    ON staging.stg_imate__document_typed (run_id, defect_reason);

-- =========================================================================
-- curated — Gold
-- =========================================================================

-- A real date dimension, so "theo thang" is a column and not a substring of a
-- timestamp. Days with no documents still exist here, which is what makes a
-- gap in the report visible instead of simply absent.
CREATE TABLE IF NOT EXISTS curated.dim_date (
    date_key     int  PRIMARY KEY,        -- yyyymmdd
    full_date    date NOT NULL UNIQUE,
    year         int  NOT NULL,
    quarter      int  NOT NULL,
    month        int  NOT NULL,
    month_key    int  NOT NULL,           -- yyyymm
    month_label  text NOT NULL,           -- 'Thang 01/2025'
    day_of_month int  NOT NULL,
    day_of_week  int  NOT NULL,           -- 1 = Monday
    day_label    text NOT NULL,
    is_weekend   boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS curated.dim_document_kind (
    kind_key    bigint PRIMARY KEY,
    kind_code   text NOT NULL UNIQUE,
    kind_name   text NOT NULL,
    kind_group  text,
    sort_order  int NOT NULL DEFAULT 99
);

CREATE TABLE IF NOT EXISTS curated.dim_issuing_body (
    body_key     bigint PRIMARY KEY,
    body_code    text NOT NULL UNIQUE,
    body_name    text NOT NULL,
    body_level   text,
    is_confirmed boolean NOT NULL DEFAULT false
);

-- Grain: one document. The count the report shows is COUNT(*) over this
-- table, so anything that would double a row here doubles the headline.
CREATE TABLE IF NOT EXISTS curated.fact_document (
    global_id        text PRIMARY KEY,
    document_id      text,
    document_no      text,
    subject          text,

    date_key         int    NOT NULL REFERENCES curated.dim_date(date_key),
    kind_key         bigint NOT NULL REFERENCES curated.dim_document_kind(kind_key),
    body_key         bigint NOT NULL REFERENCES curated.dim_issuing_body(body_key),

    uploaded_at      timestamptz,
    process_status   text,
    receipt_type     text,
    routing_count    int NOT NULL DEFAULT 0,
    attachment_count int NOT NULL DEFAULT 0,

    run_id           text NOT NULL,
    batch_id         text NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_fact_document_date ON curated.fact_document (date_key);
CREATE INDEX IF NOT EXISTS ix_fact_document_kind ON curated.fact_document (kind_key);
CREATE INDEX IF NOT EXISTS ix_fact_document_body ON curated.fact_document (body_key);

-- Grain: one routing step. Kept separate from fact_document on purpose —
-- ~16 rows per document, and joining the two without an aggregate first is
-- the single easiest way to report a document count that is 16x too high.
CREATE TABLE IF NOT EXISTS curated.fact_routing (
    global_id     text NOT NULL,
    seq           int  NOT NULL,
    date_key      int  NOT NULL REFERENCES curated.dim_date(date_key),
    sender_handle text,
    sender_contact_id text,
    receiver_handle   text,
    receiver_kind text,                  -- contact | unit | unknown
    receiver_contact_id text,
    receiver_unit_name  text,
    action        text,
    role          text,
    seen_at       timestamptz,
    acted_at      timestamptz,
    run_id        text NOT NULL,
    PRIMARY KEY (global_id, seq)
);
CREATE INDEX IF NOT EXISTS ix_fact_routing_date ON curated.fact_routing (date_key);

COMMENT ON TABLE curated.fact_document IS
  'Grain: one document. COUNT(*) is the document count — never join fact_routing without aggregating first.';
COMMENT ON COLUMN curated.fact_document.body_key IS
  'Issuing body = "don vi gui" for an incoming document, parsed from documentNo.';
