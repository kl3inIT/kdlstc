-- =========================================================================
-- Vertical slice 2 — TABMIS: budget execution submitted as Excel workbooks.
--
-- Everything structural about this source differs from QL Gia: a person
-- uploads a file instead of a service answering a request, there is no cursor,
-- and a period is REPLACED wholesale rather than merged row by row. The layers
-- below Bronze are deliberately the same shape, because that is the point.
-- =========================================================================

DROP TABLE IF EXISTS curated.fact_budget;
DROP TABLE IF EXISTS curated.dim_unit;
DROP TABLE IF EXISTS staging.stg_tabmis__budget_typed;
DROP TABLE IF EXISTS staging.stg_tabmis__budget;
DROP TABLE IF EXISTS ingestion.intake_files;
DROP TABLE IF EXISTS refdata.budget_line;
DROP TABLE IF EXISTS refdata.budget_chapter;
DROP TABLE IF EXISTS refdata.funding_source;
DROP TABLE IF EXISTS refdata.expense_sector;
DROP TABLE IF EXISTS refdata.tax_type;
DROP TABLE IF EXISTS refdata.revenue_source;
DROP TABLE IF EXISTS refdata.budget_unit;

-- ─────────────────────────────────────────────────────────────────────────
-- refdata.budget_unit — the most important dimension in the warehouse.
--
-- Units get merged, renamed and dissolved between fiscal years. superseded_by
-- points at whoever inherited the budget, so a report can be run two ways:
-- as the org chart stood at the time, or rolled up into today's org chart.
-- Losing that distinction is how last year's numbers stop reconciling.
-- ─────────────────────────────────────────────────────────────────────────
-- Versioned, not overwritten. The primary key carries valid_from because a
-- unit that is renamed, re-parented or reclassified must keep its old row —
-- overwriting it rewrites history, and every report for an earlier month
-- silently starts showing today's name.
CREATE TABLE refdata.budget_unit (
    unit_code       text NOT NULL,          -- ma DVQHNS, 7 digits — natural key
    valid_from      date NOT NULL DEFAULT '2020-01-01',
    valid_to        date NOT NULL DEFAULT '9999-12-31',
    unit_name       text NOT NULL,
    short_name      text,
    unit_level      text NOT NULL,          -- province | department | district | commune | public_service
    parent_code     text,
    locality_code   text REFERENCES refdata.locality(locality_code),
    org_type        text,                   -- state_admin | public_service | other
    superseded_by   text,                   -- who inherited this unit's budget
    change_reason   text,                   -- why this version exists
    version         int  NOT NULL DEFAULT 1,
    created_by      text NOT NULL DEFAULT 'seed',
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (unit_code, valid_from)
);
CREATE INDEX ix_budget_unit_parent  ON refdata.budget_unit (parent_code);
CREATE INDEX ix_budget_unit_current ON refdata.budget_unit (unit_code, valid_to);

-- ─────────────────────────────────────────────────────────────────────────
-- curated.dim_unit — the SCD-2 dimension the facts actually point at.
--
-- One row per VERSION of a unit, with a surrogate key. Facts carry that key,
-- so a figure booked in March stays attached to the March version of the unit
-- forever, whatever happens to the unit afterwards.
--
-- succeeded_by_key is what makes the second reading possible. A report can be
-- run two ways and both are correct answers to different questions:
--
--   as it stood   group by unit_key            — what the org chart was then;
--                                                this is what reconciles with
--                                                the treasury for that month
--   as it stands  follow succeeded_by_key to   — roll a dissolved unit into
--                 the current version            whoever inherited it, so this
--                                                year compares with next year
--
-- unit_key = -1 is the "unknown" member (B5.0): facts never carry a NULL key,
-- so a row whose unit could not be resolved still appears in every total under
-- an explicit bucket instead of vanishing from the report.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE curated.dim_unit (
    unit_key        bigint PRIMARY KEY,
    unit_code       text NOT NULL,
    unit_name       text NOT NULL,
    short_name      text,
    unit_level      text,
    parent_code     text,
    locality_code   text,
    org_type        text,
    effective_from  date NOT NULL,
    effective_to    date NOT NULL,
    is_current      boolean NOT NULL,
    change_reason   text,
    succeeded_by_key bigint,                -- current version that inherited it
    UNIQUE (unit_code, effective_from)
);
CREATE INDEX ix_dim_unit_code    ON curated.dim_unit (unit_code, effective_from, effective_to);
CREATE INDEX ix_dim_unit_current ON curated.dim_unit (is_current);

-- ─────────────────────────────────────────────────────────────────────────
-- refdata.budget_line — the state budget chart of accounts, FLAT.
--
-- Five levels (chuong / loai / khoan / muc / tieu muc) held in one table
-- rather than five joined ones: reports roll up by attribute, and a star
-- schema wants the hierarchy denormalised. Facts always key at tieu_muc, the
-- most detailed level.
--
-- Versioned by fiscal year because the chart of accounts is reissued: the
-- same tieu_muc code can mean different things in different years.
-- ─────────────────────────────────────────────────────────────────────────
-- Chapter (chuong) is deliberately NOT here. In the Vietnamese chart of
-- accounts chapter identifies the MANAGING BODY while loai/khoan/muc/tieu muc
-- classify the NATURE of the spending — two orthogonal axes. Folding chapter
-- into a table keyed by tieu muc would claim a tieu muc belongs to one
-- chapter, which is false: the same salary line appears under every chapter.
-- Chapter travels on the fact instead, as a degenerate dimension.
CREATE TABLE refdata.budget_line (
    line_code        text NOT NULL,         -- ma tieu muc — the natural key
    fiscal_year      int  NOT NULL,
    category_code    text,                  -- loai
    subcategory_code text,                  -- khoan
    category_name    text,
    item_code        text,                  -- muc
    item_name        text,
    line_name        text NOT NULL,         -- ten tieu muc
    flow_type        text NOT NULL,         -- revenue | expense
    PRIMARY KEY (line_code, fiscal_year)
);
CREATE INDEX ix_budget_line_flow ON refdata.budget_line (flow_type, fiscal_year);

CREATE TABLE refdata.budget_chapter (
    chapter_code    text PRIMARY KEY,       -- ma chuong
    chapter_name    text NOT NULL,
    admin_level     text                    -- central | province | district
);

CREATE TABLE refdata.funding_source (
    funding_code    text PRIMARY KEY,       -- ma nguon kinh phi
    funding_name    text NOT NULL,
    funding_group   text                    -- domestic | aid | bond | other
);

-- Spending sector, the axis most provincial reports break down by.
CREATE TABLE refdata.expense_sector (
    sector_code     text PRIMARY KEY,       -- ma linh vuc chi
    sector_name     text NOT NULL,
    sort_order      int
);

-- ── Revenue-only dimensions ──────────────────────────────────────────────
-- Revenue and expenditure share one fact, so each side carries dimensions the
-- other has no use for. Rather than allow NULLs on a foreign key, every
-- dimension gets a "not applicable" row and the unused slot points at it —
-- the rule B5.0 sets out, and the reason a report can GROUP BY any dimension
-- without silently dropping half the rows.
CREATE TABLE refdata.tax_type (
    tax_type_code   text PRIMARY KEY,       -- ma sac thue
    tax_type_name   text NOT NULL,
    sort_order      int
);

CREATE TABLE refdata.revenue_source (
    revenue_source_code text PRIMARY KEY,   -- ma nguon thu
    revenue_source_name text NOT NULL
);

-- ─────────────────────────────────────────────────────────────────────────
-- ingestion.intake_files — the human half of this source.
--
-- A REST source needs no such table: nobody is waiting to hear whether their
-- request was accepted. A submitted workbook does. This is what lets the
-- pipeline answer "did my file go through, and if not, which rows were wrong"
-- without anyone reading Airflow logs.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE ingestion.intake_files (
    file_id         text PRIMARY KEY,       -- f_<sha256 prefix>
    source_code     text NOT NULL REFERENCES ingestion.sources(source_code),
    object_key      text NOT NULL,          -- where it landed in the bucket
    original_name   text NOT NULL,
    flow_type       text,                   -- revenue | expense, from the filename
    period          text,                   -- declared inside the workbook
    checksum_sha256 text NOT NULL,
    size_bytes      bigint,
    submitted_by    text,
    submitted_at    timestamptz NOT NULL DEFAULT now(),
    run_id          text,
    status          text NOT NULL DEFAULT 'received',
                    -- received | rejected | accepted | superseded
    row_count       bigint,
    error_count     int NOT NULL DEFAULT 0,
    report_key      text,                   -- the .ketqua.txt written back
    processed_at    timestamptz
);
CREATE INDEX ix_intake_files_period
    ON ingestion.intake_files (source_code, period, flow_type);
CREATE UNIQUE INDEX ux_intake_files_checksum ON ingestion.intake_files (checksum_sha256);

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-1 — one row per worksheet row, verbatim, all text.
--
-- Excel hands over strings that only look like data: "1.234.567" for an
-- amount, 46242 for a date. Casting here would destroy the evidence needed to
-- tell a submitter which cell to fix, so nothing is cast until Silver-2.
--
-- row_number is kept because the whole point of this source is being able to
-- say "row 47" back to a human.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE staging.stg_tabmis__budget (
    run_id            text NOT NULL,
    file_id           text NOT NULL,
    row_number        int  NOT NULL,        -- 1-based row in the worksheet
    period            text,
    unit_code         text,
    unit_name         text,
    locality_code     text,
    chapter_code      text,
    category_code     text,
    subcategory_code  text,
    item_code         text,
    line_code         text,
    funding_code      text,
    sector_code       text,
    -- revenue workbooks carry these two, expenditure workbooks leave them blank
    tax_type_code     text,
    revenue_source_code text,
    allocated_amount  text,
    adjusted_amount   text,
    executed_amount   text,
    advance_amount    text,
    executed_ytd      text,
    raw_path          text NOT NULL,
    loaded_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stg_tabmis_run ON staging.stg_tabmis__budget (run_id);

-- ─────────────────────────────────────────────────────────────────────────
-- Silver-2 — typed, mapped, judged. Still one row per worksheet row.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE staging.stg_tabmis__budget_typed (
    run_id            text NOT NULL,
    file_id           text NOT NULL,
    row_number        int  NOT NULL,
    flow_type         text,
    period            text,
    fiscal_year       int,
    unit_code         text,
    locality_code     text,
    line_code         text,
    funding_code      text,
    sector_code       text,
    tax_type_code     text,
    revenue_source_code text,
    allocated_amount  numeric(20,0),
    adjusted_amount   numeric(20,0),
    executed_amount   numeric(20,0),
    advance_amount    numeric(20,0),
    executed_ytd      numeric(20,0),
    raw_path          text NOT NULL,
    is_valid          boolean NOT NULL,
    reject_reason     text,
    reject_detail     text,                 -- human-readable, quoted back to the submitter
    created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_stg_tabmis_typed_run   ON staging.stg_tabmis__budget_typed (run_id);
CREATE INDEX ix_stg_tabmis_typed_valid ON staging.stg_tabmis__budget_typed (run_id, is_valid);

-- ─────────────────────────────────────────────────────────────────────────
-- Gold — fact_budget, periodic snapshot. REVENUE AND EXPENDITURE TOGETHER.
--
-- Grain: one flow x one unit x one budget line x one funding source x one tax
--        type x one revenue source x one locality x one month.
--
-- One fact rather than two, because the reports that matter join across the
-- two flows. DHTC_CHI_04 carries `can_doi = thu_dia_ban - luy_ke_chi`; with
-- separate tables that is a join across two grains done by hand in every
-- report, and it is where the numbers start to disagree.
--
-- The measure names are flow-neutral on purpose — B5.3 chose them that way:
--   allocated/adjusted  budget assigned      (expense: du toan chi,  revenue: du toan thu)
--   executed            realised in month    (expense: thuc chi,    revenue: thuc thu)
--   executed_ytd        cumulative in year
--   advance_amount      expenditure only; revenue rows carry 0
--
-- Each side uses dimensions the other does not. Instead of nullable foreign
-- keys, the unused slot points at a "not applicable" row (B5.0). That keeps
-- every GROUP BY total-preserving: a report grouped by tax type still shows
-- expenditure, under an explicit "not applicable" bucket, rather than dropping
-- it silently.
--
-- Loaded by REPLACE-BY-PERIOD-AND-FLOW. Scoping the delete to the flow matters:
-- a period has two files, and a delete scoped only to the period would make
-- the expenditure submission erase the revenue already loaded for that month.
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE curated.fact_budget (
    flow_type           text NOT NULL,      -- revenue | expense
    -- The surrogate key of the unit version in effect when the money moved.
    -- unit_code stays alongside as a degenerate attribute: it is what a person
    -- types into a search box, while unit_key is what the join uses.
    unit_key            bigint NOT NULL REFERENCES curated.dim_unit(unit_key),
    unit_code           text NOT NULL,
    line_code           text NOT NULL,
    fiscal_year         int  NOT NULL,
    funding_code        text NOT NULL REFERENCES refdata.funding_source(funding_code),
    tax_type_code       text NOT NULL REFERENCES refdata.tax_type(tax_type_code),
    revenue_source_code text NOT NULL REFERENCES refdata.revenue_source(revenue_source_code),
    locality_code       text NOT NULL REFERENCES refdata.locality(locality_code),
    period              text NOT NULL,      -- YYYY-MM
    chapter_code        text,               -- degenerate: managing body, per row
    sector_code         text,

    allocated_amount    numeric(20,0) NOT NULL DEFAULT 0,
    adjusted_amount     numeric(20,0) NOT NULL DEFAULT 0,
    executed_amount     numeric(20,0) NOT NULL DEFAULT 0,
    advance_amount      numeric(20,0) NOT NULL DEFAULT 0,
    executed_ytd        numeric(20,0) NOT NULL DEFAULT 0,

    run_id              text NOT NULL,
    batch_id            text NOT NULL,
    file_id             text NOT NULL,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (period, flow_type, unit_code, line_code,
                 funding_code, tax_type_code, revenue_source_code, locality_code),
    FOREIGN KEY (line_code, fiscal_year)
        REFERENCES refdata.budget_line(line_code, fiscal_year)
);
CREATE INDEX ix_fact_budget_period   ON curated.fact_budget (period, flow_type);
CREATE INDEX ix_fact_budget_unit     ON curated.fact_budget (unit_code, period);
CREATE INDEX ix_fact_budget_unitkey  ON curated.fact_budget (unit_key);
CREATE INDEX ix_fact_budget_locality ON curated.fact_budget (locality_code, period, flow_type);

COMMENT ON COLUMN curated.fact_budget.executed_ytd IS
  'Cumulative within the fiscal year. Additive across units and lines, NEVER across periods.';
COMMENT ON COLUMN curated.fact_budget.flow_type IS
  'revenue | expense. Never aggregate across flows without splitting by this.';
COMMENT ON TABLE curated.fact_budget IS
  'Replace-by-period-AND-flow: a resubmission rewrites one month of one flow.';

-- ─────────────────────────────────────────────────────────────────────────
-- Register the source
-- ─────────────────────────────────────────────────────────────────────────
INSERT INTO ingestion.sources
    (source_code, description, ingest_method, load_mode,
     business_key, cursor_column, data_owner)
VALUES
  ('tabmis',
   'Budget execution, Excel workbooks submitted per month',
   'file',
   'full_period',
   'unit x budget_line x funding_source x period',
   NULL,
   'Phong QLNS')
ON CONFLICT (source_code) DO UPDATE
   SET description   = EXCLUDED.description,
       ingest_method = EXCLUDED.ingest_method,
       load_mode     = EXCLUDED.load_mode;
