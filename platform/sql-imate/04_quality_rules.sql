-- Quality rules live in the database, not in Python constants.
--
-- A threshold is a business decision: somebody owns it, somebody approved it,
-- and it changes when the business changes — not when an engineer redeploys.
-- Keeping it in code meant every adjustment was a code review, and nobody
-- outside the team could see what the pipeline was actually enforcing.
--
-- Each row is one expectation. great_expectations evaluates it; the blocker vs
-- scoring layer on top stays ours, because GX has no notion of "this rule stops
-- the batch and that one only costs points".

CREATE TABLE IF NOT EXISTS metadata.quality_rules (
    rule_name      text PRIMARY KEY,
    dataset_code   text NOT NULL,
    table_name     text NOT NULL,

    -- The GX expectation class, and the arguments it needs. Kept as data so a
    -- new rule is an INSERT, not a deploy.
    expectation    text NOT NULL,
    expression     jsonb NOT NULL,

    -- blocker       — batch stops here, nothing reaches the report
    -- scoring       — counts toward the score, low means somebody owes work
    -- informational — measured and recorded, never blocks. For facts worth
    --                 tracking that nobody can act on yet, such as how many
    --                 organisation names an authority has confirmed while the
    --                 directory API is still behind a token we do not hold.
    level          text NOT NULL
                   CHECK (level IN ('blocker', 'scoring', 'informational')),

    threshold      numeric(6,4) NOT NULL CHECK (threshold BETWEEN 0 AND 1),
    weight         numeric(6,4) NOT NULL DEFAULT 1.0,

    -- Who picks it up and how fast. A rule nobody owns is a rule nobody fixes.
    on_fail_action text,
    owner          text,
    due_hours      integer,

    is_active      boolean NOT NULL DEFAULT true,
    version        integer NOT NULL DEFAULT 1,
    approved_by    text,
    approved_at    timestamptz,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

COMMENT ON COLUMN metadata.quality_rules.expectation IS
  'Ten lop expectation cua great_expectations, vi du ExpectColumnValuesToNotBeNull';
COMMENT ON COLUMN metadata.quality_rules.expression IS
  'Tham so cho expectation, vi du {"column": "uploaded_date"}';
COMMENT ON COLUMN metadata.quality_rules.level IS
  'blocker chan lo; scoring gop diem; informational chi do va ghi lai';

CREATE INDEX IF NOT EXISTS ix_quality_rules_dataset
    ON metadata.quality_rules (dataset_code) WHERE is_active;


-- ── Rules for the iMate slice ────────────────────────────────────────────
--
-- Three separate facts, deliberately not folded into one number. The old code
-- claimed a single "mapping_coverage" measured whether kind AND body resolved;
-- measured on real data both conditions selected the same 5.863 rows, because
-- every numbering pattern that yields a kind also yields a body. One fact wearing
-- two names told nobody anything. These three genuinely differ.

INSERT INTO metadata.quality_rules
    (rule_name, dataset_code, table_name, expectation, expression,
     level, threshold, weight, on_fail_action, owner, due_hours,
     approved_by, approved_at)
VALUES

-- A document with no usable date cannot be counted on any daily report. This is
-- broken data, not missing homework: the batch stops.
('uploaded_date_present', 'imate', 'staging.stg_imate__document_typed',
 'ExpectColumnValuesToNotBeNull', '{"column": "uploaded_date"}'::jsonb,
 'blocker', 0.9500, 1.0,
 'Dung lo, doi chieu voi doi nguon iOffice ve cac ban ghi thieu ngay dang',
 'to-du-lieu', 24, 'nhom-ky-thuat', now()),

-- Kind resolves against the CLOSED statutory list of Nghi dinh 30/2020. Failing
-- means a numbering convention nobody has written a rule for yet — homework,
-- not an incident.
('kind_in_statutory_list', 'imate', 'staging.stg_imate__document_typed',
 'ExpectColumnValuesToBeInSet', '{"column": "kind_mapped", "value_set": [true]}'::jsonb,
 'scoring', 0.5000, 1.0,
 'Bo sung quy tac boc tach cho quy uoc danh so moi',
 'to-du-lieu', 168, 'nhom-ky-thuat', now()),

-- Body resolves against the OPEN directory: the code was extracted AND has been
-- registered. Separate from kind because the two lists behave differently — one
-- is fixed by decree, the other grows every time a sender reorganises.
('body_resolved', 'imate', 'staging.stg_imate__document_typed',
 'ExpectColumnValuesToBeInSet', '{"column": "body_mapped", "value_set": [true]}'::jsonb,
 'scoring', 0.5000, 1.0,
 'Bo sung quy tac boc tach co quan ban hanh',
 'to-du-lieu', 168, 'nhom-ky-thuat', now()),

-- How many of those organisation names an authority has actually confirmed.
-- Currently zero, and no engineering change can raise it: confirmation needs the
-- directory API, which sits behind a token the project has not been granted.
-- Recorded rather than enforced, so the gap stays visible instead of being
-- rounded away inside a coverage number.
('body_confirmed', 'imate', 'staging.stg_imate__document_typed',
 'ExpectColumnValuesToBeInSet', '{"column": "body_confirmed", "value_set": [true]}'::jsonb,
 'informational', 0.0000, 0.0,
 'Xin JWT cho /api/publishers de xac nhan ten co quan',
 'lanh-dao-so', NULL, 'nhom-ky-thuat', now())

ON CONFLICT (rule_name) DO UPDATE SET
    expectation    = EXCLUDED.expectation,
    expression     = EXCLUDED.expression,
    level          = EXCLUDED.level,
    threshold      = EXCLUDED.threshold,
    weight         = EXCLUDED.weight,
    on_fail_action = EXCLUDED.on_fail_action,
    owner          = EXCLUDED.owner,
    due_hours      = EXCLUDED.due_hours,
    version        = metadata.quality_rules.version + 1,
    updated_at     = now();
