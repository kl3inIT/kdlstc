-- =========================================================================
-- Version events for budget units, and the SCD-2 dimension built from them.
--
-- A dimension only earns Type 2 if something actually changes. Three kinds of
-- change are planted here because each one breaks a different naive query:
--
--   rename        overwrite it and every earlier report retroactively shows
--                 the new name — the reader assumes they misremembered
--   re-parent     roll up by managing body and last year's totals move
--                 between departments without anyone touching the numbers
--   merger        a unit stops existing; its spending must still resolve for
--                 the months it existed, and still roll into its successor
--                 when comparing years
-- =========================================================================

-- Every event below is written to survive being run twice. The predicate
-- `valid_from < <new version's start>` is what does it: without that clause a
-- second run matches the version this script itself just opened and closes it,
-- producing a row whose validity ends before it begins. Re-running a migration
-- is normal — after a partial failure it is unavoidable — so an event that
-- only works once is a defect, not a caveat.

-- ── Rename: a department takes on an extra remit from 2026-05-01 ─────────
UPDATE refdata.budget_unit
   SET valid_to      = DATE '2026-04-30',
       change_reason = 'doi ten tu 01/05/2026'
 WHERE unit_code = '1054012'
   AND valid_from  < DATE '2026-05-01'
   AND valid_to    = DATE '9999-12-31';

INSERT INTO refdata.budget_unit
  (unit_code, valid_from, valid_to, unit_name, short_name, unit_level,
   parent_code, locality_code, org_type, change_reason, version)
VALUES
  ('1054012', DATE '2026-05-01', DATE '9999-12-31',
   'So Van hoa, The thao, Du lich va Truyen thong', 'So VHTTDLTT',
   'department', '1054001', 'LOC00', 'state_admin',
   'tiep nhan them chuc nang truyen thong', 2)
ON CONFLICT (unit_code, valid_from) DO NOTHING;

-- ── Re-parent: a school moves from the education department to its district
UPDATE refdata.budget_unit
   SET valid_to      = DATE '2026-03-31',
       change_reason = 'chuyen co quan chu quan tu 01/04/2026'
 WHERE unit_code = '1054240'
   AND valid_from  < DATE '2026-04-01'
   AND valid_to    = DATE '9999-12-31';

INSERT INTO refdata.budget_unit
  (unit_code, valid_from, valid_to, unit_name, short_name, unit_level,
   parent_code, locality_code, org_type, change_reason, version)
SELECT
  u.unit_code, DATE '2026-04-01', DATE '9999-12-31',
  u.unit_name, u.short_name, u.unit_level,
  -- now managed by the district finance office rather than the department
  (SELECT d.unit_code FROM refdata.budget_unit d
    WHERE d.unit_level = 'district' AND d.locality_code = u.locality_code
    ORDER BY d.unit_code LIMIT 1),
  u.locality_code, u.org_type,
  'phan cap ve huyen quan ly', 2
FROM refdata.budget_unit u
WHERE u.unit_code = '1054240' AND u.valid_to = DATE '2026-03-31'
ON CONFLICT (unit_code, valid_from) DO NOTHING;

-- ── Build the dimension ──────────────────────────────────────────────────
-- DELETE rather than TRUNCATE, and the difference is not cosmetic: TRUNCATE is
-- refused outright while a fact table references this one, and CASCADE would
-- take the facts with it. DELETE fails loudly if any fact still points here,
-- which is the correct outcome — a dimension facts depend on cannot simply be
-- rebuilt from scratch.
--
-- This full rebuild only works because it runs as part of a clean schema
-- rebuild. Once real history accumulates, the dimension has to be maintained
-- incrementally: close the version that changed, open a new one, and leave
-- every existing surrogate key untouched — a re-numbered key silently
-- re-points every fact that carries it.
DELETE FROM curated.dim_unit;

-- The unknown member. Facts point here when a unit cannot be resolved, so a
-- total never silently loses rows (B5.0).
INSERT INTO curated.dim_unit
  (unit_key, unit_code, unit_name, unit_level, effective_from, effective_to,
   is_current, change_reason, succeeded_by_key)
VALUES (-1, '(khong xac dinh)', 'Khong xac dinh', NULL,
        DATE '1900-01-01', DATE '9999-12-31', TRUE, NULL, -1);

INSERT INTO curated.dim_unit
  (unit_key, unit_code, unit_name, short_name, unit_level, parent_code,
   locality_code, org_type, effective_from, effective_to, is_current,
   change_reason)
SELECT
  row_number() OVER (ORDER BY u.unit_code, u.valid_from) AS unit_key,
  u.unit_code, u.unit_name, u.short_name, u.unit_level, u.parent_code,
  u.locality_code, u.org_type,
  u.valid_from, u.valid_to,
  u.valid_to = DATE '9999-12-31',
  u.change_reason
FROM refdata.budget_unit u;

-- ── succeeded_by_key ─────────────────────────────────────────────────────
-- Where the budget ended up, expressed as a key rather than a code so an
-- as-is roll-up is one join instead of a recursive lookup.
--
-- Two cases collapse into one rule:
--   this unit was absorbed  -> the current version of the absorbing unit
--   this unit still exists  -> the current version of ITSELF, which for a
--                              superseded version means its own newer row
-- A row that is already current points at itself, so the join is total.
UPDATE curated.dim_unit d
   SET succeeded_by_key = COALESCE(absorbed.unit_key, own.unit_key, -1)
  FROM refdata.budget_unit u
  LEFT JOIN LATERAL (
        SELECT c.unit_key FROM curated.dim_unit c
         WHERE c.unit_code = u.superseded_by AND c.is_current
         LIMIT 1
      ) absorbed ON TRUE
  LEFT JOIN LATERAL (
        SELECT c.unit_key FROM curated.dim_unit c
         WHERE c.unit_code = u.unit_code AND c.is_current
         LIMIT 1
      ) own ON TRUE
 WHERE d.unit_code = u.unit_code
   AND d.effective_from = u.valid_from;

-- ── Guard ────────────────────────────────────────────────────────────────
-- A version whose validity ends before it begins is silent poison: it matches
-- no period, so the unit simply stops appearing in reports for those months
-- and nothing anywhere reports an error. Fail the migration instead.
DO $$
DECLARE bad int;
BEGIN
    SELECT count(*) INTO bad FROM curated.dim_unit WHERE effective_to < effective_from;
    IF bad > 0 THEN
        RAISE EXCEPTION 'dim_unit: % dong co khoang hieu luc rong (effective_to < effective_from)', bad;
    END IF;

    -- At most one current version, not exactly one: a dissolved unit correctly
    -- has none. Two current versions would mean overlapping validity, and a
    -- join by date would then double every figure for that unit.
    SELECT count(*) INTO bad
      FROM (SELECT unit_code FROM curated.dim_unit
             WHERE unit_code <> '(khong xac dinh)'
             GROUP BY unit_code HAVING count(*) FILTER (WHERE is_current) > 1) x;
    IF bad > 0 THEN
        RAISE EXCEPTION 'dim_unit: % don vi co nhieu hon mot phien ban hien hanh', bad;
    END IF;
END $$;

COMMENT ON COLUMN curated.dim_unit.succeeded_by_key IS
  'Current version that inherited this one; points at itself when still current. Join on this for an as-it-stands roll-up, on unit_key for as-it-stood.';
