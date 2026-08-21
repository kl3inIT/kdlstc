"""
The quality gate's engine: great_expectations for the assertions, our own layer
for what to do about them.

Why the split. GX is very good at one thing — stating an expectation about data
and producing evidence for whether it held. It has no opinion about consequence:
nothing in GX says "this failure stops the batch and that one only costs points".
That distinction is the whole point of a gate, and it is a business rule, so it
lives in metadata.quality_rules where somebody outside the team can read and
change it without a deploy.

Three levels, and the third is the one worth explaining:

    blocker         the batch stops. Broken data, an incident.
    scoring         counts toward the score. Missing homework.
    informational   measured, recorded, never blocks. For facts nobody on this
                    team can act on — such as how many organisation names an
                    authority has confirmed while the directory API is still
                    behind a token we have not been granted. Without this level
                    such a fact has nowhere to live: enforcing it would block
                    the pipeline forever, and dropping it would round a known
                    gap away to nothing.
"""

import json

import pandas as pd

from imate_common import TENANT_ID, imate_cursor


# One query feeds every rule. The joins that decide "is this body confirmed?"
# belong in SQL, not in an expectation — GX asserts about a column, it does not
# know what a reference table is.
SNAPSHOT_SQL = """
SELECT t.global_id,
       t.uploaded_date,
       t.kind_mapped,
       t.body_mapped,
       coalesce(b.is_confirmed, false) AS body_confirmed
  FROM staging.stg_imate__document_typed t
  LEFT JOIN refdata.issuing_body b ON b.body_code = t.body_code
 WHERE t.tenant_id = %s
"""


def load_rules(dataset_code):
    """Read the active rules. Order is stable so evidence is comparable."""
    with imate_cursor() as cur:
        cur.execute("""
            SELECT rule_name, expectation, expression, level,
                   threshold, weight, on_fail_action, owner, due_hours, version
              FROM metadata.quality_rules
             WHERE dataset_code = %s AND is_active
             ORDER BY level, rule_name
        """, (dataset_code,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def load_snapshot():
    """
    The WHOLE typed snapshot, not just the batch that just arrived.

    The report is drawn from the whole table, so the gate has to judge what the
    reader will actually see. A clean batch landing on top of a broken table is
    not a passing grade.
    """
    with imate_cursor() as cur:
        cur.execute(SNAPSHOT_SQL, (TENANT_ID,))
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def _batch(frame):
    """An ephemeral GX context — no project directory, nothing on disk."""
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas("imate")
    asset = source.add_dataframe_asset(name="typed_snapshot")
    definition = asset.add_batch_definition_whole_dataframe("whole")
    return definition.get_batch(batch_parameters={"dataframe": frame})


def _expectation(rule):
    import great_expectations.expectations as gxe

    cls = getattr(gxe, rule["expectation"], None)
    if cls is None:
        raise ValueError(
            f"luat {rule['rule_name']}: khong co expectation "
            f"'{rule['expectation']}' trong great_expectations")
    return cls(**rule["expression"])


def evaluate(frame, rules):
    """
    Run every rule and return one verdict per rule, plus the batch decision.

    Pass rate comes from GX's own result rather than being recounted here: two
    places computing the same number is two places that can disagree about what
    the evidence says.
    """
    batch = _batch(frame)
    total = len(frame)
    results, blocked_by, weighted, total_weight = [], [], 0.0, 0.0

    for rule in rules:
        outcome = batch.validate(_expectation(rule))
        stats = outcome.result or {}
        unexpected = stats.get("unexpected_count", 0)
        pass_rate = (total - unexpected) / total if total else 0.0
        passed = pass_rate >= float(rule["threshold"])

        results.append({
            "rule_name": rule["rule_name"],
            "level": rule["level"],
            "pass_rate": round(pass_rate, 4),
            "threshold": float(rule["threshold"]),
            "failed_rows": unexpected,
            "passed": passed,
            "version": rule["version"],
            "owner": rule["owner"],
            "on_fail_action": rule["on_fail_action"] if not passed else None,
        })

        if rule["level"] == "blocker" and not passed:
            blocked_by.append(rule["rule_name"])
        elif rule["level"] == "scoring":
            weight = float(rule["weight"])
            weighted += pass_rate * weight
            total_weight += weight

    score = weighted / total_weight if total_weight else 1.0

    # A high average never rescues a failed scoring rule. Averaging is for
    # reporting, not for deciding — one rule below its floor is still a rule
    # below its floor, whatever the others did.
    scoring_failed = [r["rule_name"] for r in results
                      if r["level"] == "scoring" and not r["passed"]]

    return {
        "total_rows": total,
        "rules": results,
        "score": round(score, 4),
        "blocked_by": blocked_by,
        "scoring_failed": scoring_failed,
        "passed": not blocked_by and not scoring_failed,
        "engine": f"great-expectations/{_gx_version()}",
    }


def _gx_version():
    import great_expectations as gx

    return getattr(gx, "__version__", "?")


def record(run_id, verdict):
    """Write one row per rule, so evidence survives the log retention."""
    rows = [
        (run_id, "stg_imate__document_typed", r["rule_name"], r["level"],
         r["failed_rows"], r["pass_rate"],
         json.dumps({"threshold": r["threshold"], "passed": r["passed"],
                     "rule_version": r["version"], "owner": r["owner"],
                     "on_fail_action": r["on_fail_action"],
                     "engine": verdict["engine"]}, ensure_ascii=False))
        for r in verdict["rules"]
    ]
    with imate_cursor() as cur:
        cur.executemany("""
            INSERT INTO metadata.quality_exceptions
                (run_id, table_name, rule_name, severity,
                 failed_rows, pass_rate, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, rows)
