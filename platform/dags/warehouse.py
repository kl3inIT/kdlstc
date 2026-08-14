"""
Shared plumbing for warehouse DAGs: database access, object storage, and the
run-ledger state machine.

Kept deliberately small and dependency-free — psycopg2, boto3 and pandas are
already in the Airflow image, so nothing here needs a custom build.
"""

import contextlib
import os

import boto3
import psycopg2

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs-s3:8333")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")

# Valid values for ingestion.runs.status, in the order a healthy batch walks
# through them. A batch that stops early keeps the status it stopped on, which
# is what makes the ledger worth reading.
RUN_STATES = (
    "received",         # ticket opened, nothing fetched yet
    "schema_blocked",   # source changed shape — refused before any data landed
    "parsed",           # bytes are in Bronze and rows are in Silver-1
    "mapped",           # source codes resolved to warehouse codes
    "quality_passed",
    "quality_failed",
    "published",        # visible in curated
)


@contextlib.contextmanager
def warehouse_cursor(autocommit=False):
    """
    Cursor on the warehouse database.

    Commits on clean exit, rolls back on exception. Callers that need to
    record a failure must use a separate connection, because the failed
    transaction is already rolled back by the time they hear about it.
    """
    conn = psycopg2.connect(
        host=os.environ["DWH_HOST"],
        dbname=os.environ["DWH_DBNAME"],
        user=os.environ["DWH_USER"],
        password=os.environ["DWH_PASSWORD"],
        connect_timeout=10,
        application_name="airflow-warehouse",
    )
    conn.autocommit = autocommit
    try:
        if autocommit:
            with conn.cursor() as cur:
                yield cur
        else:
            with conn:
                with conn.cursor() as cur:
                    yield cur
    finally:
        conn.close()


def object_store():
    """
    S3 client pointed at SeaweedFS.

    Authentication is currently disabled on the gateway, but boto3 still
    insists on credentials, hence the placeholders. When auth is switched on,
    read them from the same secret the rest of the stack uses.
    """
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "warehouse"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "warehouse"),
        region_name="us-east-1",
    )


def set_run_status(run_id, status, **fields):
    """
    Move a run to a new state and stamp whatever the caller learned.

    Uses its own connection and commits immediately: status has to survive
    even when the transaction that produced the failure did not.
    """
    if status not in RUN_STATES:
        raise ValueError(f"unknown run status: {status}")

    assignments = ["status = %(status)s"]
    params = {"run_id": run_id, "status": status}

    for column, value in fields.items():
        assignments.append(f"{column} = %({column})s")
        params[column] = value

    if status in ("published", "schema_blocked", "quality_failed"):
        assignments.append("finished_at = now()")

    with warehouse_cursor(autocommit=True) as cur:
        cur.execute(
            f"UPDATE ingestion.runs SET {', '.join(assignments)} "
            f"WHERE run_id = %(run_id)s",
            params,
        )


def write_audit(actor, action, object_ref=None, before_after=None, trace_id=None):
    """Append-only trail. Never blocks the pipeline — audit is not a gate."""
    from psycopg2.extras import Json

    with warehouse_cursor(autocommit=True) as cur:
        cur.execute(
            "INSERT INTO audit.audit_log (actor, action, object_ref, "
            "before_after, trace_id) VALUES (%s, %s, %s, %s, %s)",
            (actor, action, object_ref,
             Json(before_after) if before_after is not None else None,
             trace_id),
        )
