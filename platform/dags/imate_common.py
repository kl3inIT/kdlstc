"""
Shared constants and the API client for the iMate slice.

This module is imported by the DAGs AND executed inside the pod that does the
fetching, so it must stay importable with nothing but the standard library
plus psycopg2 — both of which the Airflow image already carries.

Why a separate pod does the fetching
------------------------------------
The API lives on 10.123.123.199:80, which is iks-node1's own address. Ordinary
pods sit on 10.42.0.0/16 and that source range is filtered on ports 80/443 —
6443 on the same host answers fine, so this is a port filter, not a missing
route. A pod with hostNetwork borrows the node's network namespace, so its
source address is 10.123.123.x and the filter does not apply.

hostNetwork is a blunt instrument: it also bypasses NetworkPolicy and exposes
node-local listeners to the pod. It is therefore confined to the two tasks that
genuinely need the network, and never applied to the Airflow workers.
"""

import contextlib
import json
import os
import threading
import urllib.parse

import dlt
from dlt.sources.helpers.rest_client import RESTClient

# Same extractor standard as the qlgia slice: dlt's REST client supplies the
# retried session; the warehouse stays the only owner of cursor and state —
# dlt persists nothing here. Version is recorded into every run summary so
# lineage can say WHICH extractor produced a given bronze object.
DLT_VERSION = dlt.__version__

SOURCE_CODE = "imate"

# Everything this slice writes to S3 lives in its OWN bucket, with medallion
# folders inside — never in the buckets the other sources share.
S3_BUCKET = os.environ.get("IMATE_S3_BUCKET", "imate")
TENANT_ID = os.environ.get("IMATE_TENANT_ID",
                           "2d4a7629-0620-504b-a70f-083958673534")
TENANT_CODE = os.environ.get("IMATE_TENANT_CODE", "doit.phuyen")

# Address plus Host header rather than the name: cluster DNS does not resolve
# imate.local, and the vhost still has to match for the server to route.
API_ADDR = os.environ.get("IMATE_API_ADDR", "http://10.123.123.199")
API_HOST = os.environ.get("IMATE_API_HOST", "imate.local")

# The API caps pageSize at 100 and answers a larger value with HTTP 200 and
# success=false. Asking for exactly the cap keeps the page count honest.
PAGE_SIZE = 100
HTTP_TIMEOUT = 45

# The list is walked newest-first, so anything new or changed is at the front.
# The walk stops after this many consecutive pages in which every document was
# already known at the same updatedAt.
STOP_AFTER_CLEAN_PAGES = 3

# Guard against a bad cursor turning one run into a full re-crawl forever.
MAX_PAGES = 200

DETAIL_INCLUDE = "attachments,receipts,routings,userGeneratedAttachments"

# What a list row must look like. Checked before anything is written, so a
# source that changed shape is refused while the work list is still clean.
LIST_CONTRACT = ("globalId", "tenantId", "documentId", "documentNo",
                 "uploadedAt", "createdAt", "updatedAt", "processStatus")


class SourceContractError(RuntimeError):
    """The source answered, but not with the shape we agreed on."""


class SourceRefusedError(RuntimeError):
    """The source answered with success=false."""


# ── database identity ────────────────────────────────────────────────────
# The iMate slice runs as its own database role (imate_etl) so its blast
# radius is exactly the grant list in sql/10_grants_imate.sql, and audit can
# tell its writes apart from every other flow. Resolution order:
#
#   1. IMATE_DWH_* env      the fetch pod — injected from Secret imate-db
#   2. Airflow Connection   worker tasks — connection id 'imate_dwh'
#   3. shared DWH_* env     LOUD fallback so a missing connection degrades
#                           to running instead of dying, but never silently
AIRFLOW_CONN_ID = "imate_dwh"


def _db_params():
    if os.environ.get("IMATE_DWH_HOST"):
        return {
            "host": os.environ["IMATE_DWH_HOST"],
            "dbname": os.environ["IMATE_DWH_DBNAME"],
            "user": os.environ["IMATE_DWH_USER"],
            "password": os.environ["IMATE_DWH_PASSWORD"],
        }
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(AIRFLOW_CONN_ID)
        return {"host": conn.host, "port": conn.port or 5432,
                "dbname": conn.schema, "user": conn.login,
                "password": conn.password}
    except Exception:                                  # noqa: BLE001
        print(f"CANH BAO: khong lay duoc connection '{AIRFLOW_CONN_ID}' — "
              "tam dung thong tin dang nhap dung chung (DWH_*)", flush=True)
        return {
            "host": os.environ["DWH_HOST"],
            "dbname": os.environ["DWH_DBNAME"],
            "user": os.environ["DWH_USER"],
            "password": os.environ["DWH_PASSWORD"],
        }


@contextlib.contextmanager
def imate_cursor(autocommit=False):
    """Same contract as warehouse_cursor, but on the slice's own identity."""
    import psycopg2

    conn = psycopg2.connect(connect_timeout=10,
                            application_name="airflow-imate", **_db_params())
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


# ── the serving identity ──────────────────────────────────────────────────
# Step 6 checks the door a BI tool actually walks through, and a door can only
# be checked from outside: connecting as imate_etl would prove nothing, because
# the writer can read everything by definition. imate_reader holds SELECT on the
# curated/refdata schemas and nothing else, which is exactly what Superset uses.
READER_CONN_ID = "imate_reader"


@contextlib.contextmanager
def reader_cursor():
    """Read-only cursor on the slice, using the identity BI tools use."""
    import psycopg2

    if os.environ.get("IMATE_READER_HOST"):
        params = {
            "host": os.environ["IMATE_READER_HOST"],
            "dbname": os.environ["IMATE_READER_DBNAME"],
            "user": os.environ["IMATE_READER_USER"],
            "password": os.environ["IMATE_READER_PASSWORD"],
        }
    else:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(READER_CONN_ID)
        params = {"host": conn.host, "port": conn.port or 5432,
                  "dbname": conn.schema, "user": conn.login,
                  "password": conn.password}

    connection = psycopg2.connect(connect_timeout=10,
                                  application_name="airflow-imate-serving",
                                  **params)
    connection.autocommit = True
    try:
        with connection.cursor() as cur:
            yield cur
    finally:
        connection.close()


def set_status(run_id, status, **fields):
    """warehouse.set_run_status, but through the slice's own connection."""
    from warehouse import RUN_STATES

    if status not in RUN_STATES:
        raise ValueError(f"unknown run status: {status}")
    assignments = ["status = %(status)s"]
    params = {"run_id": run_id, "status": status}
    for column, value in fields.items():
        assignments.append(f"{column} = %({column})s")
        params[column] = value
    if status in ("published", "schema_blocked", "quality_failed"):
        assignments.append("finished_at = now()")
    with imate_cursor(autocommit=True) as cur:
        cur.execute(
            f"UPDATE ingestion.runs SET {', '.join(assignments)} "
            f"WHERE run_id = %(run_id)s", params)


# One RESTClient per thread: land fetches details with a thread pool, and a
# requests Session is not guaranteed thread-safe. Each client carries dlt's
# retrying session (backoff on connection errors and 5xx), which the old
# urllib implementation never had.
_local = threading.local()


def _client():
    if getattr(_local, "client", None) is None:
        _local.client = RESTClient(
            base_url=API_ADDR,
            headers={"Host": API_HOST, "Accept": "application/json"},
        )
    return _local.client


def api_get(path):
    """
    One GET against the iMate API, with the two traps this API sets.

    Trap one: validation failures come back as HTTP 200 with success=false and
    body=null. Checking the status code alone would read an error as an empty
    result and quietly ingest nothing. Authentication failures, inconsistently,
    do use 401 — so both have to be handled.

    Trap two: the vhost is selected by the Host header — the client pins it,
    because cluster DNS cannot resolve imate.local and the call goes by IP.
    """
    response = _client().get(path, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    if not payload.get("success"):
        raise SourceRefusedError(
            f"{path} -> errorCode={payload.get('errorCode')} "
            f"message={payload.get('message')}"
        )
    return payload["body"]


def list_page(page, sort="DESC"):
    query = urllib.parse.urlencode({
        "filter[tenantId]": TENANT_ID,
        "sort[updatedAt]": sort,
        "page": page,
        "pageSize": PAGE_SIZE,
    })
    return api_get(f"/api/documents?{query}")


def document_detail(global_id):
    return api_get(f"/api/documents/{global_id}?include={DETAIL_INCLUDE}")["document"]


def check_list_contract(documents):
    """
    Refuse a page whose rows lost a field we depend on.

    Raised before any write: a source that silently dropped uploadedAt would
    otherwise fill the work list with rows that can never be dated, and the
    damage would only surface at publish time.
    """
    for row in documents:
        missing = [f for f in LIST_CONTRACT if f not in row]
        if missing:
            raise SourceContractError(
                f"van ban {row.get('globalId', '?')} thieu truong: {missing}"
            )
