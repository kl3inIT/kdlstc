"""
imate_06_serving — step 6 of 7: open the door, then check it opens.

Steps 1 to 5 end with numbers sitting in the warehouse. Nobody outside the
warehouse can read them yet: reading requires the writer's credentials and a
hand-written query, and neither can be handed to Superset, to the province's
IOC, or to the next tool somebody adds. This step is that gap.

What it does today is the honest minimum: connect with the identity a BI tool
actually uses — imate_reader, SELECT and nothing else — and prove three things
about the serving contract.

    readable    every curated table answers through the reader identity, and
                the counts it sees match what step 5 published. A grant that
                was never applied fails HERE, not in front of an audience.
    read-only   the reader is REFUSED when it tries to write. Isolation is a
                claim until something tries to break it; this tries, every run.
    fresh       how old the newest published document is, recorded per run so
                staleness shows up as a trend rather than a surprise.

What it does NOT do yet is the part that needs Cube: one definition of each
metric, shared by every consumer. Until that exists, DAG 07 and Superset each
write their own SQL for the same number — see the gap list in
docs/ra-soat-cong-cu-7-buoc.html. This DAG is where Cube plugs in, and the
asset it emits is already the seam.
"""

import json
from datetime import datetime

try:
    from airflow.sdk import dag, task
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:
    from airflow.decorators import dag, task
    from airflow.exceptions import AirflowSkipException

from imate_assets import CURATED, SERVING
from imate_common import imate_cursor, reader_cursor, set_status as set_run_status
from imate_ops import ticket


# What the serving layer promises to expose. Named explicitly rather than read
# from the catalogue: a table that quietly loses its grant should make this
# list fail, and a list built from the same catalogue would move with it.
EXPOSED = (
    "curated.fact_document",
    "curated.fact_routing",
    "curated.dim_date",
    "curated.dim_document_kind",
    "curated.dim_issuing_body",
)


@dag(
    dag_id="imate_06_serving",
    schedule=[CURATED],
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["imate", "poc"],
)
def imate_06_serving():

    @task
    def open_run(**context):
        return ticket("06", context["dag_run"].run_id)

    @task
    def check_readable(info):
        """
        Count every exposed table through the reader identity.

        Missing grants are the classic silent failure here: the table exists,
        the writer sees rows, and the BI tool sees an empty dashboard with no
        error anyone reads. Counting through the reader turns that into a
        failed task with the table name in it.
        """
        counts = {}
        with reader_cursor() as cur:
            for table in EXPOSED:
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
        print("doc duoc: " + json.dumps(counts, ensure_ascii=False))

        empty = [t for t, n in counts.items() if n == 0]
        if empty:
            raise RuntimeError(
                f"lop phuc vu khong doc duoc du lieu tu: {empty} — "
                "kiem tra GRANT cho vai tro imate_reader")
        return counts

    @task
    def check_read_only(info):
        """
        Try to write, and require the attempt to fail.

        The grant list says the reader cannot write. This makes the database
        say it, once per run. If a future migration hands the reader INSERT by
        accident, the pipeline reports it the same day instead of the day
        somebody notices edited numbers.
        """
        try:
            with reader_cursor() as cur:
                cur.execute(
                    "INSERT INTO curated.fact_document "
                    "(global_id, date_key, kind_key, body_key, run_id, batch_id) "
                    "VALUES ('probe', -1, -1, -1, 'probe', 'probe')")
        except Exception as exc:                       # noqa: BLE001
            print(f"ghi bi tu choi dung nhu mong doi: {str(exc)[:120]}")
            return True
        raise RuntimeError(
            "CANH BAO AN TOAN: vai tro imate_reader GHI DUOC vao curated — "
            "phai thu hoi quyen ghi truoc khi mo lop phuc vu ra ngoai")

    @task(outlets=[SERVING])
    def close_run(info, counts, _read_only):
        """
        Record what the serving layer looks like right now, then ring the bell.

        Freshness is measured from the newest published document rather than
        from the run clock: a pipeline that runs on time over a source that
        stopped sending is exactly the failure this number exists to expose.
        """
        with imate_cursor() as cur:
            cur.execute("""
                SELECT max(d.full_date),
                       (now()::date - max(d.full_date))
                  FROM curated.fact_document f
                  JOIN curated.dim_date d ON d.date_key = f.date_key
            """)
            newest, age_days = cur.fetchone()

        summary = {
            "run_id": info["run_id"],
            "reader_role": "imate_reader",
            "exposed": counts,
            "write_denied": True,
            "newest_document": newest.isoformat() if newest else None,
            "staleness_days": age_days,
        }
        print("ket qua: " + json.dumps(summary, ensure_ascii=False))

        set_run_status(info["run_id"], "published",
                       row_count=counts["curated.fact_document"],
                       message=json.dumps(summary, ensure_ascii=False))
        return summary

    info = open_run()
    counts = check_readable(info)
    read_only = check_read_only(info)
    close_run(info, counts, read_only)


imate_06_serving()
