"""
Assets that chain the seven iMate DAGs together.

Airflow matches assets by URI, so these constants are the contract between
DAGs: DAG n declares one as an outlet, DAG n+1 schedules on it. Nothing else
couples them — no DAG imports another, and none triggers another by name.

That direction matters. TriggerDagRunOperator makes the producer name every
consumer, so adding a downstream DAG means editing the upstream one. With
assets the consumer subscribes, and a stage that later feeds three DAGs
instead of one needs no change at all.
"""

try:                                        # Airflow 3
    from airflow.sdk import Asset
except ImportError:                         # Airflow 2 fallback
    from airflow.datasets import Dataset as Asset

WORKLIST = Asset("imate://worklist")            # 01 -> 02
BRONZE = Asset("imate://bronze")                # 02 -> 03
SILVER_ONE = Asset("imate://staging/silver-1")  # 03 -> 04
SILVER_TWO = Asset("imate://staging/silver-2")  # 04 -> 05
VERDICT = Asset("imate://quality/verdict")      # 05 -> 06
CURATED = Asset("imate://curated/documents")    # 06 -> 07
