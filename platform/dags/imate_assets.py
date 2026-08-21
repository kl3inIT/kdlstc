"""
Assets that chain the iMate DAGs together, one per architecture step.

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

# Named after the ARCHITECTURE step each one ends, not after the DAG that
# happens to emit it. Step 4 is split across two DAGs (see imate_04b), so two
# assets sit between Silver-2 and Gold; every other step has exactly one.

WORKLIST = Asset("imate://worklist")            # 01 nguon    -> 02 bronze
BRONZE = Asset("imate://bronze")                # 02 bronze   -> 03 silver-1
SILVER_ONE = Asset("imate://staging/silver-1")  # 03 silver-1 -> 04 silver-2
SILVER_TWO = Asset("imate://staging/silver-2")  # 04 silver-2 -> 04b gate
VERDICT = Asset("imate://quality/verdict")      # 04b gate    -> 05 gold
CURATED = Asset("imate://curated/documents")    # 05 gold     -> 06 serving
SERVING = Asset("imate://serving/documents")    # 06 serving  -> 07 khai thac
