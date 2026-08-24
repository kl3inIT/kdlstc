"""
Assets that chain the iMate DAGs together, one per architecture step.

Airflow matches assets by URI, so these constants are the contract between
DAGs: DAG n declares one as an outlet, DAG n+1 schedules on it. Per-run extra
metadata carries the control-plane correlation id without coupling a producer
to any consumer DAG name.
"""

try:                                        # Airflow 3
    from airflow.sdk import Asset
except ImportError:                         # Airflow 2 fallback
    from airflow.datasets import Dataset as Asset


WORKLIST = Asset("imate://worklist")            # 01 source   -> 02 bronze
BRONZE = Asset("imate://bronze")                # 02 bronze   -> 03 silver-1
SILVER_ONE = Asset("imate://staging/silver-1")  # 03 silver-1 -> 04 silver-2
SILVER_TWO = Asset("imate://staging/silver-2")  # 04 silver-2 -> 04b gate
VERDICT = Asset("imate://quality/verdict")      # 04b gate    -> 05 gold
CURATED = Asset("imate://curated/documents")    # 05 gold     -> 06 serving
SERVING = Asset("imate://serving/documents")    # 06 serving  -> 07 consume
COMPLETE = Asset("imate://complete")            # 07 terminal event for ledger


def chain_context(context, stage_code, step_number):
    """Resolve the same domain identity for a root run and every asset child."""
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    triggering = context.get("triggering_asset_events") or {}
    events = [event for asset_events in triggering.values()
              for event in asset_events]
    latest_event = max(events, key=lambda event: event.timestamp,
                       default=None)
    inherited = latest_event.extra if latest_event and latest_event.extra else {}

    correlation_id = (
        conf.get("correlation_id")
        or inherited.get("correlation_id")
        or f"airflow::{dag_run.run_id}"
    )
    root_dag_run_id = (
        conf.get("root_dag_run_id")
        or inherited.get("root_dag_run_id")
        or dag_run.run_id
    )
    return {
        "correlation_id": str(correlation_id),
        "root_dag_run_id": str(root_dag_run_id),
        "scope": str(conf.get("scope") or inherited.get("scope")
                     or "iMate · tăng dần"),
        "initiated_by": str(conf.get("initiated_by")
                            or inherited.get("initiated_by")
                            or "airflow"),
        "stage_code": stage_code,
        "step_number": step_number,
    }


def publish_chain_event(outlet_events, asset, info, metrics):
    """Annotate the emitted event with identity plus JSON-serializable metrics."""
    payload = dict(metrics or {})
    outlet_events[asset].extra = {
        "correlation_id": info["correlation_id"],
        "root_dag_run_id": info["root_dag_run_id"],
        "scope": info["scope"],
        "initiated_by": info["initiated_by"],
        "stage_code": info["stage_code"],
        "step_number": info["step_number"],
        "metrics": {
            **payload,
            "record_count": _first_count(
                payload, "record_count", "row_count", "documents", "staged",
                "typed", "landed", "published", "total"
            ),
            "warning_count": _first_count(
                payload, "warning_count", "warnings", "scoring_failed"
            ),
            "error_count": _first_count(
                payload, "error_count", "errors", "failed", "defects",
                "blocked_by"
            ),
        },
    }


def _first_count(values, *keys):
    for key in keys:
        value = values.get(key)
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, (list, tuple, set, dict)):
            return len(value)
    return 0
