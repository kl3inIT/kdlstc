package vn.dth.dwh.pipeline.domain;

public enum PipelineRunEventType {
    TRIGGER_REQUESTED,
    AIRFLOW_ACCEPTED,
    AIRFLOW_REJECTED,
    STEP_STATE_CHANGED,
    RUN_STATE_CHANGED
}
