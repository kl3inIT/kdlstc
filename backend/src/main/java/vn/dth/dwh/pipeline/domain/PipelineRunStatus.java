package vn.dth.dwh.pipeline.domain;

public enum PipelineRunStatus {
    WAITING,
    QUEUED,
    RUNNING,
    SUCCESS,
    PARTIAL_SUCCESS,
    FAILED,
    STOPPED,
    SCHEMA_BLOCKED;

    public String apiValue() {
        return name().toLowerCase();
    }
}
