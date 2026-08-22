package vn.dth.dwh.pipeline.domain;

public enum PipelineTriggerType {
    MANUAL,
    SCHEDULED,
    ASSET,
    REPLAY;

    public String apiValue() {
        return name().toLowerCase();
    }
}
