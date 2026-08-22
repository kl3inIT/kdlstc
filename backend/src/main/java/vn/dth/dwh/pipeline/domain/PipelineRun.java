package vn.dth.dwh.pipeline.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "pipeline_run")
public class PipelineRun {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Version
    @Column(nullable = false)
    private Integer version;

    @Column(name = "correlation_id", nullable = false, unique = true, length = 255)
    private String correlationId;

    @Column(name = "root_dag_run_id", nullable = false, unique = true, length = 255)
    private String rootDagRunId;

    @Column(name = "pipeline_code", nullable = false, length = 50)
    private String pipelineCode;

    @Column(nullable = false, length = 255)
    private String scope;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private PipelineRunStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "trigger_type", nullable = false, length = 32)
    private PipelineTriggerType triggerType;

    @Column(name = "initiated_by", nullable = false, length = 255)
    private String initiatedBy;

    @Column(name = "started_at", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Column(name = "last_synchronized_at", nullable = false)
    private OffsetDateTime lastSynchronizedAt;

    @Column(name = "record_count", nullable = false)
    private long recordCount;

    public UUID getId() {
        return id;
    }

    public Integer getVersion() {
        return version;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getRootDagRunId() {
        return rootDagRunId;
    }

    public void setRootDagRunId(String rootDagRunId) {
        this.rootDagRunId = rootDagRunId;
    }

    public String getPipelineCode() {
        return pipelineCode;
    }

    public void setPipelineCode(String pipelineCode) {
        this.pipelineCode = pipelineCode;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public PipelineRunStatus getStatus() {
        return status;
    }

    public void setStatus(PipelineRunStatus status) {
        this.status = status;
    }

    public PipelineTriggerType getTriggerType() {
        return triggerType;
    }

    public void setTriggerType(PipelineTriggerType triggerType) {
        this.triggerType = triggerType;
    }

    public String getInitiatedBy() {
        return initiatedBy;
    }

    public void setInitiatedBy(String initiatedBy) {
        this.initiatedBy = initiatedBy;
    }

    public OffsetDateTime getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(OffsetDateTime startedAt) {
        this.startedAt = startedAt;
    }

    public OffsetDateTime getFinishedAt() {
        return finishedAt;
    }

    public void setFinishedAt(OffsetDateTime finishedAt) {
        this.finishedAt = finishedAt;
    }

    public OffsetDateTime getLastSynchronizedAt() {
        return lastSynchronizedAt;
    }

    public void setLastSynchronizedAt(OffsetDateTime lastSynchronizedAt) {
        this.lastSynchronizedAt = lastSynchronizedAt;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }
}
