package vn.dth.dwh.pipeline.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;
import org.hibernate.Length;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "pipeline_step_run", uniqueConstraints = @UniqueConstraint(
        name = "uk_pipeline_step_run_stage",
        columnNames = {"pipeline_run_id", "stage_code"}
))
public class PipelineStepRun {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Version
    @Column(nullable = false)
    private Integer version;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "pipeline_run_id", nullable = false)
    private PipelineRun pipelineRun;

    @Column(name = "stage_code", nullable = false, length = 10)
    private String stageCode;

    @Column(name = "step_number", nullable = false)
    private int stepNumber;

    @Column(name = "dag_id", nullable = false, length = 100)
    private String dagId;

    @Column(name = "dag_run_id", nullable = false, length = 255)
    private String dagRunId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private PipelineRunStatus status;

    @Column(name = "started_at")
    private OffsetDateTime startedAt;

    @Column(name = "finished_at")
    private OffsetDateTime finishedAt;

    @Column(name = "record_count", nullable = false)
    private long recordCount;

    @Column(name = "warning_count", nullable = false)
    private long warningCount;

    @Column(name = "error_count", nullable = false)
    private long errorCount;

    @Column(length = Length.LONG32)
    private String details;

    public UUID getId() {
        return id;
    }

    public PipelineRun getPipelineRun() {
        return pipelineRun;
    }

    public void setPipelineRun(PipelineRun pipelineRun) {
        this.pipelineRun = pipelineRun;
    }

    public String getStageCode() {
        return stageCode;
    }

    public void setStageCode(String stageCode) {
        this.stageCode = stageCode;
    }

    public int getStepNumber() {
        return stepNumber;
    }

    public void setStepNumber(int stepNumber) {
        this.stepNumber = stepNumber;
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getDagRunId() {
        return dagRunId;
    }

    public void setDagRunId(String dagRunId) {
        this.dagRunId = dagRunId;
    }

    public PipelineRunStatus getStatus() {
        return status;
    }

    public void setStatus(PipelineRunStatus status) {
        this.status = status;
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

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getWarningCount() {
        return warningCount;
    }

    public void setWarningCount(long warningCount) {
        this.warningCount = warningCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(long errorCount) {
        this.errorCount = errorCount;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }
}
