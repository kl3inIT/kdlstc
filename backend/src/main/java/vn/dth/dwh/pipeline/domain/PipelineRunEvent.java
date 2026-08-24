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
import org.hibernate.Length;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "pipeline_run_event")
public class PipelineRunEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "pipeline_run_id", nullable = false)
    private PipelineRun pipelineRun;

    @Column(name = "idempotency_key", nullable = false, unique = true, length = 500)
    private String idempotencyKey;

    @Column(name = "stage_code", length = 10)
    private String stageCode;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 50)
    private PipelineRunEventType eventType;

    @Column(nullable = false, length = 255)
    private String actor;

    @Column(name = "occurred_at", nullable = false)
    private OffsetDateTime occurredAt;

    @Column(length = Length.LONG32)
    private String payload;

    public UUID getId() {
        return id;
    }

    public PipelineRun getPipelineRun() {
        return pipelineRun;
    }

    public void setPipelineRun(PipelineRun pipelineRun) {
        this.pipelineRun = pipelineRun;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getStageCode() {
        return stageCode;
    }

    public void setStageCode(String stageCode) {
        this.stageCode = stageCode;
    }

    public PipelineRunEventType getEventType() {
        return eventType;
    }

    public void setEventType(PipelineRunEventType eventType) {
        this.eventType = eventType;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public OffsetDateTime getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(OffsetDateTime occurredAt) {
        this.occurredAt = occurredAt;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
