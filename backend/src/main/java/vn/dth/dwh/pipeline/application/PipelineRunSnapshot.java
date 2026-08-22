package vn.dth.dwh.pipeline.application;

import vn.dth.dwh.pipeline.domain.PipelineRunEventType;
import vn.dth.dwh.pipeline.domain.PipelineRunStatus;
import vn.dth.dwh.pipeline.domain.PipelineTriggerType;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public record PipelineRunSnapshot(
        UUID id,
        String correlationId,
        String airflowRunId,
        OffsetDateTime startedAt,
        OffsetDateTime finishedAt,
        PipelineRunStatus status,
        PipelineTriggerType triggerType,
        String initiatedBy,
        String scope,
        int completedSteps,
        int totalSteps,
        long recordCount,
        List<StepSnapshot> steps
) {
    public record StepSnapshot(
            String code,
            int stepNumber,
            String title,
            String phaseLabel,
            String dagId,
            String dagRunId,
            PipelineRunStatus status,
            OffsetDateTime startedAt,
            OffsetDateTime finishedAt,
            long recordCount,
            long warningCount,
            long errorCount,
            String details
    ) {
    }

    public record EventSnapshot(
            UUID id,
            String stageCode,
            PipelineRunEventType eventType,
            String actor,
            OffsetDateTime occurredAt,
            String payload
    ) {
    }
}
