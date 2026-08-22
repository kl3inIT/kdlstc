package vn.dth.dwh.pipeline.api;

import vn.dth.dwh.pipeline.application.PipelineRunSnapshot;

public record PipelineStepResponse(
        String code,
        int stepNumber,
        String title,
        String phaseLabel,
        String dagId,
        String dagRunId,
        String status,
        String startedAt,
        String finishedAt,
        long recordCount,
        long warningCount,
        long errorCount,
        String details
) {
    static PipelineStepResponse from(PipelineRunSnapshot.StepSnapshot step) {
        return new PipelineStepResponse(
                step.code(),
                step.stepNumber(),
                step.title(),
                step.phaseLabel(),
                step.dagId(),
                step.dagRunId(),
                step.status().apiValue(),
                step.startedAt() == null ? null : step.startedAt().toString(),
                step.finishedAt() == null ? null : step.finishedAt().toString(),
                step.recordCount(),
                step.warningCount(),
                step.errorCount(),
                step.details()
        );
    }
}
