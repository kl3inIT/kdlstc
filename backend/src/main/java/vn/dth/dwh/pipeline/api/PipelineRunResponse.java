package vn.dth.dwh.pipeline.api;

import vn.dth.dwh.pipeline.application.PipelineRunSnapshot;

import java.util.List;

public record PipelineRunResponse(
        String id,
        String correlationId,
        String airflowRunId,
        String startedAt,
        String finishedAt,
        String status,
        String triggerType,
        String initiatedBy,
        String scope,
        int completedSteps,
        int totalSteps,
        long recordCount,
        List<PipelineStepResponse> steps
) {
    static PipelineRunResponse from(PipelineRunSnapshot run) {
        return new PipelineRunResponse(
                run.id().toString(),
                run.correlationId(),
                run.airflowRunId(),
                run.startedAt().toString(),
                run.finishedAt() == null ? null : run.finishedAt().toString(),
                run.status().apiValue(),
                run.triggerType().apiValue(),
                run.initiatedBy(),
                run.scope(),
                run.completedSteps(),
                run.totalSteps(),
                run.recordCount(),
                run.steps().stream().map(PipelineStepResponse::from).toList()
        );
    }
}
