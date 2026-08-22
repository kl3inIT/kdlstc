package vn.dth.dwh.pipeline.api;

import vn.dth.dwh.pipeline.application.PipelineRunSnapshot;

public record PipelineRunEventResponse(
        String id,
        String stageCode,
        String eventType,
        String actor,
        String occurredAt,
        String payload
) {
    static PipelineRunEventResponse from(PipelineRunSnapshot.EventSnapshot event) {
        return new PipelineRunEventResponse(
                event.id().toString(),
                event.stageCode(),
                event.eventType().name().toLowerCase(),
                event.actor(),
                event.occurredAt().toString(),
                event.payload()
        );
    }
}
