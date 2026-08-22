package vn.dth.dwh.pipeline.application;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

public interface AirflowGateway {

    List<DagRunSnapshot> listRootRuns(int limit);

    List<AssetEventSnapshot> listAssetEvents(int limit);

    DagRunSnapshot triggerRootRun(TriggerCommand command);

    record TriggerCommand(String dagRunId, Map<String, Object> configuration, String note) {
    }

    record DagRunSnapshot(
            String dagId,
            String runId,
            OffsetDateTime startedAt,
            OffsetDateTime finishedAt,
            String state,
            String actor,
            Map<String, Object> configuration
    ) {
    }

    record CreatedDagRunSnapshot(
            String dagId,
            String runId,
            OffsetDateTime startedAt,
            OffsetDateTime finishedAt,
            String state
    ) {
    }

    record AssetEventSnapshot(
            String uri,
            Map<String, Object> extra,
            String sourceDagId,
            String sourceRunId,
            List<CreatedDagRunSnapshot> createdRuns,
            OffsetDateTime timestamp
    ) {
    }
}
