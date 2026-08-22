package vn.dth.dwh.pipeline;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import vn.dth.dwh.pipeline.application.AirflowGateway;
import vn.dth.dwh.pipeline.application.PipelineLedgerService;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class PipelineLedgerServiceIntegrationTest {

    @Autowired
    private PipelineLedgerService ledgerService;

    @Test
    void projectsNewestFirstAirflowEventsIntoOneCorrelatedRun() {
        String correlationId = UUID.randomUUID().toString();
        String rootRunId = "dwh__" + correlationId;
        OffsetDateTime startedAt = OffsetDateTime.parse("2026-08-21T04:00:00Z");
        Map<String, Object> identity = Map.of(
                "correlation_id", correlationId,
                "root_dag_run_id", rootRunId,
                "scope", "iMate · toàn bộ chuỗi",
                "initiated_by", "operator"
        );
        ledgerService.startManualRun(correlationId, rootRunId, "iMate · toàn bộ chuỗi", "operator");

        AirflowGateway.DagRunSnapshot root = new AirflowGateway.DagRunSnapshot(
                "imate_01_discover", rootRunId, startedAt, null, "running", "dwh-api", identity);
        AirflowGateway.AssetEventSnapshot worklist = new AirflowGateway.AssetEventSnapshot(
                "imate://worklist",
                withMetrics(identity, 10),
                "imate_01_discover",
                rootRunId,
                List.of(new AirflowGateway.CreatedDagRunSnapshot(
                        "imate_02_land_bronze", "asset__bronze",
                        startedAt.plusSeconds(10), null, "running"
                )),
                startedAt.plusSeconds(9)
        );
        AirflowGateway.AssetEventSnapshot bronze = new AirflowGateway.AssetEventSnapshot(
                "imate://bronze",
                withMetrics(identity, 42),
                "imate_02_land_bronze",
                "asset__bronze",
                List.of(new AirflowGateway.CreatedDagRunSnapshot(
                        "imate_03_silver_one", "asset__silver-one",
                        startedAt.plusSeconds(20), startedAt.plusSeconds(30), "failed"
                )),
                startedAt.plusSeconds(19)
        );

        ledgerService.synchronize(List.of(root), List.of(bronze, worklist));

        var run = ledgerService.recentRuns(100).stream()
                .filter(candidate -> correlationId.equals(candidate.correlationId()))
                .findFirst()
                .orElseThrow();
        assertThat(run.status().apiValue()).isEqualTo("failed");
        assertThat(run.completedSteps()).isEqualTo(2);
        assertThat(run.recordCount()).isEqualTo(42);
        assertThat(run.steps())
                .extracting(step -> step.code() + ":" + step.status().apiValue())
                .contains("01:success", "02:success", "03:failed", "07:waiting");
        assertThat(ledgerService.events(run.id()))
                .extracting(event -> event.eventType().name())
                .contains("TRIGGER_REQUESTED", "STEP_STATE_CHANGED", "RUN_STATE_CHANGED");
    }

    private Map<String, Object> withMetrics(Map<String, Object> identity, long recordCount) {
        return Map.of(
                "correlation_id", identity.get("correlation_id"),
                "root_dag_run_id", identity.get("root_dag_run_id"),
                "scope", identity.get("scope"),
                "initiated_by", identity.get("initiated_by"),
                "metrics", Map.of("record_count", recordCount)
        );
    }
}
