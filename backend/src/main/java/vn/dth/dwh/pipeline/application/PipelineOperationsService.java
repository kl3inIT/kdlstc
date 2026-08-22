package vn.dth.dwh.pipeline.application;

import org.springframework.stereotype.Service;
import vn.dth.dwh.pipeline.infrastructure.airflow.AirflowClientException;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class PipelineOperationsService {

    private static final int ROOT_SYNC_LIMIT = 100;
    private static final int ASSET_EVENT_SYNC_LIMIT = 1000;

    private final AirflowGateway airflowGateway;
    private final PipelineLedgerService ledgerService;
    private final Clock clock;

    public PipelineOperationsService(
            AirflowGateway airflowGateway,
            PipelineLedgerService ledgerService,
            Clock clock
    ) {
        this.airflowGateway = airflowGateway;
        this.ledgerService = ledgerService;
        this.clock = clock;
    }

    public PipelineRunSnapshot trigger(String scope, String actor) {
        Instant now = clock.instant();
        String correlationId = UUID.randomUUID().toString();
        String dagRunId = "dwh__" + now + "__" + correlationId.substring(0, 8);
        ledgerService.startManualRun(correlationId, dagRunId, scope, actor);

        AirflowGateway.TriggerCommand command = new AirflowGateway.TriggerCommand(
                dagRunId,
                Map.of(
                        "scope", scope,
                        "initiated_by", actor,
                        "correlation_id", correlationId,
                        "root_dag_run_id", dagRunId
                ),
                "Khởi tạo từ DWH Operations"
        );
        try {
            return ledgerService.acceptRootRun(correlationId, airflowGateway.triggerRootRun(command));
        } catch (AirflowClientException exception) {
            ledgerService.rejectTrigger(correlationId, exception.getMessage());
            throw exception;
        }
    }

    public void reconcile() {
        ledgerService.synchronize(
                airflowGateway.listRootRuns(ROOT_SYNC_LIMIT),
                airflowGateway.listAssetEvents(ASSET_EVENT_SYNC_LIMIT)
        );
    }

    public List<PipelineRunSnapshot> recentRuns(int limit) {
        return ledgerService.recentRuns(limit);
    }

    public PipelineRunSnapshot findRun(UUID id) {
        return ledgerService.findRun(id);
    }

    public List<PipelineRunSnapshot.EventSnapshot> events(UUID id) {
        return ledgerService.events(id);
    }
}
