package vn.dth.dwh.pipeline.application;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import vn.dth.dwh.pipeline.infrastructure.airflow.AirflowClientException;

@Component
@ConditionalOnProperty(name = "app.integration.airflow.mode", havingValue = "live")
public class PipelineReconciliationJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineReconciliationJob.class);

    private final PipelineOperationsService operationsService;

    public PipelineReconciliationJob(PipelineOperationsService operationsService) {
        this.operationsService = operationsService;
    }

    @Scheduled(
            initialDelayString = "${app.integration.airflow.reconcile-delay}",
            fixedDelayString = "${app.integration.airflow.reconcile-delay}"
    )
    public void reconcile() {
        try {
            operationsService.reconcile();
        } catch (AirflowClientException exception) {
            LOGGER.warn("Không đồng bộ được Airflow; giữ nguyên ledger đã lưu: {}", exception.getMessage());
        }
    }
}
