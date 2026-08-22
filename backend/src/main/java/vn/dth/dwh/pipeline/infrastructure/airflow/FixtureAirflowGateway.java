package vn.dth.dwh.pipeline.infrastructure.airflow;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import vn.dth.dwh.pipeline.application.AirflowGateway;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
@ConditionalOnProperty(
        name = "app.integration.airflow.mode",
        havingValue = "fixture",
        matchIfMissing = true
)
public class FixtureAirflowGateway implements AirflowGateway {

    private final Clock clock;
    private final AirflowProperties properties;
    private final CopyOnWriteArrayList<DagRunSnapshot> runs = new CopyOnWriteArrayList<>();

    public FixtureAirflowGateway(Clock clock, AirflowProperties properties) {
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public List<DagRunSnapshot> listRootRuns(int limit) {
        return runs.stream().limit(Math.max(1, limit)).toList();
    }

    @Override
    public List<AssetEventSnapshot> listAssetEvents(int limit) {
        return List.of();
    }

    @Override
    public DagRunSnapshot triggerRootRun(TriggerCommand command) {
        DagRunSnapshot run = new DagRunSnapshot(
                properties.rootDagId(),
                command.dagRunId(),
                OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC),
                null,
                "queued",
                "fixture-airflow",
                command.configuration()
        );
        runs.addFirst(run);
        return run;
    }
}
