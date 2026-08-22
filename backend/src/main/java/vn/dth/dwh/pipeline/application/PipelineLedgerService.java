package vn.dth.dwh.pipeline.application;

import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;
import vn.dth.dwh.pipeline.domain.ImatePipelineDefinition;
import vn.dth.dwh.pipeline.domain.PipelineRun;
import vn.dth.dwh.pipeline.domain.PipelineRunEvent;
import vn.dth.dwh.pipeline.domain.PipelineRunEventType;
import vn.dth.dwh.pipeline.domain.PipelineRunStatus;
import vn.dth.dwh.pipeline.domain.PipelineStepRun;
import vn.dth.dwh.pipeline.domain.PipelineTriggerType;
import vn.dth.dwh.pipeline.infrastructure.persistence.PipelineRunEventRepository;
import vn.dth.dwh.pipeline.infrastructure.persistence.PipelineRunRepository;
import vn.dth.dwh.pipeline.infrastructure.persistence.PipelineStepRunRepository;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static vn.dth.dwh.pipeline.domain.ImatePipelineDefinition.PIPELINE_CODE;
import static vn.dth.dwh.pipeline.domain.ImatePipelineDefinition.STAGES;
import static vn.dth.dwh.pipeline.domain.ImatePipelineDefinition.TOTAL_STEPS;

@Service
public class PipelineLedgerService {

    private static final String DEFAULT_SCOPE = "iMate · tăng dần";
    private static final String DEFAULT_ACTOR = "airflow";

    private final PipelineRunRepository runRepository;
    private final PipelineStepRunRepository stepRepository;
    private final PipelineRunEventRepository eventRepository;
    private final ObjectMapper objectMapper;
    private final Clock clock;

    public PipelineLedgerService(
            PipelineRunRepository runRepository,
            PipelineStepRunRepository stepRepository,
            PipelineRunEventRepository eventRepository,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.runRepository = runRepository;
        this.stepRepository = stepRepository;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    @Transactional
    public void startManualRun(String correlationId, String rootDagRunId, String scope, String actor) {
        PipelineRun run = new PipelineRun();
        run.setCorrelationId(correlationId);
        run.setRootDagRunId(rootDagRunId);
        run.setPipelineCode(PIPELINE_CODE);
        run.setScope(scope);
        run.setStatus(PipelineRunStatus.WAITING);
        run.setTriggerType(PipelineTriggerType.MANUAL);
        run.setInitiatedBy(actor);
        run.setStartedAt(now());
        run.setLastSynchronizedAt(now());
        run.setRecordCount(0);
        runRepository.save(run);
        appendEvent(
                run,
                "trigger-requested:" + correlationId,
                null,
                PipelineRunEventType.TRIGGER_REQUESTED,
                actor,
                Map.of("scope", scope, "rootDagRunId", rootDagRunId),
                now()
        );
    }

    @Transactional
    public PipelineRunSnapshot acceptRootRun(String correlationId, AirflowGateway.DagRunSnapshot accepted) {
        PipelineRun run = requiredRun(correlationId);
        applyRootRun(run, accepted);
        runRepository.save(run);
        upsertStep(run, ImatePipelineDefinition.byCode("01"), accepted, null);
        appendEvent(
                run,
                "airflow-accepted:" + accepted.runId(),
                "01",
                PipelineRunEventType.AIRFLOW_ACCEPTED,
                run.getInitiatedBy(),
                Map.of("dagId", accepted.dagId(), "dagRunId", accepted.runId()),
                now()
        );
        aggregate(run);
        return snapshot(run);
    }

    @Transactional
    public void rejectTrigger(String correlationId, String reason) {
        PipelineRun run = requiredRun(correlationId);
        PipelineRunStatus previous = run.getStatus();
        run.setStatus(PipelineRunStatus.FAILED);
        run.setFinishedAt(now());
        run.setLastSynchronizedAt(now());
        runRepository.save(run);
        appendEvent(
                run,
                "airflow-rejected:" + correlationId,
                null,
                PipelineRunEventType.AIRFLOW_REJECTED,
                run.getInitiatedBy(),
                Map.of("reason", reason),
                now()
        );
        appendRunStateEvent(run, previous);
    }

    @Transactional
    public void synchronize(
            List<AirflowGateway.DagRunSnapshot> rootRuns,
            List<AirflowGateway.AssetEventSnapshot> assetEvents
    ) {
        for (AirflowGateway.DagRunSnapshot rootRun : rootRuns) {
            String correlationId = value(
                    rootRun.configuration(), "correlation_id", "airflow::" + rootRun.runId());
            PipelineRun run = runRepository.findByCorrelationId(correlationId)
                    .orElseGet(() -> newRootRun(correlationId, rootRun));
            applyRootRun(run, rootRun);
            runRepository.save(run);
            upsertStep(run, ImatePipelineDefinition.byCode("01"), rootRun, null);
        }

        assetEvents.stream()
                .filter(event -> event.timestamp() != null)
                .sorted(Comparator.comparing(AirflowGateway.AssetEventSnapshot::timestamp))
                .forEach(this::applyAssetEvent);

        runRepository.findByPipelineCodeOrderByStartedAtDesc(
                        PIPELINE_CODE, PageRequest.of(0, 200))
                .forEach(this::aggregate);
    }

    @Transactional(readOnly = true)
    public List<PipelineRunSnapshot> recentRuns(int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 100));
        return runRepository.findByPipelineCodeOrderByStartedAtDesc(
                        PIPELINE_CODE, PageRequest.of(0, safeLimit)).stream()
                .map(this::snapshot)
                .toList();
    }

    @Transactional(readOnly = true)
    public PipelineRunSnapshot findRun(UUID id) {
        return snapshot(runRepository.findById(id)
                .orElseThrow(() -> new PipelineRunNotFoundException(id)));
    }

    @Transactional(readOnly = true)
    public List<PipelineRunSnapshot.EventSnapshot> events(UUID id) {
        PipelineRun run = runRepository.findById(id)
                .orElseThrow(() -> new PipelineRunNotFoundException(id));
        return eventRepository.findByPipelineRunOrderByOccurredAtAsc(run).stream()
                .map(event -> new PipelineRunSnapshot.EventSnapshot(
                        event.getId(),
                        event.getStageCode(),
                        event.getEventType(),
                        event.getActor(),
                        event.getOccurredAt(),
                        event.getPayload()
                ))
                .toList();
    }

    private void applyAssetEvent(AirflowGateway.AssetEventSnapshot event) {
        String correlationId = value(event.extra(), "correlation_id", null);
        if (correlationId == null || event.sourceDagId() == null || event.sourceRunId() == null) {
            return;
        }
        Optional<ImatePipelineDefinition.StageDefinition> sourceStage =
                ImatePipelineDefinition.findByDagId(event.sourceDagId());
        boolean replay = "imate_08_replay".equals(event.sourceDagId());
        if (sourceStage.isEmpty() && !replay) {
            return;
        }

        PipelineRun run = runRepository.findByCorrelationId(correlationId)
                .orElseGet(() -> newRunFromEvent(correlationId, event, replay));
        sourceStage.ifPresent(stage -> upsertSuccessfulSourceStep(run, stage, event));
        for (AirflowGateway.CreatedDagRunSnapshot createdRun : event.createdRuns()) {
            ImatePipelineDefinition.findByDagId(createdRun.dagId())
                    .ifPresent(stage -> upsertStep(run, stage, createdRun, null));
        }
        aggregate(run);
    }

    private PipelineRun newRootRun(String correlationId, AirflowGateway.DagRunSnapshot rootRun) {
        PipelineRun run = new PipelineRun();
        run.setCorrelationId(correlationId);
        run.setRootDagRunId(rootRun.runId());
        run.setPipelineCode(PIPELINE_CODE);
        run.setScope(value(rootRun.configuration(), "scope", DEFAULT_SCOPE));
        run.setStatus(PipelineRunStatus.RUNNING);
        run.setTriggerType(value(rootRun.configuration(), "initiated_by", null) == null
                ? PipelineTriggerType.SCHEDULED
                : PipelineTriggerType.MANUAL);
        run.setInitiatedBy(actor(rootRun));
        run.setStartedAt(rootRun.startedAt() == null ? now() : rootRun.startedAt());
        run.setLastSynchronizedAt(now());
        return runRepository.save(run);
    }

    private PipelineRun newRunFromEvent(
            String correlationId,
            AirflowGateway.AssetEventSnapshot event,
            boolean replay
    ) {
        PipelineRun run = new PipelineRun();
        run.setCorrelationId(correlationId);
        run.setRootDagRunId(value(event.extra(), "root_dag_run_id", event.sourceRunId()));
        run.setPipelineCode(PIPELINE_CODE);
        run.setScope(value(event.extra(), "scope", DEFAULT_SCOPE));
        run.setStatus(PipelineRunStatus.RUNNING);
        run.setTriggerType(replay ? PipelineTriggerType.REPLAY : PipelineTriggerType.ASSET);
        run.setInitiatedBy(value(event.extra(), "initiated_by", DEFAULT_ACTOR));
        run.setStartedAt(event.timestamp());
        run.setLastSynchronizedAt(now());
        return runRepository.save(run);
    }

    private void applyRootRun(PipelineRun run, AirflowGateway.DagRunSnapshot rootRun) {
        run.setRootDagRunId(rootRun.runId());
        run.setScope(value(rootRun.configuration(), "scope", run.getScope()));
        if (run.getInitiatedBy() == null || run.getInitiatedBy().isBlank()) {
            run.setInitiatedBy(actor(rootRun));
        }
        if (rootRun.startedAt() != null) {
            run.setStartedAt(rootRun.startedAt());
        }
        run.setFinishedAt(rootRun.finishedAt());
        run.setStatus(mapAirflowStatus(rootRun.state()));
        run.setLastSynchronizedAt(now());
    }

    private void upsertSuccessfulSourceStep(
            PipelineRun run,
            ImatePipelineDefinition.StageDefinition stage,
            AirflowGateway.AssetEventSnapshot event
    ) {
        PipelineStepRun step = stepRepository.findByPipelineRunAndStageCode(run, stage.code())
                .orElseGet(() -> newStep(run, stage, event.sourceRunId()));
        PipelineRunStatus previous = step.getStatus();
        step.setDagRunId(event.sourceRunId());
        step.setStatus(PipelineRunStatus.SUCCESS);
        if (step.getStartedAt() == null) {
            step.setStartedAt(event.timestamp());
        }
        step.setFinishedAt(event.timestamp());
        applyMetrics(step, event.extra());
        stepRepository.save(step);
        appendStepEvent(run, step, previous, event.timestamp());
    }

    private void upsertStep(
            PipelineRun run,
            ImatePipelineDefinition.StageDefinition stage,
            AirflowGateway.DagRunSnapshot dagRun,
            Map<String, Object> metrics
    ) {
        PipelineStepRun step = stepRepository.findByPipelineRunAndStageCode(run, stage.code())
                .orElseGet(() -> newStep(run, stage, dagRun.runId()));
        PipelineRunStatus previous = step.getStatus();
        step.setDagRunId(dagRun.runId());
        step.setStatus(mapAirflowStatus(dagRun.state()));
        step.setStartedAt(dagRun.startedAt());
        step.setFinishedAt(dagRun.finishedAt());
        applyMetrics(step, metrics);
        stepRepository.save(step);
        appendStepEvent(run, step, previous, now());
    }

    private void upsertStep(
            PipelineRun run,
            ImatePipelineDefinition.StageDefinition stage,
            AirflowGateway.CreatedDagRunSnapshot dagRun,
            Map<String, Object> metrics
    ) {
        PipelineStepRun step = stepRepository.findByPipelineRunAndStageCode(run, stage.code())
                .orElseGet(() -> newStep(run, stage, dagRun.runId()));
        PipelineRunStatus previous = step.getStatus();
        step.setDagRunId(dagRun.runId());
        step.setStatus(mapAirflowStatus(dagRun.state()));
        step.setStartedAt(dagRun.startedAt());
        step.setFinishedAt(dagRun.finishedAt());
        applyMetrics(step, metrics);
        stepRepository.save(step);
        appendStepEvent(run, step, previous, now());
    }

    private PipelineStepRun newStep(
            PipelineRun run,
            ImatePipelineDefinition.StageDefinition stage,
            String dagRunId
    ) {
        PipelineStepRun step = new PipelineStepRun();
        step.setPipelineRun(run);
        step.setStageCode(stage.code());
        step.setStepNumber(stage.stepNumber());
        step.setDagId(stage.dagId());
        step.setDagRunId(dagRunId);
        step.setStatus(PipelineRunStatus.QUEUED);
        return step;
    }

    private void applyMetrics(PipelineStepRun step, Map<String, Object> eventExtra) {
        if (eventExtra == null) {
            return;
        }
        Map<String, Object> metrics = map(eventExtra.get("metrics"));
        if (metrics.isEmpty()) {
            metrics = eventExtra;
        }
        step.setRecordCount(number(metrics, "record_count", step.getRecordCount()));
        step.setWarningCount(number(metrics, "warning_count", step.getWarningCount()));
        step.setErrorCount(number(metrics, "error_count", step.getErrorCount()));
        step.setDetails(json(metrics));
    }

    private void aggregate(PipelineRun run) {
        List<PipelineStepRun> steps = stepRepository.findByPipelineRunOrderByStepNumberAscStageCodeAsc(run);
        PipelineRunStatus previous = run.getStatus();
        int completedSteps = completedSteps(steps);

        PipelineRunStatus status;
        if (steps.stream().anyMatch(step -> step.getStatus() == PipelineRunStatus.SCHEMA_BLOCKED)) {
            status = PipelineRunStatus.SCHEMA_BLOCKED;
        } else if (steps.stream().anyMatch(step -> step.getStatus() == PipelineRunStatus.FAILED)) {
            status = PipelineRunStatus.FAILED;
        } else if (steps.stream().anyMatch(step -> isActive(step.getStatus()))) {
            status = PipelineRunStatus.RUNNING;
        } else if (completedSteps == TOTAL_STEPS) {
            status = PipelineRunStatus.SUCCESS;
        } else if (completedSteps > 0) {
            status = PipelineRunStatus.PARTIAL_SUCCESS;
        } else {
            status = previous == null ? PipelineRunStatus.WAITING : previous;
        }

        run.setStatus(status);
        run.setRecordCount(steps.stream().mapToLong(PipelineStepRun::getRecordCount).max().orElse(0));
        run.setLastSynchronizedAt(now());
        if (isTerminal(status)) {
            run.setFinishedAt(steps.stream()
                    .map(PipelineStepRun::getFinishedAt)
                    .filter(Objects::nonNull)
                    .max(OffsetDateTime::compareTo)
                    .orElse(run.getFinishedAt()));
        } else {
            run.setFinishedAt(null);
        }
        runRepository.save(run);
        appendRunStateEvent(run, previous);
    }

    private PipelineRunSnapshot snapshot(PipelineRun run) {
        List<PipelineStepRun> steps = stepRepository.findByPipelineRunOrderByStepNumberAscStageCodeAsc(run);
        List<PipelineRunSnapshot.StepSnapshot> stepSnapshots = STAGES.stream()
                .map(stage -> steps.stream()
                        .filter(step -> stage.code().equals(step.getStageCode()))
                        .findFirst()
                        .map(step -> new PipelineRunSnapshot.StepSnapshot(
                                stage.code(),
                                stage.stepNumber(),
                                stage.title(),
                                stage.phaseLabel(),
                                step.getDagId(),
                                step.getDagRunId(),
                                step.getStatus(),
                                step.getStartedAt(),
                                step.getFinishedAt(),
                                step.getRecordCount(),
                                step.getWarningCount(),
                                step.getErrorCount(),
                                step.getDetails()
                        ))
                        .orElseGet(() -> new PipelineRunSnapshot.StepSnapshot(
                                stage.code(),
                                stage.stepNumber(),
                                stage.title(),
                                stage.phaseLabel(),
                                stage.dagId(),
                                "",
                                PipelineRunStatus.WAITING,
                                null,
                                null,
                                0,
                                0,
                                0,
                                null
                        )))
                .toList();
        return new PipelineRunSnapshot(
                run.getId(),
                run.getCorrelationId(),
                run.getRootDagRunId(),
                run.getStartedAt(),
                run.getFinishedAt(),
                run.getStatus(),
                run.getTriggerType(),
                run.getInitiatedBy(),
                run.getScope(),
                completedSteps(steps),
                TOTAL_STEPS,
                run.getRecordCount(),
                stepSnapshots
        );
    }

    private int completedSteps(List<PipelineStepRun> steps) {
        return (int) STAGES.stream()
                .map(ImatePipelineDefinition.StageDefinition::stepNumber)
                .distinct()
                .filter(stepNumber -> STAGES.stream()
                        .filter(stage -> stage.stepNumber() == stepNumber)
                        .allMatch(stage -> steps.stream()
                                .filter(step -> stage.code().equals(step.getStageCode()))
                                .anyMatch(step -> step.getStatus() == PipelineRunStatus.SUCCESS)))
                .count();
    }

    private void appendStepEvent(
            PipelineRun run,
            PipelineStepRun step,
            PipelineRunStatus previous,
            OffsetDateTime occurredAt
    ) {
        if (previous == step.getStatus()) {
            return;
        }
        appendEvent(
                run,
                "step-state:" + run.getCorrelationId() + ":" + step.getStageCode() + ":"
                        + step.getDagRunId() + ":" + step.getStatus(),
                step.getStageCode(),
                PipelineRunEventType.STEP_STATE_CHANGED,
                DEFAULT_ACTOR,
                Map.of("from", previous == null ? "none" : previous.name(), "to", step.getStatus().name()),
                occurredAt == null ? now() : occurredAt
        );
    }

    private void appendRunStateEvent(PipelineRun run, PipelineRunStatus previous) {
        if (previous == run.getStatus()) {
            return;
        }
        OffsetDateTime occurredAt = now();
        appendEvent(
                run,
                "run-state:" + run.getCorrelationId() + ":"
                        + (previous == null ? "none" : previous) + ":" + run.getStatus() + ":" + occurredAt,
                null,
                PipelineRunEventType.RUN_STATE_CHANGED,
                DEFAULT_ACTOR,
                Map.of("from", previous == null ? "none" : previous.name(), "to", run.getStatus().name()),
                occurredAt
        );
    }

    private void appendEvent(
            PipelineRun run,
            String idempotencyKey,
            String stageCode,
            PipelineRunEventType type,
            String actor,
            Map<String, Object> payload,
            OffsetDateTime occurredAt
    ) {
        if (eventRepository.existsByIdempotencyKey(idempotencyKey)) {
            return;
        }
        PipelineRunEvent event = new PipelineRunEvent();
        event.setPipelineRun(run);
        event.setIdempotencyKey(idempotencyKey);
        event.setStageCode(stageCode);
        event.setEventType(type);
        event.setActor(actor);
        event.setOccurredAt(occurredAt);
        event.setPayload(json(payload));
        eventRepository.save(event);
    }

    private PipelineRun requiredRun(String correlationId) {
        return runRepository.findByCorrelationId(correlationId)
                .orElseThrow(() -> new IllegalStateException("Không tìm thấy run ledger " + correlationId));
    }

    private PipelineRunStatus mapAirflowStatus(String state) {
        if (state == null) {
            return PipelineRunStatus.RUNNING;
        }
        return switch (state.toLowerCase()) {
            case "queued", "scheduled" -> PipelineRunStatus.QUEUED;
            case "success" -> PipelineRunStatus.SUCCESS;
            case "failed", "upstream_failed" -> PipelineRunStatus.FAILED;
            case "stopped" -> PipelineRunStatus.STOPPED;
            default -> PipelineRunStatus.RUNNING;
        };
    }

    private String actor(AirflowGateway.DagRunSnapshot run) {
        return value(run.configuration(), "initiated_by",
                run.actor() == null || run.actor().isBlank() ? DEFAULT_ACTOR : run.actor());
    }

    private boolean isActive(PipelineRunStatus status) {
        return status == PipelineRunStatus.WAITING
                || status == PipelineRunStatus.QUEUED
                || status == PipelineRunStatus.RUNNING;
    }

    private boolean isTerminal(PipelineRunStatus status) {
        return status == PipelineRunStatus.SUCCESS
                || status == PipelineRunStatus.PARTIAL_SUCCESS
                || status == PipelineRunStatus.FAILED
                || status == PipelineRunStatus.STOPPED
                || status == PipelineRunStatus.SCHEMA_BLOCKED;
    }

    private String value(Map<String, Object> values, String key, String fallback) {
        if (values == null || values.get(key) == null) {
            return fallback;
        }
        String result = values.get(key).toString();
        return result.isBlank() ? fallback : result;
    }

    private long number(Map<String, Object> values, String key, long fallback) {
        Object raw = values.get(key);
        if (raw instanceof Number number) {
            return number.longValue();
        }
        if (raw != null) {
            try {
                return Long.parseLong(raw.toString());
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    private Map<String, Object> map(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> raw)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, value) -> {
            if (key != null) {
                result.put(key.toString(), value);
            }
        });
        return result;
    }

    private String json(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Không tuần tự hóa được audit payload", exception);
        }
    }

    private OffsetDateTime now() {
        return OffsetDateTime.ofInstant(clock.instant(), ZoneOffset.UTC);
    }
}
