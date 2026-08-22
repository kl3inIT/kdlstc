package vn.dth.dwh.pipeline.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import vn.dth.dwh.pipeline.domain.PipelineRun;
import vn.dth.dwh.pipeline.domain.PipelineRunEvent;

import java.util.List;
import java.util.UUID;

public interface PipelineRunEventRepository extends JpaRepository<PipelineRunEvent, UUID> {

    boolean existsByIdempotencyKey(String idempotencyKey);

    List<PipelineRunEvent> findByPipelineRunOrderByOccurredAtAsc(PipelineRun pipelineRun);
}
