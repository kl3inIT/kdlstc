package vn.dth.dwh.pipeline.infrastructure.persistence;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import vn.dth.dwh.pipeline.domain.PipelineRun;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PipelineRunRepository extends JpaRepository<PipelineRun, UUID> {

    Optional<PipelineRun> findByCorrelationId(String correlationId);

    List<PipelineRun> findByPipelineCodeOrderByStartedAtDesc(String pipelineCode, Pageable pageable);
}
