package vn.dth.dwh.pipeline.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import vn.dth.dwh.pipeline.domain.PipelineRun;
import vn.dth.dwh.pipeline.domain.PipelineStepRun;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PipelineStepRunRepository extends JpaRepository<PipelineStepRun, UUID> {

    Optional<PipelineStepRun> findByPipelineRunAndStageCode(PipelineRun pipelineRun, String stageCode);

    List<PipelineStepRun> findByPipelineRunOrderByStepNumberAscStageCodeAsc(PipelineRun pipelineRun);
}
