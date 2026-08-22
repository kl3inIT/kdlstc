package vn.dth.dwh.pipeline.api;

import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import vn.dth.dwh.pipeline.application.PipelineOperationsService;
import vn.dth.dwh.shared.api.ApiDataResponse;

import java.util.List;
import java.util.UUID;

import static vn.dth.dwh.identity.security.OperationsPermissions.PIPELINE_READ;
import static vn.dth.dwh.identity.security.OperationsPermissions.PIPELINE_TRIGGER;

@RestController
@RequestMapping("/api/pipelines/imate/runs")
public class PipelineRunController {

    private final PipelineOperationsService operationsService;

    public PipelineRunController(PipelineOperationsService operationsService) {
        this.operationsService = operationsService;
    }

    @GetMapping
    @PreAuthorize("hasAuthority('" + PIPELINE_READ + "')")
    List<PipelineRunResponse> recentRuns(@RequestParam(defaultValue = "20") int limit) {
        return operationsService.recentRuns(limit).stream().map(PipelineRunResponse::from).toList();
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAuthority('" + PIPELINE_READ + "')")
    PipelineRunResponse run(@PathVariable UUID id) {
        return PipelineRunResponse.from(operationsService.findRun(id));
    }

    @GetMapping("/{id}/events")
    @PreAuthorize("hasAuthority('" + PIPELINE_READ + "')")
    List<PipelineRunEventResponse> events(@PathVariable UUID id) {
        return operationsService.events(id).stream().map(PipelineRunEventResponse::from).toList();
    }

    @PostMapping
    @PreAuthorize("hasAuthority('" + PIPELINE_TRIGGER + "')")
    ResponseEntity<ApiDataResponse<PipelineRunResponse>> trigger(
            @Valid @RequestBody TriggerPipelineRunRequest request,
            Authentication authentication
    ) {
        PipelineRunResponse response = PipelineRunResponse.from(
                operationsService.trigger(request.scope().trim(), authentication.getName()));
        return ResponseEntity.accepted().body(new ApiDataResponse<>(response));
    }
}
