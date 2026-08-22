package vn.dth.dwh.pipeline.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record TriggerPipelineRunRequest(
        @NotBlank(message = "scope không được để trống")
        @Size(max = 255, message = "scope không được dài quá 255 ký tự")
        String scope
) {
}
