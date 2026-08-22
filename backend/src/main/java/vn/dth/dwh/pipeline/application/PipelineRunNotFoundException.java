package vn.dth.dwh.pipeline.application;

import java.util.UUID;

public class PipelineRunNotFoundException extends RuntimeException {

    public PipelineRunNotFoundException(UUID id) {
        super("Không tìm thấy lượt chạy pipeline " + id);
    }
}
