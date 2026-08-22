package vn.dth.dwh.pipeline.domain;

import java.util.List;
import java.util.Optional;

public final class ImatePipelineDefinition {

    public static final String PIPELINE_CODE = "imate";
    public static final int TOTAL_STEPS = 7;

    public static final List<StageDefinition> STAGES = List.of(
            new StageDefinition("01", 1, "Nguồn", "Khám phá thay đổi", "imate_01_discover"),
            new StageDefinition("02", 2, "Bronze", "Lưu bằng chứng nguyên bản", "imate_02_land_bronze"),
            new StageDefinition("03", 3, "Silver-1", "Trải phẳng nguyên văn", "imate_03_silver_one"),
            new StageDefinition("04", 4, "Silver-2", "Ép kiểu và tra danh mục", "imate_04_silver_two"),
            new StageDefinition("04b", 4, "Silver-2", "Cổng chất lượng", "imate_04b_quality_gate"),
            new StageDefinition("05", 5, "Gold", "Dựng mô hình phân tích", "imate_05_publish"),
            new StageDefinition("06", 6, "Serving", "Kiểm tra hợp đồng phục vụ", "imate_06_serving"),
            new StageDefinition("07", 7, "Khai thác", "Xuất báo cáo và API", "imate_07_report")
    );

    private ImatePipelineDefinition() {
    }

    public static Optional<StageDefinition> findByDagId(String dagId) {
        return STAGES.stream().filter(stage -> stage.dagId().equals(dagId)).findFirst();
    }

    public static StageDefinition byCode(String code) {
        return STAGES.stream()
                .filter(stage -> stage.code().equals(code))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Công đoạn iMate không tồn tại: " + code));
    }

    public record StageDefinition(String code, int stepNumber, String title, String phaseLabel, String dagId) {
    }
}
