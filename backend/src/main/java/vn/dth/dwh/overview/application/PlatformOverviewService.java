package vn.dth.dwh.overview.application;

import org.springframework.stereotype.Service;
import vn.dth.dwh.overview.api.PlatformOverviewResponse;
import vn.dth.dwh.overview.api.QualityStepResponse;

import java.math.BigDecimal;
import java.util.List;

@Service
public class PlatformOverviewService {

    public PlatformOverviewResponse overview() {
        return new PlatformOverviewResponse(
                "province-warehouse",
                "Kho dữ liệu Sở Tài chính",
                "iMate · KPI minh họa; timeline đọc run ledger thật",
                "21/08/2026 · 10:36",
                new BigDecimal("95.47"),
                6141,
                1,
                4,
                278,
                List.of(
                        step(1, "B1", "Nguồn", "Khám phá thay đổi từ nguồn.", "healthy", "Ổn định"),
                        step(2, "B2", "Bronze", "Lưu bằng chứng nguyên bản.", "healthy", "Ổn định"),
                        step(3, "B3", "Silver-1", "Trải phẳng nguyên văn.", "healthy", "Ổn định"),
                        step(4, "B4", "Silver-2", "Ép kiểu, danh mục và cổng chất lượng.", "warning", "Cần chú ý"),
                        step(5, "B5", "Gold", "Dựng mô hình phân tích.", "healthy", "Ổn định"),
                        step(6, "B6", "Serving", "Kiểm tra hợp đồng phục vụ.", "healthy", "Ổn định"),
                        step(7, "B7", "Khai thác", "Xuất báo cáo và API.", "healthy", "Ổn định")
                )
        );
    }

    private QualityStepResponse step(
            int id,
            String code,
            String title,
            String description,
            String status,
            String statusLabel
    ) {
        return new QualityStepResponse(
                id,
                code,
                title,
                description,
                status,
                statusLabel,
                id == 4 ? "95,47%" : "6.141",
                id == 4 ? "đạt quy tắc" : "bản ghi",
                "Xem bằng chứng",
                "10:36"
        );
    }
}
