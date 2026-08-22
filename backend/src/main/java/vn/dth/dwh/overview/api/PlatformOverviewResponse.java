package vn.dth.dwh.overview.api;

import java.math.BigDecimal;
import java.util.List;

public record PlatformOverviewResponse(
        String id,
        String province,
        String source,
        String updatedAt,
        BigDecimal cleanRate,
        long managedRecords,
        int databaseCount,
        int activeRuleGroups,
        long pendingIssues,
        List<QualityStepResponse> steps
) {
}
