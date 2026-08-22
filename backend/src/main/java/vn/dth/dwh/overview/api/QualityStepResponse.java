package vn.dth.dwh.overview.api;

public record QualityStepResponse(
        int id,
        String code,
        String title,
        String description,
        String status,
        String statusLabel,
        String metric,
        String metricLabel,
        String actionLabel,
        String lastUpdated
) {
}
