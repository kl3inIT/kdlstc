package vn.dth.dwh.pipeline.infrastructure.airflow;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;

@ConfigurationProperties("app.integration.airflow")
public record AirflowProperties(
        String mode,
        URI baseUrl,
        String username,
        String password,
        String rootDagId,
        Duration connectTimeout,
        Duration readTimeout,
        Duration tokenTtl,
        Duration reconcileDelay
) {
}
