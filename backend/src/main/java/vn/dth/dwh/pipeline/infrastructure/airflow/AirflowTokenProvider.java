package vn.dth.dwh.pipeline.infrastructure.airflow;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.integration.airflow.mode", havingValue = "live")
public class AirflowTokenProvider {

    private final RestClient restClient;
    private final AirflowProperties properties;
    private final Clock clock;
    private volatile String cachedToken;
    private volatile Instant refreshAt = Instant.EPOCH;

    public AirflowTokenProvider(
            @Qualifier("airflowRestClient") RestClient restClient,
            AirflowProperties properties,
            Clock clock
    ) {
        this.restClient = restClient;
        this.properties = properties;
        this.clock = clock;
    }

    public synchronized String token() {
        Instant now = clock.instant();
        if (cachedToken != null && now.isBefore(refreshAt)) {
            return cachedToken;
        }
        if (properties.username() == null || properties.username().isBlank()
                || properties.password() == null || properties.password().isBlank()) {
            throw new AirflowClientException("Thiếu credential Airflow cho control plane");
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restClient.post()
                    .uri("/auth/token")
                    .body(Map.of("username", properties.username(), "password", properties.password()))
                    .retrieve()
                    .body(Map.class);
            Object accessToken = response == null ? null : response.get("access_token");
            if (accessToken == null || accessToken.toString().isBlank()) {
                throw new AirflowClientException("Airflow không trả access_token");
            }
            cachedToken = accessToken.toString();
            refreshAt = now.plus(properties.tokenTtl());
            return cachedToken;
        } catch (RestClientException exception) {
            throw new AirflowClientException("Không lấy được token Airflow", exception);
        }
    }

    public void invalidate() {
        cachedToken = null;
        refreshAt = Instant.EPOCH;
    }
}
