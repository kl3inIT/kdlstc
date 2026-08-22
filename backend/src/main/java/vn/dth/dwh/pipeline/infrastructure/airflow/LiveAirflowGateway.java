package vn.dth.dwh.pipeline.infrastructure.airflow;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import vn.dth.dwh.pipeline.application.AirflowGateway;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Component
@ConditionalOnProperty(name = "app.integration.airflow.mode", havingValue = "live")
@SuppressWarnings("unchecked")
public class LiveAirflowGateway implements AirflowGateway {

    private final RestClient restClient;
    private final AirflowTokenProvider tokenProvider;
    private final AirflowProperties properties;

    public LiveAirflowGateway(
            @Qualifier("airflowRestClient") RestClient restClient,
            AirflowTokenProvider tokenProvider,
            AirflowProperties properties
    ) {
        this.restClient = restClient;
        this.tokenProvider = tokenProvider;
        this.properties = properties;
    }

    @Override
    public List<DagRunSnapshot> listRootRuns(int limit) {
        Map<String, Object> response = authorized(token -> restClient.get()
                .uri(builder -> builder
                        .path("/api/v2/dags/{dagId}/dagRuns")
                        .queryParam("limit", limit)
                        .queryParam("order_by", "-start_date")
                        .build(properties.rootDagId()))
                .headers(headers -> headers.setBearerAuth(token))
                .retrieve()
                .body(Map.class));
        return maps(response, "dag_runs").stream()
                .map(run -> dagRun(properties.rootDagId(), run))
                .toList();
    }

    @Override
    public List<AssetEventSnapshot> listAssetEvents(int limit) {
        Map<String, Object> response = authorized(token -> restClient.get()
                .uri(builder -> builder
                        .path("/api/v2/assets/events")
                        .queryParam("limit", limit)
                        .queryParam("order_by", "-timestamp")
                        .build())
                .headers(headers -> headers.setBearerAuth(token))
                .retrieve()
                .body(Map.class));
        return maps(response, "asset_events").stream().map(this::assetEvent).toList();
    }

    @Override
    public DagRunSnapshot triggerRootRun(TriggerCommand command) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("dag_run_id", command.dagRunId());
        body.put("logical_date", null);
        body.put("conf", command.configuration());
        body.put("note", command.note());
        Map<String, Object> response = authorized(token -> restClient.post()
                .uri("/api/v2/dags/{dagId}/dagRuns", properties.rootDagId())
                .headers(headers -> headers.setBearerAuth(token))
                .body(body)
                .retrieve()
                .body(Map.class));
        return dagRun(properties.rootDagId(), response == null ? Map.of() : response);
    }

    private DagRunSnapshot dagRun(String dagId, Map<String, Object> values) {
        return new DagRunSnapshot(
                dagId,
                string(values, "dag_run_id"),
                dateTime(values.get("start_date")),
                dateTime(values.get("end_date")),
                string(values, "state"),
                nullableString(values.get("triggering_user_name")),
                map(values.get("conf"))
        );
    }

    private AssetEventSnapshot assetEvent(Map<String, Object> values) {
        List<CreatedDagRunSnapshot> createdRuns = maps(values, "created_dagruns").stream()
                .map(created -> new CreatedDagRunSnapshot(
                        string(created, "dag_id"),
                        string(created, "run_id"),
                        dateTime(created.get("start_date")),
                        dateTime(created.get("end_date")),
                        string(created, "state")
                ))
                .toList();
        return new AssetEventSnapshot(
                nullableString(values.get("uri")),
                map(values.get("extra")),
                nullableString(values.get("source_dag_id")),
                nullableString(values.get("source_run_id")),
                createdRuns,
                dateTime(values.get("timestamp"))
        );
    }

    private <T> T authorized(Function<String, T> request) {
        try {
            return request.apply(tokenProvider.token());
        } catch (RestClientResponseException exception) {
            if (exception.getStatusCode() == HttpStatus.UNAUTHORIZED) {
                tokenProvider.invalidate();
                try {
                    return request.apply(tokenProvider.token());
                } catch (RestClientException retryFailure) {
                    throw new AirflowClientException("Airflow từ chối request sau khi cấp lại token", retryFailure);
                }
            }
            throw new AirflowClientException(
                    "Airflow trả HTTP " + exception.getStatusCode().value(), exception);
        } catch (RestClientException exception) {
            throw new AirflowClientException("Không gọi được Airflow", exception);
        }
    }

    private List<Map<String, Object>> maps(Map<String, Object> parent, String key) {
        if (parent == null || !(parent.get(key) instanceof List<?> values)) {
            return List.of();
        }
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof Map<?, ?> raw) {
                result.add(map(raw));
            }
        }
        return List.copyOf(result);
    }

    private Map<String, Object> map(Object value) {
        if (!(value instanceof Map<?, ?> raw)) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, entryValue) -> {
            if (key != null) {
                result.put(key.toString(), entryValue);
            }
        });
        return Collections.unmodifiableMap(result);
    }

    private String string(Map<String, Object> values, String key) {
        String result = nullableString(values.get(key));
        if (result == null || result.isBlank()) {
            throw new AirflowClientException("Airflow response thiếu " + key);
        }
        return result;
    }

    private String nullableString(Object value) {
        return value == null ? null : value.toString();
    }

    private OffsetDateTime dateTime(Object value) {
        return value == null ? null : OffsetDateTime.parse(value.toString());
    }
}
