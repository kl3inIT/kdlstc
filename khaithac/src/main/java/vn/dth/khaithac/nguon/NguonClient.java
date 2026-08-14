package vn.dth.khaithac.nguon;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Gọi API viên gạch của các app Jmix, relay token user.
 * Hợp đồng 403: chỉ tin 403 KÈM body marker {error:"no_permission"};
 * 403 không marker, 5xx, timeout... đều là lỗi thật.
 */
@Component
public class NguonClient {

    private final Map<String, String> bases;
    private final RestClient http = RestClient.create();
    private final ObjectMapper mapper = new ObjectMapper();

    public NguonClient(@Value("${khaithac.nguon.thu}") String thuBase,
                       @Value("${khaithac.nguon.chi}") String chiBase) {
        this.bases = Map.of("THU", thuBase, "CHI", chiBase);
    }

    public KetQuaNguon get(String nguon, String path, String token) {
        String base = bases.get(nguon);
        if (base == null) {
            throw new NguonLoiException(nguon, "khong biet nguon nay", null);
        }
        try {
            String body = http.get()
                    .uri(base + path)
                    .header("Authorization", "Bearer " + token)
                    .retrieve()
                    .body(String.class);
            return KetQuaNguon.ok(parseRows(body));
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().equals(HttpStatusCode.valueOf(403)) && isNoPermissionMarker(e)) {
                return KetQuaNguon.thieuQuyen();
            }
            throw new NguonLoiException(nguon, "HTTP " + e.getStatusCode().value(), e);
        } catch (NguonLoiException e) {
            throw e;
        } catch (Exception e) {
            throw new NguonLoiException(nguon, e.getMessage(), e);
        }
    }

    private boolean isNoPermissionMarker(RestClientResponseException e) {
        try {
            JsonNode node = mapper.readTree(e.getResponseBodyAsString());
            return "no_permission".equals(node.path("error").asText());
        } catch (Exception ignored) {
            return false;
        }
    }

    private List<Map<String, Object>> parseRows(String body) throws Exception {
        JsonNode root = mapper.readTree(body);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (JsonNode r : root.path("rows")) {
            Map<String, Object> m = new LinkedHashMap<>();
            r.properties().forEach(en -> {
                JsonNode v = en.getValue();
                if (v.isNumber()) {
                    m.put(en.getKey(), v.asDouble());
                } else if (v.isNull()) {
                    m.put(en.getKey(), null);
                } else {
                    m.put(en.getKey(), v.asText());
                }
            });
            rows.add(m);
        }
        return rows;
    }
}
