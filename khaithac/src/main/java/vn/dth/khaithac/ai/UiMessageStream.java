package vn.dth.khaithac.ai;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Mã hóa luồng token thành AI SDK UI Message Stream v1 (SSE) — rút gọn từ
 * mẫu OrgMemory apps/api/assistant/UiMessageStream.java (bỏ citations,
 * heartbeat, timeout — thêm sau khi cần).
 * Khung: start → start-step → text-start → text-delta* → text-end
 *        → finish-step → finish → [DONE]
 */
final class UiMessageStream {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TEXT_ID = "answer";

    private UiMessageStream() {
    }

    static Flux<ServerSentEvent<String>> encode(Flux<String> tokens) {
        String messageId = UUID.randomUUID().toString();
        Flux<ServerSentEvent<String>> deltas = tokens
                .map(t -> event(fields("type", "text-delta", "id", TEXT_ID, "delta", t)));
        return Flux.concat(
                        Flux.just(
                                event(fields("type", "start", "messageId", messageId)),
                                event(fields("type", "start-step")),
                                event(fields("type", "text-start", "id", TEXT_ID))),
                        deltas,
                        Flux.just(
                                event(fields("type", "text-end", "id", TEXT_ID)),
                                event(fields("type", "finish-step")),
                                event(fields("type", "finish", "finishReason", "stop")),
                                done()))
                .onErrorResume(e -> Flux.just(
                        event(fields("type", "error", "errorText",
                                "Luong tra loi gap loi — thu lai.")),
                        done()));
    }

    private static ServerSentEvent<String> event(Map<String, Object> payload) {
        return ServerSentEvent.builder(JSON.writeValueAsString(payload)).build();
    }

    private static ServerSentEvent<String> done() {
        return ServerSentEvent.builder("[DONE]").build();
    }

    private static Map<String, Object> fields(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }
}
