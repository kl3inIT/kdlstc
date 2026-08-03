package vn.dth.khaithac.ai;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import vn.dth.khaithac.security.TokenResolver;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Chat AI:
 *  - POST /api/chat        — đồng bộ (giữ cho test/agent đơn giản).
 *  - POST /api/chat/stream — SSE theo AI SDK UI Message Stream v1
 *    (mượn protocol từ OrgMemory UiMessageStream) cho frontend useChat.
 * Token user resolve NGAY trong request thread rồi bake vào bộ tool —
 * an toàn khi tool chạy trên thread reactor lúc streaming.
 */
@RestController
@RequestMapping("/api")
public class ChatController {

    private final ChatClient chatClient;
    private final AiToolsFactory toolsFactory;
    private final TokenResolver tokenResolver;

    public ChatController(ChatModel chatModel, AiToolsFactory toolsFactory, TokenResolver tokenResolver) {
        this.chatClient = ChatClient.builder(chatModel).build();
        this.toolsFactory = toolsFactory;
        this.tokenResolver = tokenResolver;
    }

    public record ChatRequest(String message, List<Map<String, String>> history) {}

    @PostMapping("/chat")
    public Map<String, String> chat(@RequestBody ChatRequest req, Authentication auth) {
        String answer = spec(req, auth).call().content();
        return Map.of("text", answer == null ? "" : answer);
    }

    @PostMapping(value = "/chat/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> chatStream(@RequestBody ChatRequest req, Authentication auth) {
        ChatClient.ChatClientRequestSpec spec = spec(req, auth);
        return UiMessageStream.encode(Flux.defer(() -> spec.stream().content()));
    }

    private ChatClient.ChatClientRequestSpec spec(ChatRequest req, Authentication auth) {
        String system = """
                Bạn là trợ lý khai thác dữ liệu tài chính của Sở Tài chính (prototype, dữ liệu mô phỏng).
                Hôm nay là %s. Tiền tệ: đồng Việt Nam — trình bày số lớn dạng triệu/tỷ cho dễ đọc.

                QUY TẮC BẮT BUỘC:
                1. Mọi con số phải lấy từ tool. Tuyệt đối không bịa, không ước lượng khi chưa gọi tool.
                2. Nếu tool trả `thieu_quyen`/`nguon_bi_an`/`khai_bao`: phải nói rõ người dùng
                   thiếu quyền nguồn nào, phần nào của câu trả lời bị khuyết vì thế,
                   và KHÔNG suy đoán giá trị bị ẩn từ các số còn lại.
                3. Nếu tool trả `loi`: báo hệ thống nguồn đang lỗi — đó KHÔNG phải thiếu quyền.
                4. Kỳ dữ liệu: nếu người dùng không nói rõ, dùng từ 01-01 năm nay đến hôm nay và nói rõ đã giả định vậy.
                5. Trả lời tiếng Việt, ngắn gọn, có số liệu cụ thể.
                """.formatted(LocalDate.now());

        AiTools tools = toolsFactory.create(tokenResolver.resolve(auth));
        var spec = chatClient.prompt().system(system).tools(tools);
        if (req.history() != null) {
            for (Map<String, String> m : req.history()) {
                if ("user".equals(m.get("role"))) {
                    spec = spec.messages(new UserMessage(m.get("text")));
                } else if ("assistant".equals(m.get("role"))) {
                    spec = spec.messages(new AssistantMessage(m.get("text")));
                }
            }
        }
        return spec.user(req.message());
    }
}
