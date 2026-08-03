import { useChat } from "@ai-sdk/react"
import { DefaultChatTransport, type UIMessage } from "ai"
import { BotMessageSquare } from "lucide-react"
import { useMemo } from "react"

import {
  Conversation,
  ConversationContent,
  ConversationEmptyState,
  ConversationScrollButton,
} from "@/components/ai-elements/conversation"
import { Message, MessageContent } from "@/components/ai-elements/message"
import { MessageResponse } from "@/components/ai-elements/message-response"
import {
  PromptInput,
  PromptInputBody,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
  PromptInputTools,
} from "@/components/ai-elements/prompt-input"
import { Suggestion, Suggestions } from "@/components/ai-elements/suggestion"
import { Alert, AlertDescription } from "@/components/ui/alert"

/** Ghép các part text của 1 message thành chuỗi. */
function textOf(message: UIMessage): string {
  return (message.parts ?? [])
    .filter((p): p is Extract<(typeof message.parts)[number], { type: "text" }> => p.type === "text")
    .map((p) => p.text)
    .join("")
}

const GOI_Y = [
  "Thu 6 tháng đầu năm theo địa bàn? Đâu cân đối âm?",
  "Đơn vị nào giải ngân chậm hơn tiến độ chuẩn?",
  "Dựng biểu DHTC_CHI_04 nửa đầu năm nay",
]

export function Chat() {
  // Transport theo mẫu OrgMemory chat-transport.ts: gửi {message, history}
  // về backend Spring phát UI Message Stream v1.
  const transport = useMemo(
    () =>
      new DefaultChatTransport({
        api: "/api/chat/stream",
        credentials: "same-origin",
        prepareSendMessagesRequest: ({ messages }) => {
          const latest = messages.at(-1)
          const history = messages.slice(0, -1).map((m) => ({
            role: m.role,
            text: textOf(m),
          }))
          return { body: { message: latest ? textOf(latest) : "", history } }
        },
      }) as DefaultChatTransport<UIMessage>,
    [],
  )

  const { messages, sendMessage, status } = useChat({ transport })
  const busy = status === "submitted" || status === "streaming"

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2">
      <Conversation>
        <ConversationContent className="mx-auto w-full max-w-3xl">
          {messages.length === 0 ? (
            <ConversationEmptyState
              icon={<BotMessageSquare className="size-10" />}
              title="Trợ lý khai thác số liệu ngân sách"
              description="Hỏi về thu / chi / dự toán / cân đối. Thiếu quyền nguồn nào, trợ lý nói rõ thay vì bịa số."
            />
          ) : (
            messages.map((m) => (
              <Message from={m.role} key={m.id}>
                <MessageContent>
                  {m.role === "assistant" ? (
                    <MessageResponse>{textOf(m)}</MessageResponse>
                  ) : (
                    textOf(m)
                  )}
                </MessageContent>
              </Message>
            ))
          )}
          {status === "submitted" && (
            <div className="assistant-thinking-text w-fit text-sm">Đang gọi dữ liệu…</div>
          )}
        </ConversationContent>
        <ConversationScrollButton />
      </Conversation>

      {status === "error" && (
        <Alert variant="destructive" className="mx-auto w-full max-w-3xl">
          <AlertDescription>
            Luồng trả lời gặp lỗi — gửi lại câu hỏi; nếu vẫn lỗi, có thể phiên đã hết hạn, tải lại trang (F5).
          </AlertDescription>
        </Alert>
      )}

      <div className="mx-auto w-full max-w-3xl space-y-2">
        {messages.length === 0 && (
          <Suggestions>
            {GOI_Y.map((s) => (
              <Suggestion key={s} suggestion={s} onClick={(x) => void sendMessage({ text: x })} />
            ))}
          </Suggestions>
        )}
        <PromptInput
          onSubmit={(message, event) => {
            event.preventDefault()
            const text = message.text.trim()
            if (!text || busy) return
            void sendMessage({ text })
          }}
        >
          <PromptInputBody>
            <PromptInputTextarea placeholder="Hỏi về số liệu thu chi ngân sách…" />
          </PromptInputBody>
          <PromptInputFooter>
            <PromptInputTools />
            <PromptInputSubmit status={status} disabled={busy} />
          </PromptInputFooter>
        </PromptInput>
      </div>
    </div>
  )
}
