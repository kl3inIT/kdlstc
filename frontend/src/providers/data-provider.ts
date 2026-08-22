import type {
  BaseRecord,
  CustomParams,
  CustomResponse,
  DataProvider,
  GetListParams,
  GetListResponse,
  GetOneParams,
  GetOneResponse,
} from "@refinedev/core"

import { overviewFixture, pipelineRunsFixture } from "@/lib/fixtures"
import { getCsrfToken } from "@/lib/csrf"

const apiUrl = import.meta.env.VITE_API_URL ?? "/api"
const useFixtures = import.meta.env.VITE_USE_FIXTURES !== "false"


async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase()
  const csrfToken = ["POST", "PUT", "PATCH", "DELETE"].includes(method)
    ? await getCsrfToken()
    : undefined
  const response = await fetch(`${apiUrl}${path}`, {
    credentials: "include",
    ...init,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(csrfToken ? { [csrfToken.headerName]: csrfToken.token } : {}),
      ...init?.headers,
    },
  })

  if (response.status === 401) {
    window.location.assign("/login")
    throw new Error("Phiên đăng nhập đã hết hạn")
  }
  if (!response.ok) throw new Error(`API trả về ${response.status}`)
  return response.json() as Promise<T>
}

export const dataProvider: DataProvider = {
  getApiUrl: () => apiUrl,

  async getOne<TData extends BaseRecord = BaseRecord>({ resource }: GetOneParams): Promise<GetOneResponse<TData>> {
    if (resource !== "platform-overview") throw new Error(`Resource chưa hỗ trợ: ${resource}`)
    const data = useFixtures ? overviewFixture : await request<typeof overviewFixture>("/platform/overview")
    return { data: data as unknown as TData }
  },

  async getList<TData extends BaseRecord = BaseRecord>({ resource }: GetListParams): Promise<GetListResponse<TData>> {
    if (resource !== "pipeline-runs") throw new Error(`Resource chưa hỗ trợ: ${resource}`)
    const data = useFixtures
      ? pipelineRunsFixture
      : await request<typeof pipelineRunsFixture>("/pipelines/imate/runs")
    return { data: data as unknown as TData[], total: data.length }
  },

  async custom<TData extends BaseRecord = BaseRecord, TQuery = unknown, TPayload = unknown>({
    url,
    method,
    payload,
  }: CustomParams<TQuery, TPayload>): Promise<CustomResponse<TData>> {
    if (useFixtures && url === "pipelines/imate/runs" && method === "post") {
      const now = new Date()
      const correlationId = crypto.randomUUID()
      const data = {
        id: crypto.randomUUID(),
        correlationId,
        airflowRunId: `manual__${now.toISOString()}__${correlationId.slice(0, 8)}`,
        startedAt: now.toISOString(),
        status: "running" as const,
        triggerType: "manual" as const,
        initiatedBy: "Nguyễn Minh An",
        scope: String((payload as { scope?: string } | undefined)?.scope ?? "iMate · toàn bộ chuỗi"),
        completedSteps: 0,
        totalSteps: 7,
        recordCount: 0,
        steps: pipelineRunsFixture[0].steps.map((step) => ({
          ...step,
          status: step.stepNumber === 1 ? "running" as const : "waiting" as const,
          startedAt: step.stepNumber === 1 ? now.toISOString() : undefined,
          finishedAt: undefined,
          recordCount: 0,
          warningCount: 0,
          errorCount: 0,
        })),
      }
      pipelineRunsFixture.unshift(data)
      return { data: data as unknown as TData }
    }

    const verb = method.toUpperCase()
    return request<CustomResponse<TData>>(`${url.startsWith("/") ? url : `/${url}`}`, {
      method: verb,
      body: payload ? JSON.stringify(payload) : undefined,
    })
  },

  create: async () => {
    throw new Error("Dùng action nghiệp vụ thay cho CRUD trực tiếp")
  },
  update: async () => {
    throw new Error("Dùng action nghiệp vụ thay cho CRUD trực tiếp")
  },
  deleteOne: async () => {
    throw new Error("Dữ liệu vận hành không được xóa từ frontend")
  },
}

export const fixtureMode = useFixtures
