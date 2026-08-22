import { type HttpError, useCustomMutation, useList, useOne } from "@refinedev/core"
import { useForm } from "@tanstack/react-form"
import {
  CheckCircle2,
  ChevronRight,
  Circle,
  Clock3,
  LoaderCircle,
  RefreshCw,
  TriangleAlert,
  Workflow,
  XCircle,
} from "lucide-react"
import { useMemo, useState } from "react"
import { z } from "zod"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import type {
  PipelineRun,
  PipelineStepRun,
  PlatformOverview,
  RunStatus,
} from "@/lib/contracts"
import { fixtureMode } from "@/providers/data-provider"

const runLabel: Record<RunStatus, string> = {
  waiting: "Chờ gửi",
  queued: "Đang xếp hàng",
  running: "Đang chạy",
  success: "Hoàn thành",
  partial_success: "Hoàn thành một phần",
  failed: "Thất bại",
  stopped: "Đã dừng",
  schema_blocked: "Bị chặn do schema",
}

const runVariant: Record<RunStatus, "default" | "secondary" | "destructive" | "outline"> = {
  waiting: "outline",
  queued: "secondary",
  running: "secondary",
  success: "default",
  partial_success: "outline",
  failed: "destructive",
  stopped: "outline",
  schema_blocked: "destructive",
}

const triggerLabel: Record<PipelineRun["triggerType"], string> = {
  manual: "Thủ công",
  scheduled: "Theo lịch",
  asset: "Theo dữ liệu",
  replay: "Phát lại",
}

const statusPriority: Record<RunStatus, number> = {
  schema_blocked: 8,
  failed: 7,
  stopped: 6,
  running: 5,
  queued: 4,
  partial_success: 3,
  waiting: 2,
  success: 1,
}
const triggerRunSchema = z.object({
  scope: z.literal("iMate · toàn bộ chuỗi"),
})


function Metric({ value, label }: { value: string; label: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{label}</CardDescription>
        <CardTitle className="text-2xl tabular-nums">{value}</CardTitle>
      </CardHeader>
    </Card>
  )
}

function formatDateTime(value?: string) {
  if (!value) return "—"
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return value
  return new Intl.DateTimeFormat("vi-VN", {
    dateStyle: "short",
    timeStyle: "short",
    hour12: false,
  }).format(parsed)
}

function StatusIcon({ status }: { status: RunStatus }) {
  const className = "size-5 shrink-0"
  if (status === "success") return <CheckCircle2 className={`${className} text-emerald-600`} />
  if (status === "running") return <LoaderCircle className={`${className} animate-spin text-blue-600`} />
  if (status === "queued" || status === "waiting") return <Clock3 className={`${className} text-slate-500`} />
  if (status === "partial_success" || status === "stopped") {
    return <TriangleAlert className={`${className} text-amber-600`} />
  }
  return <XCircle className={`${className} text-red-600`} />
}

interface StepGroup {
  stepNumber: number
  title: string
  status: RunStatus
  phases: PipelineStepRun[]
}

function groupRunSteps(steps: PipelineStepRun[]): StepGroup[] {
  const groups = new Map<number, PipelineStepRun[]>()
  for (const step of steps) {
    groups.set(step.stepNumber, [...(groups.get(step.stepNumber) ?? []), step])
  }
  return [...groups.entries()]
    .sort(([left], [right]) => left - right)
    .map(([stepNumber, phases]) => ({
      stepNumber,
      title: phases[0].title,
      status: phases.reduce(
        (current, phase) => statusPriority[phase.status] > statusPriority[current] ? phase.status : current,
        "success" as RunStatus,
      ),
      phases,
    }))
}

function RunTimeline({
  run,
  selectedCode,
  onSelect,
}: {
  run: PipelineRun
  selectedCode?: string
  onSelect: (step: PipelineStepRun) => void
}) {
  const groups = groupRunSteps(run.steps)
  if (groups.length === 0) {
    return <p className="text-sm text-muted-foreground">Airflow chưa phát sinh công đoạn cho lượt chạy này.</p>
  }

  return (
    <ol className="space-y-1" aria-label={`Tiến độ ${run.completedSteps} trên ${run.totalSteps} bước`}>
      {groups.map((group, index) => {
        const selected = group.phases.some((phase) => phase.code === selectedCode)
        const primaryPhase = group.phases.find((phase) => phase.code === selectedCode) ?? group.phases[0]
        return (
          <li key={group.stepNumber} className="relative pl-8">
            {index < groups.length - 1 && (
              <span className="absolute left-[0.625rem] top-8 h-[calc(100%-1rem)] w-px bg-border" aria-hidden />
            )}
            <span className="absolute left-0 top-4 z-10 bg-card" aria-hidden>
              <StatusIcon status={group.status} />
            </span>
            <div
              className={`group rounded-lg border transition-colors ${
                selected ? "border-primary bg-muted/70" : "border-transparent hover:border-border hover:bg-muted/40"
              }`}
            >
              <button
                type="button"
                className="flex w-full items-start justify-between gap-4 px-4 pb-2 pt-3 text-left"
                aria-pressed={selected}
                onClick={() => onSelect(primaryPhase)}
              >
                <span className="flex min-w-0 flex-wrap items-center gap-2">
                  <span className="font-medium">B{group.stepNumber} · {group.title}</span>
                  <Badge variant={runVariant[group.status]}>{runLabel[group.status]}</Badge>
                </span>
                <span className="flex shrink-0 items-center gap-3">
                  <span className="text-right">
                    <span className="block font-medium tabular-nums">
                      {Math.max(...group.phases.map((phase) => phase.recordCount)).toLocaleString("vi-VN")}
                    </span>
                    <span className="block text-xs text-muted-foreground">bản ghi</span>
                  </span>
                  <ChevronRight className="size-4 text-muted-foreground transition-transform group-hover:translate-x-0.5" />
                </span>
              </button>
              <div className="flex flex-wrap gap-1.5 px-4 pb-3">
                {group.phases.map((phase) => (
                  <button
                    type="button"
                    key={phase.code}
                    className={`rounded border px-1.5 py-0.5 text-xs ${
                      phase.code === selectedCode
                        ? "border-primary bg-background text-foreground"
                        : "bg-background text-muted-foreground hover:text-foreground"
                    }`}
                    aria-pressed={phase.code === selectedCode}
                    onClick={() => onSelect(phase)}
                  >
                    {phase.code} · {phase.phaseLabel}
                  </button>
                ))}
              </div>
            </div>
          </li>
        )
      })}
    </ol>
  )
}

function PhaseDetail({ phase }: { phase?: PipelineStepRun }) {
  if (!phase) {
    return <p className="text-sm text-muted-foreground">Chọn một bước để xem bằng chứng vận hành.</p>
  }
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Badge variant="outline">{phase.code}</Badge>
        <Badge variant={runVariant[phase.status]}>{runLabel[phase.status]}</Badge>
      </div>
      <div>
        <h3 className="font-semibold">{phase.title}</h3>
        <p className="text-sm text-muted-foreground">{phase.phaseLabel}</p>
      </div>
      <div className="grid grid-cols-3 gap-2">
        <div className="rounded-lg border p-3">
          <p className="text-lg font-semibold tabular-nums">{phase.recordCount.toLocaleString("vi-VN")}</p>
          <p className="text-xs text-muted-foreground">Bản ghi</p>
        </div>
        <div className="rounded-lg border p-3">
          <p className="text-lg font-semibold tabular-nums">{phase.warningCount.toLocaleString("vi-VN")}</p>
          <p className="text-xs text-muted-foreground">Cảnh báo</p>
        </div>
        <div className="rounded-lg border p-3">
          <p className="text-lg font-semibold tabular-nums">{phase.errorCount.toLocaleString("vi-VN")}</p>
          <p className="text-xs text-muted-foreground">Lỗi</p>
        </div>
      </div>
      <dl className="space-y-3 text-sm">
        <div>
          <dt className="text-muted-foreground">DAG</dt>
          <dd className="break-all font-mono text-xs">{phase.dagId}</dd>
        </div>
        <div>
          <dt className="text-muted-foreground">Lượt DAG</dt>
          <dd className="break-all font-mono text-xs">{phase.dagRunId}</dd>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div><dt className="text-muted-foreground">Bắt đầu</dt><dd>{formatDateTime(phase.startedAt)}</dd></div>
          <div><dt className="text-muted-foreground">Kết thúc</dt><dd>{formatDateTime(phase.finishedAt)}</dd></div>
        </div>
      </dl>
      {phase.details && (
        <details className="rounded-lg border bg-muted/30 p-3">
          <summary className="cursor-pointer text-sm font-medium">Metrics từ công đoạn</summary>
          <pre className="mt-3 max-h-40 overflow-auto whitespace-pre-wrap break-words text-xs text-muted-foreground">
            {phase.details}
          </pre>
        </details>
      )}
    </div>
  )
}

function RunsTable({ runs, onSelect }: { runs: PipelineRun[]; onSelect: (run: PipelineRun) => void }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Trạng thái</TableHead>
          <TableHead>Phạm vi</TableHead>
          <TableHead>Bắt đầu</TableHead>
          <TableHead>Tiến độ</TableHead>
          <TableHead>Bản ghi</TableHead>
          <TableHead>Khởi tạo bởi</TableHead>
          <TableHead className="text-right">Chi tiết</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {runs.length === 0 && (
          <TableRow>
            <TableCell colSpan={7} className="py-8 text-center text-muted-foreground">
              Chưa có lượt chạy nào trong sổ cái.
            </TableCell>
          </TableRow>
        )}
        {runs.map((run) => (
          <TableRow key={run.id}>
            <TableCell><Badge variant={runVariant[run.status]}>{runLabel[run.status]}</Badge></TableCell>
            <TableCell>
              <div>{run.scope}</div>
              <code className="text-xs text-muted-foreground">{run.correlationId}</code>
            </TableCell>
            <TableCell>{formatDateTime(run.startedAt)}</TableCell>
            <TableCell className="tabular-nums">{run.completedSteps}/{run.totalSteps}</TableCell>
            <TableCell className="tabular-nums">{run.recordCount.toLocaleString("vi-VN")}</TableCell>
            <TableCell>
              <div>{run.initiatedBy}</div>
              <span className="text-xs text-muted-foreground">{triggerLabel[run.triggerType]}</span>
            </TableCell>
            <TableCell className="text-right">
              <Button size="sm" variant="outline" onClick={() => onSelect(run)}>Mở</Button>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  )
}

export function OverviewPage() {
  const [selectedPhaseCode, setSelectedPhaseCode] = useState("01")
  const [confirming, setConfirming] = useState(false)
  const [triggeredRun, setTriggeredRun] = useState<string>()
  const [detailRun, setDetailRun] = useState<PipelineRun>()
  const [detailPhaseCode, setDetailPhaseCode] = useState("01")

  const { result: overview, query: overviewQuery } = useOne<PlatformOverview>({
    resource: "platform-overview",
    id: "province-warehouse",
  })
  const { result: runsResult, query: runsQuery } = useList<PipelineRun>({
    resource: "pipeline-runs",
    pagination: { mode: "off" },
  })
  const { mutate: triggerRun, mutation } = useCustomMutation<PipelineRun, HttpError, { scope: string }>()

  const runs = runsResult.data ?? []
  const latestRun = runs[0]
  const selectedPhase = useMemo(
    () => latestRun?.steps.find((step) => step.code === selectedPhaseCode) ?? latestRun?.steps[0],
    [latestRun, selectedPhaseCode],
  )
  const detailPhase = detailRun?.steps.find((step) => step.code === detailPhaseCode) ?? detailRun?.steps[0]

  const triggerForm = useForm({
    defaultValues: {
      scope: "iMate · toàn bộ chuỗi" as const,
    },
    validators: {
      onSubmit: triggerRunSchema,
    },
    onSubmit: ({ value }) => new Promise<void>((resolve) => {
      triggerRun(
        { url: "pipelines/imate/runs", method: "post", values: value },
        {
          onSuccess: ({ data }) => {
            setTriggeredRun(data.correlationId)
            setConfirming(false)
            void runsQuery.refetch()
            resolve()
          },
          onError: () => resolve(),
        },
      )
    }),
  })

  function openRun(run: PipelineRun) {
    setDetailRun(run)
    setDetailPhaseCode(run.steps[0]?.code ?? "01")
  }

  if (overviewQuery.isLoading) return <p>Đang tải trạng thái nền tảng…</p>
  if (!overview) return <Alert variant="destructive"><AlertTitle>Không tải được dữ liệu</AlertTitle></Alert>

  return (
    <div className="space-y-6 pb-8">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <p className="text-sm text-muted-foreground">Điều hành cấp Sở / Chuỗi dữ liệu iMate</p>
            {fixtureMode && <Badge variant="secondary">Dữ liệu minh hoạ</Badge>}
          </div>
          <h1 className="text-2xl font-semibold tracking-tight">Tiến độ pipeline 7 bước</h1>
          <p className="text-sm text-muted-foreground">
            Một mã tương quan xuyên suốt từ nguồn đến khai thác; trạng thái được đọc từ run ledger của Spring backend.
          </p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => { void overviewQuery.refetch(); void runsQuery.refetch() }}>
            <RefreshCw /> Làm mới
          </Button>
          <Button onClick={() => setConfirming(true)}><Workflow /> Chạy toàn bộ chuỗi</Button>
        </div>
      </div>

      {triggeredRun && (
        <Alert>
          <AlertTitle>Đã ghi yêu cầu vào sổ cái</AlertTitle>
          <AlertDescription>Mã tương quan: <code>{triggeredRun}</code></AlertDescription>
        </Alert>
      )}

      <section aria-labelledby="overview-metrics">
        <h2 id="overview-metrics" className="sr-only">Chỉ số tổng quan</h2>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <Metric value={`${overview.cleanRate.toLocaleString("vi-VN")}%`} label="Tỷ lệ dữ liệu đạt quy tắc" />
          <Metric value={overview.managedRecords.toLocaleString("vi-VN")} label={`Bản ghi đang quản lý · ${overview.databaseCount} CSDL`} />
          <Metric value={String(overview.activeRuleGroups)} label="Nhóm quy tắc đang gác tự động" />
          <Metric value={overview.pendingIssues.toLocaleString("vi-VN")} label="Vấn đề chờ steward xử lý" />
        </div>
      </section>

      {runsQuery.isError && (
        <Alert variant="destructive">
          <AlertTitle>Không đồng bộ được sổ cái pipeline</AlertTitle>
          <AlertDescription>Lịch sử đã lưu vẫn được giữ; thử làm mới khi Airflow hoạt động trở lại.</AlertDescription>
        </Alert>
      )}

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_24rem]" aria-labelledby="steps-title">
        <Card>
          <CardHeader>
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <CardTitle id="steps-title">Lượt chạy gần nhất</CardTitle>
                <CardDescription>
                  {latestRun
                    ? `${formatDateTime(latestRun.startedAt)} · ${latestRun.completedSteps}/${latestRun.totalSteps} bước`
                    : "Chưa có dữ liệu trong sổ cái"}
                </CardDescription>
              </div>
              {latestRun && <Badge variant={runVariant[latestRun.status]}>{runLabel[latestRun.status]}</Badge>}
            </div>
          </CardHeader>
          <CardContent>
            {latestRun
              ? <RunTimeline run={latestRun} selectedCode={selectedPhase?.code} onSelect={(step) => setSelectedPhaseCode(step.code)} />
              : <p className="text-sm text-muted-foreground">Chạy pipeline để bắt đầu theo dõi bảy bước.</p>}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Bằng chứng công đoạn</CardTitle>
            <CardDescription>Metrics và định danh Airflow chỉ mở khi cần điều tra.</CardDescription>
          </CardHeader>
          <CardContent><PhaseDetail phase={selectedPhase} /></CardContent>
        </Card>
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Lịch sử lượt chạy</CardTitle>
          <CardDescription>
            Spring backend sở hữu lịch sử và ghép các DAG bằng correlation ID; trình duyệt không gọi trực tiếp Airflow.
          </CardDescription>
        </CardHeader>
        <CardContent><RunsTable runs={runs} onSelect={openRun} /></CardContent>
      </Card>

      <Dialog open={confirming} onOpenChange={setConfirming}>
        <DialogContent>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault()
              event.stopPropagation()
              void triggerForm.handleSubmit()
            }}
          >
            <DialogHeader>
              <DialogTitle>Chạy toàn bộ chuỗi iMate?</DialogTitle>
              <DialogDescription>
                Backend sẽ ghi ledger trước, sau đó kích hoạt DAG Nguồn. Các bước còn lại thức dậy qua Airflow Asset.
              </DialogDescription>
            </DialogHeader>
            <triggerForm.Field name="scope">
              {(field) => <input type="hidden" name={field.name} value={field.state.value} />}
            </triggerForm.Field>
            <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
              <dt className="text-muted-foreground">Phạm vi</dt><dd>Toàn bộ chuỗi · iMate</dd>
              <dt className="text-muted-foreground">Chế độ</dt><dd>Tăng dần, có thể phát lại</dd>
              <dt className="text-muted-foreground">Audit</dt><dd>Ghi người yêu cầu và mã tương quan</dd>
            </dl>
            <DialogFooter>
              <DialogClose asChild><Button type="button" variant="outline">Hủy</Button></DialogClose>
              <triggerForm.Subscribe selector={(state) => [state.canSubmit, state.isSubmitting]}>
                {([canSubmit, isSubmitting]) => (
                  <Button type="submit" disabled={!canSubmit || isSubmitting || mutation.isPending}>
                    {isSubmitting || mutation.isPending ? "Đang gửi…" : "Xác nhận chạy"}
                  </Button>
                )}
              </triggerForm.Subscribe>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(detailRun)} onOpenChange={(open) => { if (!open) setDetailRun(undefined) }}>
        <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-5xl">
          {detailRun && (
            <>
              <DialogHeader>
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={runVariant[detailRun.status]}>{runLabel[detailRun.status]}</Badge>
                  <Badge variant="outline">{triggerLabel[detailRun.triggerType]}</Badge>
                </div>
                <DialogTitle>Chi tiết lượt chạy</DialogTitle>
                <DialogDescription className="break-all font-mono text-xs">
                  {detailRun.correlationId}
                </DialogDescription>
              </DialogHeader>
              <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_20rem]">
                <RunTimeline
                  run={detailRun}
                  selectedCode={detailPhase?.code}
                  onSelect={(step) => setDetailPhaseCode(step.code)}
                />
                <div className="rounded-lg border p-4"><PhaseDetail phase={detailPhase} /></div>
              </div>
            </>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
