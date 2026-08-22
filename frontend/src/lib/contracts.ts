export type HealthStatus = "healthy" | "running" | "warning" | "pending"
export type RunStatus =
  | "waiting"
  | "queued"
  | "running"
  | "success"
  | "partial_success"
  | "failed"
  | "stopped"
  | "schema_blocked"

export interface QualityStep {
  id: number
  code: string
  title: string
  description: string
  status: HealthStatus
  statusLabel: string
  metric: string
  metricLabel: string
  actionLabel: string
  lastUpdated: string
}

export interface PlatformOverview {
  id: string
  province: string
  source: string
  updatedAt: string
  cleanRate: number
  managedRecords: number
  databaseCount: number
  activeRuleGroups: number
  pendingIssues: number
  steps: QualityStep[]
}

export interface PipelineStepRun {
  code: string
  stepNumber: number
  title: string
  phaseLabel: string
  dagId: string
  dagRunId: string
  status: RunStatus
  startedAt?: string
  finishedAt?: string
  recordCount: number
  warningCount: number
  errorCount: number
  details?: string
}

export interface PipelineRun {
  id: string
  correlationId: string
  airflowRunId: string
  startedAt: string
  finishedAt?: string
  status: RunStatus
  triggerType: "manual" | "scheduled" | "asset" | "replay"
  initiatedBy: string
  scope: string
  completedSteps: number
  totalSteps: number
  recordCount: number
  steps: PipelineStepRun[]
}
