/** 运行健康评估的数据契约：健康指标与缺陷证据同源返回。 */

export type HealthDefect = {
  defect_id: string
  category: 'execution_failure' | 'batch_omission' | 'task_not_closed'
  category_label: string
  execution_no?: string | null
  execution_nos?: string[]
  task_no?: string | null
  batch_no?: string | null
  reason: string
  status: string
  is_late: boolean
  late_policy?: string | null
  metric_keys: string[]
  evidence: Record<string, unknown>
}

export type HealthMetric = {
  key: string
  label: string
  value: number
  baseline: number | null
  delta: number | null
  direction: string
  polarity: 'positive' | 'negative' | 'neutral'
  unit: string
  declined: boolean
  evidence_execution_nos: string[]
  evidence_batch_nos: string[]
  defect_ids: string[]
}

export type BatchConclusion = {
  conclusion_id: string
  batch_no: string
  flow: string
  date: string
  version: number
  state: string
  category: string
  category_label: string
  failed_execution_nos: string[]
  failed_task_nos: string[]
  defect_ids: string[]
  late_defect_ids: string[]
  late_policy: string
  missed_count: number
  supersedes: string | null
}

export type Reconciliation = {
  status: string
  runtime_metric_version: string | null
  arrived_at: string | null
  matches: { field: string; value: number }[]
  mismatches: { field: string; runtime: number; computed: number; delta: number }[]
}

export type HealthTodo = {
  todo_id: string
  defect_id: string
  category: string
  execution_no?: string | null
  batch_no?: string | null
  summary: string
  status: string
  metric_keys: string[]
}

export type HealthReport = {
  report_id: string
  scope_key: string
  scope: { date: string; flow: string }
  version: number
  status: string
  late_policy: string
  generated_at: string
  late_conflict: {
    present: boolean
    batch_nos: string[]
    late_execution_nos: string[]
    late_task_nos: string[]
    options: string[]
    resolved: string | null
  }
  metrics: HealthMetric[]
  metric_values: Record<string, number>
  baseline: Record<string, unknown>
  defects: HealthDefect[]
  batch_conclusions: BatchConclusion[]
  reconciliation: Reconciliation
  todos: HealthTodo[]
  history: { version: number; status: string; late_policy: string; published_at: string }[]
  reused_evidence?: boolean
  evidence: {
    executions: Record<string, string | number>[]
    batches: Record<string, string | number>[]
    tasks: Record<string, string | number>[]
    duplicates: DuplicateRow[]
  }
}

export type DuplicateRow = {
  module: string
  field: string
  key: string
  duplicate_id: number
  kept_id: number
  status?: string
  reason?: string
}

export type HealthListResponse = {
  available_dates: string[]
  available_flows: string[]
  filters: { date: string | null; flow: string | null; status: string | null; policy: string | null }
  reports: HealthReport[]
  duplicates: DuplicateRow[]
}
