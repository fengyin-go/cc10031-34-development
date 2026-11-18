<template>
  <section class="page" data-module="report_dashboard">
    <header class="page-head">
      <div>
        <h2>运行概览</h2>
        <p class="page-desc">
          健康指标与缺陷证据一起发布：每个下降指标都能定位到具体执行编号与采集批次。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="reassess" type="button" :disabled="loading" @click="refresh">
          {{ loading ? '刷新中…' : '刷新概览' }}
        </button>
      </div>
    </header>

    <form class="analysis-form" @submit.prevent="reload">
      <label>
        <span>统计日期</span>
        <select v-model="filters.date" data-testid="filter-date" @change="reload">
          <option v-for="item in availableDates" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label>
        <span>流程</span>
        <select v-model="filters.flow" data-testid="filter-flow" @change="reload">
          <option value="">全部流程</option>
          <option v-for="item in flowsForDate" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label>
        <span>健康状态</span>
        <select v-model="filters.status" data-testid="filter-status" @change="reload">
          <option value="">全部状态</option>
          <option v-for="item in statuses" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label>
        <span>迟到完成事件</span>
        <select v-model="filters.policy" data-testid="filter-policy" @change="reload">
          <option value="">按已发布结论</option>
          <option value="retain">保留原指标 + 追加迟到缺陷</option>
          <option value="reopen">重开批次报告 + 替换指标</option>
        </select>
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="health-error">{{ errorMessage }}</p>

    <div class="stat-row">
      <article v-for="card in summaryCards" :key="card.label" class="analysis-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
        <span class="stat-hint">{{ card.hint }}</span>
      </article>
    </div>

    <section v-for="report in reports" :key="report.scope_key" class="health-report" :data-testid="`report-${report.scope.flow}`">
      <header class="report-head">
        <h3>{{ report.scope.date }} · {{ report.scope.flow }}</h3>
        <span class="badge" :class="`badge-${report.status}`" :data-testid="`status-${report.scope.flow}`">
          {{ report.status }}
        </span>
        <span class="report-meta">报告版本 v{{ report.version }} · 迟到策略 {{ policyLabel(report.late_policy) }}</span>
        <span v-if="report.reused_evidence" class="report-meta">沿用同一份证据链</span>
      </header>

      <table class="data-table metric-table">
        <thead>
          <tr>
            <th>健康指标</th>
            <th>当前值</th>
            <th>基线</th>
            <th>变化</th>
            <th>关联执行编号</th>
            <th>关联批次编号</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="metric in report.metrics"
            :key="metric.key"
            :class="{ 'metric-declined': metric.declined }"
            :data-testid="`metric-${report.scope.flow}-${metric.key}`"
          >
            <td>
              <button
                v-if="metric.declined"
                class="link metric-link"
                type="button"
                :data-testid="`drill-${report.scope.flow}-${metric.key}`"
                @click="drillDown(report, metric)"
              >
                {{ metric.label }} {{ metric.direction === '上升' ? '↑' : '↓' }}
              </button>
              <span v-else>{{ metric.label }}</span>
            </td>
            <td>{{ formatMetric(metric) }}</td>
            <td>{{ metric.baseline === null ? '—' : metric.baseline }}</td>
            <td :class="trendClass(metric)">{{ deltaText(metric) }}</td>
            <td class="refs">
              <button
                v-for="no in metric.evidence_execution_nos"
                :key="no"
                class="link"
                type="button"
                :data-testid="`metric-exec-${no}`"
                @click="openExecution(no)"
              >{{ no }}</button>
              <span v-if="!metric.evidence_execution_nos.length">—</span>
            </td>
            <td class="refs">
              <button
                v-for="no in metric.evidence_batch_nos"
                :key="no"
                class="link"
                type="button"
                :data-testid="`metric-batch-${no}`"
                @click="focusBatch(report, no)"
              >{{ no }}</button>
              <span v-if="!metric.evidence_batch_nos.length">—</span>
            </td>
            <td>
              <button
                v-if="metric.declined"
                class="link"
                type="button"
                @click="drillDown(report, metric)"
              >查看证据</button>
              <span v-else>—</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div v-if="report.late_conflict.present" class="late-panel" :data-testid="`late-${report.scope.flow}`">
        <strong>迟到失败完成事件：</strong>
        <span>{{ report.late_conflict.late_execution_nos.join('、') || '—' }}</span>
        <span>落在批次 {{ report.late_conflict.batch_nos.join('、') }}</span>
        <span>当前做法：{{ policyLabel(report.late_policy) }}</span>
        <button class="link" type="button" @click="switchPolicy(report, 'retain')">保留原指标</button>
        <button class="link" type="button" @click="switchPolicy(report, 'reopen')">重开批次报告</button>
      </div>

      <div class="health-columns">
        <div>
          <h4>缺陷归因（每个缺陷对应一个当前批次结论）</h4>
          <table class="data-table">
            <thead>
              <tr><th>缺陷</th><th>归类</th><th>执行编号</th><th>批次编号</th><th>任务编号</th><th>说明</th><th>状态</th></tr>
            </thead>
            <tbody>
              <tr
                v-for="defect in report.defects"
                :key="defect.defect_id"
                :data-testid="`defect-${defect.defect_id}`"
                :data-category="defect.category"
              >
                <td>{{ defect.defect_id }}</td>
                <td>
                  {{ defect.category_label }}
                  <span v-if="defect.is_late" class="tag-late">迟到</span>
                </td>
                <td>
                  <button class="link" type="button" @click="openExecution(defect.execution_no!)">
                    {{ defect.execution_no || '—' }}
                  </button>
                </td>
                <td>
                  <button class="link" type="button" @click="focusBatch(report, defect.batch_no!)">
                    {{ defect.batch_no || '—' }}
                  </button>
                </td>
                <td>{{ defect.task_no || '—' }}</td>
                <td>{{ defect.reason }}</td>
                <td>{{ defect.status }}</td>
              </tr>
              <tr v-if="!report.defects.length">
                <td colspan="7" class="empty-state">该流程本日没有执行缺陷</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div>
          <h4>批次结论（同一失败只归入一个当前结论）</h4>
          <table class="data-table">
            <thead>
              <tr><th>批次编号</th><th>结论</th><th>归类</th><th>失败执行</th><th>失败任务</th><th>版本</th></tr>
            </thead>
            <tbody>
              <tr
                v-for="conclusion in report.batch_conclusions"
                :key="conclusion.conclusion_id"
                :data-testid="`conclusion-${conclusion.batch_no}`"
              >
                <td>{{ conclusion.batch_no }}</td>
                <td>{{ conclusion.conclusion_id }}</td>
                <td>{{ conclusion.category_label }}</td>
                <td>{{ conclusion.failed_execution_nos.join('、') || '—' }}</td>
                <td>{{ conclusion.failed_task_nos.join('、') || '—' }}</td>
                <td>v{{ conclusion.version }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div class="health-columns">
        <div>
          <h4>新到达运行指标对账</h4>
          <p class="report-meta" data-testid="reconcile-status">
            {{ report.reconciliation.status }}
            <template v-if="report.reconciliation.runtime_metric_version">
              · 指标版本 {{ report.reconciliation.runtime_metric_version }}
              · 到达 {{ report.reconciliation.arrived_at }}
            </template>
          </p>
          <table class="data-table">
            <thead><tr><th>字段</th><th>运行指标</th><th>证据重算</th><th>差异</th></tr></thead>
            <tbody>
              <tr v-for="row in report.reconciliation.mismatches" :key="row.field">
                <td>{{ row.field }}</td>
                <td>{{ row.runtime }}</td>
                <td>{{ row.computed }}</td>
                <td class="trend-down">{{ row.delta }}</td>
              </tr>
              <tr v-if="!report.reconciliation.mismatches.length">
                <td colspan="4" class="empty-state">
                  已核对 {{ report.reconciliation.matches.length }} 个字段，全部一致
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div>
          <h4>待办（由缺陷自动联动关闭）</h4>
          <ul class="todo-list" data-testid="health-todos">
            <li v-for="todo in report.todos" :key="todo.todo_id">
              {{ todo.todo_id }} · {{ todo.summary }}
              <span class="report-meta">({{ todo.execution_no || '—' }} / {{ todo.batch_no || '—' }})</span>
            </li>
            <li v-if="!report.todos.length" class="empty-state">没有待处理缺陷</li>
          </ul>
          <p class="report-meta" data-testid="report-history">
            版本轨迹：
            <span v-for="item in report.history" :key="item.version">
              v{{ item.version }}({{ item.late_policy }}) </span>
          </p>
        </div>
      </div>

      <details
        class="evidence-drawer"
        :open="expanded === report.scope_key"
        :data-testid="`evidence-${report.scope.flow}`"
      >
        <summary>执行日志 / 批次 / 任务证据（{{ report.evidence.executions.length }} 执行 · {{ report.evidence.batches.length }} 批次 · {{ report.evidence.tasks.length }} 任务）</summary>
        <table class="data-table">
          <thead><tr><th>执行编号</th><th>状态</th><th>开始</th><th>结束</th><th>批次编号</th><th>任务编号</th><th>错误摘要</th></tr></thead>
          <tbody>
            <tr
              v-for="row in focusedExecutions(report)"
              :key="String(row['执行编号'])"
              :data-testid="`evidence-exec-${row['执行编号']}`"
            >
              <td>{{ row['执行编号'] }}</td>
              <td>{{ row['status'] }}</td>
              <td>{{ row['开始时间'] }}</td>
              <td>{{ row['结束时间'] || '—' }}</td>
              <td>{{ row['批次编号'] }}</td>
              <td>{{ row['任务编号'] }}</td>
              <td>{{ row['错误摘要'] || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <table class="data-table">
          <thead><tr><th>批次编号</th><th>状态</th><th>目标条数</th><th>成功条数</th><th>遗漏条数</th><th>汇总时间</th></tr></thead>
          <tbody>
            <tr
              v-for="row in focusedBatches(report)"
              :key="String(row['批次编号'])"
              :data-testid="`evidence-batch-${row['批次编号']}`"
            >
              <td>{{ row['批次编号'] }}</td>
              <td>{{ row['status'] }}</td>
              <td>{{ row['目标条数'] }}</td>
              <td>{{ row['成功条数'] }}</td>
              <td>{{ row['遗漏条数'] }}</td>
              <td>{{ row['汇总时间'] }}</td>
            </tr>
          </tbody>
        </table>
        <p class="report-meta">
          重复导入留痕（只认第一条）：
          <span v-for="item in report.evidence.duplicates" :key="`${item.field}-${item.duplicate_id}`">
            {{ item.field }} {{ item.key }}（保留 #{{ item.kept_id }}，忽略 #{{ item.duplicate_id }}）
          </span>
          <span v-if="!report.evidence.duplicates.length">无</span>
        </p>
      </details>
    </section>

    <p v-if="!reports.length && !errorMessage" class="empty-state" data-testid="health-empty">
      当前筛选条件下没有运行健康报告
    </p>

    <p v-if="duplicates.length" class="report-meta" data-testid="duplicate-summary">
      重复导入已按第一条收敛：
      <span v-for="item in duplicates" :key="`${item.module}-${item.key}`">
        {{ item.module }} · {{ item.key }}（保留 #{{ item.kept_id }}）</span>
    </p>

    <div v-if="attribution" class="modal-backdrop" data-testid="attribution-modal">
      <div class="modal">
        <h3>执行归因 · {{ attribution.execution_no }}</h3>
        <p v-if="attribution.current" class="report-meta">
          当前结论 {{ attribution.current.conclusion_id }} · 归类
          {{ attribution.current.conclusion?.category_label }} · 批次
          {{ attribution.current.batch_no || '—' }} · 版本 v{{ attribution.current.version }}
        </p>
        <p v-else class="report-meta">该执行没有当前缺陷归因</p>
        <ol class="evidence-chain">
          <li v-for="(step, index) in attribution.evidence_chain" :key="index">
            <strong>{{ step.type }}</strong> {{ step.ref }}
            <code>{{ JSON.stringify(step.detail) }}</code>
          </li>
        </ol>
        <p class="report-meta" v-if="attribution.history?.length">
          历史归因：
          <span v-for="item in attribution.history" :key="item.conclusion_id">
            {{ item.conclusion_id }}{{ item.is_current ? '(当前)' : '(已被替换)' }}
          </span>
        </p>
        <button class="btn" type="button" @click="attribution = null">关闭</button>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { loadExecutionAttribution, loadHealthReports, reassessHealth } from '@/api/backend-service'
import type { HealthDefect, HealthListResponse, HealthMetric, HealthReport } from '@/data/health'

const statuses = ['正常', '下降', '异常']
const reports = ref<HealthReport[]>([])
const availableDates = ref<string[]>([])
const availableFlows = ref<string[]>([])
const duplicates = ref<HealthListResponse['duplicates']>([])
const filters = ref<{ date: string; flow: string; status: string; policy: string }>({
  date: '',
  flow: '',
  status: '',
  policy: '',
})
const loading = ref(false)
const errorMessage = ref('')
const expanded = ref('')
const focusedBatchNo = ref('')
const attribution = ref<Record<string, any> | null>(null)

const flowsForDate = computed(() => availableFlows.value)

const summaryCards = computed(() => {
  const declining = reports.value.flatMap((report) =>
    report.metrics.filter((metric) => metric.declined))
  const defects = reports.value.flatMap((report) => report.defects)
  const byCategory = (category: HealthDefect['category']) =>
    defects.filter((defect) => defect.category === category).length
  return [
    { label: '健康报告', value: reports.value.length, hint: '按日期 + 流程发布' },
    { label: '下降指标', value: declining.length, hint: '均可下钻到执行与批次' },
    { label: '执行失败', value: byCategory('execution_failure'), hint: '成功率 / 执行失败' },
    { label: '批次遗漏', value: byCategory('batch_omission'), hint: '遗漏批次单独归类' },
    { label: '任务未闭环', value: byCategory('task_not_closed'), hint: '任务状态未闭环' },
    {
      label: '对账状态',
      value: !reports.value.length
        ? '—'
        : reports.value.every((report) => report.reconciliation.status === '一致')
          ? '一致'
          : '有差异',
      hint: '新到达运行指标 vs 证据重算',
    },
  ]
})

function policyLabel(policy: string | null | undefined) {
  if (policy === 'reopen') return '重开批次报告并替换指标'
  if (policy === 'retain') return '保留原指标并追加迟到缺陷'
  return '—'
}

function formatMetric(metric: HealthMetric) {
  if (metric.unit === 'percent') return `${metric.value}%`
  if (metric.unit === 'minutes') return `${metric.value} 分钟`
  return String(metric.value)
}

function deltaText(metric: HealthMetric) {
  if (metric.delta === null) return '—'
  const sign = metric.delta > 0 ? '+' : ''
  return `${sign}${metric.delta}`
}

function trendClass(metric: HealthMetric) {
  if (metric.delta === null) return ''
  if (metric.declined) return 'trend-down'
  return metric.delta > 0 ? 'trend-up' : ''
}

async function reload() {
  loading.value = true
  errorMessage.value = ''
  try {
    const payload = await loadHealthReports({
      date: filters.value.date || undefined,
      flow: filters.value.flow || undefined,
      status: filters.value.status || undefined,
      policy: filters.value.policy || undefined,
    })
    availableDates.value = payload.available_dates
    availableFlows.value = payload.available_flows
    reports.value = payload.reports
    duplicates.value = payload.duplicates
    if (!filters.value.date && payload.available_dates.length) {
      filters.value.date = payload.available_dates[payload.available_dates.length - 1]
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '健康评估读取失败'
  } finally {
    loading.value = false
  }
}

async function refresh() {
  loading.value = true
  errorMessage.value = ''
  try {
    await reassessHealth({
      date: filters.value.date || undefined,
      flow: filters.value.flow || undefined,
      policy: filters.value.policy || undefined,
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '健康评估刷新失败'
    loading.value = false
    return
  }
  await reload()
}

function resetFilters() {
  filters.value = { date: '', flow: '', status: '', policy: '' }
  focusedBatchNo.value = ''
  expanded.value = ''
  void reload()
}

function drillDown(report: HealthReport, metric: HealthMetric) {
  expanded.value = report.scope_key
  focusedBatchNo.value = metric.evidence_batch_nos[0] || ''
}

function focusBatch(report: HealthReport, batchNo: string) {
  if (!batchNo) return
  expanded.value = report.scope_key
  focusedBatchNo.value = batchNo
}

function switchPolicy(report: HealthReport, policy: string) {
  filters.value.policy = policy
  filters.value.date = report.scope.date
  filters.value.flow = report.scope.flow
  focusedBatchNo.value = ''
  void reload()
}

async function openExecution(executionNo: string) {
  errorMessage.value = ''
  try {
    attribution.value = await loadExecutionAttribution(executionNo)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '执行归因读取失败'
  }
}

function focusedExecutions(report: HealthReport) {
  if (!focusedBatchNo.value) return report.evidence.executions
  return report.evidence.executions.filter(
    (row) => String(row['批次编号']) === focusedBatchNo.value,
  )
}

function focusedBatches(report: HealthReport) {
  if (!focusedBatchNo.value) return report.evidence.batches
  return report.evidence.batches.filter(
    (row) => String(row['批次编号']) === focusedBatchNo.value,
  )
}

onMounted(reload)
</script>

<style scoped>
.health-report {
  margin-top: 18px;
  padding: 14px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 10px;
}
.report-head { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 10px; }
.report-head h3 { margin: 0; font-size: 15px; }
.report-meta { color: var(--muted); font-size: 12px; margin: 4px 0; }
.badge { padding: 2px 8px; border-radius: 10px; font-size: 12px; }
.badge-正常 { background: #dcfce7; color: #15803d; }
.badge-下降 { background: #fef3c7; color: #b45309; }
.badge-异常 { background: #fee2e2; color: #b42318; }
.metric-table { margin-bottom: 12px; }
.metric-declined td { background: #fff7ed; }
.metric-link { font-weight: 600; }
.refs { display: flex; flex-direction: column; gap: 2px; }
.trend-down { color: #b42318; }
.trend-up { color: #15803d; }
.health-columns { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-bottom: 12px; }
.health-columns h4 { margin: 0 0 6px; font-size: 13px; }
.late-panel {
  display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
  padding: 8px 10px; margin-bottom: 12px;
  border: 1px dashed #f59e0b; border-radius: 8px; background: #fffbeb; font-size: 12px;
}
.tag-late { font-size: 11px; color: #b45309; background: #fef3c7; border-radius: 4px; padding: 0 4px; }
.todo-list { list-style: none; margin: 0 0 8px; padding: 0; }
.todo-list li { border-bottom: 1px solid #f0f2f5; padding: 4px 0; font-size: 12px; }
.evidence-drawer { border-top: 1px solid var(--border); padding-top: 8px; }
.evidence-drawer summary { cursor: pointer; font-size: 13px; color: var(--brand); margin-bottom: 8px; }
.evidence-chain { font-size: 12px; padding-left: 18px; }
.evidence-chain code { display: block; color: var(--muted); word-break: break-all; }
.stat-hint { display: block; color: var(--muted); font-size: 11px; margin-top: 2px; }
@media (max-width: 1080px) {
  .health-columns { grid-template-columns: 1fr; }
}
</style>
