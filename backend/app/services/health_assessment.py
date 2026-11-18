"""跨模块运行健康评估：把执行日志、采集批次、任务队列和新到达运行指标串成一份可核查的资产。

解决的核心问题：运行概览过去只汇总分散计数，健康下降时无法确认是执行失败、
批次遗漏还是任务状态未闭环。本服务把每个下降指标绑定到具体执行编号、批次编号
和任务编号，并持久化：

1. 健康报告版本：同一（统计日期 + 流程）只发布一个当前版本，重评时版本递增并留历史；
2. 执行缺陷归因：同一执行失败只保留一个“当前”归因，历史归因留作证据链；
3. 批次结论：按批次号保存当前结论，失败执行只能归入所属批次的一个结论；
4. 迟到完成事件：批次汇总后才到达的失败事件，支持
   - ``reopen``：重开批次报告并替换指标；
   - ``retain``：保留原指标并追加迟到缺陷记录；
   两种做法都只给该失败一个当前归因。

统计口径沿用既有模块字段，不做破坏性改造；重复导入的执行编号/批次编号/任务编号
只认第一条，其余记录进入 ``duplicates`` 留痕，不参与展示与指标计算。
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable

from app.store import store

FAILED_STATUSES = ("执行失败", "已超时")
TASK_OPEN_STATUSES = ("排队中", "执行中")
LATE_POLICIES = ("retain", "reopen")
DEFAULT_LATE_POLICY = "retain"

CATEGORY_LABELS = {
    "execution_failure": "执行失败",
    "batch_omission": "批次遗漏",
    "task_not_closed": "任务状态未闭环",
}

METRIC_LABELS = {
    "total_executions": "总执行数",
    "success_rate": "成功率",
    "failed_tasks": "失败任务数",
    "missed_batches": "遗漏批次数",
    "open_tasks": "未闭环任务数",
    "availability": "任务闭环可用率",
    "collected_total": "采集总量",
    "avg_duration": "平均耗时",
    "late_defects": "迟到缺陷数",
}

# 指标极性：越高越好 / 越高越差 / 仅参考。健康“下降”按极性判定，
# 失败任务、遗漏批次、未闭环任务上升也算健康下降，并同样绑定证据。
METRIC_POLARITY = {
    "success_rate": "positive",
    "availability": "positive",
    "collected_total": "positive",
    "failed_tasks": "negative",
    "missed_batches": "negative",
    "open_tasks": "negative",
    "late_defects": "negative",
    "total_executions": "neutral",
    "avg_duration": "neutral",
}


# ---------------------------------------------------------------------------
# 通用小工具
# ---------------------------------------------------------------------------

def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_dt(value: Any) -> datetime | None:
    raw = _text(value)
    if not raw:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _date_of(value: Any) -> str:
    parsed = _parse_dt(value)
    return parsed.strftime("%Y-%m-%d") if parsed else ""


def _minutes(start: Any, end: Any) -> float | None:
    left, right = _parse_dt(start), _parse_dt(end)
    if left is None or right is None:
        return None
    return round((right - left).total_seconds() / 60, 1)


def _safe_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        return float(str(value).replace("%", ""))
    except (TypeError, ValueError):
        return None


def _slug(value: str) -> str:
    """生成稳定标识：保留中文和字母数字，空白与标点折叠为短横线。"""
    cleaned = re.sub(r"[^\w一-鿿-]+", "-", value, flags=re.UNICODE).strip("-")
    return cleaned or f"f{hashlib.sha1(value.encode('utf-8')).hexdigest()[:8]}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def dedupe_rows(rows: Iterable[dict[str, Any]], key_field: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按业务编号去重：只保留第一条，其余作为重复导入留痕。"""
    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen: dict[str, int] = {}
    for row in rows:
        key = _text(row.get(key_field))
        if not key:
            kept.append(row)
            continue
        if key in seen:
            duplicates.append({
                "field": key_field,
                "key": key,
                "duplicate_id": row.get("id"),
                "kept_id": seen[key],
                "status": _text(row.get("status")),
                "reason": "重复导入，只认第一条记录",
            })
            continue
        seen[key] = int(row.get("id") or 0)
        kept.append(row)
    return kept, duplicates


def deduped_execution_rows() -> list[dict[str, Any]]:
    return dedupe_rows(store.rows("execution_log"), "执行编号")[0]


def deduped_batch_rows() -> list[dict[str, Any]]:
    return dedupe_rows(store.rows("batch_tracker"), "批次编号")[0]


def deduped_task_rows() -> list[dict[str, Any]]:
    return dedupe_rows(store.rows("task_queue"), "任务编号")[0]


def duplicate_rows() -> list[dict[str, Any]]:
    groups = [
        ("execution_log", "执行编号", dedupe_rows(store.rows("execution_log"), "执行编号")[1]),
        ("batch_tracker", "批次编号", dedupe_rows(store.rows("batch_tracker"), "批次编号")[1]),
        ("task_queue", "任务编号", dedupe_rows(store.rows("task_queue"), "任务编号")[1]),
    ]
    merged: list[dict[str, Any]] = []
    for module, field, items in groups:
        for item in items:
            merged.append({"module": module, **item})
    return merged


def _batch_summary_at(batch: dict[str, Any]) -> datetime | None:
    return _parse_dt(batch.get("汇总时间")) or _parse_dt(batch.get("采集时间"))


def execution_terminal_at(execution: dict[str, Any]) -> datetime | None:
    return _parse_dt(execution.get("结束时间"))


def task_terminal_at(task: dict[str, Any]) -> datetime | None:
    return _parse_dt(task.get("完成时间"))


# ---------------------------------------------------------------------------
# 报告构建
# ---------------------------------------------------------------------------

class HealthAssessmentService:
    """跨模块健康评估服务：唯一的健康指标 + 缺陷证据发布入口。"""

    SERVICE_NAME = "health_assessment"

    # ---- 数据读取 ----

    def available_dates(self) -> list[str]:
        dates = {
            _date_of(row.get("开始时间"))
            for row in deduped_execution_rows()
        } | {
            _date_of(row.get("采集时间"))
            for row in deduped_batch_rows()
        } | {
            _date_of(row.get("提交时间"))
            for row in deduped_task_rows()
        } | {
            _date_of(row.get("统计日期"))
            for row in store.rows("report_dashboard")
        }
        return sorted(item for item in dates if item)

    def available_flows(self, date: str | None = None) -> list[str]:
        flows = {
            _text(row.get("所属流程")) for row in deduped_execution_rows()
            if not date or _date_of(row.get("开始时间")) == date
        } | {
            _text(row.get("触发流程")) for row in deduped_batch_rows()
            if not date or _date_of(row.get("采集时间")) == date
        } | {
            _text(row.get("流程名称")) for row in deduped_task_rows()
            if not date or _date_of(row.get("提交时间")) == date
        }
        return sorted(item for item in flows if item)

    def scope_rows(self, date: str, flow: str) -> dict[str, list[dict[str, Any]]]:
        executions = [
            row for row in deduped_execution_rows()
            if _date_of(row.get("开始时间")) == date and _text(row.get("所属流程")) == flow
        ]
        batches = [
            row for row in deduped_batch_rows()
            if _date_of(row.get("采集时间")) == date and _text(row.get("触发流程")) == flow
        ]
        tasks = [
            row for row in deduped_task_rows()
            if _date_of(row.get("提交时间")) == date and _text(row.get("流程名称")) == flow
        ]
        return {"executions": executions, "batches": batches, "tasks": tasks}

    def runtime_metrics_for(self, date: str, flow: str) -> list[dict[str, Any]]:
        rows = [
            row for row in store.rows("report_dashboard")
            if _date_of(row.get("统计日期")) == date
            and _text(row.get("所属流程") or flow) == flow
        ]
        rows.sort(key=lambda row: _parse_dt(row.get("到达时间")) or datetime.min)
        return rows

    # ---- 迟到事件 ----

    def _late_ids(self, date: str, flow: str) -> dict[str, dict[str, Any]]:
        scope = self.scope_rows(date, flow)
        batch_by_no = {_text(b.get("批次编号")): b for b in scope["batches"]}
        late_executions: dict[str, str] = {}
        late_tasks: dict[str, str] = {}
        for batch_no, batch in batch_by_no.items():
            summary_at = _batch_summary_at(batch)
            if summary_at is None:
                continue
            for execution in scope["executions"]:
                if _text(execution.get("批次编号")) != batch_no:
                    continue
                if _text(execution.get("status")) not in FAILED_STATUSES:
                    continue
                ended = execution_terminal_at(execution)
                if ended is not None and ended > summary_at:
                    late_executions[_text(execution.get("执行编号"))] = batch_no
            for task in scope["tasks"]:
                if _text(task.get("批次编号")) != batch_no:
                    continue
                if _text(task.get("status")) != "执行失败":
                    continue
                ended = task_terminal_at(task)
                if ended is not None and ended > summary_at:
                    late_tasks[_text(task.get("任务编号"))] = batch_no
        return {
            "executions": late_executions,
            "tasks": late_tasks,
            "batches": batch_by_no,
        }

    # ---- 指标计算 ----

    def _metrics(
        self,
        date: str,
        flow: str,
        *,
        include_late: bool,
    ) -> dict[str, Any]:
        scope = self.scope_rows(date, flow)
        late = self._late_ids(date, flow)
        late_execution_nos = set(late["executions"])

        executions = list(scope["executions"])
        tasks = list(scope["tasks"])
        batches = list(scope["batches"])
        if not include_late:
            executions = [e for e in executions if _text(e.get("执行编号")) not in late_execution_nos]
            tasks = [t for t in tasks if _text(t.get("任务编号")) not in late["tasks"]]

        success = sum(1 for e in executions if _text(e.get("status")) == "执行成功")
        failed = sum(1 for e in executions if _text(e.get("status")) == "执行失败")
        timeout = sum(1 for e in executions if _text(e.get("status")) == "已超时")
        open_exec = sum(1 for e in executions if _text(e.get("status")) == "执行中")
        finished = success + failed + timeout
        success_rate = round(success / finished * 100, 1) if finished else 100.0

        tasks_failed = sum(1 for t in tasks if _text(t.get("status")) == "执行失败")
        tasks_open = sum(1 for t in tasks if _text(t.get("status")) in TASK_OPEN_STATUSES)
        availability = round(
            (len(tasks) - tasks_open) / len(tasks) * 100, 1) if tasks else 100.0

        missed_batches = [
            b for b in batches
            if _text(b.get("status")) == "有遗漏" or _safe_int(b.get("遗漏条数")) > 0
        ]
        collected = sum(_safe_int(b.get("成功条数")) for b in batches)

        durations = [
            minutes for minutes in (
                _minutes(e.get("开始时间"), e.get("结束时间")) for e in executions
            ) if minutes is not None
        ]
        avg_duration = round(sum(durations) / len(durations), 1) if durations else 0.0

        return {
            "total_executions": len(executions),
            "success_count": success,
            "failed_count": failed + timeout,
            "execution_failed": failed,
            "timeout_count": timeout,
            "open_executions": open_exec,
            "success_rate": success_rate,
            "failed_tasks": tasks_failed,
            "open_tasks": tasks_open,
            "task_total": len(tasks),
            "availability": availability,
            "missed_batches": len(missed_batches),
            "missed_records": sum(_safe_int(b.get("遗漏条数")) for b in missed_batches),
            "collected_total": collected,
            "avg_duration": avg_duration,
            "late_executions": sorted(late_execution_nos & {
                _text(e.get("执行编号")) for e in scope["executions"]
            }),
            "late_tasks": sorted(set(late["tasks"])),
        }

    # ---- 基线 ----

    def _baseline(self, date: str, flow: str, metrics: dict[str, Any]) -> dict[str, Any]:
        dates = [item for item in self.available_dates() if item < date]
        for earlier in reversed(dates):
            flows = self.available_flows(earlier)
            if flow in flows:
                base = self._metrics(earlier, flow, include_late=True)
                return {**base, "source": earlier}
        return {
            "total_executions": None,
            "success_rate": 100.0,
            "failed_tasks": 0,
            "missed_batches": 0,
            "open_tasks": 0,
            "availability": 100.0,
            "collected_total": None,
            "avg_duration": None,
            "source": "target",
        }

    def _metric_rows(self, metrics: dict[str, Any], baseline: dict[str, Any]) -> list[dict[str, Any]]:
        keys = [
            ("total_executions", "count"),
            ("success_rate", "percent"),
            ("failed_tasks", "count"),
            ("missed_batches", "count"),
            ("open_tasks", "count"),
            ("availability", "percent"),
            ("collected_total", "count"),
            ("avg_duration", "minutes"),
        ]
        rows: list[dict[str, Any]] = []
        for key, unit in keys:
            value = metrics.get(key, 0)
            base = baseline.get(key)
            delta = None if base is None else round(value - base, 1)
            polarity = METRIC_POLARITY.get(key, "neutral")
            if delta is None or delta == 0:
                direction = "持平"
            elif delta > 0:
                direction = "上升"
            else:
                direction = "下降"
            # 负向指标上升等同健康下降；中性指标不参与健康判定。
            declined = delta is not None and (
                (polarity == "positive" and delta < 0)
                or (polarity == "negative" and delta > 0)
            )
            rows.append({
                "key": key,
                "label": METRIC_LABELS[key],
                "value": value,
                "baseline": base,
                "delta": delta,
                "direction": direction,
                "polarity": polarity,
                "unit": unit,
                "declined": declined,
                "evidence_execution_nos": [],
                "evidence_batch_nos": [],
                "defect_ids": [],
            })
        return rows

    # ---- 缺陷构建 ----

    def _build_defects(
        self,
        date: str,
        flow: str,
        *,
        policy: str,
        include_late: bool,
    ) -> list[dict[str, Any]]:
        scope = self.scope_rows(date, flow)
        late = self._late_ids(date, flow)
        batch_by_no = {_text(b.get("批次编号")): b for b in scope["batches"]}
        defects: list[dict[str, Any]] = []

        for execution in scope["executions"]:
            status = _text(execution.get("status"))
            if status not in FAILED_STATUSES:
                continue
            execution_no = _text(execution.get("执行编号"))
            batch_no = _text(execution.get("批次编号"))
            is_late = execution_no in late["executions"]
            if is_late and not include_late:
                # 保留原指标时，迟到失败以“迟到缺陷记录”形式追加。
                defect_id = f"DEF-LATE-{_slug(execution_no)}"
                metric_keys = ["late_defects"]
            else:
                defect_id = f"DEF-EXEC-{_slug(execution_no)}"
                metric_keys = ["success_rate", "total_executions"]
                task_row = next((
                    t for t in scope["tasks"]
                    if _text(t.get("任务编号")) == _text(execution.get("任务编号"))
                ), None)
                if task_row and _text(task_row.get("status")) == "执行失败":
                    metric_keys.append("failed_tasks")
            batch = batch_by_no.get(batch_no, {})
            defect = {
                "defect_id": defect_id,
                "category": "execution_failure",
                "category_label": CATEGORY_LABELS["execution_failure"],
                "scope": {"date": date, "flow": flow},
                "execution_no": execution_no,
                "task_no": _text(execution.get("任务编号")),
                "batch_no": batch_no,
                "reason": _text(execution.get("错误摘要")) or f"执行状态：{status}",
                "status": status,
                "is_late": is_late,
                "late_policy": policy if is_late else None,
                "metric_keys": metric_keys,
                "evidence": {
                    "execution": {
                        "执行编号": execution_no,
                        "执行状态": status,
                        "开始时间": execution.get("开始时间"),
                        "结束时间": execution.get("结束时间"),
                        "错误摘要": execution.get("错误摘要"),
                        "采集行数": execution.get("采集行数"),
                    },
                    "batch": {
                        "批次编号": batch_no,
                        "校验状态": batch.get("校验状态"),
                        "目标条数": batch.get("目标条数"),
                        "成功条数": batch.get("成功条数"),
                        "遗漏条数": batch.get("遗漏条数"),
                        "汇总时间": batch.get("汇总时间"),
                    },
                    "task_no": _text(execution.get("任务编号")),
                },
            }
            defects.append(defect)

        for batch in scope["batches"]:
            batch_no = _text(batch.get("批次编号"))
            missed = _safe_int(batch.get("遗漏条数"))
            status = _text(batch.get("status"))
            if status != "有遗漏" and missed <= 0:
                continue
            linked_execs = [
                _text(e.get("执行编号")) for e in scope["executions"]
                if _text(e.get("批次编号")) == batch_no
            ]
            defect = {
                "defect_id": f"DEF-BATCH-{_slug(batch_no)}",
                "category": "batch_omission",
                "category_label": CATEGORY_LABELS["batch_omission"],
                "scope": {"date": date, "flow": flow},
                "execution_no": linked_execs[0] if linked_execs else None,
                "execution_nos": linked_execs,
                "task_no": next((
                    _text(t.get("任务编号")) for t in scope["tasks"]
                    if _text(t.get("批次编号")) == batch_no
                ), None),
                "batch_no": batch_no,
                "reason": f"批次遗漏 {missed} 条（目标 {batch.get('目标条数')} / 成功 {batch.get('成功条数')}）",
                "status": status,
                "is_late": False,
                "late_policy": None,
                "metric_keys": ["missed_batches", "collected_total"],
                "evidence": {
                    "batch": {
                        "批次编号": batch_no,
                        "校验状态": batch.get("校验状态"),
                        "目标条数": batch.get("目标条数"),
                        "成功条数": batch.get("成功条数"),
                        "遗漏条数": batch.get("遗漏条数"),
                        "采集时间": batch.get("采集时间"),
                        "汇总时间": batch.get("汇总时间"),
                    },
                    "execution_nos": linked_execs,
                },
            }
            defects.append(defect)

        # 任务状态未闭环：批次已汇总，任务仍停在排队中/执行中。
        for task in scope["tasks"]:
            status = _text(task.get("status"))
            if status not in TASK_OPEN_STATUSES:
                continue
            task_no = _text(task.get("任务编号"))
            if policy == "retain" and task_no in late["tasks"]:
                # 迟到失败任务在 retain 下由迟到缺陷承载，不重复生成未闭环缺陷。
                continue
            batch_no = _text(task.get("批次编号"))
            batch = batch_by_no.get(batch_no, {})
            summary_at = _batch_summary_at(batch)
            submitted_at = _parse_dt(task.get("提交时间"))
            if summary_at is not None and submitted_at is not None and submitted_at > summary_at:
                continue
            defect = {
                "defect_id": f"DEF-TASK-{_slug(task_no)}",
                "category": "task_not_closed",
                "category_label": CATEGORY_LABELS["task_not_closed"],
                "scope": {"date": date, "flow": flow},
                "execution_no": _text(task.get("执行编号")) or None,
                "task_no": task_no,
                "batch_no": batch_no,
                "reason": f"批次已汇总，任务仍为“{status}”未闭环",
                "status": status,
                "is_late": False,
                "late_policy": None,
                "metric_keys": ["availability", "open_tasks"],
                "evidence": {
                    "task": {
                        "任务编号": task_no,
                        "当前状态": status,
                        "提交时间": task.get("提交时间"),
                        "完成时间": task.get("完成时间"),
                    },
                    "batch": {
                        "批次编号": batch_no,
                        "汇总时间": batch.get("汇总时间"),
                    },
                },
            }
            defects.append(defect)

        defects.sort(key=lambda item: (item["is_late"], item["defect_id"]))
        return defects

    # ---- 批次结论 ----

    def _build_conclusions(
        self,
        date: str,
        flow: str,
        *,
        version: int,
        metrics: dict[str, Any],
        defects: list[dict[str, Any]],
        policy: str,
    ) -> list[dict[str, Any]]:
        scope = self.scope_rows(date, flow)
        conclusions: list[dict[str, Any]] = []
        for batch in scope["batches"]:
            batch_no = _text(batch.get("批次编号"))
            linked = [d for d in defects if d.get("batch_no") == batch_no]
            previous = store.health.batch_conclusion(batch_no)
            batch_version = (previous or {}).get("version", 0) + 1
            failed_execs = [
                d["execution_no"] for d in linked
                if d["category"] == "execution_failure" and d.get("execution_no")
            ]
            failed_tasks = sorted({
                d.get("task_no") for d in linked
                if d.get("task_no") and (
                    d["category"] == "task_not_closed"
                    or (d["category"] == "execution_failure" and d.get("status") == "执行失败")
                )
            })
            categories = [d["category"] for d in linked]
            category = next((
                item for item in ("execution_failure", "batch_omission", "task_not_closed")
                if item in categories
            ), "healthy")
            conclusion_id = f"BC-{date}-{_slug(batch_no)}-v{batch_version}"
            conclusion = {
                "conclusion_id": conclusion_id,
                "batch_no": batch_no,
                "flow": flow,
                "date": date,
                "version": batch_version,
                "state": "当前",
                "category": category,
                "category_label": CATEGORY_LABELS.get(category, "无缺陷"),
                "failed_execution_nos": failed_execs,
                "failed_task_nos": failed_tasks,
                "defect_ids": [d["defect_id"] for d in linked],
                "late_defect_ids": [d["defect_id"] for d in linked if d.get("is_late")],
                "late_policy": policy,
                "missed_count": _safe_int(batch.get("遗漏条数")),
                "metrics": {
                    "success_rate": metrics["success_rate"],
                    "failed_tasks": metrics["failed_tasks"],
                    "availability": metrics["availability"],
                    "missed_batches": metrics["missed_batches"],
                },
                "evidence": {
                    "batch": {
                        "批次编号": batch_no,
                        "校验状态": batch.get("校验状态"),
                        "目标条数": batch.get("目标条数"),
                        "成功条数": batch.get("成功条数"),
                        "遗漏条数": batch.get("遗漏条数"),
                        "汇总时间": batch.get("汇总时间"),
                    },
                    "execution_nos": failed_execs,
                    "task_nos": failed_tasks,
                },
                "supersedes": (previous or {}).get("conclusion_id"),
                "published_at": _now(),
                "report_version": version,
            }
            conclusions.append(conclusion)
        return conclusions

    # ---- 报告签名与持久化 ----

    def _signature(self, date: str, flow: str, policy: str) -> str:
        payload = {
            "date": date,
            "flow": flow,
            "policy": policy,
            "executions": [
                [row.get("执行编号"), row.get("status"), row.get("结束时间")]
                for row in self.scope_rows(date, flow)["executions"]
            ],
            "batches": [
                [row.get("批次编号"), row.get("status"), row.get("遗漏条数"), row.get("汇总时间")]
                for row in self.scope_rows(date, flow)["batches"]
            ],
            "tasks": [
                [row.get("任务编号"), row.get("status"), row.get("完成时间")]
                for row in self.scope_rows(date, flow)["tasks"]
            ],
        }
        return hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:16]

    def _resolve_policy(self, date: str, flow: str, policy: str | None) -> str:
        if policy in LATE_POLICIES:
            return policy
        for batch in self.scope_rows(date, flow)["batches"]:
            stored = store.health.late_policy_for(_text(batch.get("批次编号")))
            if stored in LATE_POLICIES:
                return stored
        return DEFAULT_LATE_POLICY

    def assess(
        self,
        date: str | None = None,
        flow: str | None = None,
        policy: str | None = None,
    ) -> list[dict[str, Any]]:
        dates = self.available_dates()
        if not dates:
            return []
        target_date = date or dates[-1]
        flows = [flow] if flow else self.available_flows(target_date)
        return [
            self._assess_one(target_date, item, policy)
            for item in flows
        ]

    def _assess_one(self, date: str, flow: str, policy: str | None) -> dict[str, Any]:
        resolved_policy = self._resolve_policy(date, flow, policy)
        late = self._late_ids(date, flow)
        has_late = bool(late["executions"] or late["tasks"])
        changed_batches = {
            batch_no for batch_no in set(late["executions"].values()) | set(late["tasks"].values())
        }

        # 迟到冲突必须先于指标计算：重开=替换指标，保留=原指标 + 迟到缺陷。
        include_late = resolved_policy == "reopen"
        metrics = self._metrics(date, flow, include_late=include_late)
        baseline = self._baseline(date, flow, metrics)
        metric_rows = self._metric_rows(metrics, baseline)
        defects = self._build_defects(
            date, flow, policy=resolved_policy, include_late=include_late)

        if resolved_policy == "reopen" and has_late:
            for batch_no, batch in late["batches"].items():
                summary_at = _batch_summary_at(batch)
                for execution_no, late_batch in late["executions"].items():
                    if late_batch != batch_no:
                        continue
                    if not store.health.late_event_exists(
                            execution_no, self._late_end(execution_no)):
                        store.health.record_late_event({
                            "execution_no": execution_no,
                            "batch_no": batch_no,
                            "summary_at": summary_at.strftime("%Y-%m-%d %H:%M")
                            if summary_at else "",
                            "ended_at": self._late_end(execution_no),
                            "resolution": "reopen",
                            "recorded_at": _now(),
                        })
        for batch_no in changed_batches:
            store.health.set_late_policy(batch_no, resolved_policy)

        scope_key = store.health.scope_key(date, flow)
        signature = self._signature(date, flow, resolved_policy)
        previous = store.health.get_report(scope_key)
        if previous and previous.get("signature") == signature:
            # 同一份证据链重复进入运行概览：不重发版本，直接复用已发布的指标与归因，
            # 只刷新新到达运行指标的对账结果，保证“同一份证据链”前后可核对。
            reused = self._reuse_report(previous, date, flow, metrics)
            store.health.sync_todos(scope_key, reused.get("defects", []))
            store.health.save_report(scope_key, reused)
            return reused

        version = store.health.next_version(scope_key)
        conclusions = self._build_conclusions(
            date, flow, version=version, metrics=metrics, defects=defects,
            policy=resolved_policy)
        for conclusion in conclusions:
            store.health.save_batch_conclusion(conclusion)

        for defect in defects:
            execution_no = defect.get("execution_no")
            if not execution_no or defect["category"] != "execution_failure":
                continue
            batch_no = defect.get("batch_no") or ""
            conclusion = next(
                (c for c in conclusions if c["batch_no"] == batch_no), None)
            if conclusion is None:
                # 失败执行找不到批次时，仍需一个当前批次结论，避免无归属。
                conclusion = {
                    "conclusion_id": f"BC-{date}-UNASSIGNED-{_slug(execution_no)}-v1",
                    "batch_no": batch_no or "UNASSIGNED",
                    "flow": flow,
                    "date": date,
                    "version": 1,
                    "state": "当前",
                    "category": "execution_failure",
                    "category_label": CATEGORY_LABELS["execution_failure"],
                    "failed_execution_nos": [execution_no],
                    "failed_task_nos": [defect.get("task_no")] if defect.get("task_no") else [],
                    "defect_ids": [defect["defect_id"]],
                    "late_defect_ids": [],
                    "late_policy": resolved_policy,
                    "missed_count": 0,
                    "metrics": {},
                    "evidence": {"execution_nos": [execution_no]},
                    "supersedes": None,
                    "published_at": _now(),
                    "report_version": version,
                }
                store.health.save_batch_conclusion(conclusion)
            existing = store.health.attribution_for(execution_no)
            if existing and existing.get("conclusion_id") == conclusion["conclusion_id"]:
                continue
            # 同一执行失败只保留一个当前归因，旧归因自动降级为历史证据。
            store.health.save_attribution(
                execution_no, conclusion, [defect["defect_id"]], version, resolved_policy)

        attribution_by_exec = {
            item["execution_no"]: item for item in (
                store.health.attribution_for(
                    defect.get("execution_no") or "") for defect in defects
            ) if item
        }

        # 把指标 → 缺陷 → 执行/批次证据连通。
        for metric in metric_rows:
            for defect in defects:
                if metric["key"] in defect["metric_keys"]:
                    metric["defect_ids"].append(defect["defect_id"])
                    if defect.get("execution_no"):
                        metric["evidence_execution_nos"].append(defect["execution_no"])
                    for exec_no in defect.get("execution_nos", []):
                        metric["evidence_execution_nos"].append(exec_no)
                    if defect.get("batch_no"):
                        metric["evidence_batch_nos"].append(defect["batch_no"])
            metric["evidence_execution_nos"] = sorted(set(metric["evidence_execution_nos"]))
            metric["evidence_batch_nos"] = sorted(set(metric["evidence_batch_nos"]))
            metric["defect_ids"] = sorted(set(metric["defect_ids"]))

        declining = [m for m in metric_rows if m["declined"]]
        status = self._status(defects, declining)

        report = {
            "report_id": f"HR-{date}-{_slug(flow)}-v{version}",
            "scope_key": scope_key,
            "scope": {"date": date, "flow": flow},
            "version": version,
            "status": status,
            "signature": signature,
            "generated_at": _now(),
            "late_policy": resolved_policy,
            "late_conflict": {
                "present": has_late,
                "batch_nos": sorted(changed_batches),
                "late_execution_nos": sorted(late["executions"]),
                "late_task_nos": sorted(late["tasks"]),
                "options": list(LATE_POLICIES),
                "resolved": resolved_policy if has_late else None,
            },
            "metrics": metric_rows,
            "metric_values": metrics,
            "baseline": baseline,
            "defects": defects,
            "batch_conclusions": conclusions,
            "attributions": attribution_by_exec,
            "evidence": {
                "executions": [
                    {
                        "执行编号": row.get("执行编号"),
                        "所属流程": row.get("所属流程"),
                        "status": row.get("status"),
                        "开始时间": row.get("开始时间"),
                        "结束时间": row.get("结束时间"),
                        "错误摘要": row.get("错误摘要"),
                        "批次编号": row.get("批次编号"),
                        "任务编号": row.get("任务编号"),
                    }
                    for row in self.scope_rows(date, flow)["executions"]
                ],
                "batches": [
                    {
                        "批次编号": row.get("批次编号"),
                        "触发流程": row.get("触发流程"),
                        "status": row.get("status"),
                        "目标条数": row.get("目标条数"),
                        "成功条数": row.get("成功条数"),
                        "遗漏条数": row.get("遗漏条数"),
                        "汇总时间": row.get("汇总时间"),
                    }
                    for row in self.scope_rows(date, flow)["batches"]
                ],
                "tasks": [
                    {
                        "任务编号": row.get("任务编号"),
                        "流程名称": row.get("流程名称"),
                        "status": row.get("status"),
                        "提交时间": row.get("提交时间"),
                        "完成时间": row.get("完成时间"),
                        "批次编号": row.get("批次编号"),
                        "执行编号": row.get("执行编号"),
                    }
                    for row in self.scope_rows(date, flow)["tasks"]
                ],
                "duplicates": duplicate_rows(),
            },
            "reconciliation": self._reconcile(date, flow, metrics),
            "todos": store.health.sync_todos(scope_key, defects),
            "history": store.health.history_for(scope_key),
        }
        store.health.record_history(scope_key, {
            "version": version,
            "report_id": report["report_id"],
            "status": status,
            "late_policy": resolved_policy,
            "published_at": report["generated_at"],
            "defect_ids": [d["defect_id"] for d in defects],
            "metric_snapshot": {
                key: metrics[key] for key in (
                    "success_rate", "failed_tasks", "missed_batches",
                    "availability", "collected_total")
            },
        })
        store.health.save_report(scope_key, report)
        return report

    @staticmethod
    def _late_end(execution_no: str) -> str:
        for row in deduped_execution_rows():
            if _text(row.get("执行编号")) == execution_no:
                return _text(row.get("结束时间"))
        return ""

    @staticmethod
    def _status(defects: list[dict[str, Any]], declining: list[dict[str, Any]]) -> str:
        if not declining and not defects:
            return "正常"
        high = {"execution_failure", "batch_omission"}
        if any(d["category"] in high for d in defects):
            return "异常"
        if declining or defects:
            return "下降"
        return "正常"

    def _reuse_report(
        self,
        report: dict[str, Any],
        date: str,
        flow: str,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        reused = dict(report)
        reused["defects"] = copy.deepcopy(report.get("defects", []))
        # 迟到方案若被显式改写，证据链已变化，签名会不同，不会走到这里。
        reused["metric_values"] = metrics
        reused["reconciliation"] = self._reconcile(date, flow, metrics)
        reused["reused_evidence"] = True
        reused["todos"] = store.health.sync_todos(report["scope_key"], reused["defects"])
        reused["history"] = store.health.history_for(report["scope_key"])
        return reused

    # ---- 新到达运行指标对账 ----

    def _reconcile(self, date: str, flow: str, metrics: dict[str, Any]) -> dict[str, Any]:
        runtime_rows = self.runtime_metrics_for(date, flow)
        if not runtime_rows:
            return {
                "status": "缺少运行指标",
                "runtime_metric_version": None,
                "arrived_at": None,
                "mismatches": [],
                "matches": [],
            }
        latest = runtime_rows[-1]
        expected = {
            "总执行数": metrics["total_executions"],
            "成功数": metrics["success_count"],
            "失败数": metrics["failed_count"],
            "成功率": metrics["success_rate"],
            "遗漏批次数": metrics["missed_batches"],
            "失败任务数": metrics["failed_tasks"],
            "未闭环任务数": metrics["open_tasks"],
            "采集总量": metrics["collected_total"],
        }
        matches: list[dict[str, Any]] = []
        mismatches: list[dict[str, Any]] = []
        for field, computed in expected.items():
            runtime_value = _safe_float(latest.get(field))
            if runtime_value is None:
                continue
            if abs(runtime_value - computed) < 0.05:
                matches.append({"field": field, "value": computed})
            else:
                mismatches.append({
                    "field": field,
                    "runtime": runtime_value,
                    "computed": computed,
                    "delta": round(computed - runtime_value, 1),
                })
        return {
            "status": "有差异" if mismatches else "一致",
            "runtime_metric_version": latest.get("版本号"),
            "arrived_at": latest.get("到达时间"),
            "matches": matches,
            "mismatches": mismatches,
        }

    def ingest_runtime_metric(self, payload: dict[str, Any]) -> dict[str, Any]:
        """接收新到达的运行指标，落库后立即与执行/批次证据对账。"""
        date = _text(payload.get("date") or payload.get("统计日期"))
        flow = _text(payload.get("flow") or payload.get("所属流程"))
        if not date or not flow:
            return {"ok": False, "message": "缺少统计日期或所属流程"}
        rows = store.rows("report_dashboard")
        values = {
            "统计日期": date,
            "所属流程": flow,
            "总执行数": _safe_int(payload.get("total_executions", payload.get("总执行数"))),
            "成功数": _safe_int(payload.get("success_count", payload.get("成功数"))),
            "失败数": _safe_int(payload.get("failed_count", payload.get("失败数"))),
            "成功率": _safe_float(payload.get("success_rate", payload.get("成功率"))) or 0.0,
            "遗漏批次数": _safe_int(payload.get("missed_batches", payload.get("遗漏批次数"))),
            "失败任务数": _safe_int(payload.get("failed_tasks", payload.get("失败任务数"))),
            "未闭环任务数": _safe_int(payload.get("open_tasks", payload.get("未闭环任务数"))),
            "采集总量": _safe_int(payload.get("collected_total", payload.get("采集总量"))),
            "平均耗时": payload.get("avg_duration", payload.get("平均耗时", "")),
            "可用率": _safe_float(payload.get("availability", payload.get("可用率"))) or 0.0,
            "版本号": _text(payload.get("version") or f"RM-{date}-NEW"),
            "到达时间": _text(payload.get("arrived_at")) or datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M"),
            "status": "正常",
            "pending": False,
            "abnormal": False,
        }
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1, **values}
        rows.append(entry)
        reports = self.assess(date=date, flow=flow)
        report = reports[0] if reports else None
        return {"ok": True, "metric": entry, "reconciliation": report["reconciliation"] if report else None}

    # ---- 对外查询 ----

    def list_reports(
        self,
        date: str | None = None,
        flow: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if not store.health.reports:
            # 尚未发布过报告时按当前证据链补发一次，保证查询接口可用。
            self.assess(date=date, flow=flow)
        reports: list[dict[str, Any]] = []
        for report in store.health.reports.values():
            scope = report.get("scope", {})
            if date and scope.get("date") != date:
                continue
            if flow and scope.get("flow") != flow:
                continue
            if status and report.get("status") != status:
                continue
            reports.append(report)
        reports.sort(key=lambda item: (
            item["scope"].get("date", ""), item["scope"].get("flow", "")))
        return reports

    def get_report(
        self,
        date: str | None = None,
        flow: str | None = None,
        policy: str | None = None,
        refresh: bool = True,
    ) -> dict[str, Any] | None:
        dates = self.available_dates()
        if not dates:
            return None
        target_date = date or dates[-1]
        target_flow = flow or (self.available_flows(target_date) or [None])[0]
        if not target_flow:
            return None
        scope_key = store.health.scope_key(target_date, target_flow)
        if refresh:
            reports = self.assess(date=target_date, flow=target_flow, policy=policy)
            return reports[0] if reports else None
        return store.health.get_report(scope_key)

    def execution_attribution(self, execution_no: str) -> dict[str, Any]:
        attribution = store.health.attribution_for(execution_no)
        history = store.health.attribution_history_for(execution_no)
        return {
            "execution_no": execution_no,
            "current": attribution,
            "history": history,
            "evidence_chain": self._evidence_chain(execution_no),
        }

    def _evidence_chain(self, execution_no: str) -> list[dict[str, Any]]:
        chain: list[dict[str, Any]] = []
        execution = next(
            (r for r in deduped_execution_rows()
             if _text(r.get("执行编号")) == execution_no), None)
        if execution is None:
            return chain
        chain.append({
            "type": "execution",
            "ref": execution_no,
            "detail": {
                "status": execution.get("status"),
                "start": execution.get("开始时间"),
                "end": execution.get("结束时间"),
                "error": execution.get("错误摘要"),
            },
        })
        batch_no = _text(execution.get("批次编号"))
        if batch_no:
            batch = next(
                (r for r in deduped_batch_rows()
                 if _text(r.get("批次编号")) == batch_no), None)
            chain.append({
                "type": "batch",
                "ref": batch_no,
                "detail": {
                    "status": (batch or {}).get("status"),
                    "target": (batch or {}).get("目标条数"),
                    "success": (batch or {}).get("成功条数"),
                    "missed": (batch or {}).get("遗漏条数"),
                    "summary_at": (batch or {}).get("汇总时间"),
                },
            })
        task_no = _text(execution.get("任务编号"))
        if task_no:
            task = next(
                (r for r in deduped_task_rows()
                 if _text(r.get("任务编号")) == task_no), None)
            chain.append({
                "type": "task",
                "ref": task_no,
                "detail": {
                    "status": (task or {}).get("status"),
                    "submitted": (task or {}).get("提交时间"),
                    "finished": (task or {}).get("完成时间"),
                },
            })
        conclusion = store.health.current_conclusion_for(batch_no) if batch_no else None
        if conclusion:
            chain.append({
                "type": "batch_conclusion",
                "ref": conclusion["conclusion_id"],
                "detail": conclusion,
            })
        return chain

    def todos(self, status: str = "待处理") -> list[dict[str, Any]]:
        return [
            dict(todo) for todo in store.health.todos.values()
            if not status or todo.get("status") == status
        ]

    def duplicates(self) -> list[dict[str, Any]]:
        return duplicate_rows()

    def retry_execution(self, execution_no: str) -> dict[str, Any]:
        """失败重试：新增一次关联同一批次/任务的执行，再重跑同一证据链。"""
        original = next(
            (row for row in deduped_execution_rows()
             if _text(row.get("执行编号")) == execution_no), None)
        if original is None:
            return {"ok": False, "message": f"执行 {execution_no} 不存在"}
        if _text(original.get("status")) not in FAILED_STATUSES:
            return {"ok": False, "message": "只有失败/超时执行可以重试"}
        rows = store.rows("execution_log")
        retry_no = f"{execution_no}-R{sum(1 for r in rows if _text(r.get('执行编号')).startswith(execution_no + '-R')) + 1}"
        now = datetime.now(timezone.utc)
        # 重试仍归在同一统计日期下，保证再次进入运行概览时能沿同一证据链核对。
        original_date = _date_of(original.get("开始时间"))
        start_at = f"{original_date} {now.strftime('%H:%M')}" if original_date else now.strftime(
            "%Y-%m-%d %H:%M")
        retry = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "status": "执行中",
            "pending": True,
            "abnormal": False,
            "执行编号": retry_no,
            "所属流程": original.get("所属流程"),
            "开始时间": start_at,
            "结束时间": "",
            "执行状态": "执行中",
            "采集行数": 0,
            "错误摘要": f"对 {execution_no} 的失败重试",
            "批次编号": original.get("批次编号"),
            "任务编号": original.get("任务编号"),
            "重试来源": execution_no,
        }
        rows.append(retry)
        date = _date_of(original.get("开始时间"))
        reports = self.assess(date=date, flow=_text(original.get("所属流程")))
        report = reports[0] if reports else None
        defects = (report or {}).get("defects", [])
        original_defect = next(
            (d for d in defects if d.get("execution_no") == execution_no), None)
        attribution = store.health.attribution_for(execution_no)
        return {
            "ok": True,
            "retry_execution_no": retry_no,
            "execution": retry,
            "report_version": (report or {}).get("version"),
            "evidence_chain": self._evidence_chain(execution_no),
            "reconciliation": (report or {}).get("reconciliation"),
            "original_defect": original_defect,
            "original_attribution": attribution,
        }


health_assessment = HealthAssessmentService()
