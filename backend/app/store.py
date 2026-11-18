"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import copy
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS
from app.seed_health import HEALTH_SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 健康评估台账 (HealthLedger) — 报告版本、执行归因、批次结论
# ---------------------------------------------------------------------------

class HealthLedger:
    """运行健康评估台账：健康指标与缺陷证据同源保存。

    关键约束：
    - 同一执行失败只保留一个“当前”归因，历史归因保留为证据链；
    - 批次结论按批次号保存最新版本，重开/追加都会留下版本轨迹；
    - 报告按（统计日期 + 流程）作用域缓存，版本号单调递增。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.reports: dict[str, dict[str, Any]] = {}
        self.report_history: dict[str, list[dict[str, Any]]] = {}
        self.batch_conclusions: dict[str, dict[str, Any]] = {}
        self.current_batch_conclusion: dict[str, str] = {}
        self.current_attribution: dict[str, dict[str, Any]] = {}
        self.attribution_history: list[dict[str, Any]] = []
        self.late_events: list[dict[str, Any]] = []
        self.late_policies: dict[str, str] = {}
        self.todos: dict[str, dict[str, Any]] = {}
        self.versions: dict[str, int] = {}

    @staticmethod
    def scope_key(date: str, flow: str | None) -> str:
        return f"{date}|{flow or '*'}"

    def next_version(self, scope: str) -> int:
        with self._lock:
            self.versions[scope] = self.versions.get(scope, 0) + 1
            return self.versions[scope]

    def get_report(self, scope: str) -> dict[str, Any] | None:
        with self._lock:
            report = self.reports.get(scope)
            return copy.deepcopy(report) if report else None

    def save_report(self, scope: str, report: dict[str, Any]) -> None:
        with self._lock:
            self.reports[scope] = copy.deepcopy(report)

    def record_history(self, scope: str, entry: dict[str, Any]) -> None:
        with self._lock:
            self.report_history.setdefault(scope, []).append(copy.deepcopy(entry))

    def history_for(self, scope: str) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self.report_history.get(scope, []))

    def save_batch_conclusion(self, conclusion: dict[str, Any]) -> None:
        """写入批次的最新结论，并为批次编号记录“当前结论”指针。"""
        key = str(conclusion.get("batch_no") or conclusion["conclusion_id"])
        with self._lock:
            self.batch_conclusions[key] = copy.deepcopy(conclusion)
            self.current_batch_conclusion[key] = conclusion["conclusion_id"]

    def batch_conclusion(self, batch_no: str) -> dict[str, Any] | None:
        with self._lock:
            conclusion = self.batch_conclusions.get(batch_no)
            return copy.deepcopy(conclusion) if conclusion else None

    def current_conclusion_for(self, batch_no: str) -> dict[str, Any] | None:
        with self._lock:
            conclusion_id = self.current_batch_conclusion.get(batch_no)
            if not conclusion_id:
                return None
            conclusion = self.batch_conclusions.get(batch_no)
            if conclusion and conclusion.get("conclusion_id") == conclusion_id:
                return copy.deepcopy(conclusion)
            return None

    def save_attribution(
        self,
        execution_no: str,
        conclusion: dict[str, Any],
        defect_ids: list[str],
        version: int,
        mode: str,
    ) -> dict[str, Any]:
        """为一次执行写入当前归因，旧的当前归因自动降级为历史。"""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            for item in self.attribution_history:
                if item["execution_no"] == execution_no and item["is_current"]:
                    item["is_current"] = False
                    item["superseded_by"] = conclusion["conclusion_id"]
                    item["superseded_at"] = now
            record = {
                "execution_no": execution_no,
                "conclusion_id": conclusion["conclusion_id"],
                "batch_no": conclusion.get("batch_no"),
                "category": conclusion.get("category"),
                "version": version,
                "mode": mode,
                "defect_ids": list(defect_ids),
                "is_current": True,
                "created_at": now,
                "conclusion": copy.deepcopy(conclusion),
            }
            self.attribution_history.append(record)
            self.current_attribution[execution_no] = copy.deepcopy(record)
            return record

    def attribution_for(self, execution_no: str) -> dict[str, Any] | None:
        with self._lock:
            record = self.current_attribution.get(execution_no)
            return copy.deepcopy(record) if record else None

    def attribution_history_for(self, execution_no: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(item) for item in self.attribution_history
                if item["execution_no"] == execution_no
            ]

    def set_late_policy(self, batch_no: str, policy: str) -> None:
        with self._lock:
            self.late_policies[batch_no] = policy

    def late_policy_for(self, batch_no: str) -> str | None:
        with self._lock:
            return self.late_policies.get(batch_no)

    def record_late_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.late_events.append(copy.deepcopy(event))

    def late_event_exists(self, execution_no: str, ended_at: str) -> bool:
        with self._lock:
            return any(
                item.get("execution_no") == execution_no
                and item.get("ended_at") == ended_at
                for item in self.late_events
            )

    def late_events_for(self, batch_no: str) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(item) for item in self.late_events
                if item.get("batch_no") == batch_no
            ]

    def sync_todos(self, scope: str, defects: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """按缺陷同步待办：新缺陷开待办，消失的缺陷关闭待办。"""
        now = datetime.now(timezone.utc).isoformat()
        active_ids = {d["defect_id"] for d in defects}
        with self._lock:
            for defect in defects:
                todo = self.todos.get(defect["defect_id"]) or {
                    "todo_id": f"TODO-{defect['defect_id']}",
                    "defect_id": defect["defect_id"],
                    "created_at": now,
                }
                todo.update({
                    "scope": scope,
                    "category": defect["category"],
                    "execution_no": defect.get("execution_no"),
                    "batch_no": defect.get("batch_no"),
                    "metric_keys": defect.get("metric_keys", []),
                    "summary": defect.get("reason", ""),
                    "status": "待处理",
                    "updated_at": now,
                })
                self.todos[defect["defect_id"]] = todo
            for defect_id, todo in self.todos.items():
                if (todo.get("scope") == scope and defect_id not in active_ids
                        and todo.get("status") == "待处理"):
                    todo["status"] = "已关闭"
                    todo["closed_at"] = now
                    todo["updated_at"] = now
            return [
                copy.deepcopy(todo) for todo in self.todos.values()
                if todo.get("scope") == scope and todo.get("status") == "待处理"
            ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "reports": len(self.reports),
                "report_versions": sum(self.versions.values()),
                "batch_conclusions": len(self.batch_conclusions),
                "current_attributions": len(self.current_attribution),
                "attribution_history": len(self.attribution_history),
                "late_events": len(self.late_events),
                "open_todos": sum(1 for t in self.todos.values() if t.get("status") == "待处理"),
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 运行健康评估依赖的既有模块改用能串起执行/批次/任务证据链的示例数据。
        # 其他模块继续沿用通用示例数据，既有业务口径不受影响。
        for name, rows in HEALTH_SEED_ROWS.items():
            self._tables[name] = [dict(row) for row in rows]
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        self.health = HealthLedger()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()