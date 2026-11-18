"""执行记录领域服务 — 记录和查询 RPA 执行日志，输出 domain_snapshot。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

执行记录接口直接使用运行概览记录的运行指标数据集：同一执行编号只认第一条，
并按运行概览版本和健康评估报告补充缺陷归因与批次结论。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.health_assessment import (
    deduped_execution_rows,
    duplicate_rows,
    health_assessment,
)
from app.store import store


class ExecutionLogService(BaseModuleService):
    MODULE = "execution_log"
    ENTITY = "执行日志"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "执行编号", "所属流程", "开始时间", "结束时间", "执行状态", "采集行数", "错误摘要",
        "批次编号", "任务编号",
    ]
    REQUIRED_FIELDS = ["执行编号", "所属流程", "开始时间"]
    STATUS_ORDER = ["执行中", "执行成功", "执行失败", "已超时", "已取消"]
    ACTION_RULES = {"归档日志": "执行成功"}
    NEGATIVE_ACTIONS = []
    DEDUPE_FIELD = "执行编号"

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        """执行列表：按执行编号去重，并按运行概览版本补充缺陷归因与批次结论。"""
        items, total = super().list_entries(**kwargs)
        return [self._with_attribution(row) for row in items], total

    def _with_attribution(self, entry: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(entry)
        enriched["重复导入"] = self._is_duplicate(entry)
        if not enriched["重复导入"]:
            attribution = self._attribution_for(entry)
            enriched["归因"] = {
                "report_id": attribution["report_id"],
                "report_version": attribution["report_version"],
                "late_policy": attribution["late_policy"],
                "conclusion_id": (attribution["current"] or {}).get("conclusion_id"),
                "batch_no": (attribution["batch_conclusion"] or {}).get("batch_no")
                or entry.get("批次编号"),
                "category": (attribution["current"] or {}).get("category"),
                "is_current": True,
            }
        return enriched

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """执行详情：附上健康评估的缺陷归因、批次结论与证据链。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        enriched = dict(entry)
        enriched["重复导入"] = self._is_duplicate(entry)
        if not enriched["重复导入"]:
            enriched["归因"] = self._attribution_for(entry)
        return enriched

    def _attribution_for(self, entry: dict[str, Any]) -> dict[str, Any]:
        execution_no = str(entry.get("执行编号") or "")
        date = str(entry.get("开始时间") or "")[:10]
        flow = str(entry.get("所属流程") or "")
        reports = health_assessment.assess(date=date, flow=flow) if date and flow else []
        report = reports[0] if reports else None
        current = store.health.attribution_for(execution_no)
        conclusion = store.health.current_conclusion_for(
            str(entry.get("批次编号") or ""))
        return {
            "report_id": (report or {}).get("report_id"),
            "report_version": (report or {}).get("version"),
            "report_status": (report or {}).get("status"),
            "late_policy": (report or {}).get("late_policy"),
            "current": current,
            "batch_conclusion": conclusion,
            "reconciliation": (report or {}).get("reconciliation"),
        }

    def query_execution_history(self,
                                 bot_name: str | None = None,
                                 days: int = 7) -> list[dict[str, Any]]:
        """查询执行历史记录（同一执行编号只返回第一条）。

        每条执行都按运行概览记录的最新运行指标版本对齐，供运行概览核对。
        """
        rows = deduped_execution_rows()
        if bot_name:
            rows = [
                r for r in rows if bot_name in r.get("所属流程", "")
            ]
        metric_versions = self._runtime_metric_versions()
        return [
            {
                "id": r.get("id"), "exec_no": r.get("执行编号"),
                "bot": r.get("所属流程"), "status": r.get("status"),
                "start": r.get("开始时间"), "end": r.get("结束时间"),
                "error": r.get("错误摘要"),
                "batch_no": r.get("批次编号"), "task_no": r.get("任务编号"),
                "metric_version": metric_versions.get(
                    (str(r.get("开始时间") or "")[:10], str(r.get("所属流程") or ""))),
            }
            for r in rows
        ]

    @staticmethod
    def _runtime_metric_versions() -> dict[tuple[str, str], str]:
        """运行概览记录的运行指标数据集：按（统计日期, 流程）取最新到达版本。"""
        versions: dict[tuple[str, str], tuple[str, str]] = {}
        for row in store.rows("report_dashboard"):
            date = str(row.get("统计日期") or "")
            flow = str(row.get("所属流程") or "")
            if not date or not flow:
                continue
            arrived = str(row.get("到达时间") or "")
            current = versions.get((date, flow))
            if current is None or arrived >= current[0]:
                versions[(date, flow)] = (arrived, str(row.get("版本号") or ""))
        return {key: value[1] for key, value in versions.items()}

    def get_execution_stats(self) -> dict[str, Any]:
        """获取执行统计概览，口径与运行概览一致（去重后统计）。"""
        rows = deduped_execution_rows()
        total = len(rows)
        success = sum(1 for r in rows if r.get("status") == "执行成功")
        failed = sum(1 for r in rows if r.get("status") == "执行失败")
        timeout = sum(1 for r in rows if r.get("status") == "已超时")
        finished = success + failed + timeout
        return {
            "total_executions": total,
            "success_rate": round(success / max(finished, 1) * 100, 1),
            "success": success,
            "failed": failed,
            "timeout": timeout,
            "in_progress": sum(1 for r in rows if r.get("status") == "执行中"),
            "duplicates": sum(1 for item in duplicate_rows() if item["module"] == self.MODULE),
            "metrics_source": "运行概览运行指标数据集",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_execution_stats()
        return {**base, **stats}
