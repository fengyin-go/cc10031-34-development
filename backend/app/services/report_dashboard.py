"""运行概览领域服务 — 聚合展示 RPA 平台运行统计，并发布跨模块健康评估。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

运行概览不再只给出分散计数：每次读取概览都会调用健康评估服务，
把成功率、失败任务、遗漏批次、任务闭环率绑定到具体执行编号与批次编号，
健康指标和缺陷证据一起返回。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.health_assessment import (
    deduped_execution_rows,
    deduped_task_rows,
    health_assessment,
)
from app.store import store


class ReportDashboardService(BaseModuleService):
    MODULE = "report_dashboard"
    ENTITY = "运行数据"
    OPERATION_TERM = "采集"
    LIST_FIELDS = [
        "统计日期", "所属流程", "总执行数", "成功数", "失败数", "成功率",
        "遗漏批次数", "失败任务数", "未闭环任务数", "平均耗时", "采集总量", "可用率",
    ]
    REQUIRED_FIELDS = ["统计日期", "总执行数", "成功数"]
    STATUS_ORDER = ["正常", "下降", "异常"]
    ACTION_RULES = {"刷新概览": "正常"}
    NEGATIVE_ACTIONS: list[str] = []
    DEDUPE_FIELD = "版本号"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """刷新概览：重新评估证据链并发布新的健康报告版本。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message
        date = str(entry.get("统计日期") or "")
        flow = str(entry.get("所属流程") or "")
        if date:
            reports = health_assessment.assess(date=date, flow=flow or None)
            if reports:
                report = reports[0]
                # 概览状态跟随健康报告，避免“刷新即正常”掩盖下降。
                entry["status"] = report["status"]
                entry["pending"] = report["status"] != "正常"
                entry["abnormal"] = report["status"] == "异常"
                entry["健康状态"] = report["status"]
                entry["报告版本"] = report["report_id"]
                entry["下降指标"] = [
                    metric["label"] for metric in report["metrics"] if metric["declined"]
                ]
                message = f"运行概览已刷新，报告 {report['report_id']}"
        return entry, message

    def health_summary(self, date: str | None = None, flow: str | None = None) -> dict[str, Any]:
        """健康概览：状态、下降指标数、缺陷归类计数和最新报告版本。"""
        reports = health_assessment.assess(date=date, flow=flow)
        declining = [
            {"scope": report["scope"], "metric": metric["key"],
             "label": metric["label"], "value": metric["value"],
             "baseline": metric["baseline"], "delta": metric["delta"]}
            for report in reports
            for metric in report["metrics"] if metric["declined"]
        ]
        by_category: dict[str, int] = {}
        for report in reports:
            for defect in report["defects"]:
                by_category[defect["category"]] = by_category.get(defect["category"], 0) + 1
        return {
            "reports": len(reports),
            "status": self._overall_status(reports),
            "declining_metrics": declining,
            "defects_by_category": by_category,
            "late_conflicts": [
                {"scope": report["scope"], **report["late_conflict"]}
                for report in reports if report["late_conflict"]["present"]
            ],
            "versions": {
                report["scope_key"]: report["version"] for report in reports
            },
            "todos": [
                todo for report in reports for todo in report.get("todos", [])
            ],
        }

    @staticmethod
    def _overall_status(reports: list[dict[str, Any]]) -> str:
        statuses = {report["status"] for report in reports}
        if "异常" in statuses:
            return "异常"
        if "下降" in statuses:
            return "下降"
        return "正常"

    def aggregate_platform_stats(self) -> dict[str, Any]:
        """跨模块聚合平台运行统计，并附上健康评估得到的缺陷资产。"""
        collect = store.collect_records.snapshot()
        dirty = store.dirty_vault.snapshot()
        recovery = store.recovery.snapshot()

        # 任务队列状态（去重后，与健康评估口径一致）
        task_rows = deduped_task_rows()
        task_completed = sum(1 for r in task_rows if r.get("status") == "已完成")
        task_failed = sum(1 for r in task_rows if r.get("status") == "执行失败")

        # 执行日志
        exec_rows = deduped_execution_rows()
        exec_success = sum(1 for r in exec_rows if r.get("status") == "执行成功")

        # 通道流量
        total_msgs = store.channel.total_messages
        unconfirmed = store.channel.uncomfirmed_count()

        health = self.health_summary()

        return {
            "collect_records": collect["total"],
            "validated_records": collect["validated"],
            "dirty_records": dirty["total"],
            "recovery_pending": recovery["pending"],
            "tasks_completed": task_completed,
            "tasks_failed": task_failed,
            "exec_success": exec_success,
            "channel_messages": total_msgs,
            "channel_unconfirmed": unconfirmed,
            "platform_health": health["status"],
            "health": health,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        agg = self.aggregate_platform_stats()
        return {**base, **agg}
