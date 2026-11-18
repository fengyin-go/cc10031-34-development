"""跨模块运行健康评估测试：指标、缺陷归因、批次结论、迟到冲突与对账。

覆盖题位要求：
1. 成功率、失败任务、遗漏批次、任务未闭环分别汇总，并都能定位到执行与批次；
2. 每个下降指标都有执行编号与批次编号证据；
3. 同一执行失败只归入一个当前批次结论；
4. 重复导入只认第一条记录；
5. 失败重试后再次进入运行概览，能从同一份证据链核对更新后的指标；
6. 迟到失败完成事件的两种处理都只给失败一个当前归因。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from app.services.health_assessment import (
    HealthAssessmentService,
    dedupe_rows,
    deduped_batch_rows,
    deduped_execution_rows,
    duplicate_rows,
    health_assessment,
)
from app.services.report_dashboard import ReportDashboardService
from app.store import HealthLedger, store


DATE = "2026-11-18"
GOODS = "商品价格采集"
ORDER = "订单对账采集"
PUBLIC = "舆情抓取"


@pytest.fixture(autouse=True)
def fresh_ledger():
    """每个用例从干净的评估台账和模块数据开始，避免版本号/重试记录互相污染。"""
    rows = {
        module: [dict(row) for row in store.rows(module)]
        for module in ("execution_log", "batch_tracker", "task_queue", "report_dashboard")
    }
    store.health = HealthLedger()
    yield
    store.health = HealthLedger()
    for module, snapshot in rows.items():
        store.rows(module)[:] = [dict(row) for row in snapshot]


def _report(flow: str, policy: str | None = None) -> dict:
    reports = health_assessment.assess(date=DATE, flow=flow, policy=policy)
    assert reports, f"{flow} 没有健康报告"
    return reports[0]


def _metric(report: dict, key: str) -> dict:
    return next(item for item in report["metrics"] if item["key"] == key)


# ── 1. 指标分别汇总且可定位 ──

def test_success_rate_failure_links_to_execution_and_batch():
    report = _report(GOODS)
    metric = _metric(report, "success_rate")
    assert metric["declined"] is True
    assert metric["value"] == 0.0
    assert metric["evidence_execution_nos"] == ["EXEC-20261118-001"]
    assert metric["evidence_batch_nos"] == ["BATC-20261118-01"]


def test_failed_task_and_missed_batch_are_separately_summarised():
    report = _report(GOODS)
    failed_tasks = _metric(report, "failed_tasks")
    missed_batches = _metric(report, "missed_batches")
    assert failed_tasks["value"] == 1
    assert missed_batches["value"] == 1
    assert failed_tasks["key"] != missed_batches["key"]
    categories = {defect["category"] for defect in report["defects"]}
    assert {"execution_failure", "batch_omission"} <= categories


def test_task_not_closed_is_its_own_category():
    report = _report(PUBLIC)
    availability = _metric(report, "availability")
    assert availability["declined"] is True
    assert availability["evidence_execution_nos"] == ["EXEC-20261118-003"]
    assert availability["evidence_batch_nos"] == ["BATC-20261118-03"]
    categories = {defect["category"] for defect in report["defects"]}
    assert "task_not_closed" in categories


def test_every_declining_metric_has_drilldown_evidence():
    for flow in (GOODS, ORDER, PUBLIC):
        report = _report(flow)
        for metric in report["metrics"]:
            if not metric["declined"]:
                continue
            assert metric["evidence_execution_nos"], metric["key"]
            assert metric["evidence_batch_nos"], metric["key"]
            assert metric["defect_ids"], metric["key"]


# ── 2. 失败归因与批次结论唯一 ──

def test_execution_failure_has_exactly_one_current_attribution():
    _report(GOODS)
    attribution = health_assessment.execution_attribution("EXEC-20261118-001")
    current = [item for item in attribution["history"] if item["is_current"]]
    assert attribution["current"] is not None
    assert len(current) == 1
    assert attribution["current"]["batch_no"] == "BATC-20261118-01"


def test_batch_conclusion_collects_failure_once():
    report = _report(GOODS)
    conclusion = next(
        item for item in report["batch_conclusions"]
        if item["batch_no"] == "BATC-20261118-01")
    assert conclusion["failed_execution_nos"] == ["EXEC-20261118-001"]
    assert conclusion["category"] == "execution_failure"
    assert conclusion["defect_ids"].count("DEF-EXEC-EXEC-20261118-001") == 1


def test_evidence_chain_links_execution_batch_task():
    _report(GOODS)
    chain = health_assessment.execution_attribution("EXEC-20261118-001")["evidence_chain"]
    types = [item["type"] for item in chain]
    assert types[:3] == ["execution", "batch", "task"]
    assert chain[0]["ref"] == "EXEC-20261118-001"
    assert chain[1]["ref"] == "BATC-20261118-01"
    assert chain[2]["ref"] == "TASK-20261118-001"
    assert any(item["type"] == "batch_conclusion" for item in chain)


# ── 3. 重复导入只认第一条 ──

def test_dedupe_keeps_first_row():
    rows = [{"执行编号": "X", "id": 1}, {"执行编号": "X", "id": 2}, {"执行编号": "Y", "id": 3}]
    kept, duplicates = dedupe_rows(rows, "执行编号")
    assert [row["id"] for row in kept] == [1, 3]
    assert duplicates == [{
        "field": "执行编号", "key": "X", "duplicate_id": 2,
        "kept_id": 1, "status": "", "reason": "重复导入，只认第一条记录",
    }]


def test_duplicate_execution_does_not_change_metrics():
    executions = deduped_execution_rows()
    assert len(executions) == len({row["执行编号"] for row in executions})
    kept = next(row for row in executions if row["执行编号"] == "EXEC-20261118-001")
    assert kept["status"] == "执行失败"
    assert any(item["key"] == "EXEC-20261118-001" for item in duplicate_rows())


def test_duplicate_batch_and_task_are_reported():
    batches = deduped_batch_rows()
    assert len(batches) == len({row["批次编号"] for row in batches})
    modules = {item["module"] for item in duplicate_rows()}
    assert {"execution_log", "batch_tracker", "task_queue"} <= modules


# ── 4. 新到达运行指标对账 ──

def test_reconciliation_matches_seeded_runtime_metrics():
    report = _report(GOODS)
    assert report["reconciliation"]["status"] == "一致"
    assert report["reconciliation"]["runtime_metric_version"] == "RM-20261118-01"


def test_ingest_runtime_metric_flags_mismatch():
    payload = {
        "date": DATE, "flow": GOODS, "total_executions": 1, "success_count": 1,
        "failed_count": 0, "success_rate": 100.0, "missed_batches": 0,
        "failed_tasks": 0, "open_tasks": 0, "collected_total": 90,
        "version": "RM-TEST",
    }
    result = health_assessment.ingest_runtime_metric(payload)
    assert result["ok"] is True
    fields = {item["field"] for item in result["reconciliation"]["mismatches"]}
    assert {"成功率", "失败任务数", "遗漏批次数"} <= fields


# ── 5. 迟到完成事件的两种做法 ──

def test_late_failure_retain_keeps_metrics_and_appends_late_defect():
    report = _report(ORDER, policy="retain")
    late = [defect for defect in report["defects"] if defect["is_late"]]
    assert [item["execution_no"] for item in late] == ["EXEC-20261118-005"]
    # 原指标保留：迟到失败不计入成功率/失败任务
    assert _metric(report, "success_rate")["value"] == 100.0
    assert _metric(report, "failed_tasks")["value"] == 0
    assert late[0]["metric_keys"] == ["late_defects"]
    assert report["late_conflict"]["resolved"] == "retain"


def test_late_failure_reopen_replaces_metrics():
    _report(ORDER, policy="retain")
    report = _report(ORDER, policy="reopen")
    assert _metric(report, "success_rate")["value"] == 50.0
    assert _metric(report, "failed_tasks")["value"] == 1
    assert report["late_conflict"]["resolved"] == "reopen"
    assert report["version"] >= 2


def test_both_late_policies_give_failure_one_current_attribution():
    _report(ORDER, policy="retain")
    retain_current = health_assessment.execution_attribution("EXEC-20261118-005")["current"]
    _report(ORDER, policy="reopen")
    attribution = health_assessment.execution_attribution("EXEC-20261118-005")
    current_items = [item for item in attribution["history"] if item["is_current"]]
    assert len(current_items) == 1
    assert attribution["current"]["mode"] == "reopen"
    assert attribution["current"]["conclusion_id"] != retain_current["conclusion_id"]
    assert len(attribution["history"]) == 2


def test_reopen_marks_batch_conclusion_version_bump():
    _report(ORDER, policy="retain")
    reopened = _report(ORDER, policy="reopen")
    conclusion = next(
        item for item in reopened["batch_conclusions"]
        if item["batch_no"] == "BATC-20261118-02")
    assert conclusion["version"] >= 2
    assert conclusion["supersedes"] is not None


# ── 6. 同一证据链重复核对与重试 ──

def test_reentering_overview_reuses_evidence_chain():
    first = _report(GOODS)
    second = _report(GOODS)
    assert second["version"] == first["version"]
    assert second.get("reused_evidence") is True
    assert second["report_id"] == first["report_id"]
    assert second["metrics"] == first["metrics"]


def test_retry_keeps_evidence_chain_and_updates_metrics():
    before = _report(GOODS)
    before_total = _metric(before, "total_executions")["value"]
    result = health_assessment.retry_execution("EXEC-20261118-001")
    assert result["ok"] is True
    assert result["retry_execution_no"].startswith("EXEC-20261118-001-R")
    assert result["evidence_chain"][0]["ref"] == "EXEC-20261118-001"
    after = health_assessment.get_report(date=DATE, flow=GOODS)
    assert after["version"] > before["version"]
    # 重试先进入执行中：总执行数包含重试，失败归因仍挂在原执行上。
    assert _metric(after, "total_executions")["value"] == before_total + 1
    assert _metric(after, "failed_tasks")["value"] == 1
    assert health_assessment.execution_attribution(
        "EXEC-20261118-001")["current"] is not None


def test_report_versions_are_monotonic():
    _report(GOODS)
    _report(GOODS, policy="reopen")
    _report(GOODS)
    versions = [item["version"] for item in health_assessment.get_report(
        date=DATE, flow=GOODS)["history"]]
    assert versions == sorted(versions)
    assert len(versions) == len(set(versions))


# ── 7. 待办联动与既有口径 ──

def test_defects_create_and_close_todos():
    report = _report(GOODS)
    assert report["todos"], "缺陷应生成待办"
    defect_ids = {defect["defect_id"] for defect in report["defects"]}
    assert {todo["defect_id"] for todo in report["todos"]} <= defect_ids


def test_existing_modules_still_list_and_flow():
    from app.services import SERVICES

    for module in ("execution_log", "batch_tracker", "task_queue", "report_dashboard"):
        items, total = SERVICES[module].list_entries(page=1, size=200)
        assert total == len(items)
        assert total > 0
    # 既有动作流转仍然生效
    entry, message = SERVICES["execution_log"].run_action(1, "归档日志")
    assert entry is not None
    assert entry["status"] == "执行成功"


def test_report_dashboard_refresh_publishes_health_report():
    from app.services import SERVICES

    entry, message = SERVICES["report_dashboard"].run_action(4, "刷新概览")
    assert entry is not None
    assert entry["健康状态"] == "异常"
    assert entry["报告版本"].startswith("HR-2026-11-18-")
    assert "成功率" in entry["下降指标"]
    assert "报告" in message


def test_duplicate_rows_cannot_be_archived():
    from app.services import SERVICES

    # id=7 是 EXEC-20261118-001 的重复导入，只认第一条 id=4
    entry, message = SERVICES["execution_log"].run_action(7, "归档日志")
    assert entry is None
    assert "重复导入" in message
    assert SERVICES["execution_log"].get_entry(7)["重复导入"] is True
    assert SERVICES["execution_log"].get_entry(4)["重复导入"] is False


def test_report_dashboard_exposes_health_assets():
    dashboard = ReportDashboardService()
    summary = dashboard.health_summary(date=DATE)
    expected_flows = health_assessment.available_flows(DATE)
    assert summary["reports"] == len(expected_flows)
    assert summary["status"] in ("下降", "异常")
    assert summary["declining_metrics"]
    assert summary["defects_by_category"]
    assert summary["versions"]
    snapshot = dashboard.domain_snapshot()
    latest = dashboard.health_summary()
    assert snapshot["platform_health"] == latest["status"]
    assert snapshot["health"]["defects_by_category"] == latest["defects_by_category"]


def test_available_filters_cover_date_flow_status():
    service = HealthAssessmentService()
    assert {"2026-11-17", "2026-11-18"} <= set(service.available_dates())
    assert {GOODS, ORDER, PUBLIC} <= set(service.available_flows("2026-11-18"))
    reports = service.list_reports(date=DATE, status="异常")
    assert {GOODS, ORDER, PUBLIC} <= {report["scope"]["flow"] for report in reports}
    assert all(report["scope"]["date"] == DATE for report in reports)
