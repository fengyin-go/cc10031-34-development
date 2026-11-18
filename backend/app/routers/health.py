"""运行健康评估接口：健康指标、缺陷归因、批次结论和新到达指标对账一起发布。

固定路径必须注册在通用资源路由 ``/api/{module}`` 之前，否则会被动态路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.health_assessment import health_assessment

router = APIRouter(prefix="/api/health-assessment", tags=["运行健康评估"])


@router.get("", response_model=dict)
def list_health_reports(
    date: str | None = Query(default=None),
    flow: str | None = Query(default=None),
    status: str | None = Query(default=None),
    policy: str | None = Query(default=None),
) -> dict[str, Any]:
    """按日期、流程和状态查看健康指标、缺陷归因和批次证据。"""
    reports = health_assessment.assess(date=date, flow=flow, policy=policy)
    if status:
        reports = [item for item in reports if item.get("status") == status]
    return {
        "available_dates": health_assessment.available_dates(),
        "available_flows": health_assessment.available_flows(date),
        "filters": {"date": date, "flow": flow, "status": status, "policy": policy},
        "reports": reports,
        "duplicates": health_assessment.duplicates(),
    }


@router.get("/report", response_model=dict)
def get_health_report(
    date: str | None = Query(default=None),
    flow: str | None = Query(default=None),
    policy: str | None = Query(default=None),
    refresh: bool = Query(default=True),
) -> dict[str, Any]:
    report = health_assessment.get_report(
        date=date, flow=flow, policy=policy, refresh=refresh)
    if report is None:
        raise HTTPException(status_code=404, detail="没有可评估的运行数据")
    return report


@router.post("/reassess", response_model=dict)
def reassess(
    date: str | None = Query(default=None),
    flow: str | None = Query(default=None),
    policy: str | None = Query(default=None),
) -> dict[str, Any]:
    """刷新概览：重算指标并发布新的健康报告版本。"""
    reports = health_assessment.assess(date=date, flow=flow, policy=policy)
    return {"ok": True, "reports": reports}


@router.get("/executions/{execution_no}/attribution", response_model=dict)
def execution_attribution(execution_no: str) -> dict[str, Any]:
    """按执行编号补充缺陷归因，并返回 执行 → 批次 → 任务 → 批次结论 的证据链。"""
    result = health_assessment.execution_attribution(execution_no)
    if result["current"] is None and not result["evidence_chain"]:
        raise HTTPException(status_code=404, detail=f"执行 {execution_no} 无归因记录")
    return result


@router.post("/executions/{execution_no}/retry", response_model=dict)
def retry_execution(execution_no: str) -> dict[str, Any]:
    """失败重试后再次进入运行概览：同一证据链产出更新后的指标与归因。"""
    result = health_assessment.retry_execution(execution_no)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message", "重试失败"))
    return result


@router.post("/runtime-metrics", response_model=dict)
def ingest_runtime_metric(payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    """新到达的运行指标落库后立即与执行/批次证据对账。"""
    result = health_assessment.ingest_runtime_metric(payload)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("message", "运行指标不完整"))
    return result


@router.get("/todos", response_model=dict)
def health_todos(status: str = Query(default="待处理")) -> dict[str, Any]:
    todos = health_assessment.todos(status=status)
    return {"count": len(todos), "todos": todos}
