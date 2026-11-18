"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。

跨模块数据流基础设施：DataChannel, DirtyDataVault, CollectRecordStore, FailureRecovery
"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.bot_definition import BotDefinitionService
from app.services.step_editor import StepEditorService
from app.services.element_selector import ElementSelectorService
from app.services.trigger_config import TriggerConfigService
from app.services.execution_log import ExecutionLogService
from app.services.data_extract import DataExtractService
from app.services.credential_store import CredentialStoreService
from app.services.env_profile import EnvProfileService
from app.services.anti_block import AntiBlockService
from app.services.result_verify import ResultVerifyService
from app.services.batch_tracker import BatchTrackerService
from app.services.page_model import PageModelService
from app.services.data_enrich import DataEnrichService
from app.services.proxy_router import ProxyRouterService
from app.services.rate_limiter import RateLimiterService
from app.services.output_format import OutputFormatService
from app.services.anomaly_log import AnomalyLogService
from app.services.scheduler import SchedulerService
from app.services.report_dashboard import ReportDashboardService
from app.services.task_queue import TaskQueueService
from app.services.runtime_session import RuntimeSessionService, runtime_session
from app.services.callback_hook import CallbackHookService, callback_hook
from app.services.health_assessment import HealthAssessmentService, health_assessment
from app.services.pipeline import (  # noqa: F401
    run_all_pipelines,
    pipeline_rpa_lifecycle,
    pipeline_trigger_to_queue,
    pipeline_element_to_output,
    pipeline_proxy_anti_block,
    pipeline_enrich_verify,
    pipeline_credential_env,
    pipeline_anomaly_to_exec,
    pipeline_batch_resupply,
    detect_missed_batches,
    trigger_resupply,
    PipelineResult,
    DirtyDataVault,
    RecoveryManager,
)

SERVICES: dict[str, BaseModuleService] = {
    "bot_definition": BotDefinitionService(),
    "step_editor": StepEditorService(),
    "element_selector": ElementSelectorService(),
    "trigger_config": TriggerConfigService(),
    "execution_log": ExecutionLogService(),
    "data_extract": DataExtractService(),
    "credential_store": CredentialStoreService(),
    "env_profile": EnvProfileService(),
    "anti_block": AntiBlockService(),
    "result_verify": ResultVerifyService(),
    "batch_tracker": BatchTrackerService(),
    "page_model": PageModelService(),
    "data_enrich": DataEnrichService(),
    "proxy_router": ProxyRouterService(),
    "rate_limiter": RateLimiterService(),
    "output_format": OutputFormatService(),
    "anomaly_log": AnomalyLogService(),
    "scheduler": SchedulerService(),
    "report_dashboard": ReportDashboardService(),
    "task_queue": TaskQueueService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)