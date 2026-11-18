"""任务队列领域服务 — 消费编排结果生成执行任务。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor
    → task_queue 消费 → 生成排队任务 + execution_log 记录
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "流程名称", "提交时间", "优先级", "预计执行", "排队时长", "当前状态"]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []
    DEDUPE_FIELD = "任务编号"

    # ---- 跨模块业务方法 ----

    def consume_steps_to_queue(self) -> dict[str, Any]:
        """消费 step_editor 提交的编排结果，生成任务队列条目。

        从 channel("rpa.steps_submitted") 读取编排计划，
        为每个 bot 的每一步创建一个排队任务。

        数据流终点：这里是编排链路的末端 — 任务进入队列等待调度执行。

        Returns:
            {"generated": int, "plans_consumed": int, "task_ids": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        if not msgs:
            return {"generated": 0, "plans_consumed": 0, "task_ids": []}

        task_ids = []
        for msg in msgs:
            plan = msg.payload
            bot_id = plan.get("bot_id", 0)
            steps = plan.get("steps", [])
            if not steps:
                # 确认空计划
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            for i, step in enumerate(steps):
                seq = i + 1
                now = datetime.now(timezone.utc)
                task_number = f"TASK-{bot_id}-{seq:03d}"

                values = {
                    "任务编号": task_number,
                    "流程名称": f"Bot-{bot_id}",
                    "提交时间": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "优先级": "高" if seq <= 3 else "中",
                    "预计执行": step.get("name", f"Step-{seq}"),
                    "排队时长": "0s",
                    "当前状态": "排队中",
                }
                entry, missing = self.create_entry(values)
                if entry:
                    entry["step_id"] = step.get("step_id")
                    entry["bot_id"] = bot_id
                    entry["step_type"] = step.get("type", "")
                    task_ids.append(entry["id"])

            store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "generated": len(task_ids),
            "plans_consumed": len(msgs),
            "task_ids": task_ids,
        }

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log。

        Args:
            task_id: 任务 ID
            status: 目标状态（"执行中", "已完成", "执行失败"）
            result: 执行结果数据（可选）

        Returns:
            更新后的任务信息或 None
        """
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        entry["status"] = status
        entry["pending"] = status not in ("已完成", "已取消")
        entry["abnormal"] = status == "执行失败"

        if result:
            entry["result"] = result

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
        }

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """从排队中取下一批任务标记为执行中。

        模拟调度器的取任务逻辑 — 按优先级 + 提交时间排序。

        Returns:
            {"scheduled": int, "task_ids": [...]}
        """
        rows = store.rows(self.MODULE)
        queued = [
            r for r in rows
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        # 按优先级排序（高 > 中 > 低）
        priority_order = {"高": 0, "中": 1, "低": 2}
        queued.sort(key=lambda r: priority_order.get(r.get("优先级", "低"), 99))

        scheduled = []
        for task in queued[:max_tasks]:
            task["status"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            # 模拟执行记录
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "start_time": task["开始执行时间"],
                },
            )

        return {"scheduled": len(scheduled), "task_ids": scheduled}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
        }