"""模块服务基类：共享查询、登记、动作和统计骨架，业务差异留在各模块文件。"""
from __future__ import annotations

from typing import Any

from app.store import store


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []
    DEDUPE_FIELD = ""

    def __init__(self) -> None:
        self.last_create_note = ""

    def _dedupe(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """重复导入只认第一条：按业务编号保留首个登记记录。"""
        if not self.DEDUPE_FIELD:
            return rows
        seen: set[str] = set()
        kept: list[dict[str, Any]] = []
        for row in rows:
            key = str(row.get(self.DEDUPE_FIELD) or "").strip()
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            kept.append(row)
        return kept

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._dedupe(list(store.rows(self.MODULE)))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        if self.DEDUPE_FIELD:
            key = str(values.get(self.DEDUPE_FIELD) or "").strip()
            existing = next(
                (row for row in rows
                 if key and str(row.get(self.DEDUPE_FIELD) or "").strip() == key),
                None,
            )
            if existing is not None:
                # 重复导入只认第一条：直接返回首条记录，不新增副本。
                self.last_create_note = (
                    f"{self.DEDUPE_FIELD} {key} 已存在，重复导入只认第一条")
                return existing, []
        self.last_create_note = ""
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if self.DEDUPE_FIELD and self._is_duplicate(entry):
            return None, (
                f"{self.DEDUPE_FIELD} {entry.get(self.DEDUPE_FIELD)} 为重复导入，"
                "只认第一条记录")
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def _is_duplicate(self, entry: dict[str, Any]) -> bool:
        if not self.DEDUPE_FIELD:
            return False
        key = str(entry.get(self.DEDUPE_FIELD) or "").strip()
        if not key:
            return False
        first = next(
            (row for row in store.rows(self.MODULE)
             if str(row.get(self.DEDUPE_FIELD) or "").strip() == key),
            None,
        )
        return first is not None and int(first.get("id") or 0) != int(entry.get("id") or 0)

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
