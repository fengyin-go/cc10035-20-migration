"""Schema对齐领域服务：统一不同数据源的Schema到标准数据模型。

对外交付两类东西，供输出目标消费：

* ``build_output_schema`` / ``deliver_column_contract``：带字段类型、可空性、
  来源字段和默认值的列定义（字段契约）；
* ``align_entities_typed``：按契约对齐实体，保留空值（``None``）和整数，
  同时产出历史 JSON 读者使用的旧列名行。

返回的回执会列出哪些字段契约已经切换（改名/改类型）、哪些沿用旧列名，
由输出分发流水线写回迁移清单。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

SUPPORTED_TYPES = {"string", "integer", "number", "boolean"}
TYPE_ALIASES = {
    "str": "string",
    "text": "string",
    "varchar": "string",
    "utf8": "string",
    "int": "integer",
    "int32": "integer",
    "int64": "integer",
    "long": "integer",
    "float": "number",
    "double": "number",
    "decimal": "number",
    "bool": "boolean",
}


def normalise_type(raw: str | None) -> str:
    """把 ``int64`` / ``double`` / ``str`` 等写法归一到四种列类型。"""
    value = (raw or "string").strip().lower()
    if value in SUPPORTED_TYPES:
        return value
    return TYPE_ALIASES.get(value, "string")


def parse_target_schema(raw: str) -> list[dict[str, str]]:
    """解析 ``name:type, name2:type`` 形式的目标Schema。"""
    columns: list[dict[str, str]] = []
    for part in (raw or "").split(","):
        item = part.strip()
        if not item:
            continue
        if ":" in item:
            name, type_name = item.split(":", 1)
            columns.append({"name": name.strip(), "type": normalise_type(type_name)})
        else:
            columns.append({"name": item, "type": "string"})
    return columns


class SchemaAlignService(BaseModuleService):
    MODULE = "schema_align"
    ENTITY = "对齐规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则名称", "源Schema", "目标Schema", "字段映射", "值转换", "默认填充", "冲突处理"]
    REQUIRED_FIELDS = ["规则名称", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def parse_field_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射字符串 'src1->tgt1, src2->tgt2' 为字典。"""
        pairs = [p.strip() for p in mapping_str.split(",") if "->" in p]
        result: dict[str, str] = {}
        for pair in pairs:
            parts = pair.split("->", 1)
            if len(parts) == 2:
                result[parts[0].strip()] = parts[1].strip()
        return result

    def _default_map(self, raw: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for item in (raw or "").split(","):
            if ":" in item:
                key, value = item.split(":", 1)
                result[key.strip()] = value.strip()
        return result

    def legacy_rule(self, rule_id: int | None = None) -> dict[str, Any] | None:
        """取历史列语义的规则：显式指定，否则取第一条已停用规则。"""
        if rule_id is not None:
            return store.find(self.MODULE, rule_id)
        for row in store.rows(self.MODULE):
            if row.get("status") == "已停用":
                return row
        return None

    def deliver_column_contract(self, rule_id: int,
                                legacy_rule_id: int | None = None) -> dict[str, Any]:
        """把对齐规则解析成交付给输出目标的字段契约。"""
        rule = store.find(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "reason": f"对齐规则 {rule_id} 不存在"}

        mapping = self.parse_field_mapping(rule.get("字段映射", ""))
        defaults = self._default_map(rule.get("默认填充", ""))
        columns = parse_target_schema(rule.get("目标Schema", ""))
        target_names = [column["name"] for column in columns]

        legacy = self.legacy_rule(legacy_rule_id)
        legacy_mapping = self.parse_field_mapping(legacy.get("字段映射", "")) if legacy else {}
        legacy_columns = parse_target_schema(legacy.get("目标Schema", "")) if legacy else []
        # 源字段 -> 旧版目标列名（旧 JSON 正文的列语义）。
        legacy_by_source = {source: target for source, target in legacy_mapping.items()}
        # 源字段 -> 新版目标列名。
        target_by_source = {source: target for source, target in mapping.items()}

        for column in columns:
            name = column["name"]
            source = next((s for s, t in mapping.items() if t == name), None)
            column["source_field"] = source or ""
            column["legacy_field"] = legacy_by_source.get(source, "") if source else ""
            column["has_default"] = name in defaults
            column["default"] = defaults.get(name, "")
            column["nullable"] = True
            if column["legacy_field"] and column["legacy_field"] != name:
                column["contract_state"] = "renamed"
            elif column["legacy_field"] == name:
                column["contract_state"] = "kept"
            elif legacy and source:
                column["contract_state"] = "new"
            else:
                column["contract_state"] = "new"
            column["switched"] = column["contract_state"] in {"renamed", "new"}
            column["type_changed"] = False
            if legacy:
                old = next((c for c in legacy_columns if c["name"] == column["legacy_field"]), None)
                if old and old["type"] != column["type"]:
                    column["type_changed"] = True
                    column["switched"] = True

        legacy_only = [
            column for column in legacy_columns
            if column["name"] not in {
                target_by_source.get(source, "") for source in legacy_mapping
            }
        ]

        contract_payload = {
            "rule_id": rule_id,
            "rule_name": rule.get("规则名称"),
            "schema_version": rule.get("字段版本") or f"v{rule_id}",
            "columns": [
                {key: column[key] for key in
                 ("name", "type", "source_field", "legacy_field", "nullable", "default")}
                for column in columns
            ],
            "legacy_rule_id": legacy.get("id") if legacy else None,
        }
        digest = hashlib.sha256(
            json.dumps(contract_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()

        switched = [column for column in columns if column["switched"]]
        # 旧读者按旧列名取值；新旧列名相同时用同名字段承接。
        legacy_sources = {
            column["name"]: column["legacy_field"] or column["source_field"] or column["name"]
            for column in columns
        }
        for column in columns:
            column["legacy_key"] = legacy_sources[column["name"]]
        return {
            "ok": True,
            "rule_id": rule_id,
            "rule_name": rule.get("规则名称"),
            "schema_version": contract_payload["schema_version"],
            "contract_hash": digest[:16],
            "columns": columns,
            "column_names": target_names,
            "legacy_sources": legacy_sources,
            "legacy_rule_id": legacy.get("id") if legacy else None,
            "legacy_rule_name": legacy.get("规则名称") if legacy else None,
            "legacy_columns": legacy_columns,
            "legacy_only_columns": [column["name"] for column in legacy_only],
            "switched_fields": [column["name"] for column in switched],
            "kept_fields": [c["name"] for c in columns if c["contract_state"] == "kept"],
            "pending_fields": [c["name"] for c in columns if not c["source_field"]],
            "field_types": {column["name"]: column["type"] for column in columns},
        }

    def align_entities_typed(self, entity_extract_records: list[dict[str, Any]],
                             rule_id: int,
                             legacy_rule_id: int | None = None) -> dict[str, Any]:
        """按字段契约对齐实体，空值保留为 None，数字保留原类型。"""
        contract = self.deliver_column_contract(rule_id, legacy_rule_id)
        if not contract["ok"]:
            return contract

        aligned: list[dict[str, Any]] = []
        legacy_rows: list[dict[str, Any]] = []
        for record in entity_extract_records:
            target_row: dict[str, Any] = {}
            legacy_row: dict[str, Any] = {}
            for column in contract["columns"]:
                source = column["source_field"]
                value = record.get(source) if source else None
                if value in (None, "") and column["has_default"]:
                    value = column["default"]
                value = self._coerce(value, column["type"])
                target_row[column["name"]] = value
                legacy_row[column["legacy_key"]] = value
            aligned.append(target_row)
            legacy_rows.append(legacy_row)

        return {
            "ok": True,
            "rule_id": rule_id,
            "rule_name": contract["rule_name"],
            "schema_version": contract["schema_version"],
            "contract_hash": contract["contract_hash"],
            "columns": contract["columns"],
            "column_names": contract["column_names"],
            "legacy_columns": [column["name"] for column in contract["legacy_columns"]],
            "legacy_only_columns": contract["legacy_only_columns"],
            "legacy_sources": contract["legacy_sources"],
            "switched_fields": contract["switched_fields"],
            "kept_fields": contract["kept_fields"],
            "pending_fields": contract["pending_fields"],
            "field_types": contract["field_types"],
            "legacy_rule_id": contract["legacy_rule_id"],
            "legacy_rule_name": contract["legacy_rule_name"],
            "input_count": len(entity_extract_records),
            "aligned_records": aligned,
            "legacy_records": legacy_rows,
        }

    @staticmethod
    def _coerce(value: Any, type_name: str) -> Any:
        if value is None:
            return None
        if type_name == "integer":
            try:
                return int(value)
            except (TypeError, ValueError):
                return value
        if type_name == "number":
            try:
                return float(value)
            except (TypeError, ValueError):
                return value
        if type_name == "boolean":
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "是"}
            return bool(value)
        return value if isinstance(value, str) else str(value)

    def align_entities(self, entity_extract_records: list[dict[str, Any]],
                       rule_id: int) -> dict[str, Any]:
        """兼容旧调用：映射到目标Schema，缺失字段用默认值或空串填充。"""
        result = self.align_entities_typed(entity_extract_records, rule_id)
        if not result["ok"]:
            return result
        filled = []
        for row in result["aligned_records"]:
            filled.append({
                key: ("" if value is None else value) for key, value in row.items()
            })
        return {
            "ok": True,
            "rule_name": result["rule_name"],
            "source_schema": [s.strip() for s in
                              (store.find(self.MODULE, rule_id) or {}).get("源Schema", "").split(",")
                              if s.strip()],
            "target_schema": result["column_names"],
            "input_count": result["input_count"],
            "output_count": len(filled),
            "aligned_records": filled,
        }

    def build_output_schema(self, rule_id: int,
                            legacy_rule_id: int | None = None) -> dict[str, Any]:
        """构建输出目标消费的Schema定义（含类型、来源字段和契约状态）。"""
        contract = self.deliver_column_contract(rule_id, legacy_rule_id)
        if not contract["ok"]:
            return {"ok": False, "reason": "规则不存在"}
        rule = store.find(self.MODULE, rule_id) or {}
        return {
            "ok": True,
            "rule_id": rule_id,
            "rule_name": contract["rule_name"],
            "schema_version": contract["schema_version"],
            "contract_hash": contract["contract_hash"],
            "target_fields": contract["column_names"],
            "field_count": len(contract["columns"]),
            "columns": contract["columns"],
            "field_types": contract["field_types"],
            "mapping_rules": self.parse_field_mapping(rule.get("字段映射", "")),
            "value_transform": rule.get("值转换", ""),
            "conflict_handler": rule.get("冲突处理", "忽略"),
            "legacy_rule_id": contract["legacy_rule_id"],
            "switched_fields": contract["switched_fields"],
            "kept_fields": contract["kept_fields"],
            "pending_fields": contract["pending_fields"],
        }

    def contract_receipt(self, rule_id: int,
                         legacy_rule_id: int | None = None) -> dict[str, Any]:
        """字段契约回执：哪些字段完成切换、哪些沿用旧列名。"""
        contract = self.deliver_column_contract(rule_id, legacy_rule_id)
        if not contract["ok"]:
            return contract
        return {
            "ok": True,
            "rule_id": rule_id,
            "rule_name": contract["rule_name"],
            "schema_version": contract["schema_version"],
            "contract_hash": contract["contract_hash"],
            "legacy_rule_id": contract["legacy_rule_id"],
            "legacy_rule_name": contract["legacy_rule_name"],
            "switched_fields": contract["switched_fields"],
            "kept_fields": contract["kept_fields"],
            "pending_fields": contract["pending_fields"],
            "legacy_only_fields": contract["legacy_only_columns"],
            "columns": contract["columns"],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["alignment_stats"] = {
            "total_rules": len(rows),
            "active_rules": sum(1 for r in rows if r.get("status") == "已启用"),
            "with_mapping": sum(1 for r in rows if r.get("字段映射", "").strip()),
        }
        return base
