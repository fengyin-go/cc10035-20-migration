"""输出目标领域服务：管理结构化数据的输出分发配置。

实体分发数据的实际编解码、版本记录、读回核对和回滚都交给
``app.services.distribution``；本服务负责把配置、Schema 对齐规则和
迁移清单串起来，并在动作结算时更新分发状态。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.distribution import (
    FORMAT_MIME,
    DistributionError,
    DistributionService,
    _sniff,
)
from app.services.schema_align import SchemaAlignService
from app.store import store
from app.config import settings

OUTPUT_FORMATS = {"json": "application/json", "csv": "text/csv",
                  "parquet": "application/x-parquet", "avro": "application/avro"}

# 分发动作走迁移流水线；状态动作仍按 ACTION_RULES 流转。
DISTRIBUTION_ACTIONS = {"构建分发": "交付", "回滚版本": "回滚", "读回核对": "核对"}
ALL_ACTIONS = ["启用配置", "暂停配置", *DISTRIBUTION_ACTIONS]


def align_rule_for(config: dict[str, Any]) -> int | None:
    """输出配置绑定的对齐规则：显式「对齐规则」优先，否则取第一条已启用规则。"""
    value = config.get("对齐规则")
    if value not in (None, ""):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    for row in store.rows("schema_align"):
        if row.get("status") == "已启用":
            return int(row["id"])
    return None


class OutputTargetService(BaseModuleService):
    MODULE = "output_target"
    ENTITY = "输出配置"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["配置名称", "输出类型", "目标地址", "输出格式", "对齐规则", "分区策略", "刷新频率", "校验方式", "字段版本"]
    REQUIRED_FIELDS = ["配置名称", "输出类型", "目标地址"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用配置": "草稿", "暂停配置": "已启用"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.schema_align = SchemaAlignService()
        self._distribution: DistributionService | None = None

    # -- 分发服务（延迟创建，便于测试指向临时目录） -------------------------
    @property
    def distribution(self) -> DistributionService:
        if self._distribution is None:
            self._distribution = DistributionService()
        return self._distribution

    def reset_distribution(self, base_dir: str | None = None) -> DistributionService:
        self._distribution = DistributionService(base_dir or settings.output_data_dir)
        return self._distribution

    def _config_with_manifest(self, config_id: int) -> dict[str, Any] | None:
        config = store.find(self.MODULE, config_id)
        if config is None:
            return None
        self.distribution.attach_target(config, align_rule_for(config))
        return config

    # -- 原格式化接口：按声明格式编码，不再用 JSON 冒充其它格式 ---------------
    def format_entity_output(self, entities: list[dict[str, Any]],
                             config_id: int) -> dict[str, Any]:
        """将实体按输出配置的声明格式编码；正文格式始终与声明一致。"""
        config = self._config_with_manifest(config_id)
        if config is None:
            return {"ok": False, "reason": "输出配置不存在"}
        output_format = str(config.get("输出格式", "json")).lower()
        rule_id = align_rule_for(config)
        if rule_id is None:
            return {"ok": False, "reason": "输出配置没有可用的 Schema 对齐规则"}

        aligned = self.schema_align.align_entities_typed(entities, rule_id)
        if not aligned.get("ok"):
            return {"ok": False, "reason": aligned.get("reason", "Schema 对齐失败")}
        columns = aligned["column_names"]
        types = aligned["field_types"]
        try:
            payload = self.distribution._encode(
                output_format, aligned["aligned_records"], columns, types)
        except DistributionError as exc:
            return {"ok": False, "reason": str(exc)}
        return {
            "ok": True,
            "config_name": config.get("配置名称"),
            "output_type": config.get("输出类型"),
            "output_format": output_format,
            "target": config.get("目标地址"),
            "mime_type": FORMAT_MIME.get(output_format, "application/octet-stream"),
            "body_format": _sniff(payload),
            "declared_matches_actual": _sniff(payload) == output_format,
            "record_count": len(aligned["aligned_records"]),
            "columns": columns,
            "payload_size": len(payload),
            "payload_sample": payload[:200],
        }

    def build_output_chain(self, entity_extract_count: int,
                           schema_align_rule_id: int) -> dict[str, Any]:
        """构建从实体抽取到输出的完整链路的摘要信息。"""
        rule = store.find("schema_align", schema_align_rule_id)
        rows = store.rows(self.MODULE)
        active_outputs = [r for r in rows if r.get("status") == "已启用"]
        return {
            "entity_count": entity_extract_count,
            "align_rule": rule.get("规则名称") if rule else "未知",
            "active_output_targets": len(active_outputs),
            "target_types": [r.get("输出类型") for r in active_outputs],
            "output_chain_ready": len(active_outputs) > 0,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作结算：分发动作走迁移流水线，状态动作沿用基类规则。"""
        if action in DISTRIBUTION_ACTIONS:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if action == "构建分发":
                result = self.run_migration(entry_id)
                if not result.get("ok"):
                    entry["abnormal"] = True
                    return entry, f"构建分发失败：{result.get('error') or result.get('reason')}"
                entry["abnormal"] = False
                entry["pending"] = False
                return entry, (
                    f"已按 {result['declared_format']} 交付 v{result['version']}，"
                    f"读回 {result['verification']['row_count']} 行")
            if action == "回滚版本":
                result = self.rollback(entry_id)
                if not result.get("ok"):
                    return entry, f"回滚失败：{result.get('reason')}"
                entry["abnormal"] = False
                return entry, (
                    f"已回滚 v{result['previous_version']} → "
                    f"v{result['current_version']}，读回 {result['row_count']} 行")
            readback = self.read_version(entry_id, fallback_to_actual=True)
            if not readback.get("ok"):
                return entry, f"读回核对失败：{readback.get('reason')}"
            detail = (
                f"按 {readback['read_format_used']} 读回 {readback['row_count']} 行，"
                f"声明格式 {readback['declared_format']}，正文 {readback['body_sniff']}")
            detail += ("，与声明一致" if readback.get("readable_by_declared")
                       else f"，{readback.get('warning') or '声明与正文不一致'}")
            return entry, detail
        return super().run_action(entry_id, action)
        return super().run_action(entry_id, action)

    # -- 分发流水线 ---------------------------------------------------------
    def run_migration(self, config_id: int, *, dry_run: bool = False,
                      entities: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        config = self._config_with_manifest(config_id)
        if config is None:
            return {"ok": False, "reason": f"输出配置 {config_id} 不存在"}
        rule_id = align_rule_for(config)
        if rule_id is None:
            return {"ok": False, "reason": "输出配置没有可用的 Schema 对齐规则"}
        legacy_rule = self.schema_align.legacy_rule()
        result = self.distribution.run_migration(
            config, rule_id, entities=entities, dry_run=dry_run,
            legacy_rule_id=legacy_rule["id"] if legacy_rule else None,
        )
        result["align_rule_name"] = (store.find("schema_align", rule_id) or {}).get("规则名称")
        return result

    def rollback(self, config_id: int, to_version: int | None = None) -> dict[str, Any]:
        return self.distribution.rollback(config_id, to_version)

    def read_version(self, config_id: int, version: int | None = None,
                     fmt: str | None = None,
                     fallback_to_actual: bool = False,
                     reader: str = "declared") -> dict[str, Any]:
        return self.distribution.read_version(config_id, version, fmt,
                                              fallback_to_actual=fallback_to_actual,
                                              reader=reader)

    def migration_ledger(self) -> dict[str, Any]:
        rows = self.distribution.ledger_view()
        return {
            "ok": True,
            "total_targets": len(rows),
            "targets": rows,
            "summary": {
                "migrated": sum(1 for row in rows
                                if row["current_declared_matches_actual"]),
                "mismatched": sum(1 for row in rows
                                  if row["current_declared_matches_actual"] is False),
            },
        }

    def distribution_overview(self) -> dict[str, Any]:
        """给每个目标一屏可见：真实格式、字段版本、交付结果。"""
        ledger = self.migration_ledger()
        rows: list[dict[str, Any]] = []
        for config in store.rows(self.MODULE):
            config_id = int(config["id"])
            entry = next((item for item in ledger["targets"]
                          if item["config_id"] == config_id), None)
            latest = (entry["versions"][-1] if entry and entry["versions"] else None)
            current = next((version for version in (entry["versions"] if entry else [])
                            if version["is_current"]), None)
            rows.append({
                "config_id": config_id,
                "config_name": config.get("配置名称"),
                "declared_format": config.get("输出格式"),
                "target": config.get("目标地址"),
                "schema_version": config.get("字段版本"),
                "align_rule_id": align_rule_for(config),
                "current_version": entry["current_version"] if entry else None,
                "current_actual_format": current["actual_format"] if current else None,
                "declared_matches_actual": (
                    current["declared_matches_actual"] if current else None),
                "delivery_status": current["delivery_status"] if current else "未发放",
                "latest_version": latest["version"] if latest else None,
                "latest_status": latest["delivery_status"] if latest else None,
                "latest_conclusion": latest["conclusion"] if latest else None,
                "schema_receipt": (latest or {}).get("schema_receipt"),
                "version_count": len(entry["versions"]) if entry else 0,
            })
        return {"ok": True, "targets": rows, "summary": ledger["summary"]}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["output_stats"] = {
            "total_configs": len(rows),
            "formats": {r.get("输出格式"): 0 for r in rows},
            "active_targets": sum(1 for r in rows if r.get("status") == "已启用"),
        }
        for row in rows:
            fmt = row.get("输出格式")
            base["output_stats"]["formats"][fmt] = base["output_stats"]["formats"].get(fmt, 0) + 1
        return base
