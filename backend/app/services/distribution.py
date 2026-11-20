"""输出分发流水线：把存量列式目标迁移成“声明格式 == 实际正文”的版本。

每个输出目标维护一串不可变的版本记录（``v1..vN``），并指向一个当前版本。
切换时：

1. 用 Schema 对齐的字段契约编码实体分发数据集，按目标声明的格式真正写出；
2. 读回新文件核对行数、空值位置、整数类型和列顺序；
3. 为旧 JSON 读取方额外写一份历史列语义的 JSON 正文（同一批数据双读）；
4. 全部通过后才原子替换当前版本；任一步失败只记录一条失败尝试，
   当前版本和既有文件保持不变，保证可回滚。

历史版本（含“声明 Parquet、正文 JSON”的存量文件）按实际正文格式登记，
读取时如实报告 ``declared_format`` 与 ``actual_format`` 是否一致。
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import settings
from app.seed_distribution import LEGACY_DELIVERIES, LEGACY_RECORDS, SAMPLE_ENTITIES
from app.services import parquet_codec
from app.services.schema_align import SchemaAlignService
from app.store import store

FORMAT_EXT = {"json": "json", "csv": "csv", "parquet": "parquet"}
FORMAT_MIME = {
    "json": "application/json",
    "csv": "text/csv",
    "parquet": "application/x-parquet",
    "avro": "application/avro",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DistributionError(ValueError):
    """分发流水线无法完成切换。"""


class DistributionLedger:
    """迁移清单：版本记录 + 当前版本指针，落盘为 manifest.json。"""

    def __init__(self, base_dir: str | Path) -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.base_dir / "manifest.json"
        self.targets: dict[str, dict[str, Any]] = {}
        self._load()

    # -- persistence --------------------------------------------------------
    def _load(self) -> None:
        if self.path.exists():
            try:
                payload = json.loads(self.path.read_text(encoding="utf-8"))
                self.targets = payload.get("targets", {})
                return
            except (json.JSONDecodeError, OSError):
                self.targets = {}
        self._seed()

    def _seed(self) -> None:
        for config_id, versions in LEGACY_DELIVERIES.items():
            self.targets[str(config_id)] = {
                "config_id": config_id,
                "current_version": max(item["version"] for item in versions),
                "versions": [self._legacy_version(config_id, item) for item in versions],
            }
        self.save()

    @staticmethod
    def _legacy_version(config_id: int, item: dict[str, Any]) -> dict[str, Any]:
        actual_format = "json"
        return {
            "version": item["version"],
            "declared_format": item["declared_format"],
            "actual_format": actual_format,
            "schema_version": item["schema_version"],
            "created_at": item.get("created_at") or _now(),
            "note": item.get("note", ""),
            "row_count": len(LEGACY_RECORDS),
            "size": 0,
            "path": "",
            "legacy_format": actual_format,
            "legacy_path": "",
            "declared_mime": FORMAT_MIME.get(item["declared_format"], "application/octet-stream"),
            "delivery_status": "历史版本",
            "readable_by_declared": item["declared_format"] == actual_format,
            "readable_by_actual": True,
            "conclusion": {
                "migrated": False,
                "declared_matches_actual": item["declared_format"] == actual_format,
                "detail": (
                    "存量文件声明为 " + item["declared_format"]
                    + "，正文实际为 " + actual_format
                    if item["declared_format"] != actual_format
                    else "声明与正文一致"
                ),
            },
            "schema_receipt": None,
            "attempts": [],
        }

    def save(self) -> None:
        payload = {"generated_at": _now(), "targets": self.targets}
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    # -- helpers ------------------------------------------------------------
    def ensure_legacy_artifact(self, config_id: int, version: dict[str, Any]) -> None:
        """把种子里的历史版本落成真实文件，供切换前按声明格式读回核对。"""
        if version.get("path"):
            return
        target_dir = self.base_dir / f"target_{config_id}"
        target_dir.mkdir(parents=True, exist_ok=True)
        file_name = f"v{version['version']}.{version['actual_format']}"
        path = target_dir / file_name
        if not path.exists():
            path.write_text(
                json.dumps(LEGACY_RECORDS, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        version["path"] = str(path.relative_to(self.base_dir))
        version["size"] = path.stat().st_size
        version["row_count"] = len(LEGACY_RECORDS)

    def next_version(self, config_id: int) -> int:
        record = self.targets.setdefault(str(config_id), {
            "config_id": config_id, "current_version": 0, "versions": [],
        })
        return max((item["version"] for item in record["versions"]), default=0) + 1

    def current(self, config_id: int) -> dict[str, Any] | None:
        record = self.targets.get(str(config_id))
        if not record:
            return None
        for item in record["versions"]:
            if item["version"] == record["current_version"]:
                return item
        return None


class DistributionService:
    """输出目标 -> 实体分发数据集的构建、核对与版本管理。"""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self.base_dir = Path(base_dir or settings.output_data_dir)
        self.ledger = DistributionLedger(self.base_dir)
        self.schema_align = SchemaAlignService()

    # -- manifest -----------------------------------------------------------
    def ensure_seeded(self) -> None:
        """为没有落盘文件的历史版本补齐正文，并回写清单。"""
        changed = False
        for config_id, record in self.ledger.targets.items():
            for version in record["versions"]:
                if not version.get("path"):
                    self.ledger.ensure_legacy_artifact(int(config_id), version)
                    changed = True
        if changed:
            self.ledger.save()

    def attach_target(self, config: dict[str, Any], align_rule_id: int | None) -> dict[str, Any]:
        """把输出配置的声明信息同步进迁移清单。"""
        config_id = int(config["id"])
        record = self.ledger.targets.setdefault(str(config_id), {
            "config_id": config_id, "current_version": 0, "versions": [],
        })
        record.update({
            "config_name": config.get("配置名称"),
            "target": config.get("目标地址"),
            "declared_format": config.get("输出格式", "json"),
            "schema_version": config.get("字段版本", ""),
            "align_rule_id": align_rule_id if align_rule_id is not None else config.get("对齐规则"),
        })
        return record

    def ledger_view(self) -> list[dict[str, Any]]:
        self.ensure_seeded()
        rows: list[dict[str, Any]] = []
        for key in sorted(self.ledger.targets, key=lambda item: int(item)):
            record = self.ledger.targets[key]
            current = self.ledger.current(int(key))
            config = store.find("output_target", int(key)) if store else None
            if config is not None:
                self.attach_target(config, config.get("对齐规则"))
                record = self.ledger.targets[key]
                current = self.ledger.current(int(key))
            versions = []
            for version in record["versions"]:
                versions.append({
                    "version": version["version"],
                    "declared_format": version["declared_format"],
                    "actual_format": version["actual_format"],
                    "schema_version": version["schema_version"],
                    "row_count": version["row_count"],
                    "size": version["size"],
                    "created_at": version["created_at"],
                    "delivery_status": version.get("delivery_status", ""),
                    "readable_by_declared": version.get("readable_by_declared"),
                    "readable_by_actual": version.get("readable_by_actual"),
                    "declared_matches_actual": (
                        version["declared_format"] == version["actual_format"]),
                    "conclusion": version.get("conclusion"),
                    "note": version.get("note", ""),
                    "is_current": current is not None
                    and version["version"] == current["version"],
                })
            rows.append({
                "config_id": int(key),
                "config_name": record.get("config_name", ""),
                "target": record.get("target", ""),
                "declared_format": record.get("declared_format", ""),
                "schema_version": record.get("schema_version", ""),
                "align_rule_id": record.get("align_rule_id"),
                "current_version": record.get("current_version"),
                "current_actual_format": current["actual_format"] if current else None,
                "current_declared_matches_actual": (
                    current["declared_format"] == current["actual_format"]
                    if current else None),
                "versions": versions,
            })
        return rows

    # -- reading ------------------------------------------------------------
    def _resolve(self, config_id: int, version: int | None) -> dict[str, Any]:
        if str(config_id) not in self.ledger.targets:
            self.ensure_seeded()
        record = self.ledger.targets.get(str(config_id))
        if record is None:
            raise DistributionError(f"输出目标 {config_id} 没有迁移清单记录")
        if version is None:
            current = self.ledger.current(config_id)
            if current is None:
                raise DistributionError(f"输出目标 {config_id} 没有可用版本")
            return current
        for item in record["versions"]:
            if item["version"] == version:
                return item
        raise DistributionError(f"输出目标 {config_id} 不存在版本 v{version}")

    def read_version(self, config_id: int, version: int | None = None,
                     fmt: str | None = None,
                     fallback_to_actual: bool = False,
                     reader: str = "declared") -> dict[str, Any]:
        """读回某个版本。

        ``reader`` 指读取方的口径：

        * ``declared``：按声明格式读主体文件（默认）；
        * ``actual``：按正文真实格式读主体文件；
        * ``legacy``：按旧 JSON 读者口径读同一批数据的历史列语义副本。

        ``readable_by_declared`` 只在“按声明格式解码成功、且正文嗅探确为声明
        格式”时为真；存量 JSON 正文冒充 Parquet 的版本会如实返回 False。
        ``fallback_to_actual`` 打开时，声明格式读取失败会按正文真实格式再读一次，
        并把 ``read_format_used``/``fallback_used`` 标出来。
        """
        item = self._resolve(config_id, version)
        self.ledger.ensure_legacy_artifact(config_id, item)
        actual = item["actual_format"]
        if reader == "legacy":
            if not item.get("legacy_path"):
                if actual != "json":
                    return {
                        "config_id": config_id,
                        "version": item["version"],
                        "reader": reader,
                        "ok": False,
                        "reason": "该版本没有旧 JSON 读者副本",
                        "declared_format": item["declared_format"],
                        "actual_format": actual,
                    }
                # 存量版本的主体正文本身就是旧 JSON 正文，旧读者直接读它。
                item = {**item, "legacy_path": item["path"], "legacy_is_primary": True}
            item = {**item, "path": item["legacy_path"]}
            fmt = "json"
        elif reader == "actual":
            fmt = actual
        elif reader != "declared":
            raise DistributionError(f"未知的读取方口径 {reader}")
        path = self.base_dir / item["path"] if item.get("path") else None
        underlying = fmt or item["declared_format"]
        base = {
            "config_id": config_id,
            "version": item["version"],
            "reader": reader,
            "requested_format": underlying,
            "declared_format": item["declared_format"],
            "actual_format": actual,
            "schema_version": item["schema_version"],
        }
        if path is None or not path.exists():
            return {**base, "ok": False, "reason": "版本文件不存在",
                    "readable_by_declared": False,
                    "declared_matches_actual": item["declared_format"] == actual}
        raw = path.read_bytes()
        sniffed = _sniff(raw)
        base["size"] = len(raw)
        base["sha256"] = _sha256(raw)
        base["body_sniff"] = sniffed
        if reader == "legacy":
            # 旧读者副本本来就是 JSON，声明格式不适用于这份副本。
            base["reader_copy"] = True
            base["declared_matches_actual"] = True
        else:
            base["declared_matches_actual"] = sniffed == item["declared_format"]
        base["readable_by_declared"] = False
        try:
            columns, records = self._decode(raw, underlying, item)
        except Exception as exc:  # noqa: BLE001 - 读取失败要如实回报而不是抛出
            failed = {
                **base,
                "ok": False,
                "reason": f"按 {underlying} 读取失败：{exc}",
                "read_format_used": underlying,
                "fallback_used": False,
            }
            if not fallback_to_actual or sniffed == underlying or sniffed == "unknown":
                return failed
            try:
                columns, records = self._decode(raw, sniffed, item)
            except Exception as retry_exc:  # noqa: BLE001
                return {**failed, "reason": failed["reason"] + f"；按 {sniffed} 读取也失败：{retry_exc}"}
            return {
                **base,
                "ok": True,
                "readable_by_declared": False,
                "path": str(path),
                "read_format_used": sniffed,
                "fallback_used": True,
                "columns": columns,
                "row_count": len(records),
                "null_counts": {
                    column: sum(1 for row in records if row.get(column) is None)
                    for column in columns
                },
                "integer_columns": [],
                "records": records,
                "warning": failed["reason"] + f"；已按实际正文格式 {sniffed} 读回",
            }
        return {
            **base,
            "ok": True,
            "readable_by_declared": (
                True if reader == "legacy" else sniffed == item["declared_format"]),
            "read_format_used": underlying,
            "fallback_used": False,
            "path": str(path),
            "columns": columns,
            "row_count": len(records),
            "null_counts": {
                column: sum(1 for row in records if row.get(column) is None)
                for column in columns
            },
            "integer_columns": [
                column for column in columns
                if all(isinstance(row.get(column), int) and not isinstance(row.get(column), bool)
                       for row in records if row.get(column) is not None)
            ],
            "records": records,
        }

    def _decode(self, raw: bytes, fmt: str,
                item: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
        if fmt == "parquet":
            if not parquet_codec.is_parquet(raw):
                raise DistributionError("文件缺少 PAR1 魔数，正文不是 Parquet")
            return parquet_codec.read_parquet(raw)
        if fmt == "json":
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise DistributionError("正文不是合法 JSON") from exc
            if not isinstance(payload, list):
                raise DistributionError("JSON 正文不是记录数组")
            columns: list[str] = []
            for row in payload:
                for key in row:
                    if key not in columns:
                        columns.append(key)
            return columns, payload
        if fmt == "csv":
            text = raw.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            records = [dict(row) for row in reader]
            return list(reader.fieldnames or []), records
        raise DistributionError(f"暂不支持读取格式 {fmt}")

    # -- building -----------------------------------------------------------
    def _encode(self, fmt: str, records: list[dict[str, Any]],
                columns: list[str], types: dict[str, str]) -> bytes:
        if fmt == "parquet":
            return parquet_codec.write_parquet(records, columns=columns, types=types)
        if fmt == "json":
            return json.dumps(records, ensure_ascii=False, indent=2).encode("utf-8")
        if fmt == "csv":
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
            writer.writeheader()
            for row in records:
                writer.writerow({
                    column: ("" if row.get(column) is None else row.get(column))
                    for column in columns
                })
            return buffer.getvalue().encode("utf-8")
        raise DistributionError(f"暂不支持写出格式 {fmt}")

    def _verify(self, fmt: str, raw: bytes, expected: list[dict[str, Any]],
                columns: list[str], types: dict[str, str]) -> dict[str, Any]:
        decoded_columns, decoded = self._decode(raw, fmt, {})
        issues: list[str] = []
        if decoded_columns != columns:
            issues.append(f"列顺序不一致：{decoded_columns} != {columns}")
        if len(decoded) != len(expected):
            issues.append(f"行数不一致：{len(decoded)} != {len(expected)}")
        for index, (actual_row, expected_row) in enumerate(zip(decoded, expected)):
            for column in columns:
                actual = actual_row.get(column)
                wanted = expected_row.get(column)
                if fmt == "csv" and wanted is None:
                    wanted = ""
                if actual != wanted:
                    issues.append(
                        f"第 {index + 1} 行字段 {column} 不一致：{actual!r} != {wanted!r}")
                wanted_type = types.get(column)
                if wanted_type == "integer" and wanted is not None and fmt != "csv":
                    if not isinstance(actual, int) or isinstance(actual, bool):
                        issues.append(f"第 {index + 1} 行字段 {column} 不是整数类型")
        null_counts = {
            column: sum(1 for row in decoded if row.get(column) is None)
            for column in decoded_columns
        }
        return {
            "ok": not issues,
            "format": fmt,
            "row_count": len(decoded),
            "columns": decoded_columns,
            "null_counts": null_counts,
            "issues": issues,
        }

    def _backend_readback(self, raw: bytes) -> dict[str, Any]:
        """有 pyarrow 时用真实 Parquet 实现再验一遍（可选依赖）。"""
        try:
            import pyarrow.parquet as pq  # type: ignore
        except ImportError:
            return {"available": False, "ok": None}
        import pyarrow as pa  # type: ignore

        try:
            table = pq.read_table(pa.BufferReader(raw))
        except Exception as exc:  # noqa: BLE001
            return {"available": True, "ok": False, "reason": str(exc)}
        return {
            "available": True,
            "ok": True,
            "schema": [
                {"name": field.name, "type": str(field.type)} for field in table.schema
            ],
            "row_count": table.num_rows,
        }

    def run_migration(self, config: dict[str, Any], align_rule_id: int,
                      entities: list[dict[str, Any]] | None = None,
                      dry_run: bool = False,
                      legacy_rule_id: int | None = None) -> dict[str, Any]:
        """把一批实体按声明格式构建成新版本；失败不留半成品版本。"""
        config_id = int(config["id"])
        declared_format = str(config.get("输出格式", "json")).lower()
        self.ensure_seeded()
        record = self.attach_target(config, align_rule_id)

        source = entities if entities is not None else list(SAMPLE_ENTITIES)
        aligned = self.schema_align.align_entities_typed(
            source, align_rule_id, legacy_rule_id=legacy_rule_id)
        if not aligned.get("ok"):
            raise DistributionError(aligned.get("reason", "Schema 对齐失败"))
        receipt = self.schema_align.contract_receipt(align_rule_id, legacy_rule_id)
        columns = aligned["column_names"]
        types = aligned["field_types"]
        rows = aligned["aligned_records"]

        version = self.ledger.next_version(config_id)
        target_dir = self.base_dir / f"target_{config_id}"
        file_name = f"v{version}.{FORMAT_EXT.get(declared_format, declared_format)}"
        legacy_name = f"v{version}.legacy.json"
        path = target_dir / file_name

        summary: dict[str, Any] = {
            "config_id": config_id,
            "config_name": config.get("配置名称"),
            "declared_format": declared_format,
            "align_rule_id": align_rule_id,
            "legacy_rule_id": legacy_rule_id,
            "schema_version": receipt["schema_version"],
            "contract_hash": receipt["contract_hash"],
            "version": version,
            "dry_run": dry_run,
            "input_count": len(source),
            "row_count": len(rows),
            "columns": columns,
            "field_types": types,
            "schema_receipt": {
                "switched_fields": receipt["switched_fields"],
                "kept_fields": receipt["kept_fields"],
                "pending_fields": receipt["pending_fields"],
                "legacy_only_fields": receipt["legacy_only_fields"],
                "columns": receipt["columns"],
            },
        }

        try:
            if declared_format not in FORMAT_EXT:
                raise DistributionError(
                    f"输出格式 {declared_format} 不在分发支持范围内")
            target_dir.mkdir(parents=True, exist_ok=True)
            raw = self._encode(declared_format, rows, columns, types)
            verification = self._verify(declared_format, raw, rows, columns, types)
            if declared_format == "parquet":
                backend = self._backend_readback(raw)
            else:
                backend = {"available": False, "ok": None}
            legacy_raw = json.dumps(aligned["legacy_records"], ensure_ascii=False,
                                    indent=2).encode("utf-8")
            legacy_verification = self._verify(
                "json", legacy_raw, aligned["legacy_records"],
                list(aligned["legacy_records"][0].keys()) if aligned["legacy_records"] else [],
                {})
            body_ok = verification["ok"] and legacy_verification["ok"] and backend["ok"] is not False
            if not body_ok:
                raise DistributionError(
                    "构建产物读回核对失败：" + "；".join(
                        verification["issues"] + legacy_verification["issues"]
                        + ([backend.get("reason", "")] if backend["ok"] is False else [])
                    ))
        except Exception as exc:  # noqa: BLE001 - 失败尝试要落清单，保持旧版本可用
            record["versions"].append({
                "version": version,
                "declared_format": declared_format,
                "actual_format": declared_format,
                "schema_version": receipt["schema_version"],
                "created_at": _now(),
                "row_count": 0,
                "size": 0,
                "path": "",
                "note": "迁移尝试失败",
                "delivery_status": "切换失败",
                "readable_by_declared": False,
                "readable_by_actual": False,
                "conclusion": {"migrated": False, "detail": str(exc)},
                "schema_receipt": summary["schema_receipt"],
                "attempts": [{"at": _now(), "ok": False, "error": str(exc)}],
            })
            self.ledger.save()
            return {
                **summary,
                "ok": False,
                "status": "切换失败",
                "error": str(exc),
                "current_version": record.get("current_version"),
                "rollback_available": record.get("current_version") is not None,
                "verification": {"ok": False, "issues": [str(exc)]},
            }

        if dry_run:
            return {
                **summary,
                "ok": True,
                "status": "试跑通过",
                "dry_run": True,
                "size": len(raw),
                "verification": verification,
                "legacy_verification": legacy_verification,
                "backend_readback": backend,
                "current_version": record.get("current_version"),
                "conclusion": {
                    "migrated": False,
                    "detail": "试跑完成，未写入新版本",
                    "declared_matches_actual": True,
                    "schema_receipt": summary["schema_receipt"],
                    "verification": {
                        "ok": verification["ok"],
                        "row_count": verification["row_count"],
                        "null_counts": verification["null_counts"],
                    },
                    "legacy_verification": {
                        "ok": legacy_verification["ok"],
                        "row_count": legacy_verification["row_count"],
                    },
                },
            }

        tmp_dir = Path(tempfile.mkdtemp(prefix=".staging-", dir=self.base_dir))
        try:
            staged = tmp_dir / file_name
            staged.write_bytes(raw)
            staged_legacy = tmp_dir / legacy_name
            staged_legacy.write_bytes(legacy_raw)
            os.replace(staged, path)
            os.replace(staged_legacy, target_dir / legacy_name)
            # 落盘后再读一次，确认最终文件仍然可读。
            final_raw = path.read_bytes()
            final_check = self._verify(declared_format, final_raw, rows, columns, types)
            if not final_check["ok"]:
                path.unlink(missing_ok=True)
                raise DistributionError("落盘后复核失败：" + "；".join(final_check["issues"]))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

        previous = record.get("current_version")
        entry = {
            "version": version,
            "declared_format": declared_format,
            "actual_format": declared_format,
            "schema_version": receipt["schema_version"],
            "created_at": _now(),
            "row_count": len(rows),
            "size": len(raw),
            "path": str(path.relative_to(self.base_dir)),
            "legacy_format": "json",
            "legacy_path": str((target_dir / legacy_name).relative_to(self.base_dir)),
            "note": "按声明格式重建实体分发数据集",
            "delivery_status": "已交付",
            "readable_by_declared": True,
            "readable_by_actual": True,
            "conclusion": {
                "migrated": True,
                "declared_matches_actual": True,
                "previous_version": previous,
                "detail": f"实体分发数据集已按 {declared_format} 重建并读回核对",
                "schema_receipt": summary["schema_receipt"],
                "verification": {
                    "ok": verification["ok"],
                    "row_count": verification["row_count"],
                    "null_counts": verification["null_counts"],
                },
            },
            "schema_receipt": summary["schema_receipt"],
            "verification": verification,
            "attempts": [{"at": _now(), "ok": True}],
        }
        record["versions"].append(entry)
        record["current_version"] = version
        record.update({
            "config_name": config.get("配置名称"),
            "target": config.get("目标地址"),
            "declared_format": declared_format,
            "schema_version": receipt["schema_version"],
            "align_rule_id": align_rule_id,
        })
        self.ledger.save()

        return {
            **summary,
            "ok": True,
            "status": "已交付",
            "previous_version": previous,
            "size": len(raw),
            "verification": verification,
            "legacy_verification": legacy_verification,
            "legacy_artifact": str((target_dir / legacy_name).relative_to(self.base_dir)),
            "backend_readback": backend,
            "schema_receipt": summary["schema_receipt"],
            "conclusion": entry["conclusion"],
        }

    # -- rollback -----------------------------------------------------------
    def rollback(self, config_id: int, to_version: int | None = None) -> dict[str, Any]:
        record = self.ledger.targets.get(str(config_id))
        if record is None:
            return {"ok": False, "reason": f"输出目标 {config_id} 没有迁移清单记录"}
        current = record.get("current_version")
        if to_version is None:
            candidates = [item["version"] for item in record["versions"]
                          if item["version"] < (current or 0) and item.get("path")]
            if not candidates:
                return {"ok": False, "reason": "没有可回滚的历史版本"}
            to_version = max(candidates)
        target = next((item for item in record["versions"]
                       if item["version"] == to_version), None)
        if target is None:
            return {"ok": False, "reason": f"版本 v{to_version} 不存在"}
        self.ledger.ensure_legacy_artifact(config_id, target)

        for item in record["versions"]:
            if item["version"] == current and current != to_version:
                item["delivery_status"] = "已回滚"
                item["conclusion"] = {
                    **(item.get("conclusion") or {}),
                    "rolled_back": True,
                    "rollback_to": to_version,
                }
        record["current_version"] = to_version
        target["delivery_status"] = "回滚后当前版本"
        self.ledger.save()

        readback = self.read_version(config_id, to_version,
                                     fallback_to_actual=True)
        return {
            "ok": readback.get("ok", False),
            "config_id": config_id,
            "previous_version": current,
            "current_version": to_version,
            "declared_format": target["declared_format"],
            "actual_format": target["actual_format"],
            "declared_matches_actual": target["declared_format"] == target["actual_format"],
            "body_sniff": readback.get("body_sniff"),
            "row_count": readback.get("row_count", 0),
            "columns": readback.get("columns", []),
            "readback_ok": readback.get("ok", False),
            "readback_format": readback.get("read_format_used"),
            "fallback_used": readback.get("fallback_used", False),
            "warning": readback.get("warning"),
            "records": readback.get("records", [])[:5],
            "reason": readback.get("reason"),
            "versions_retained": len(record["versions"]),
        }

    # -- artifacts ----------------------------------------------------------
    def artifact(self, config_id: int, version: int | None = None,
                 kind: str = "current") -> tuple[str, str, bytes]:
        item = self._resolve(config_id, version)
        self.ledger.ensure_legacy_artifact(config_id, item)
        is_legacy = kind == "legacy"
        rel = item.get("legacy_path" if is_legacy else "path")
        if not rel:
            raise DistributionError(
                f"版本 v{item['version']} 没有{'旧读者' if is_legacy else ''}产物")
        path = self.base_dir / rel
        if not path.exists():
            raise DistributionError("产物文件不存在")
        fmt = "json" if is_legacy else item["actual_format"]
        name = f"output_target_{config_id}_v{item['version']}_{kind}.{FORMAT_EXT[fmt]}"
        return name, FORMAT_MIME.get(fmt, "application/octet-stream"), path.read_bytes()


def _sha256(raw: bytes) -> str:
    import hashlib

    return hashlib.sha256(raw).hexdigest()[:16]


def _sniff(raw: bytes) -> str:
    if parquet_codec.is_parquet(raw):
        return "parquet"
    stripped = raw.lstrip()
    if stripped[:1] in (b"[", b"{"):
        return "json"
    if raw[:3] == b"\xef\xbb\xbf" or (raw and b"," in raw[:200] and b"\n" in raw):
        return "csv"
    return "unknown"
