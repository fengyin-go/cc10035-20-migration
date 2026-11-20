"""输出分发接口：迁移清单、试跑、切换、回滚、按格式读回和产物下载。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel

from app.services import service_for
from app.services.distribution import DistributionError, _sniff
from app.services.output_target import OutputTargetService
from app.services.schema_align import SchemaAlignService
from app.store import store

router = APIRouter(prefix="/api", tags=["输出分发"])


def output_service() -> OutputTargetService:
    service = service_for("output_target")
    assert isinstance(service, OutputTargetService)
    return service


class MigrationPayload(BaseModel):
    dry_run: bool = False
    entities: list[dict[str, Any]] | None = None


class RollbackPayload(BaseModel):
    to_version: int | None = None


@router.get("/output_target/distribution/overview")
def distribution_overview() -> dict[str, Any]:
    """每个目标的真实格式、字段版本和交付结果。"""
    return output_service().distribution_overview()


@router.get("/output_target/distribution/manifest")
def distribution_manifest() -> dict[str, Any]:
    """迁移清单：版本流水和字段契约回执。"""
    return output_service().migration_ledger()


@router.post("/output_target/{config_id}/distribution/run")
def run_migration(config_id: int, payload: MigrationPayload | None = None) -> dict[str, Any]:
    """试跑或正式切换：构建 → 读回核对 → 写回迁移清单。"""
    service = output_service()
    payload = payload or MigrationPayload()
    try:
        result = service.run_migration(
            config_id, dry_run=payload.dry_run, entities=payload.entities)
    except DistributionError as exc:
        return {"ok": False, "reason": str(exc)}
    if result.get("ok") and not result.get("dry_run"):
        config = store.find("output_target", config_id)
        if config is not None and config.get("status") == "草稿":
            config["status"] = "已启用"
            config["pending"] = True
    return result


@router.post("/output_target/{config_id}/distribution/rollback")
def rollback(config_id: int, payload: RollbackPayload | None = None) -> dict[str, Any]:
    """回滚到指定（默认上一个）版本；回滚后读回旧文件核对。"""
    payload = payload or RollbackPayload()
    result = output_service().rollback(config_id, payload.to_version)
    if result.get("ok"):
        config = store.find("output_target", config_id)
        if config is not None:
            config["abnormal"] = False
    return result


@router.get("/output_target/{config_id}/distribution/read")
def read_version(
    config_id: int,
    version: int | None = Query(default=None),
    format: str | None = Query(default=None),
    fallback: bool = Query(default=False),
    reader: str = Query(default="declared", pattern="^(declared|actual|legacy)$"),
) -> dict[str, Any]:
    """读回某个版本。

    * ``reader=declared``（默认）按声明格式读主体文件；
    * ``reader=actual`` 按正文真实格式读主体文件；
    * ``reader=legacy`` 按旧 JSON 读者口径读历史列语义副本。
    ``fallback=true`` 时声明格式读不动会按实际正文格式再读一次，并显式标注。
    """
    result = output_service().read_version(
        config_id, version, format, fallback_to_actual=fallback, reader=reader)
    if result.get("ok"):
        result["records"] = result.get("records", [])[:50]
    return result


@router.get("/output_target/{config_id}/distribution/artifact")
def download_artifact(
    config_id: int,
    version: int | None = Query(default=None),
    kind: str = Query(default="current", pattern="^(current|legacy)$"),
) -> Response:
    """下载当前产物：kind=current 为声明格式正文，legacy 为旧 JSON 读者副本。"""
    try:
        name, mime, data = output_service().distribution.artifact(config_id, version, kind)
    except DistributionError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(content=data, media_type=mime,
                    headers={"Content-Disposition": f'attachment; filename="{name}"'})


@router.get("/output_target/{config_id}/distribution/inspect")
def inspect_blob(config_id: int, version: int | None = Query(default=None)) -> dict[str, Any]:
    """只嗅探正文格式，不解码全部记录。"""
    try:
        _name, _mime, data = output_service().distribution.artifact(config_id, version, "current")
    except DistributionError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"ok": True, "config_id": config_id, "sniffed_format": _sniff(data),
            "size": len(data)}


@router.get("/schema_align/{rule_id}/contract")
def schema_contract(rule_id: int,
                    legacy_rule_id: int | None = Query(default=None)) -> dict[str, Any]:
    """Schema 对齐的字段契约：哪些字段完成切换、哪些沿用旧列名。"""
    service = SchemaAlignService()
    receipt = service.contract_receipt(rule_id, legacy_rule_id)
    if not receipt.get("ok"):
        raise HTTPException(status_code=404, detail=receipt.get("reason", "规则不存在"))
    return receipt


@router.get("/schema_align/contracts/overview")
def schema_contracts_overview() -> dict[str, Any]:
    service = SchemaAlignService()
    legacy = service.legacy_rule()
    rows = []
    for rule in store.rows("schema_align"):
        receipt = service.contract_receipt(int(rule["id"]),
                                           legacy["id"] if legacy else None)
        if receipt.get("ok"):
            rows.append(receipt)
    return {"ok": True, "rules": rows, "legacy_rule_id": legacy["id"] if legacy else None}
