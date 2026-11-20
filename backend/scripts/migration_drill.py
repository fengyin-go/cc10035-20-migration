#!/usr/bin/env python3
"""本地迁移试跑：列式目标（声明 Parquet、正文 JSON）的切换、读回核对与回滚。

全程只走 HTTP 输出接口，不直接调用服务内部函数，验证的是交付口径本身：

1. 列式目标当前版本声明 parquet、正文实际是 JSON —— 按声明格式读会失败，
   这正是需要迁移的存量问题；
2. 用含空值和整数的实体批次先试跑（不落版本），再正式切换；
3. 切换后按声明格式（parquet）读回，核对行数、空值位置、整数列类型；
   同时按旧 JSON 读者的口径读回历史列语义副本；
4. 回滚到切换前版本，再调用输出接口读取既有数据，确认仍可读。

用法::

    cd backend && .venv/bin/python scripts/migration_drill.py [--keep] [--port 8123]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# 含空值和整数的实体批次：空值在 confidence / text_length / page_url 上。
DRILL_ENTITIES = [
    {"entity_id": "drill-01", "entity_type": "命名实体", "entity_name": "调度中心",
     "page_url": "https://example.com/d/1", "confidence": 0.91,
     "extracted_at": "2026-11-20", "text_length": 24},
    {"entity_id": "drill-02", "entity_type": "数值实体", "entity_name": None,
     "page_url": "https://example.com/d/2", "confidence": None,
     "extracted_at": "2026-11-20", "text_length": 0},
    {"entity_id": "drill-03", "entity_type": "链接实体", "entity_name": "入口页",
     "page_url": None, "confidence": 0.5,
     "extracted_at": "2026-11-20", "text_length": None},
    {"entity_id": "drill-04", "entity_type": "时间实体", "entity_name": "2026-11-20",
     "page_url": "https://example.com/d/4", "confidence": 1,
     "extracted_at": "2026-11-20", "text_length": 10},
]

TARGET_ID = 2  # 存量列式目标：v2 声明 parquet、正文是 JSON


class DrillError(RuntimeError):
    pass


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def api(base: str, path: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base}{path}", data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if payload is not None else "GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:  # 404 等也要把 detail 带出来
        detail = exc.read().decode("utf-8", "replace")
        return {"ok": False, "http_status": exc.code, "reason": detail}


def wait_ready(base: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if api(base, "/api/health").get("ok"):
                return
        except (urllib.error.URLError, ConnectionError):
            pass
        time.sleep(0.2)
    raise DrillError("后端未能在超时时间内启动")


def check(condition: bool, message: str, report: list[str]) -> None:
    report.append(("PASS " if condition else "FAIL ") + message)
    if not condition:
        raise DrillError(message)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true", help="保留分发产物目录")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="migration-drill-"))
    port = args.port or _free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, OUTPUT_DATA_DIR=str(workdir))
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
    )
    report: list[str] = []
    try:
        wait_ready(base)
        report.append(f"INFO 分发产物目录：{workdir}")

        # ---- 1. 切换前：存量列式目标 --------------------------------
        before_declared = api(base, f"/api/output_target/{TARGET_ID}/distribution/read")
        before_actual = api(
            base, f"/api/output_target/{TARGET_ID}/distribution/read?format=json")
        check(not before_declared["ok"], "切换前按声明 parquet 读取失败（存量正文是 JSON）", report)
        check(before_declared["body_sniff"] == "json",
              f"切换前正文嗅探为 json（声明 {before_declared['declared_format']}）", report)
        check(before_actual["ok"] and before_actual["row_count"] == 4,
              f"切换前按实际 JSON 读取 {before_actual.get('row_count')} 行", report)
        check(before_declared["declared_matches_actual"] is False,
              "清单如实标注：切换前声明与正文不一致", report)
        before_version = before_declared["version"]

        # ---- 2. 试跑：含空值和整数的实体 ----------------------------
        dry = api(base, f"/api/output_target/{TARGET_ID}/distribution/run",
                  {"dry_run": True, "entities": DRILL_ENTITIES})
        check(dry.get("ok") and dry.get("dry_run"), "试跑通过且未写入新版本", report)
        check(dry["verification"]["ok"], "试跑产物按声明格式读回一致", report)
        check(dry.get("previous_version") == before_version or dry.get("current_version") == before_version,
              "试跑没有推进当前版本", report)
        expected_nulls = {
            "page_url": 1, "confidence": 1, "text_length": 1, "entity_name": 1,
        }
        null_counts = dry["verification"]["null_counts"]
        check(all(null_counts.get(key) == value for key, value in expected_nulls.items())
              and sum(null_counts.values()) == sum(expected_nulls.values()),
              f"试跑空值位置符合预期：{expected_nulls}", report)

        # ---- 3. 正式切换 ------------------------------------------
        switched = api(base, f"/api/output_target/{TARGET_ID}/distribution/run",
                       {"entities": DRILL_ENTITIES})
        check(switched.get("ok") and switched.get("status") == "已交付",
              f"切换交付 v{switched.get('version')}", report)
        check(switched["declared_format"] == "parquet", "交付声明格式为 parquet", report)
        backend = switched.get("backend_readback") or {}
        if backend.get("available"):
            check(backend.get("ok") is True,
                  "独立 Parquet 实现（pyarrow）读回一致", report)
        else:
            report.append("SKIP 未安装 pyarrow，跳过独立 Parquet 实现交叉核对")

        # ---- 4. 切换后：两类读取方式 ------------------------------
        after_declared = api(base, f"/api/output_target/{TARGET_ID}/distribution/read")
        check(after_declared["ok"] and after_declared["body_sniff"] == "parquet",
              "切换后按声明 parquet 读取成功，正文确为 Parquet", report)
        check(after_declared["row_count"] == len(DRILL_ENTITIES),
              f"切换后行数 {after_declared['row_count']} 与实体批次数一致", report)
        check("text_length" in after_declared["integer_columns"],
              "切换后 text_length 读回仍是整数类型", report)
        check(after_declared["null_counts"]["text_length"] == 1
              and after_declared["null_counts"]["confidence"] == 1,
              "切换后空值位置保持不变", report)
        zero_row = next(row for row in after_declared["records"] if row.get("entity_id") == "drill-02")
        check(zero_row["text_length"] == 0 and zero_row["confidence"] is None,
              "0 值没有被写成空值，空值仍是 null", report)

        legacy = api(
            base, f"/api/output_target/{TARGET_ID}/distribution/read?reader=legacy")
        check(legacy["ok"] and legacy.get("row_count") == len(DRILL_ENTITIES),
              "旧 JSON 读者仍能按历史列语义读到同一批数据", report)
        check(legacy["columns"][:4] == ["实体ID", "实体类型", "实体名称", "页面来源"],
              f"旧列语义列名保留：{legacy['columns'][:4]}", report)

        manifest = api(base, "/api/output_target/distribution/manifest")
        target = next(item for item in manifest["targets"]
                      if item["config_id"] == TARGET_ID)
        check(target["current_declared_matches_actual"] is True,
              "迁移清单回写：当前版本声明与正文一致", report)
        receipt = target["versions"][-1]["conclusion"].get("schema_receipt")
        check(bool(receipt) and bool(receipt["switched_fields"]),
              f"字段契约回执已完成切换：{receipt['switched_fields'] if receipt else []}", report)
        confidence = next((column for column in receipt["columns"]
                           if column["source_field"] == "confidence"), None)
        check(confidence is not None and confidence["type"] == "number"
              and confidence["legacy_field"] == "置信度",
              "回执标明 confidence 已切换为 number，旧列语义仍为「置信度」", report)

        # ---- 5. 回滚后仍能读取既有数据 ----------------------------
        rolled = api(base, f"/api/output_target/{TARGET_ID}/distribution/rollback", {})
        check(rolled.get("ok"), f"回滚到 v{rolled.get('current_version')}", report)
        after_rollback = api(
            base, f"/api/output_target/{TARGET_ID}/distribution/read?reader=legacy")
        check(after_rollback["ok"] and after_rollback["row_count"] == 4,
              "回滚后调用输出接口仍能读取既有 4 行数据", report)
        listed = api(base, "/api/output_target")
        check(listed.get("total") == 3, "输出配置列表接口未受影响", report)
        versions = target["versions"]
        check(len(versions) >= 3, f"可回滚版本记录保留 {len(versions)} 条", report)
    except DrillError as exc:
        report.append(f"ERROR {exc}")
        for line in report:
            print(line)
        return 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)

    for line in report:
        print(line)
    passed = sum(1 for line in report if line.startswith("PASS"))
    skipped = sum(1 for line in report if line.startswith("SKIP"))
    print(f"\n迁移试跑完成：{passed} 项通过" + (f"，{skipped} 项跳过" if skipped else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
