"""存量分发的历史快照：声明格式与正文格式不一致的列式目标。

这些记录代表上线前已经分发出去的文件。列式目标当时只改了声明，文件里仍是
按旧列语义序列化的 JSON 正文；迁移清单需要把这种“声明与正文不一致”原样
保留下来，才能在切换前核对两类读取方的交付口径。
"""
from __future__ import annotations

from typing import Any

LEGACY_COLUMNS: list[str] = [
    "实体ID", "实体类型", "实体名称", "页面来源", "置信度", "抽取时间", "原文长度",
]

# 旧 JSON 正文：置信度是字符串，原文长度是整数，并且带有空值。
LEGACY_RECORDS: list[dict[str, Any]] = [
    {
        "实体ID": "ent-1001",
        "实体类型": "命名实体",
        "实体名称": "分布式采集平台",
        "页面来源": "https://example.com/p/1001",
        "置信度": "0.94",
        "抽取时间": "2026-11-12",
        "原文长度": 18,
    },
    {
        "实体ID": "ent-1002",
        "实体类型": "数值实体",
        "实体名称": None,
        "页面来源": "https://example.com/p/1002",
        "置信度": "0.81",
        "抽取时间": "2026-11-13",
        "原文长度": 42,
    },
    {
        "实体ID": "ent-1003",
        "实体类型": "链接实体",
        "实体名称": "站点导航入口",
        "页面来源": "https://example.com/p/1003",
        "置信度": None,
        "抽取时间": "2026-11-14",
        "原文长度": 0,
    },
    {
        "实体ID": "ent-1004",
        "实体类型": "属性实体",
        "实体名称": "采集深度",
        "页面来源": "https://example.com/p/1004",
        "置信度": "0.67",
        "抽取时间": "2026-11-15",
        "原文长度": None,
    },
]

# 本地迁移试跑用的实体批次：含空值和整数列。
SAMPLE_ENTITIES: list[dict[str, Any]] = [
    {
        "entity_id": "ent-2001",
        "entity_type": "命名实体",
        "entity_name": "向量检索服务",
        "page_url": "https://example.com/p/2001",
        "confidence": 0.97,
        "extracted_at": "2026-11-18",
        "text_length": 16,
    },
    {
        "entity_id": "ent-2002",
        "entity_type": "数值实体",
        "entity_name": "",
        "page_url": "https://example.com/p/2002",
        "confidence": None,
        "extracted_at": "2026-11-18",
        "text_length": 0,
    },
    {
        "entity_id": "ent-2003",
        "entity_type": "链接实体",
        "entity_name": "采集入口",
        "page_url": "https://example.com/p/2003",
        "confidence": 0.42,
        "extracted_at": "2026-11-19",
        "text_length": None,
    },
    {
        "entity_id": "ent-2004",
        "entity_type": "属性实体",
        "entity_name": "抓取周期",
        "page_url": None,
        "confidence": 0.88,
        "extracted_at": "2026-11-19",
        "text_length": 7,
    },
    {
        "entity_id": "ent-2005",
        "entity_type": "时间实体",
        "entity_name": "2026-11-19",
        "page_url": "https://example.com/p/2005",
        "confidence": 1,
        "extracted_at": "2026-11-20",
        "text_length": 10,
    },
]

# target_id -> 已分发版本（version 升序）。
LEGACY_DELIVERIES: dict[int, list[dict[str, Any]]] = {
    1: [
        {
            "version": 1,
            "declared_format": "json",
            "schema_version": "v1",
            "created_at": "2026-10-08T09:20:00+00:00",
            "note": "JSON 数据湖历史交付，旧列语义",
        },
    ],
    2: [
        {
            "version": 1,
            "declared_format": "json",
            "schema_version": "v1",
            "created_at": "2026-10-12T02:10:00+00:00",
            "note": "列式目标的首个历史交付，正文为 JSON",
        },
        {
            "version": 2,
            "declared_format": "parquet",
            "schema_version": "v1",
            "created_at": "2026-11-10T06:45:00+00:00",
            "note": "声明已切换为 Parquet，正文仍是旧 JSON（待迁移）",
        },
    ],
    3: [
        {
            "version": 1,
            "declared_format": "csv",
            "schema_version": "v1",
            "created_at": "2026-10-20T11:00:00+00:00",
            "note": "搜索副本的历史 CSV 交付，旧列语义",
        },
    ],
}
