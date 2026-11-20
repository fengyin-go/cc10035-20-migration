"""运行配置：端口、跨域、运行环境、分发产物目录。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "var" / "output_distribution"


def _output_data_dir() -> str:
    return str(os.environ.get("OUTPUT_DATA_DIR") or DEFAULT_OUTPUT_DIR)


@dataclass(frozen=True)
class Settings:
    app_name: str = "Web语义抽取与内容理解中心"
    env: str = "local"
    port: int = 8000
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5173",
            "http://localhost:5173",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    output_data_dir: str = field(default_factory=_output_data_dir)



settings = Settings()
