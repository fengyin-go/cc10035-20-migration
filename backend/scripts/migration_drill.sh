#!/usr/bin/env bash
# 本地迁移试跑（不依赖第三方库）：
#   1. 用含空值和整数的实体批次，对列式目标试跑一次（--dry-run），再正式切换；
#   2. 切换前、后分别按「声明格式」和「实际正文格式」读回文件并核对；
#   3. 回滚后确认输出接口仍能读到既有数据。
#
# 用法：backend/scripts/migration_drill.py [--keep]
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python scripts/migration_drill.py "$@"
