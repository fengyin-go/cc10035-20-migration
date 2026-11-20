"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import distribution, resource

# 分发接口含 /api/output_target/distribution/... 等固定子路径，
# 必须早于 /api/{module}/{entry_id} 这类动态路径注册。
ROUTERS = [distribution, resource]
