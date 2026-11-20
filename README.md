# Web语义抽取与内容理解中心

面向Web结构化数据采集场景，提供页面DOM解析、实体识别、关系抽取、内容分类和知识图谱构建的语义抽取平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py      通用查询/导出/登记/动作接口
│   ├── app/routers/distribution.py  输出分发：迁移清单/试跑/切换/回滚/读回
│   ├── app/services/<key>.py        每个模块独立领域逻辑
│   ├── app/services/distribution.py 版本化分发流水线（构建→核对→写回→回滚）
│   ├── app/services/parquet_codec.py 纯 Python Parquet 读写（无第三方依赖）
│   ├── app/services/base.py         共享服务骨架
│   ├── app/seed_distribution.py     存量分发版本与旧 JSON 正文
│   ├── app/store.py                 内存数据仓库与示例数据
│   └── scripts/migration_drill.py   本地迁移试跑（含空值/整数的实体批次）
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| DOM解析 | `dom_parser` | 解析配置 | 配置名称、目标URI、解析引擎 |
| 实体抽取 | `entity_extract` | 实体条目 | 实体ID、实体类型、实体名称 |
| 关系图谱 | `relation_graph` | 关系边 | 边ID、源实体、目标实体 |
| 内容分类 | `content_classifier` | 分类标签 | 标签ID、页面URI、一级分类 |
| 模板规则 | `template_rule` | 规则模板 | 模板名称、适用站点、提取字段 |
| 文本分块 | `text_segment` | 文本块 | 块ID、来源页面、块类型 |
| 情感标注 | `sentiment_tag` | 情感记录 | 记录ID、文本片段、情感倾向 |
| Schema对齐 | `schema_align` | 对齐规则 | 规则名称、源Schema、目标Schema |
| 种子调度 | `feed_scheduler` | 调度任务 | 任务名称、URL种子、抓取深度 |
| 质量检查 | `quality_check` | 检查规则 | 规则名称、检查字段、检查类型 |
| 探针选择 | `probe_selector` | 探针配置 | 探针名称、目标页面、选择器列表 |
| 页面快照 | `page_snapshot` | 快照记录 | 快照ID、页面URI、DOM哈希 |
| 抽取指标 | `extract_metric` | 指标记录 | 记录时间、所属任务、成功页面数 |
| 语义结果 | `nlu_result` | 语义记录 | 记录ID、输入文本、意图标签 |
| 知识链接 | `knowledge_link` | 链接记录 | 链接ID、源实体、外部URI |
| 输出目标 | `output_target` | 输出配置 | 配置名称、输出类型、目标地址、输出格式 |
| 抽取工坊 | `extract_studio` | 交互任务 | 任务名称、示例页面、标注区域 |
| 聚合视图 | `aggregate_view` | 视图配置 | 视图名称、统计维度、聚合字段 |

## 输出分发与迁移口径

存量列式目标里有“声明为 Parquet、正文实际是 JSON”的历史版本。输出目标模块
现在按下面的口径交付，每个目标都能看到真实格式、字段版本和交付结果：

- **列定义来自 Schema 对齐**：`/api/schema_align/{rule_id}/contract` 给出带类型、
  可空性、来源字段、默认值和契约状态的列定义；回执里的 `switched_fields`
  说明哪些字段已完成切换（改名/改类型），`kept_fields` 说明哪些沿用旧列名。
- **按声明格式构建**：输出目标把实体对齐后的记录编码成声明格式。Parquet 由
  `app/services/parquet_codec.py` 真实写出（PAR1 魔数、Thrift 页脚、定义层级），
  不再用 JSON 正文冒充列式文件；任何产物都要先读回核对行数、列顺序、空值位置
  和整数类型才算交付成功。
- **双读取方**：同一批数据同时写一份历史列语义的 JSON 副本，旧 JSON 读者
  用 `reader=legacy` 就能拿到原先的列名和值。
- **可回滚**：每次切换追加不可变版本，当前版本指针原子更新；构建或核对失败只
  记录一条“切换失败”，当前版本和既有文件保持可读，回滚后输出接口照常读取。

迁移清单落盘在 `backend/var/output_distribution/manifest.json`（可用
`OUTPUT_DATA_DIR` 改目录），接口：

```text
GET  /api/output_target/distribution/overview        每个目标的真实格式/字段版本/交付结果
GET  /api/output_target/distribution/manifest        版本流水与字段契约回执
POST /api/output_target/{id}/distribution/run        试跑或正式切换（body: {"dry_run": true}）
POST /api/output_target/{id}/distribution/rollback   回滚到上一版或 {"to_version": n}
GET  /api/output_target/{id}/distribution/read       读回（reader=declared|actual|legacy）
GET  /api/output_target/{id}/distribution/artifact   下载声明格式产物（kind=legacy 为旧读者副本）
```

前端“输出目标”页面底部有分发面板：每个目标的声明格式、真实正文格式、字段
版本和交付结果，可试跑、切换、按声明格式读回、回滚并查看版本流水。

本地迁移试跑（启动临时后端，含空值/整数的实体，核对切换前后并验证回滚）：

```bash
cd backend
.venv/bin/python scripts/migration_drill.py
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
