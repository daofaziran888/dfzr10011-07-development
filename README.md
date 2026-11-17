# 分布式采集调度引擎

面向大规模数据采集团队的分布式采集调度引擎。覆盖采集节点集群管理、任务分片调度、结果聚合归并、节点健康监控、采集流量均衡、断点续采恢复与全局采集看板的完整分布式采集链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
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
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点集群 | `cluster` | 集群节点 | 节点编号、节点地址、节点角色 |
| 任务分片调度 | `shard` | 分片任务 | 分片编号、采集计划、分片范围 |
| 结果聚合归并 | `merge` | 聚合任务 | 任务编号、采集计划、分片总数 |
| 节点状态巡检 | `nodecheck` | 巡检记录 | 记录编号、节点编号、CPU使用率 |
| 采集流量均衡 | `balance` | 均衡策略 | 策略编号、负载算法、权重因子 |
| 断点续采恢复 | `resume` | 断点记录 | 断点编号、采集计划、断点位置 |
| 全局采集看板 | `dashboard` | 看板面板 | 面板编号、面板名称、显示指标 |
| 采集配置模板 | `crawlconf` | 配置模板 | 模板编号、模板名称、目标类型 |
| 采集数据源池 | `datasource` | 数据源条目 | 条目编号、数据源名称、源类型 |
| 采集日志解析 | `logparse` | 日志模板 | 模板编号、日志类型、解析规则 |
| 采集频率控制 | `ratelimit` | 频率规则 | 规则编号、目标站点、请求间隔 |
| 采集代理路由 | `proxy` | 代理配置 | 配置编号、代理地址、代理协议 |
| RPA执行记录 | `robot` | 执行记录 | 记录编号、流程模板、执行节点 |
| 采集结果校验 | `verify` | 校验规则 | 规则编号、采集计划、校验类型 |
| 采集操作审计 | `audit` | 审计记录 | 记录编号、操作类型、操作对象 |
| 采集存储分层 | `storage` | 存储策略 | 策略编号、存储层级、文件格式 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 归并水位与快照

结果聚合归并（`merge`）、任务分片调度（`shard`）和采集存储分层（`storage`）共享同一套口径，
核心实现在 `backend/app/services/mergeflow.py`，示例计划与分片完成记录在 `backend/app/seed_mergeflow.py`。

- **分片完成水位**：从分片序号 1 开始只统计连续完成的记录，遇到第一个缺口就停住；
  缺口之后即使已经完成也不抬高水位。空分片（记录数为 0）单独展示，不产生输出数据。
- **归并快照**：每次生成固定一个水位，已归并数 = 水位，输出数据总量 = 水位内所有分片的记录合计，
  两个数字都从同一份分片视图计算，不会来自不同刷新时刻。
- **生成期间又有分片完成**：`policy=freeze` 冻结启动时水位，归档清单只显示启动水位，
  新增分片计入下一批；`policy=extend` 延长到最后确认水位，归档清单包含延长后的范围，
  下一批只保留水位之后的记录。
- **幂等与续跑**：同一条聚合任务只有一个归并批次；重复启动复用已有快照，不会生成第二份；
  失败后可以从缺口继续，缺口未闭合时直接拦下并说明原因。
- **写入异常**：快照、归并结果、归档条目、闸门、复核清单、分待办一次性提交，
  模拟写入异常时整批回滚，不会留下有归档条目却没有归并结果的半快照。
- **存储读取**：存储分层按快照版本（`snapshot_version` / `snapshot_id`）读取归档清单，
  旧版本保留原水位与原数据口径，新版本不会覆盖旧版本的依据。

相关接口：

| 接口 | 说明 |
| --- | --- |
| `GET /api/merge/watermark?plan_id=PLAN-0002` | 读取分片完成水位与同水位下的输出数据总量 |
| `GET /api/merge/completion` | 分片完成记录与空分片状态（分开展示） |
| `POST /api/merge/{id}/actions` | `启动归并` / `重试归并` / `归档任务`，可带 `policy`、`concurrent_shards`、`simulate_write_error` |
| `GET /api/merge/snapshots[/{reference}]` | 归并快照列表与明细（含归并结果、归档条目、一致性核对） |
| `GET /api/storage/gates` | 新增归并闸门 |
| `GET /api/storage/pending` | 分待办（下一批待归并数） |
| `GET /api/storage/reviews` | 分复核清单 |
| `GET/POST /api/storage/manifest` | 按快照版本读取 / 幂等生成归档清单 |

后端自测：`cd backend && PYTHONPATH=.venv python3 -m unittest discover -s tests -v`（复用 `.venv` 里的依赖，无需重新安装）。

领域：数据工程
子方向：数据采集、爬虫与RPA
