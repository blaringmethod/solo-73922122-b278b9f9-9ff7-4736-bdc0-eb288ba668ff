# 气象观测数据湖

FastAPI + PyArrow(Parquet) + SQLite 的观测数据湖示例。网页维护一张观测表：

- 固定列：`station_id`（站号）、`observed_at`（UTC 时刻，写入前统一换算为 UTC）；
- 最多 8 个可空 `int64` 测量列，可新增 / 改名 / 删除；
- 每批上传采集站导出的 CSV（≤1000 行），转换为 Parquet 入湖；
- 可按**当前**列名查看、下载任一批次（CSV / Parquet）；
- 上传时留存**逐字节原始 CSV**，网页显示原件 SHA-256；后续改列不影响原件，
  争议报文可按登记摘要取回原件核对。

## 设计要点（对应需求）

| 需求 | 实现 |
| --- | --- |
| 改名沿用旧值 | Parquet 的物理列名是永久 `field_id`（`f_<uuid4>`），列名只存在于 `fields` 表；改名只更新 `fields.name`，历史文件不动 |
| 删除后同名新增不串值 | 删除是软删除（`status='deleted'`，保留 id）；同名新增生成新的 `field_id`；历史批次中没有新 id 的列在读取时补 `NULL` |
| 历史 Parquet 不得重写 | 只做“读 + 内存投影”，从不以写方式打开历史文件；新文件以临时文件写入后 rename |
| 结构版本 | SQLite `meta.schema_version` 单调递增；每次新增/改名/删除提交时 +1 |
| 过期整次拒绝 | 表单/API 必须携带页面所见版本；写事务内 `BEGIN IMMEDIATE` 先比对版本，不等则整次拒绝（HTTP 409 / 网页提示） |
| 转换期间仍可改结构 | 上传路径只在解析前后短暂读库，写 Parquet 时不持锁；提交登记时二次比对版本，若结构已变则拒绝登记并删除刚写的文件 |
| 双进程并发 | SQLite WAL + `busy_timeout` + `BEGIN IMMEDIATE` 串行化写入；两实例共享数据目录 |
| 失败/重启后一致 | 批次登记与结构变更同一事务；原件与 Parquet 都先落盘再登记，登记失败删除两份文件；启动时清理陈旧临时文件、过期租约与（超过 1 小时的）未登记孤儿文件 |
| 原件留存 | 上传字节写入 `data/source/<batch_id>.csv`（临时文件 → fsync → rename），登记 `source_sha256/source_size`；下载按登记摘要逐字节返回，不重新生成 |
| 只有结构仍有效才登记 | 原件、Parquet 都落盘后，在 `BEGIN IMMEDIATE` 事务里再次比对 schema_version；同时复核两份文件在位，绝不登记“只有一份文件”的新批次 |
| 原件缺失 / 摘要不符 | 拒绝原件下载（`source_integrity_error`，说明缺失还是摘要不符），Parquet 取数不受影响；`/api/integrity` 单独列出缺失 / 摘要不符 / 孤儿原件 |
| 旧批次无原件 | 登记列为 NULL，网页批次页与列表明确提示“没有保存原始 CSV”；不影响一致性自检与 Parquet 读取 |
| 未登记残件回收 | 删除前在 `BEGIN IMMEDIATE` 写事务中核对登记表，与批次登记互斥；在途上传有 `.lock` 租约标记，未过期的标记保护文件，崩溃遗留的过期标记会被清理 |
| 查询只用同一已提交结构 | `get_batch` / 页面在单个读事务中读取结构与批次登记；不读未提交数据 |
| 失败保留已有批次 | 任何失败路径都不改已有登记与文件；`/api/integrity` 可核对登记与两类文件的 sha256 |

## 本机运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data INSTANCE_NAME=node-a uvicorn app.main:app --port 8000
```

另一个终端用相同 `DATA_DIR`、`INSTANCE_NAME=node-b` 起第二个实例（端口 8001），
分别打开网页即可演示双进程共享结构与批次。

## Docker Compose（双实例 + 共享持久卷）

```bash
docker compose up --build -d
# 浏览器打开 http://localhost:8001 (node-a) 与 http://localhost:8002 (node-b)
docker compose down          # 保留命名卷 lake-data
docker compose down -v       # 连同数据一起删除
```

两个服务共用命名卷 `lake-data`，`DATA_DIR=/data`。

## 网页演示流程

1. `http://localhost:8001/` 新增测量列 `temp_c`、`humidity`；
2. 下载“示例 CSV”，或自行准备：

   ```csv
   station_id,observed_at,temp_c,humidity
   54511,2026-01-01T00:00:00Z,7,40
   54512,2026-01-01T01:00:00Z,9,
   ```

3. 上传后进入批次页查看：可看到原件 SHA-256 与“下载上传时的原始 CSV”；
   下载字节与摘要一致，随后改列也不改变它；
4. 在 `node-b` 把 `temp_c` 改名为 `temperature_c`，回到 `node-a` 刷新批次页：列名新、值仍为旧值；
5. 删除 `temp_c`（或已改名的列）后新增同名 `temp_c`，再上传新批次：
   旧批次该列显示 `NULL`，新批次显示新值；旧 Parquet 与旧原件文件字节均未变；
6. 打开两个标签页（或分别用两个实例）同时提交基于同一版本的结构修改：
   一个成功、另一个提示“结构版本已过期”并被整次拒绝；
7. `/api/integrity` 或首页“一致性自检”核对登记与 Parquet / 原件一致；
8. 手工删除或改写某个批次的 `data/source/<id>.csv`：原件下载被拒绝并说明
   缺失 / 摘要不符，同一批次的 Parquet 查看与下载不受影响。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/state` | 结构快照 + 批次列表（含 `source_sha256` / `has_source`） |
| GET | `/healthz` | 实例名、pid、版本 |
| POST | `/api/fields` | `{"schema_version": v, "name": "..."}` |
| PATCH | `/api/fields/{field_id}` | 改名，body 同上 |
| DELETE | `/api/fields/{field_id}` | 软删除，body `{"schema_version": v}` |
| POST | `/api/batches` | multipart：`version` + `file`（原始字节留存） |
| GET | `/api/batches/{id}?format=json\|csv\|parquet` | 按当前结构读取/下载 |
| GET | `/api/batches/{id}/source` | 下载上传时的原始 CSV（逐字节） |
| GET | `/batches/{id}/download.source.csv` | 网页版原件下载 |
| GET | `/api/integrity` | Parquet / 原件与登记的一致性自检 |

过期版本返回 409 `{"error": "stale_schema", ...}`；参数错误 400；Parquet 文件缺失 500
`integrity_error`；原件缺失或摘要不符 500 `source_integrity_error`；旧批次无原件 404
`source_missing`（详情说明原因与替代方案）。

## 测试

```bash
python -m pytest -q                 # 40 项：核心/网页/原件留存/一致性；含两个真实 uvicorn 进程的并发上传测试
python scripts/selftest_dual.py     # 端到端演示脚本：起两个真实进程 + 共享目录，逐项打印 PASS/FAIL
```

## 已知限制

- 单机 SQLite + 共享卷方案适用于演示与中小规模；跨主机需换成服务端数据库与对象存储。
- 原件与 Parquet 分两个目录（`data/source`、`data/parquet`），同一批次靠 id 关联；
  未登记残件按“超过 1 小时 + 无在途租约”回收，因此崩溃后最坏需等 1 小时才清理。
- 网页为服务端渲染，无前端路由；并发冲突通过版本号拒绝，不做自动合并。
- 删除只做逻辑删除，Parquet 中历史字段的物理列会一直保留（这是“不重写历史”的代价）。
- 沙箱环境无 `docker`，Compose 配置与镜像构建未在本机实际运行；已用两个真实 uvicorn 进程共享目录完成同等并发验证。
