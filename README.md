# 风机打分数仓对账 API

工程师排查「打分时到底用了什么读数、哪些读数后来才变」的核对服务。
FastAPI + SQLite，`/docs` 可直接操作全部接口。

## 业务口径

时间一律 **UTC**，采样与打分都按分钟对齐。

| 概念 | 定义 |
| --- | --- |
| 窗口 | 打分前一小时的 60 个采样分钟：`[打分分钟 − 1h, 打分分钟)` |
| 当时应取 | 窗口内、**入仓时刻严格早于打分时刻**（`ingested_at < score_minute`）的修订中，**修订号最高**的那条 |
| 缺测留空 | 窗口内没有符合条件的读数时，「当时应取」为空，不报错、不猜测 |
| 当前最高修订 | 数仓里该 (机组, 采样分钟) 现在存在的最高修订（不论入仓时间） |

每张回执输出两份清单：

1. **`usage` — 实际用数 vs 当时应取**
   - `missing`（缺行/缺测）：回执引用的修订在数仓中不存在；或实际用了数但当时应缺测。
   - `window`（越界）：实际使用的采样分钟落在打分前一小时窗口之外。
   - `revision`（换版）：实际用的修订 ≠ 当时应取的最高修订。
   - 两侧各带 `label / sample_minute / revision / wind_speed / ingested_at / ingested_before_score`。
2. **`drift` — 当时应取 vs 当前最高修订**
   - `missing`：当时缺测、后来才补录，或当时有数、现在反而缺行。
   - `revision`：当时取到的修订之后又出了更高修订（读数事后变化）。
   - 采样分钟本就在窗口外时不产生 drift（该行按规则从未参与打分，比对无意义）。

`evidence` 里固定给出：规则原文、窗口（起止、开闭区间）、机组、采样分钟、
打分截止时刻、实际/应取/当前三侧快照，以及该采样分钟**全部修订的入仓轨迹**
（每条带 `ingested_before_score` 和 `eligible`），便于人工复核。

## 一致性与性能

* **单一已提交快照**：一次批量核对在一个 SQLite WAL 读事务里完成。核对期间
  数仓照常补录（写入不阻塞），补录只体现在下一批，不会让同一份报告前后矛盾。
  快照指纹 `wind_max_id` 随报告一起保存。
* **批量上限 10,000 张回执**（按编号或按机组/打分分钟筛选，超限返回 413）。
* **不按回执扫表**：整批只发一次 `wind_readings ⋈ 临时配对表` 关联查询，
  实测 1 万张回执 / 14.4 万条风速约 0.6 s，SQL 查询条数为常数（3 条 SELECT）。
* **报告原子落库**：报告头 + 全部条目在同一个 `BEGIN IMMEDIATE` 事务里写入，
  失败整体回滚，不会留下半份报告。历史报告按 `report_id` 永久可查。
* 同一 (机组, 采样分钟, 修订号) 重复导入为覆盖（按后到的入仓时刻/读数更新），
  保证补录幂等。

## 启动（docker compose）

```bash
docker compose up --build -d
open http://127.0.0.1:8000/docs
```

数据落在命名卷 `windscore-data`（`/data/windscore.db`），重启不丢。
端口可用环境变量覆盖：`WINDSCORE_PORT=9000 docker compose up -d`。

本地直接跑：

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/readings` | 导入分钟风速（JSON 行数组 / `{"items": [...]}` / 中文列名） |
| POST | `/api/readings/import` | 上传 CSV（UTF-8 或 GBK）或 JSON 文件 |
| POST | `/api/receipts` | 导入打分回执（同上格式） |
| POST | `/api/receipts/import` | 上传回执文件 |
| POST | `/api/reconcile` | 批量核对，生成并保存报告 |
| GET | `/api/reports` | 报告列表 |
| GET | `/api/reports/{report_id}` | 按编号取报告（含两份清单与依据，可分页/按回执筛） |
| GET | `/api/receipts/{receipt_id}/reports` | 反查某回执出现在哪些报告里 |
| GET | `/api/readings`、`/api/receipts` | 查看已导入数据 |
| GET | `/health` | 健康检查 |

### 导入字段

风速表：`turbine_id`（机组）、`sample_minute`（UTC 采样分钟）、`revision`（修订号）、
`ingested_at`（生产入仓时刻）、`wind_speed`（风速，空 = 缺测）。
回执表：`receipt_id`（编号）、`turbine_id`、`score_minute`（UTC 打分分钟）、
`used_sample_minute`（实际使用的采样分钟）、`used_revision`（实际使用的修订号）。

也接受中文表头：`机组 / 采样分钟 / 修订号 / 生产入仓时刻 / 风速`，
`编号 / 机组 / 打分分钟 / 实际使用的采样分钟 / 实际使用修订号`。

### 示例

```bash
curl -X POST localhost:8000/api/readings -H 'content-type: application/json' -d '[
  {"turbine_id":"WT-001","sample_minute":"2026-01-01T10:00:00Z","revision":1,
   "ingested_at":"2026-01-01T10:01:00Z","wind_speed":6.0},
  {"turbine_id":"WT-001","sample_minute":"2026-01-01T10:00:00Z","revision":2,
   "ingested_at":"2026-01-01T10:02:00Z","wind_speed":6.4},
  {"turbine_id":"WT-001","sample_minute":"2026-01-01T10:00:00Z","revision":5,
   "ingested_at":"2026-01-01T11:20:00Z","wind_speed":7.1}]'

curl -X POST localhost:8000/api/receipts -H 'content-type: application/json' -d '[
  {"receipt_id":"R-1","turbine_id":"WT-001","score_minute":"2026-01-01T11:00:00Z",
   "used_sample_minute":"2026-01-01T10:00:00Z","used_revision":1}]'

curl -X POST localhost:8000/api/reconcile -H 'content-type: application/json' \
  -d '{"receipt_ids":["R-1"]}'
```

该回执的结论：`usage` 报换版（用了 rev 1，应取 rev 2），`drift` 也报换版
（rev 5 在 11:20 才入仓，属事后回填）。报告可直接用返回的 `report_id` 再查。

## 测试

```bash
pip install pytest httpx ruff
python -m pytest
python -m ruff check app tests --select F,E9
```

覆盖：窗口边界（起点含、终点不含）、入仓时刻与打分同刻、缺行/缺测、
事后补录的 drift、批次中并发补录不影响快照、报告失败不留存、
单批 SQL 条数与 1 万条上限、CSV/JSON/中文表头导入等。
