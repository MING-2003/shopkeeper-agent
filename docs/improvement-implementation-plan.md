# 电商问数改进实施方案

> 本文档是 `improvement-roadmap.md` 的配套实施手册：每项改进给出**涉及文件、具体代码、验证方式**。
> 阅读前提：已通读 roadmap，理解"先度量、后优化，硬约束优先"的总体原则。
> 标注说明：【新增】= 新建文件；【修改】= 改现有文件；代码块中 `# ...` 表示省略的现有代码。

---

## 〇、总览

| # | 改进项 | 对应 roadmap 不足 | 涉及核心文件 | 工作量 |
| --- | --- | --- | --- | --- |
| 1 | SQL 修正循环 + 重试上限 | 不足1 | `agent/state.py` `agent/graph.py` `nodes/correct_sql.py` | 0.5 天 |
| 2 | SQL 安全四道防线 + 标识符校验 | 不足5、6 | 【新增】`core/sql_guard.py`、`repositories/mysql/dw/dw_mysql_repository.py` | 1 天 |
| 3 | 评测集与跑分脚本 | 不足3 | 【新增】`eval/` 目录 | 1~2 天 |
| 4 | 召回精排 rerank | 不足4 | docker、【新增】rerank 仓储 | 1 天 |
| 5 | 多轮对话记忆 | 不足10 | 【新增】`nodes/rewrite_query.py`、`services/query_service.py`、API 层 | 1~2 天 |
| 6 | 召回与 ES 查询并行化 | 不足7 | `nodes/recall_*.py` | 0.5 天 |
| 7 | 分层缓存 | 不足8 | 【新增】`clients/redis_client_manager.py`、`services/query_service.py` | 1 天 |
| 8 | 模型分级路由 | 不足9 | `conf/`、`agent/llm.py` | 0.5 天 |
| 9 | 洞察摘要节点 | 不足11 | 【新增】`nodes/summarize_result.py` | 0.5 天 |
| 10 | 元数据增量同步 | 不足12 | 【新增】`scripts/sync_meta_incremental.py` | 1~2 天 |
| 11 | Langfuse 追踪 | 不足13 | `services/query_service.py`、`.env` | 0.5 天 |
| 12 | SSE 断线续传 | 不足14 | `api/routers/query_router.py`、`services/query_service.py` | 1 天 |

**建议依赖安装**（国内网络慢时给 pip 指定清华镜像源）：

```bash
pip install sqlglot langgraph-checkpoint-sqlite aiosqlite redis httpx langfuse -i https://pypi.tuna.tsinghua.edu.cn/simple
```

---

## 一、P0-1：SQL 修正循环 + 重试上限

**目标**：`校验失败 → 修正 → 再校验` 闭环，最多修正 2 次，超限走兜底而不是裸报错。

### 涉及文件

- 【修改】`app/agent/state.py`
- 【修改】`app/agent/graph.py`
- 【修改】`app/agent/nodes/correct_sql.py`
- 【新增】`app/agent/nodes/fail_fast.py`

### 实现方案

**1. State 增加重试计数字段**（`app/agent/state.py`）：

```python
class DataAgentState(TypedDict):
    # ... 现有字段 ...

    sql: str
    error: str
    retry_count: int  # SQL 校验失败后已经修正过的次数
```

**2. 图结构改造**（`app/agent/graph.py`）。核心变化：`correct_sql` 不再直接连 `run_sql`，而是回连 `validate_sql` 形成环；条件边增加"超限兜底"分支：

```python
from app.agent.nodes.fail_fast import fail_fast

# SQL 自动修正的最大次数，防止失败 case 无限循环烧 token
MAX_SQL_RETRIES = 2

graph_builder.add_node("fail_fast", fail_fast)


def route_after_validate(state: DataAgentState) -> str:
    """校验通过就执行；失败且未超限则修正；超限走兜底"""
    if state["error"] is None:
        return "run_sql"
    # state 运行时是 dict，首轮 retry_count 尚未写入，用 get 兜底
    if state.get("retry_count", 0) < MAX_SQL_RETRIES:
        return "correct_sql"
    return "fail_fast"


graph_builder.add_conditional_edges(
    source="validate_sql",
    path=route_after_validate,
    path_map={
        "run_sql": "run_sql",
        "correct_sql": "correct_sql",
        "fail_fast": "fail_fast",
    },
)

# 修正后重新校验，形成闭环；兜底后优雅结束
graph_builder.add_edge("correct_sql", "validate_sql")
graph_builder.add_edge("fail_fast", END)
graph_builder.add_edge("run_sql", END)
```

**3. 修正节点写入计数**（`app/agent/nodes/correct_sql.py`，只改返回值）：

```python
        writer({"type": "progress", "step": step, "status": "success"})
        # 计数 +1，供条件边判断是否超过 MAX_SQL_RETRIES
        return {"sql": result, "retry_count": state.get("retry_count", 0) + 1}
```

**4. 兜底节点**（【新增】`app/agent/nodes/fail_fast.py`）：

```python
"""SQL 重试超限兜底节点：优雅结束流程，把失败原因以 SSE 错误消息返回前端"""

from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.log import logger


async def fail_fast(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """多次修正仍失败时，向前端返回友好错误并结束，而不是抛异常中断"""

    writer = runtime.stream_writer
    writer({"type": "progress", "step": "SQL生成失败", "status": "error"})
    writer(
        {
            "type": "error",
            "message": f"SQL 多次自动修正后仍无法通过校验：{state['error']}",
        }
    )
    logger.warning(f"SQL 修正重试超限，最后错误：{state['error']}")
    return {"error": state["error"]}
```

### 验证方式

- 日志确认循环路径：`generate_sql → validate → correct(1) → validate → correct(2) → validate → fail_fast`；
- 在评测集（见 P0-3）上统计**修正成功率**：首次校验失败但最终执行成功的 case 比例，对比改造前后。

---

## 二、P0-2：SQL 安全四道防线 + 标识符校验

**目标**：LLM 生成的 SQL 经过 AST 白名单检查、强制 LIMIT、超时控制、只读账号四层防护后才接触数据库；仓储层消除字符串拼接注入面。

### 涉及文件

- 【新增】`app/core/sql_guard.py`
- 【修改】`app/repositories/mysql/dw/dw_mysql_repository.py`
- 【新增】`docker/mysql/02-init-readonly-user.sql`
- 【修改】`conf/app_config.yaml`（数仓账号切只读）

### 实现方案

**1. 安全防护核心**（【新增】`app/core/sql_guard.py`）：

```python
"""
SQL 安全防护

用 sqlglot 把 SQL 解析成 AST 后做三件事：
1. 语句白名单：只放行单条 SELECT（含 CTE/UNION），拒绝 DML/DDL/多语句
2. 强制 LIMIT：无 LIMIT 自动补，已有 LIMIT 超过上限则压回
3. 还原为规范 SQL 文本，交给仓储层执行
Prompt 里"只生成 SELECT"是软约束，这里是程序性硬约束
"""

import sqlglot
from sqlglot import expressions as exp

MAX_ROW_LIMIT = 1000


class SQLGuardError(Exception):
    """生成的 SQL 未通过安全防护"""


def guard_sql(sql: str, max_row_limit: int = MAX_ROW_LIMIT) -> str:
    statements = sqlglot.parse(sql, read="mysql")

    # 多语句（如 "select 1; delete from ..."）直接拒绝
    if len(statements) != 1:
        raise SQLGuardError("只允许执行单条查询语句")

    stmt = statements[0]
    # CTE(WITH) 在 sqlglot 中也是 exp.Select；UNION 是 exp.Union
    if not isinstance(stmt, (exp.Select, exp.Union)):
        raise SQLGuardError("只允许执行 SELECT 查询")

    # SELECT ... INTO OUTFILE/DUMPFILE 的顶层节点同样是 exp.Select（INTO 挂在
    # args["into"] 上），上面的类型检查拦不住它，必须显式拒绝——否则可写任意服务器文件
    if stmt.args.get("into"):
        raise SQLGuardError("不允许 SELECT INTO OUTFILE/DUMPFILE")

    # LIMIT 只看最外层（对 UNION 整体生效）：若逐分支加 LIMIT，
    # 总行数上限会变成 分支数 × max_row_limit，与"强制行数上限"语义不符
    limit = stmt.args.get("limit")
    if limit is None:
        stmt.limit(max_row_limit, copy=False)
    else:
        literal = limit.expression
        if isinstance(literal, exp.Literal) and literal.is_int:
            if int(literal.name) > max_row_limit:
                limit.set("expression", exp.Literal.number(max_row_limit))

    return stmt.sql(dialect="mysql")
```

**2. 仓储层接入防护 + 超时**（【修改】`app/repositories/mysql/dw/dw_mysql_repository.py`）：

```python
import asyncio
import re

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.sql_guard import guard_sql

# 数仓查询兜底超时（秒）：客户端侧防止请求悬挂
DW_QUERY_TIMEOUT_SECONDS = 30

# 标识符白名单：防注入的第一道闸
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_identifier(name: str) -> str:
    """校验表名/字段名，杜绝标识符注入"""
    if not _IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"非法 SQL 标识符：{name!r}")
    return name


class DWMySQLRepository:
    # ... 现有代码 ...

    async def get_column_types(self, table_name: str) -> dict[str, str]:
        table_name = _check_identifier(table_name)
        sql = f"show columns from {table_name}"
        # ... 现有代码 ...

    async def get_column_values(
        self, table_name: str, column_name: str, limit: int = 10
    ) -> list:
        table_name = _check_identifier(table_name)
        column_name = _check_identifier(column_name)
        # ... 现有代码 ...

    async def validate(self, sql: str):
        """先过安全防护，再用 EXPLAIN 让数据库解析"""
        guarded_sql = guard_sql(sql)
        await self.session.execute(text(f"explain {guarded_sql}"))

    async def run(self, sql: str) -> list[dict]:
        guarded_sql = guard_sql(sql)
        # 客户端兜底超时；服务端限制见下方 MAX_EXECUTION_TIME 说明
        async with asyncio.timeout(DW_QUERY_TIMEOUT_SECONDS):
            result = await self.session.execute(text(guarded_sql))
        return [dict(row) for row in result.mappings().fetchall()]
```

> **超时的两层语义（面试常问）**：`asyncio.timeout` 只取消客户端等待，服务端查询可能仍在跑；真正的服务端限制用 MySQL 8.0 的 `SET SESSION MAX_EXECUTION_TIME = 30000`（毫秒），可在 `dependencies.py` 的 `get_dw_session` 创建会话后执行一次。两层配合：客户端保证接口不悬挂，服务端保证数据库不被拖垮。

**3. 只读账号**（【新增】`docker/mysql/02-init-readonly-user.sql`）：

```sql
-- 数仓只读账号：即使前两层防护被击穿，数据库层面也无法写入
CREATE USER IF NOT EXISTS 'dw_ro'@'%' IDENTIFIED BY 'dw_ro_pass';
GRANT SELECT ON dw.* TO 'dw_ro'@'%';
FLUSH PRIVILEGES;
```

两个部署细节必须注意：

- `initdb.d` 中的脚本**只在 MySQL 数据卷首次初始化时执行**。已有 `mysql_data` 卷的环境，新增脚本会被静默跳过，切账号后直接连不上库——需手动补执行一次：
  `docker exec -i mysql mysql -uroot -pdili123 < docker/mysql/02-init-readonly-user.sql`
- `initdb.d` 按文件名字典序执行，数字前缀排在 `dw.sql`/`meta.sql`（无前缀）**之前**；`CREATE USER`/`GRANT` 不依赖库表存在，功能上顺序无碍，但别被 "02-" 这个名字误导为"在 dw.sql 之后执行"。

然后把 `conf/app_config.yaml` 的 `db_dw` 切到只读账号：

```yaml
db_dw:
  host: localhost
  port: 3306
  user: dw_ro
  password: dw_ro_pass
  database: dw
```

> 说明：数仓侧全部操作（SHOW COLUMNS、抽样值、EXPLAIN、执行 SELECT）都只读，元数据库账号保持读写（构建脚本要写 meta），互不影响。

### 验证方式

pytest 表格驱动单测（恶意/边界 SQL 用例）：

```python
import pytest
from app.core.sql_guard import guard_sql, SQLGuardError

@pytest.mark.parametrize("sql", [
    "delete from dw_order_info",
    "update t set a = 1",
    "drop table t",
    "select 1; select 2",
    "select * from t into outfile '/tmp/x'",
    "select * from t into dumpfile '/tmp/x'",
])
def test_reject(sql):
    with pytest.raises(SQLGuardError):
        guard_sql(sql)

def test_add_limit():
    assert "LIMIT 1000" in guard_sql("select * from t").upper()

def test_cap_limit():
    assert "LIMIT 1000" in guard_sql("select * from t limit 99999999").upper()

def test_union_ok():
    guard_sql("select a from t1 union all select b from t2")
```

---

## 三、P0-3：评测集与跑分脚本

**目标**：50~100 条评测 case，一键跑出召回命中率、执行成功率、修正成功率、延迟分布，作为后续所有改进的基准。

### 涉及文件

- 【新增】`eval/cases.yaml`（评测集）
- 【新增】`eval/run_eval.py`（跑分脚本）

### 评测集格式（`eval/cases.yaml`）

> 表名/字段名按 `docker/mysql/dw.sql` 的真实 schema 填写；`expected_result` 建议人工写一条正确 SQL 跑一次，把结果存下来用于执行准确率比对。

```yaml
# 每条 case：问题 + 期望命中的表/字段/关键词 + 可选的期望结果
- id: sales_total_by_region
  question: 统计华北地区的销售总额
  expected:
    tables: [fact_order, dim_region]        # 按真实表名填写
    sql_must_contain: ["SUM", "华北"]        # 生成 SQL 必须包含的片段（大小写不敏感）

- id: no_answer
  question: 2026年全球宇宙飞船销量是多少
  expected:
    should_fail: true   # 数仓没有对应数据，期望系统不编造结果
```

### 跑分脚本骨架（`eval/run_eval.py`）

```python
"""批量评测：跑评测集，产出召回命中、执行成功率、延迟分布报告"""

import argparse
import asyncio
import json
import time

import yaml

from app.agent.context import DataAgentContext
from app.agent.graph import graph
from app.agent.state import DataAgentState
from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import (
    dw_mysql_client_manager,
    meta_mysql_client_manager,
)
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


def build_context(meta_session, dw_session) -> DataAgentContext:
    """复用 graph.py 本地调试的初始化方式组装上下文（session 由调用方管理）"""
    return DataAgentContext(
        column_qdrant_repository=ColumnQdrantRepository(
            qdrant_client_manager.client
        ),
        embedding_client=embedding_client_manager.client,
        metric_qdrant_repository=MetricQdrantRepository(
            qdrant_client_manager.client
        ),
        value_es_repository=ValueESRepository(es_client_manager.client),
        meta_mysql_repository=MetaMySQLRepository(meta_session),
        dw_mysql_repository=DWMySQLRepository(dw_session),
    )


def check_recall(final_state: dict, expected: dict) -> bool:
    """召回命中：期望的表是否都进入最终表上下文"""
    got_tables = {t["name"] for t in final_state.get("table_infos", [])}
    return all(t in got_tables for t in expected.get("tables", []))


def check_sql(final_state: dict, expected: dict) -> bool:
    """SQL 必含片段检查（宽松版；更严格用结果集比对）"""
    sql = (final_state.get("sql") or "").lower()
    return all(kw.lower() in sql for kw in expected.get("sql_must_contain", []))


def check_no_answer(
    final_state: dict, dw_mysql_repository
) -> bool:
    """无答案 case：期望明确失败或空结果，而不是编造数据

    注意：state 在 P2-9 落地前没有 result 字段，error 为空时要由评测侧
    自己执行生成的 SQL 核对是否空结果，不能直接读 state.result。
    """
    if final_state.get("error"):
        return True
    sql = final_state.get("sql")
    if not sql:
        return False
    rows = await dw_mysql_repository.run(sql)
    # SUM/COUNT 等聚合在空集上返回"一行 NULL"而不是零行（如查南极销售额
    # 得到 [{'销售额': None}]），系统没给出假数字就算没有编造
    return not rows or all(
        all(value is None for value in row.values()) for row in rows
    )


async def run_case(case: dict, context: DataAgentContext) -> dict:
    start = time.perf_counter()
    # ainvoke 返回最终 state，比 astream 更适合批量评测
    final_state = await graph.ainvoke(
        input=DataAgentState(query=case["question"]), context=context
    )
    latency = time.perf_counter() - start

    expected = case.get("expected", {})
    if expected.get("should_fail"):
        # 无答案 case 不套用召回/SQL 指标（否则 all([]) 恒为 True 会虚高），
        # 单独判定系统是否"诚实拒绝"
        record = {
            "id": case["id"],
            "latency": round(latency, 2),
            "passed": await check_no_answer(
                final_state, context["dw_mysql_repository"]
            ),
            "sql": final_state.get("sql"),
            "error": final_state.get("error"),
        }
    else:
        record = {
            "id": case["id"],
            "latency": round(latency, 2),
            "passed": check_recall(final_state, expected)
            and check_sql(final_state, expected),
            "recall_hit": check_recall(final_state, expected),
            "sql_ok": check_sql(final_state, expected),
            "sql": final_state.get("sql"),
            "error": final_state.get("error"),
            "retried": final_state.get("retry_count", 0) > 0,
        }
    # 有 expected_result 时做执行准确率比对（结果集排序后比较）
    print(json.dumps(record, ensure_ascii=False))
    return record


def summarize(records: list[dict]) -> dict:
    latencies = sorted(r["latency"] for r in records)
    normal = [r for r in records if "recall_hit" in r]
    no_answer = [r for r in records if "recall_hit" not in r]
    summary = {
        "total": len(records),
        "pass_rate": sum(r["passed"] for r in records) / len(records),
        "p50": latencies[len(latencies) // 2],
        "p95": latencies[int(len(latencies) * 0.95)],
    }
    if normal:
        summary.update(
            recall_rate=sum(r["recall_hit"] for r in normal) / len(normal),
            sql_ok_rate=sum(r["sql_ok"] for r in normal) / len(normal),
            retry_rate=sum(r["retried"] for r in normal) / len(normal),
        )
    if no_answer:
        summary.update(
            no_answer_pass_rate=sum(r["passed"] for r in no_answer) / len(no_answer)
        )
    return summary


async def run_all(cases: list[dict]) -> list[dict]:
    """初始化客户端，并在 session 存活范围内跑完全部 case

    不能在 async with 块内 return context——退出 with 时 session 会被关闭，
    仓储将持有已关闭的连接；必须让整个评测循环都留在块内。
    """
    qdrant_client_manager.init()
    embedding_client_manager.init()
    es_client_manager.init()
    meta_mysql_client_manager.init()
    dw_mysql_client_manager.init()

    async with (
        meta_mysql_client_manager.session_factory() as meta_session,
        dw_mysql_client_manager.session_factory() as dw_session,
    ):
        context = build_context(meta_session, dw_session)
        records = [await run_case(case, context) for case in cases]

    await qdrant_client_manager.close()
    await es_client_manager.close()
    await meta_mysql_client_manager.close()
    await dw_mysql_client_manager.close()
    return records


async def main(cases_file: str):
    with open(cases_file, encoding="utf-8") as f:
        cases = yaml.safe_load(f)

    records = await run_all(cases)

    report = summarize(records)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    with open("eval/report.json", "w", encoding="utf-8") as f:
        json.dump({"summary": report, "records": records}, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default="eval/cases.yaml")
    args = parser.parse_args()
    asyncio.run(main(args.cases))
```

运行：`python eval/run_eval.py --cases eval/cases.yaml`

### 验证方式

- 先用 10 条 case 人工核对脚本判定是否正确（特别是 `should_fail` case 是否真的在判"诚实拒绝"），再扩到 50~100 条；
- `eval/report.json` 即基线报告，之后每项改进跑一次对比 `summary`。

---

## 四、P1-4：召回精排 rerank

**目标**：粗排候选交给 reranker 精排，降低上下文噪声。部署方式与现有 TEI embedding 保持一致。

### 涉及文件

- 【修改】`docker/docker-compose.yaml`（新增 reranker 服务）
- 【新增】`app/repositories/rerank/rerank_repository.py`
- 【修改】`app/agent/context.py`、`app/api/dependencies.py`、`app/services/query_service.py`（注入）
- 【修改】`app/agent/nodes/merge_retrieved_info.py`（接入精排）

### 实现方案

**1. docker-compose 新增服务**（模型先下载到 `docker/embedding/bge-reranker-v2-m3`）：

```yaml
  tei-rerank:
    image: ghcr.io/huggingface/text-embeddings-inference:cpu-1.7
    volumes:
      - ./embedding/bge-reranker-v2-m3:/data
    command: --model-id /data
    ports:
      - "8082:80"
```

**2. 精排仓储**（【新增】`app/repositories/rerank/rerank_repository.py`，TEI 原生提供 `/rerank` 接口）：

```python
import httpx


class RerankRepository:
    """调用 TEI /rerank 接口，返回 (原始下标, 相关性分数) 列表，按分数降序"""

    def __init__(self, base_url: str, client: httpx.AsyncClient):
        self.base_url = base_url
        self.client = client

    async def rerank(
        self, query: str, documents: list[str], top_n: int
    ) -> list[tuple[int, float]]:
        resp = await self.client.post(
            f"{self.base_url}/rerank",
            json={"query": query, "texts": documents, "return_text": False, "top_n": top_n},
        )
        resp.raise_for_status()
        return [(item["index"], item["score"]) for item in resp.json()]
```

**3. 接入合并节点**（`app/agent/nodes/merge_retrieved_info.py`，在"第 1 步按 column_id 合并去重"之后、"第 2 步补齐指标依赖字段"之前插入）：

```python
        # 1.5 对召回字段做精排：把"字段名+描述+别名"拼成文档，用 reranker 按问题相关性重排
        # 精排只排序不增删，所以放在去重后、依赖字段补齐前，避免误删主外键等必要字段
        column_list = list(retrieved_column_infos_map.values())
        documents = [
            f"{info.name}：{info.description}（别名：{'、'.join(info.alias)}）"
            for info in column_list
        ]
        ranked = await rerank_repository.rerank(state["query"], documents, top_n=20)
        retrieved_column_infos_map = {
            column_list[idx].id: column_list[idx] for idx, _ in ranked
        }
        # 指标候选同理精排（用 name+description+alias 拼文档）
```

`rerank_repository` 通过 `DataAgentContext` 注入（与现有仓储同模式）：`app/agent/context.py` 加字段，`dependencies.py` 加 `get_rerank_repository()`，`query_service.py` 构造函数接收并放入 context。注意 `httpx.AsyncClient` 是应用级资源：应在 `lifespan` 中创建一次、应用关闭时释放（仿照现有 client manager 模式，新建 `app/clients/rerank_client_manager.py`），由依赖层取用，而不是每个请求新建连接。

### 验证方式

评测集对比接入前后：Recall@K、SQL 执行准确率、进入 SQL 生成上下文的 token 数（精排应让 token 变小而准确率不降或上升）。

---

## 五、P1-5：多轮对话记忆

**目标**：支持"那华东呢？"式追问。通过 checkpointer 持久化会话状态 + 入口改写节点做指代消解。

### 涉及文件

- 【新增】`app/agent/nodes/rewrite_query.py`
- 【修改】`app/agent/state.py`、`app/agent/graph.py`、`app/api/schemas/query_schema.py`、`app/api/routers/query_router.py`、`app/services/query_service.py`、`app/api/lifespan.py`

### 实现方案

**1. State 增加历史字段**（`app/agent/state.py`）：

```python
    history: list[dict]  # 会话历史 [{"question": ..., "sql": ...}]
```

**2. 图入口增加改写节点**（【新增】`app/agent/nodes/rewrite_query.py`）：

```python
"""查询改写节点：结合历史对话把追问改写成独立完整的问题，并重置上一轮的中间状态"""

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.core.log import logger
from app.prompt.prompt_loader import load_prompt

# checkpointer 会让 state 跨请求持久化，
# 上一轮的召回/SQL/错误等中间结果必须在本轮入口清空，否则会污染本轮链路
_RESET_FIELDS = {
    "keywords": [],
    "retrieved_column_infos": [],
    "retrieved_metric_infos": [],
    "retrieved_value_infos": [],
    "table_infos": [],
    "metric_infos": [],
    "sql": "",
    "error": None,
    "retry_count": 0,
}


async def rewrite_query(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    step = "改写查询"
    writer({"type": "progress", "step": step, "status": "running"})

    query = state["query"]
    history = state.get("history", [])

    # 首轮提问无需改写，直接放行
    if not history:
        return {"query": query, "history": [{"question": query, "sql": None}], **_RESET_FIELDS}

    prompt = PromptTemplate(
        template=load_prompt("rewrite_query"),
        input_variables=["history", "query"],
    )
    chain = prompt | llm | StrOutputParser()
    history_text = "\n".join(
        f"问：{h['question']}\nSQL：{h['sql'] or '（生成失败）'}" for h in history[-3:]
    )
    rewritten = (await chain.ainvoke({"history": history_text, "query": query})).strip()

    logger.info(f"查询改写：{query!r} -> {rewritten!r}")
    writer({"type": "progress", "step": step, "status": "success"})
    return {
        "query": rewritten,
        "history": history + [{"question": query, "sql": None}],
        **_RESET_FIELDS,
    }
```

配套【新增】`prompts/rewrite_query.prompt`：要求"结合历史对话，把最新追问改写成不依赖上下文、语义完整的独立问题；若已完整则原样输出；只输出改写后的问题"。

**3. 图结构**（`app/agent/graph.py`）：`START → rewrite_query → extract_keywords`，其余不变；编译时挂 checkpointer。

**4. checkpointer 接入**。注意：当前 `graph.py` 在模块导入期编译，而 checkpointer 需要生命周期管理，推荐把装配逻辑收敛成工厂函数：

```python
# app/agent/graph.py
def build_graph(checkpointer=None):
    builder = StateGraph(state_schema=DataAgentState, context_schema=DataAgentContext)
    # ... 现有的 add_node / add_edge 全部搬进来 ...
    return builder.compile(checkpointer=checkpointer)


# 保留无 checkpointer 的全局实例，兼容 graph.py 本地调试
graph = build_graph()
```

```python
# app/api/lifespan.py
from contextlib import asynccontextmanager
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ... 现有客户端初始化 ...
    async with AsyncSqliteSaver.from_conn_string("data/checkpoints.sqlite") as checkpointer:
        app.state.graph = build_graph(checkpointer)
        yield
    # ... 现有清理 ...
```

**5. API 与服务层传递会话 ID**：

```python
# app/api/schemas/query_schema.py
class QuerySchema(BaseModel):
    query: str
    conversation_id: str | None = None  # 前端每个会话生成一个 UUID
```

```python
# app/services/query_service.py
async def query(self, query: str, conversation_id: str | None = None):
    state = DataAgentState(query=query, history=[])
    config = {"configurable": {"thread_id": conversation_id}} if conversation_id else {}
    async for chunk in graph.astream(input=state, context=context, stream_mode="custom", config=config):
        ...
```

> 同一会话内多次调用共享 thread_id，checkpointer 自动带上历史 state——这正是改写节点能读到 `history` 的原因。另外 `run_sql` 成功后建议把 SQL 回填进 `history[-1]["sql"]`，追问改写时"沿用上次的表和口径"会准很多。

> **graph 实例的衔接（容易漏）**：`query_service.py` 目前是模块级 `from app.agent.graph import graph`，拿到的是无 checkpointer 的全局实例。改造后 QueryService 应改为构造注入：`__init__` 增加 `graph` 参数，`dependencies.py` 的 `get_query_service` 加 `request: Request` 参数、从 `request.app.state.graph` 取实例传入——否则 lifespan 里挂的带 checkpointer 的图根本没被用到。

### 验证方式

手工链路："华北地区销售额多少" → "那华东呢？" → "按月拆分看华东"，用日志确认第二、三轮改写后的完整问题与最终 SQL 正确；同时检查跨会话（不同 conversation_id）互不串扰。

---

## 六、P2-6：召回与 ES 查询并行化

**目标**：三个召回节点内的逐关键词串行 IO 改为并发，降低端到端延迟。

### 涉及文件

- 【修改】`app/agent/nodes/recall_column.py`、`recall_metric.py`、`recall_value.py`

### 实现方案

`recall_column.py`（字段召回，Embedding 批量化 + Qdrant 并发；`recall_metric` 同理）：

```python
import asyncio

        # ... LLM 关键词扩展不变 ...
        keywords = list(set(keywords + result))

        # 之前：逐关键词 await embedding + await search，延迟线性叠加
        # 现在：Embedding 批量接口一次拿到全部向量，再并发检索
        embeddings = await embedding_client.aembed_documents(keywords)
        search_results = await asyncio.gather(
            *(column_qdrant_repository.search(e) for e in embeddings)
        )

        column_info_map: dict[str, ColumnInfo] = {}
        for current_column_infos in search_results:
            for column_info in current_column_infos:
                column_info_map.setdefault(column_info.id, column_info)
```

`recall_value.py`（ES 并发）：

```python
        keywords = list(set(keywords + result))
        search_results = await asyncio.gather(
            *(value_es_repository.search(keyword) for keyword in keywords)
        )
        value_infos_map: dict[str, ValueInfo] = {}
        for current_value_infos in search_results:
            for current_value_info in current_value_infos:
                value_infos_map.setdefault(current_value_info.id, current_value_info)
```

> 关键词数量级在十几个，无需限流；若未来扩展到上百，用 `asyncio.Semaphore` 包一层。更进一步可改 ES `msearch` 把 N 次网络往返合成 1 次，属于锦上添花。

### 验证方式

评测集 P50/P95 延迟对比；日志中单节点耗时（可临时在节点入口/出口打点）。

---

## 七、P2-7：分层缓存

**目标**：相同问题命中缓存直接重放，不再全链路重跑；schema 变更可整体失效。

### 涉及文件

- 【修改】`docker/docker-compose.yaml`（加 Redis 服务）
- 【新增】`app/clients/redis_client_manager.py`
- 【修改】`conf/app_config.yaml`、`app/conf/app_config.py`、`app/services/query_service.py`

### 实现方案

docker-compose 增加：

```yaml
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

缓存策略设计（核心在失效）：

- **键**：`qa:sse:{schema_version}:{归一化问题的 SHA256 前 32 位}`；归一化 = 去空白 + 转小写；
- **值**：该次问数产生的完整 SSE 事件列表（JSON），命中后按原顺序重放；
- **失效**：缓存键里带 `schema_version`（存 Redis），元数据增量同步脚本（见 P2-10）发现 schema diff 时自增版本号——旧版本键前缀失配自然失效，再配 TTL（如 1 小时）兜底清理。**不逐 key 删除，用版本号整体切换**，避免"改了一张表、清了半个缓存"的不一致。

`query_service.py` 改造：

```python
import hashlib
import json

def _cache_key(query: str, schema_version: str) -> str:
    normalized = "".join(query.split()).lower()
    digest = hashlib.sha256(normalized.encode()).hexdigest()[:32]
    return f"qa:sse:{schema_version}:{digest}"


class QueryService:
    def __init__(self, ..., redis_client):  # 经依赖层注入
        self.redis_client = redis_client
        ...

    async def query(self, query: str):
        schema_version = await self.redis_client.get("dw:schema_version") or "0"
        key = _cache_key(query, schema_version)

        cached = await self.redis_client.get(key)
        if cached:
            for chunk in json.loads(cached):
                yield f"data: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"
            return

        events = []
        async for chunk in graph.astream(...):
            events.append(chunk)
            yield f"data: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"

        # 含 error 事件的流不缓存：fail_fast 兜底的失败带有随机性（重问一次可能就成功），
        # 缓存 1 小时会让用户失去重试机会
        if not any(e.get("type") == "error" for e in events):
            await self.redis_client.set(
                key, json.dumps(events, ensure_ascii=False, default=str), ex=3600
            )
```

### 验证方式

同一问题连续请求两次，第二次日志无 LLM 调用且毫秒级返回；自增 `schema_version` 后同问题重新全链路执行。

---

## 八、P2-8：模型分级路由

**目标**：轻量任务（关键词抽取/扩展、表与指标过滤）换小模型，SQL 生成与修正保留大模型，压延迟降成本。

### 涉及文件

- 【修改】`conf/app_config.yaml`、`app/conf/app_config.py`、`app/agent/llm.py`
- 【修改】`app/agent/nodes/extract_keywords.py`、`recall_column.py`、`recall_value.py`、`recall_metric.py`、`filter_table.py`、`filter_metric.py`（把 `from app.agent.llm import llm` 改为 `from app.agent.llm import llm_light`。注意项目里没有独立的 `extend_keywords_*.py` 节点——关键词扩展逻辑内嵌在三个召回节点中）

### 实现方案

```yaml
# conf/app_config.yaml 追加
llm_light:
  model_name: qwen-turbo
  api_key: ${oc.env:LLM_API_KEY}
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1
```

```python
# app/conf/app_config.py：AppConfig 增加 llm_light 字段
@dataclass
class AppConfig:
    # ... 现有字段 ...
    llm: LLMConfig
    llm_light: LLMConfig  # 轻量任务模型
```

```python
# app/agent/llm.py
llm = init_chat_model(
    model=app_config.llm.model_name,
    model_provider="openai",
    base_url=app_config.llm.base_url,
    api_key=app_config.llm.api_key,
    temperature=0,
)

# 抽取/过滤类节点专用：任务简单，用小而快的模型
llm_light = init_chat_model(
    model=app_config.llm_light.model_name,
    model_provider="openai",
    base_url=app_config.llm_light.base_url,
    api_key=app_config.llm_light.api_key,
    temperature=0,
)
```

### 验证方式

评测集确认执行准确率无可感知下降；对比单次问数 token 消耗与 P50 延迟。

---

## 九、P2-9：洞察摘要节点

**目标**：执行成功后追加 1~3 句自然语言结论，前端展示"数据 + 结论"。

### 涉及文件

- 【修改】`app/agent/state.py`（加 `result: list[dict]`）、`app/agent/nodes/run_sql.py`（返回结果行）、`app/agent/graph.py`
- 【新增】`app/agent/nodes/summarize_result.py`、`prompts/summarize_result.prompt`

### 实现方案

`run_sql.py` 在 writer 输出 result 的同时把行数据写回 state：

```python
        writer({"type": "progress", "step": step, "status": "success"})
        writer({"type": "result", "data": result})
        return {"result": result}   # 供摘要节点消费
```

图改为：`run_sql → summarize_result → END`。注意这条边是在 P0-1 的基础上叠加：把 P0-1 建立的 `run_sql → END` 改接 `summarize_result`——两份方案改的是同一条边，实施时合并处理，别漏。

```python
# app/agent/nodes/summarize_result.py
"""结果摘要节点：基于查询结果生成简短业务结论，流式返回"""

import yaml
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


async def summarize_result(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    result = state.get("result") or []
    if not result:
        return

    writer = runtime.stream_writer
    writer({"type": "progress", "step": "生成结论", "status": "running"})

    # 大结果集只采样前 20 行给模型，避免超长上下文
    sample = result[:20]
    prompt = PromptTemplate(
        template=load_prompt("summarize_result"),
        input_variables=["query", "result"],
    )
    chain = prompt | llm | StrOutputParser()
    summary = await chain.ainvoke(
        {
            "query": state["query"],
            "result": yaml.dump(sample, allow_unicode=True, sort_keys=False),
        }
    )
    writer({"type": "summary", "data": summary})
    return {"summary": summary}
```

前端在 SSE 处理中增加 `summary` 类型（建议流式逐字渲染）。自动图表推荐用**规则**而非 LLM：结果含时间列 + 数值列 → 折线；分类列 + 单数值列 → 柱状/饼图；无需后端改动时前端本地判断即可。

### 验证方式

人工检查 10 个不同形态的结果（空结果、单行、多行时间序列）摘要是否准确、是否编造了结果里不存在的数字（Prompt 中明确"只基于给定数据，禁止推测"）。

---

## 十、P2-10：元数据增量同步

**目标**：数仓 schema 演进后，元数据库、Qdrant 向量、ES 索引自动保持一致，消除"静默不一致"。

### 涉及文件

- 【新增】`app/scripts/sync_meta_incremental.py`

### 实现方案（流程 + 关键代码骨架）

```
定时任务（APScheduler / crontab，如每小时）
  1. 读真实 schema：information_schema.columns（table_name, column_name, column_type, column_comment）
  2. 读元数据库当前快照
  3. 求 diff：新增表/字段、变更字段（类型/描述）、删除字段
  4. 无 diff → 结束；有 diff →
     a. 增量 upsert 元数据库（与首次构建同一套 Service/Repository）
     b. 对变更字段重新构造 embedding 文本 → Qdrant upsert（point id = column_id）
     c. 对变更字段重新抽样值 → 重建对应 ES 文档
     d. Redis 自增 dw:schema_version（联动 P2-7 缓存整体失效）
  5. 记录同步日志（版本号、变更清单）
```

两个关键约束：

- **embedding 文本构造必须与首次构建完全一致**（同一模板、同一字段拼接顺序），否则同一字段新旧向量语义空间不一致，精排/召回质量漂移且难排查。建议把文本模板抽成共享函数，构建脚本和同步脚本共同引用；
- **删除用软删**：元数据标记 `is_deleted` 而非物理删除，Qdrant/ES 同步移除，保留追溯能力。

diff 检测核心查询：

```sql
SELECT table_name, column_name, column_type, column_comment
FROM information_schema.columns
WHERE table_schema = 'dw'
```

与元数据库逐行比对，产生 `added / changed / removed` 三个列表后分派处理。

### 验证方式

手工改数仓（加一列、改一个 comment）→ 触发同步 → 验证 meta/Qdrant/ES 三处一致 → 用受影响的评测 case 确认问数链路能召回新字段。

---

## 十一、P3-11：Langfuse 追踪

**目标**：每次问数一条 trace，节点级 span 记录 LLM 输入输出、token、耗时。

### 涉及文件

- 【修改】`pyproject.toml`（langfuse）、`.env`、`app/services/query_service.py`

### 实现方案

LangChain 生态接入最省事：一个 callback 传入 `astream` 的 config，LangGraph 会自动传播到所有节点的 LLM 调用，无需改任何节点代码。

```bash
# .env
LANGFUSE_PUBLIC_KEY=pk-...
LANGFUSE_SECRET_KEY=sk-...
LANGFUSE_HOST=http://localhost:3000   # 私有化部署；也可用 SaaS
```

```python
# app/services/query_service.py
from langfuse.langchain import CallbackHandler

class QueryService:
    def __init__(self, ...):
        ...
        self.langfuse_handler = CallbackHandler()

    async def query(self, query: str):
        ...
        async for chunk in graph.astream(
            input=state,
            context=context,
            stream_mode="custom",
            config={
                "callbacks": [self.langfuse_handler],
                # 与现有 request_id 日志打通，Langfuse 里可按会话过滤
                "metadata": {"request_id": request_id},
            },
        ):
            ...
```

### 验证方式

发起一次问数，Langfuse 控制台看到完整 trace：节点树、每次 LLM 调用的 prompt/completion/token/耗时；坏 case 可直接导出沉淀进评测集（与 P0-3 形成闭环）。

---

## 十二、P3-12：SSE 断线续传

**目标**：连接中断后按事件序号补发已产生的事件；原请求已完成的场景不重跑链路、不重复消耗 token（架构局限见本节文末说明）。

### 涉及文件

- 【修改】`app/api/routers/query_router.py`、`app/services/query_service.py`（依赖 P2-7 的 Redis）

### 实现方案

**事件格式**带自增 `id`，服务端用 Redis 缓冲事件流（TTL 10 分钟）：

```python
# query_service.py
def _sse(seq: int, chunk: dict) -> str:
    return f"id: {seq}\ndata: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"


_END_EVENT = json.dumps({"type": "end"})


async def query(self, query: str, request_id: str, resume_from: int | None = None):
    buffer_key = f"qa:sse_buffer:{request_id}"

    if resume_from is not None:
        # 断线重连：从缓冲补发，而不是重跑图
        buffered = await self.redis_client.lrange(buffer_key, resume_from + 1, -1)
        if buffered and buffered[-1] == _END_EVENT:
            # 末尾是 end 哨兵 → 缓冲完整，安全补发
            for i, chunk_json in enumerate(buffered, start=resume_from + 1):
                yield f"id: {i}\ndata: {chunk_json}\n\n"
            return
        if buffered:
            # 半截流：原请求断开时 generator 已被取消、图执行中止，
            # 缺失的事件不会再产生。必须明确告知前端中断，不能假装续传成功
            yield (
                "data: "
                + json.dumps(
                    {"type": "interrupted", "message": "连接中断且无法续传，请重新提问"},
                    ensure_ascii=False,
                )
                + "\n\n"
            )
            return
        # 缓冲已过期 → 前端需重新发起完整请求（配合前端提示）

    seq = 0
    async for chunk in graph.astream(...):
        chunk_json = json.dumps(chunk, ensure_ascii=False, default=str)
        await self.redis_client.rpush(buffer_key, chunk_json)
        await self.redis_client.expire(buffer_key, 600)
        yield f"id: {seq}\ndata: {chunk_json}\n\n"
        seq += 1

    # 正常结束写入 end 哨兵并推给前端；异常/客户端断开时不会执行到这里，
    # 缓冲末尾因此没有 end——重连方据此区分"完整流"与"半截流"
    await self.redis_client.rpush(buffer_key, _END_EVENT)
    await self.redis_client.expire(buffer_key, 600)
    yield f"id: {seq}\ndata: {_END_EVENT}\n\n"
```

```python
# query_router.py
import uuid

@query_router.post("/api/query")
async def query_handler(
    query: QuerySchema,
    request: Request,
    query_service: Annotated[QueryService, Depends(get_query_service)],
):
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    last_event_id = request.headers.get("last-event-id")

    return StreamingResponse(
        query_service.query(
            query.query,
            request_id=request_id,
            resume_from=int(last_event_id) if last_event_id else None,
        ),
        media_type="text/event-stream",
        headers={"X-Request-Id": request_id},
    )
```

> **一个现实约束（值得写进面试谈资）**：浏览器原生 `EventSource` 只支持 GET，而问数接口是 POST 且需要带请求体。所以前端要用 `fetch` + `ReadableStream` 自己解析 SSE 流，并把收到的最后一个 `id` 记下来，重连时带上 `Last-Event-ID` 与 `X-Request-Id` 两个头。协议语义（SSE 标准的续传机制）和工程实现（fetch 手工解析）是两回事。

> **架构局限（心里要有数）**：当前"边执行边 yield"的结构下，客户端断开时 Starlette 会取消 generator，图执行随之中止——所以本方案只能覆盖"重连时原请求已跑完"的场景；断线瞬间正在执行的 LLM 调用结果会作废。要做到"断线也不重跑"，需把 graph 执行挪进后台任务（`asyncio.create_task`）写 Redis，HTTP 层只读缓冲（轮询或 pub/sub），复杂度上一个台阶。除非弱网重连是硬需求，否则到此为止即可。

### 验证方式

发起长查询途中手动断网 2 秒再恢复，确认前端补齐缺失事件、后端没有重跑图（Langfuse/日志中无第二次 LLM 调用）。

---

## 十三、实施顺序（每步都过评测集）

```
第 1 步  P0-3 评测集            → 产出基线数字与失败 case 分布
第 2 步  P0-2 安全防线 + P0-1 修正循环
        → 产出：恶意 SQL 全拦截（单测证明）、修正成功率提升（评测对比）
第 3 步  P2-6 并行化 + P2-8 模型分级（低风险性能优化）
        → 产出：P50/P95 下降、token 成本下降，准确率不降
第 4 步  P1-5 多轮 或 P1-4 rerank（二选一做深）
        → 产出：可演示的多轮追问 / 精排前后召回指标对比
第 5 步  P2-7 缓存 + P2-10 增量同步 + P3-11 追踪 + P2-9 摘要
        → 工程化补全，按需排期
第 6 步  P3-12 SSE 续传（锦上添花）
```

每完成一项，运行一次 `eval/run_eval.py`，把 `summary` 追加到改进记录表——最终你会得到一张"每次改动 → 指标变化"的完整对照表，这既是回归保护，也是最有说服力的项目成果证明。
