r"""
批量评测：跑评测集，产出召回命中、执行成功率、延迟分布报告

整体逻辑（三层）：
    1. 读 eval/cases.yaml：每条 case = 用户问题 + 期望（期望命中的表 / SQL 必含片段 / 是否无答案）
    2. 对每条 case 调 graph.ainvoke()，让项目真实跑一遍完整问数链路
       （关键词抽取 -> 三路召回 -> 合并过滤 -> 生成 SQL -> EXPLAIN 校验 -> 修正 -> 执行），
       拿到跑完后的最终 state——里面带着 table_infos、sql、error 等过程产物
    3. 按 case 类型做"阅卷"：普通 case 查检索层 + 生成层两关，
       无答案 case 反向判"系统是否诚实拒绝而不是编造数据"，最后汇总成指标报告

完整启动流程（Windows + conda 环境 shopkeeper，命令均在 cmd 执行）：

【首次准备（只做一次，详见 docs/环境搭建踩坑记录.md）】
    conda create -n shopkeeper python=3.14 -c https://mirrors.tuna.tsinghua.edu.cn/anaconda/cloud/conda-forge --override-channels --yes
    conda activate shopkeeper
    cd /d d:\BaiduNetdiskDownload\shopkeeper-agent
    pip install -e . -i https://pypi.tuna.tsinghua.edu.cn/simple
    copy .env.example .env    然后编辑 .env 写入 LLM_API_KEY（阿里云百炼的 key）
    启动 Docker Desktop，等左下角 Engine running（docker ps 不报错才算数）
    docker compose -f docker/docker-compose.yaml up -d    启动 5 个基础容器
    python -m app.scripts.build_meta_knowledge -c conf/meta_config.yaml    构建元数据知识库

【日常运行评测（每次都要做的）】
    1. 启动 Docker Desktop（容器配置了 restart: unless-stopped，会自动拉起）
    2. conda activate shopkeeper
    3. cd /d d:\BaiduNetdiskDownload\shopkeeper-agent
    4. python eval/run_eval.py --cases eval/cases.yaml
       脚本会自动等待 ES / Embedding 服务就绪（最多 90 秒），就绪后开始逐条评测

    前置条件检查清单：5 个容器 Up（docker ps）、.env 里有 LLM_API_KEY、
    元数据已构建（首次跑 build 脚本后，数据在 named volume 里持久存在，不用重复构建）。
    注意：项目 MySQL 走宿主机 3307、ES 走 9201（端口冲突已挪，见 conf/app_config.yaml 注释）。

产出（四件，历史互不覆盖）：
    - 终端逐条打印 [PASS]/[FAIL] + 一句话判定说明，失败的给出人话归因，
      并自动与上一次同评测集的跑分对比（改进/回归一眼可见）
    - eval/report.html：可视化报告（浏览器打开，红色行 = 失败记录）
    - eval/report.json：最近一次的完整报告（summary + failed 汇总区 + 逐条 record 带 note）
    - eval/reports/report_<时间戳>.json：每次跑分的完整快照，历史细节可回溯
    - eval/history.jsonl：每次跑分的 summary 追加记录（一行一次），趋势数据源
"""

import argparse
import asyncio
import html
import json
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

# 评测脚本是 app 的"纯消费者"：只 import 项目模块来复用真实链路，
# 不反向修改任何生产代码。import graph 时会连带加载全部节点定义。
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
from app.conf.app_config import app_config
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository


def _http_ok(url: str) -> bool:
    """同步探活：2 秒超时，任何异常（连接拒绝/超时/非 200）都视为未就绪"""
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


async def wait_for_services(timeout_seconds: int = 90) -> None:
    """跑评测前等待外部服务就绪

    为什么需要这一步："容器 Up"不等于"服务就绪"——
    ES 启动后要先恢复分片（几十秒），TEI 要加载 1.3GB 模型（约 1 分钟），
    这个窗口期端口虽然开着，请求却会被断开（ServerDisconnectedError）
    或返回 502。这里轮询两个 HTTP 健康端点，全部就绪才开始评测。

    端口从 app_config 读取：改了 docker-compose 映射后这里自动跟随。

    MySQL 和 Qdrant 不单独检查：前者卷已初始化、恢复很快，
    后者连接稳定；它们若没起来，评测会在第一个 case 上快速失败，容易定位。
    """
    endpoints = {
        "Elasticsearch": f"http://localhost:{app_config.es.port}/_cluster/health",
        "Embedding(TEI)": f"http://localhost:{app_config.embedding.port}/health",
    }
    pending = dict(endpoints)
    print(f"等待外部服务就绪（最多 {timeout_seconds}s）：{', '.join(pending)}")
    deadline = time.monotonic() + timeout_seconds
    while pending:
        if time.monotonic() > deadline:
            raise RuntimeError(
                f"服务在 {timeout_seconds}s 内未就绪：{', '.join(pending)}。"
                "请确认 Docker Desktop 已启动，且执行过 "
                "docker compose -f docker/docker-compose.yaml up -d"
            )
        # to_thread 把同步探活丢进线程池，不阻塞事件循环
        results = await asyncio.gather(
            *(asyncio.to_thread(_http_ok, url) for url in pending.values())
        )
        for (name, _), ok in zip(list(pending.items()), results):
            if ok:
                print(f"[就绪] {name}")
                pending.pop(name)
        if pending:
            await asyncio.sleep(2)


def build_context(meta_session, dw_session) -> DataAgentContext:
    """复用 graph.py 本地调试的初始化方式组装上下文（session 由调用方管理）

    session 不在这里创建和关闭，而是由 run_all 统一管理——
    原因见 run_all 的注释：session 的存活范围必须覆盖全部 case 的执行。
    """
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
    """召回命中：期望的表是否都进入最终表上下文

    判定语义是"子集匹配"：expected.tables 里的每张表都必须出现在
    final_state 的 table_infos 中；但 state 里多出其他表不扣分——
    一个业务问题关联到比预期更多的表是合理现象（比如顺带补齐主外键依赖表）。

    注意 table_infos 是"过滤节点处理之后"还活着的表，所以这一关
    实际同时考核了召回（recall_* 节点）和过滤（filter_table）两层。
    """
    got_tables = {t["name"] for t in final_state.get("table_infos", [])}
    return all(t in got_tables for t in expected.get("tables", []))


def check_sql(final_state: dict, expected: dict) -> bool:
    """SQL 必含片段检查（宽松版；更严格用结果集比对）

    为什么是"片段包含"而不是整条 SQL 比对？
    同一个业务问题可以写出多条正确的 SQL：字段顺序、JOIN 路径、
    别名、聚合写法都可能不同，逐字符对答案会大量误杀。
    真正稳定不变的是"业务上必须出现的要素"——比如问销售额
    必然有 SUM、过滤华北必然有华北这个值。片段全命中才算过。

    已知盲区：SQL 语义错了但片段恰好都在（如时间范围写错）测不出来，
    严格方案是为重点 case 补 expected_result 做结果集比对。
    """
    sql = (final_state.get("sql") or "").lower()
    return all(kw.lower() in sql for kw in expected.get("sql_must_contain", []))


def judge_note(
    final_state: dict, expected: dict, recall_hit: bool, sql_ok: bool
) -> str:
    """普通 case 的一句话判定说明：通过时简述依据，失败时给出人话归因

    归因按链路分层，直接指出坏在哪一层、缺了什么：
    - 检索层：哪些期望的表没进入最终上下文（实际命中了哪些）
    - 生成层：SQL 缺了哪些必含片段
    - 执行层：校验/执行的报错原文
    """
    if recall_hit and sql_ok and not final_state.get("error"):
        return "表召回与 SQL 片段全部命中"

    problems = []
    if not recall_hit:
        expected_tables = set(expected.get("tables", []))
        got_tables = {t["name"] for t in final_state.get("table_infos", [])}
        missing = sorted(expected_tables - got_tables)
        problems.append(
            f"检索层：期望的表 {missing} 未进入最终上下文（实际命中 {sorted(got_tables)}）"
        )
    if not sql_ok:
        sql_lower = (final_state.get("sql") or "").lower()
        missing_keywords = [
            kw
            for kw in expected.get("sql_must_contain", [])
            if kw.lower() not in sql_lower
        ]
        problems.append(f"生成层：SQL 缺少片段 {missing_keywords}")
    if final_state.get("error"):
        problems.append(f"执行层：{final_state['error']}")
    return "；".join(problems)


async def check_no_answer(
    final_state: dict, dw_mysql_repository: DWMySQLRepository
) -> tuple[bool, str]:
    """无答案 case：期望系统明确失败或返回空结果，而不是编造数据

    这是评测里唯一"反向"的判定——考的不是"答对了没"，
    而是"数仓没数据时，系统敢不敢承认没有"。模型编造一个数字
    是问数系统最危险的失败模式，单独用这类 case 兜住它。

    判定路径（返回 (是否通过, 人话说明)）：
    - error 非空 -> 系统明确报了失败，算诚实拒绝，通过；
    - error 为空但生成了 SQL -> 评测侧自己拿这条 SQL 去数仓执行一次：
      - 结果为空 -> 没有编造，通过；
      - 结果全为 NULL -> 同样通过。注意 SUM/COUNT 等聚合在空集上
        返回"一行 NULL"而不是零行（如查南极销售额得到 [{'销售额': None}]），
        系统没给出假数字，就算没有编造；
      - 有真实值 -> 数仓里不存在的数据被"查"出来了，判定为编造，不通过。

    （state 在 P2-9 摘要节点落地前没有 result 字段，所以必须自己执行核对；
    P2-9 落地后可直接读 state 里的结果。）

    已知局限：本判定只考"不编造"这一层。"主动告知用户没有数据"
    是另一个产品能力（空结果友好提示，已在 run_sql 里落地 empty 标记）。
    """
    if final_state.get("error"):
        return True, "系统明确报错，未编造数据"

    sql = final_state.get("sql")
    if not sql:
        return False, "未生成 SQL 也未报错（异常状态，需要排查）"

    rows = await dw_mysql_repository.run(sql)
    if not rows:
        return True, "查询结果为空，系统没有编造数据"
    if all(all(value is None for value in row.values()) for row in rows):
        return True, "查询结果全为 NULL（聚合空集），系统没有编造数据"

    preview = rows[0]
    return (
        False,
        f"编造：数仓中并不存在该数据，但查询返回了真实值 {preview}"
        "（常见于 COALESCE 等把 NULL 兜底成 0 的写法）",
    )


async def run_case(case: dict, context: DataAgentContext) -> dict:
    """执行单条 case 并产出判定记录

    用 ainvoke 而不是 astream：评测只关心跑完后的最终 state，
    不需要流式进度；ainvoke 直接返回完整 state，最省事。
    每条 case 计时一次，latency 就是端到端延迟（LLM 调用占绝对大头）。

    record 里的 note 是一句话人话判定说明：通过时说依据，
    失败时精确归因到链路层——report.json 有了它，事后翻报告不用再推理。
    """
    start = time.perf_counter()
    # ainvoke 返回最终 state，比 astream 更适合批量评测
    final_state = await graph.ainvoke(
        input=DataAgentState(query=case["question"]), context=context
    )
    latency = time.perf_counter() - start

    expected = case.get("expected", {})
    if expected.get("should_fail"):
        # 无答案 case 不套用召回/SQL 指标——expected 里没有 tables/sql_must_contain，
        # 硬套的话 all([]) 恒为 True，会把指标虚高，所以单独判定"是否诚实拒绝"
        passed, note = await check_no_answer(
            final_state, context["dw_mysql_repository"]
        )
        record = {
            "id": case["id"],
            "latency": round(latency, 2),
            "passed": passed,
            "note": note,
            "sql": final_state.get("sql"),
            "error": final_state.get("error"),
        }
    else:
        recall_hit = check_recall(final_state, expected)
        sql_ok = check_sql(final_state, expected)
        note = judge_note(final_state, expected, recall_hit, sql_ok)
        # record 里刻意保留分层字段，跑完分不止一个总分，还能归因：
        #   recall_hit=False               -> 坏在检索/过滤层（表没召回或被误杀）
        #   recall_hit=True 但 sql_ok=False -> 坏在 SQL 生成层（用错字段/漏了过滤值）
        #   error 非空                     -> SQL 没通过校验或执行失败
        #   retried=True                   -> 修正循环被触发过（P0-1 落地后统计修正成功率）
        record = {
            "id": case["id"],
            "latency": round(latency, 2),
            "passed": recall_hit and sql_ok,
            "note": note,
            "recall_hit": recall_hit,
            "sql_ok": sql_ok,
            "sql": final_state.get("sql"),
            "error": final_state.get("error"),
            "retried": final_state.get("retry_count", 0) > 0,
        }
    return record


def summarize(records: list[dict]) -> dict:
    """把逐条记录汇总成指标报告

    普通 case 和无答案 case 分开统计：两者的"通过"含义不同
    （前者是答对，后者是诚实拒绝），而且无答案 case 没有
    recall_hit/sql_ok 字段，混在一起算会除零或虚高。

    P50/P95：把所有 case 的延迟排序后取中位数和 95 分位——
    P50 代表典型体验，P95 代表最慢的 5% case 有多慢（长尾）。
    """
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


def print_report(records: list[dict]) -> None:
    """终端人话版报告：一眼看出哪条过、哪条挂、为什么挂"""
    print("=" * 78)
    print("逐条结果（PASS/FAIL + 耗时 + 判定说明）")
    print("=" * 78)
    for index, record in enumerate(records, 1):
        mark = "PASS" if record["passed"] else "FAIL"
        # PASS 行尾直接跟判定依据；FAIL 的原因较长，换行缩进单独展示
        tail = record["note"] if record["passed"] else ""
        print(
            f"{index:>2}. [{mark}] {record['id']:<22} {record['latency']:>5.1f}s  {tail}"
        )
        if not record["passed"]:
            print(f"      └ 原因：{record['note']}")

    failed_ids = [r["id"] for r in records if not r["passed"]]
    print("=" * 78)
    if failed_ids:
        print(f"失败 {len(failed_ids)} 条：{failed_ids}")
        print("（对应原因见上方 └ 行，或 report.json 里各条 record 的 note 字段）")
    else:
        print("全部通过。")


_HTML_CSS = """
  body { font-family: "Microsoft YaHei", sans-serif; max-width: 1200px; margin: 24px auto; padding: 0 16px; color: #222; }
  h1 { font-size: 20px; } h2 { font-size: 16px; margin-top: 24px; }
  table { border-collapse: collapse; width: 100%; margin: 10px 0 20px; }
  th, td { border: 1px solid #ddd; padding: 7px 10px; text-align: left; font-size: 14px; vertical-align: top; }
  th { background: #f5f2ea; white-space: nowrap; }
  tr.fail td { background: #fdecea; }
  .status-pass { color: #2e7d32; font-weight: 700; }
  .status-fail { color: #c62828; font-weight: 700; }
  .banner { padding: 10px 14px; margin: 10px 0; font-weight: 600; }
  .banner.fail { background: #fdecea; border: 1px solid #f5c6cb; color: #c62828; }
  .banner.pass { background: #e8f5e9; border: 1px solid #c8e6c9; color: #2e7d32; }
  pre { background: #f7f7f2; padding: 8px; white-space: pre-wrap; word-break: break-all; font-size: 12px; margin: 4px 0; }
  details summary { cursor: pointer; color: #1565c0; }
"""


def build_html_report(
    run_at: str, report: dict, records: list[dict], failed_records: list[dict]
) -> str:
    """把评测结果渲染成静态 HTML（report.html）：失败行红色高亮、SQL 可折叠

    report.json 是给程序读的，人眼在 JSON 里找 false 很费劲；
    这个文件是给人看的：浏览器打开，红色行就是失败记录，
    每行还带可折叠的 SQL 原文和归因说明（note）。
    """

    def esc(value) -> str:
        return html.escape(str(value))

    if failed_records:
        banner = (
            f'<div class="banner fail">失败 {len(failed_records)} 条：'
            f'{esc("、".join(r["id"] for r in failed_records))} —— 对应下表红色行</div>'
        )
    else:
        banner = '<div class="banner pass">全部通过。</div>'

    summary_rows = "".join(
        f"<tr><th>{esc(key)}</th><td>{esc(value)}</td></tr>"
        for key, value in report.items()
    )

    rows = []
    for index, record in enumerate(records, 1):
        css = "pass" if record["passed"] else "fail"
        status = "PASS" if record["passed"] else "FAIL"
        error = record.get("error") or "-"
        sql = record.get("sql") or "-"
        rows.append(
            f'<tr class="{css}">'
            f"<td>{index}</td>"
            f'<td><span class="status-{css}">{status}</span></td>'
            f"<td>{esc(record['id'])}</td>"
            f"<td>{record['latency']:.1f}s</td>"
            f"<td>{esc(record['note'])}</td>"
            f"<td>{esc(error)}</td>"
            f"<td><details><summary>查看</summary><pre>{esc(sql)}</pre></details></td>"
            "</tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>评测报告 {esc(run_at)}</title>
<style>{_HTML_CSS}</style>
</head>
<body>
<h1>评测报告 <small style="color:#888;font-weight:400">{esc(run_at)}</small></h1>
{banner}
<h2>汇总指标</h2>
<table>{summary_rows}</table>
<h2>逐条记录（红色行 = 失败）</h2>
<table>
<tr><th>#</th><th>状态</th><th>case</th><th>耗时</th><th>判定说明</th><th>错误</th><th>SQL</th></tr>
{''.join(rows)}
</table>
</body>
</html>"""


async def run_all(cases: list[dict]) -> list[dict]:
    """初始化客户端，并在 session 存活范围内跑完全部 case

    这里有个容易踩的坑：不能在 async with 块内 return context——
    Python 会先执行 __aexit__ 关闭 session 再返回，返回的仓储
    持有的就是已关闭的连接，后续执行直接报错。
    所以整个评测循环必须留在块内，出块后只返回已经拿到的 records。
    """
    # 先等外部服务真正就绪，再初始化客户端（原因见 wait_for_services 注释）
    await wait_for_services()

    # 与 FastAPI lifespan 同样的初始化顺序：五类外部客户端各初始化一次
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
        # 串行逐条跑（不并发）：评测要的是稳定的延迟数字，
        # 并发会让 case 之间互相抢 LLM/数据库资源，P50/P95 就失真了
        records = [await run_case(case, context) for case in cases]

    await qdrant_client_manager.close()
    await es_client_manager.close()
    await meta_mysql_client_manager.close()
    await dw_mysql_client_manager.close()
    return records


def load_previous_summary(history_path: Path, cases_file: str) -> dict | None:
    """读 history.jsonl 里同评测集的最近一次记录，用于前后对比

    从文件尾部倒着找第一条 cases 匹配的记录——
    不同评测集的记录混在同一个文件里，对比时必须过滤，否则指标没有可比性。
    """
    if not history_path.exists():
        return None
    for line in reversed(history_path.read_text(encoding="utf-8").splitlines()):
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry["cases"] == cases_file:
            return entry
    return None


def append_history(history_path: Path, cases_file: str, report: dict) -> None:
    """把本次 summary 追加进 history.jsonl（一行一次跑分，趋势数据源）"""
    entry = {
        "run_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "cases": cases_file,
        "summary": report,
    }
    with history_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def print_comparison(previous: dict | None, report: dict) -> None:
    """与同评测集上一次跑分对比：让"改进/回归"一眼可见，而不是翻两个文件心算"""
    if previous is None:
        print("（history.jsonl 中没有同评测集的历史记录，这是该评测集的第一次跑分）")
        return

    print(f"与上次跑分（{previous['run_at']}）对比：")
    for key, current in report.items():
        prev = previous["summary"].get(key)
        if prev is None:
            continue
        delta = current - prev
        change = "持平" if abs(delta) < 1e-9 else f"{delta:+.2f}"
        print(f"  {key:<20} {prev:>8.2f} -> {current:>8.2f}  ({change})")


async def main(cases_file: str):
    with open(cases_file, encoding="utf-8") as f:
        cases = yaml.safe_load(f)

    records = await run_all(cases)

    # 终端人话版：PASS/FAIL + 归因，跑完立刻能看
    print_report(records)

    report = summarize(records)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    eval_dir = Path(cases_file).parent
    history_path = eval_dir / "history.jsonl"

    # 对比必须在追加之前做：先读出"上一次"，再把本次写进去
    previous = load_previous_summary(history_path, cases_file)
    print_comparison(previous, report)

    # 失败记录单独提成 failed 汇总区：打开 report.json 第一眼就是它，
    # 不用在 records 数组里逐条找 passed=false
    failed_records = [r for r in records if not r["passed"]]
    run_at = time.strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "run_at": run_at,
        "summary": report,
        "failed": failed_records,
        "records": records,
    }

    # 历史四件套（互不覆盖）：
    # 1) 快照：完整 records 按时间戳存档，任意一次的细节都能回溯
    stamp = time.strftime("%Y%m%d_%H%M%S")
    snapshot_path = eval_dir / "reports" / f"report_{stamp}.json"
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    # 2) 最新：report.json 保持指向最近一次（查看入口不变）
    report_path = eval_dir / "report.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    # 3) 趋势：summary 追加进 history.jsonl
    append_history(history_path, cases_file, report)
    # 4) 可视化：HTML 版，浏览器打开红色行即失败记录
    html_path = eval_dir / "report.html"
    html_path.write_text(
        build_html_report(run_at, report, records, failed_records), encoding="utf-8"
    )
    print(f"快照：{snapshot_path}")
    print(f"最新：{report_path}")
    print(f"趋势：{history_path}")
    print(f"可视化：{html_path}（浏览器打开，红色行即失败记录）")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", default="eval/cases.yaml")
    args = parser.parse_args()
    asyncio.run(main(args.cases))
