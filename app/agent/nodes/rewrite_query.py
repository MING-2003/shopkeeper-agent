"""
查询改写节点

多轮对话的图入口：结合会话历史把"那华东呢？""按月拆分看看"这类追问
改写成不依赖上下文、语义完整的独立问题，再进入既有主链路。
首轮提问（无历史）不调 LLM，直接放行。

checkpointer 让 state 跨请求持久化，上一轮的召回/SQL/错误等中间结果
会残留在 state 里，所以每轮入口都要用 _RESET_FIELDS 清空中间字段，
只保留 query 和 history 两个跨轮字段——这是多轮对话正确性的关键。
"""

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import llm
from app.agent.state import DataAgentState
from app.core.log import logger
from app.prompt.prompt_loader import load_prompt

# 每轮入口重置的中间状态字段：这些是单轮链路的临时产物，
# 残留到下一轮会污染检索和生成；history 在返回值里单独维护，不在这里重置
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
    """把带指代的追问改写成独立完整的问题，并重置上一轮的中间状态"""

    writer = runtime.stream_writer
    step = "改写查询"
    writer({"type": "progress", "step": step, "status": "running"})

    query = state["query"]
    # state 运行时是 dict：首轮（或未挂 checkpointer 时）history 尚未写入，用 get 兜底
    history = state.get("history") or []

    try:
        # 首轮提问没有历史，无需改写，直接放行（也不烧 LLM token）
        if not history:
            writer({"type": "progress", "step": step, "status": "success"})
            return {
                "query": query,
                "history": [{"question": query, "sql": None}],
                **_RESET_FIELDS,
            }

        prompt = PromptTemplate(
            template=load_prompt("rewrite_query"),
            input_variables=["history", "query"],
        )
        chain = prompt | llm | StrOutputParser()
        # 只看最近 3 轮，控制上下文长度；sql 为空说明上一轮生成失败
        history_text = "\n".join(
            f"问：{h['question']}\nSQL：{h['sql'] or '（生成失败）'}"
            for h in history[-3:]
        )
        rewritten = (
            await chain.ainvoke({"history": history_text, "query": query})
        ).strip()

        logger.info(f"查询改写：{query!r} -> {rewritten!r}")
        writer({"type": "progress", "step": step, "status": "success"})
        # 追加进历史的是用户的原始问法（对话视角），SQL 由 run_sql 执行成功后回填
        return {
            "query": rewritten,
            "history": history + [{"question": query, "sql": None}],
            **_RESET_FIELDS,
        }
    except Exception as e:
        logger.error(f"{step} failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise
