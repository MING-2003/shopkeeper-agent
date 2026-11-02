"""
SQL 执行节点

负责执行最终 SQL，并记录查询结果。
它是当前 SQL 闭环的结束节点，执行完成后流程进入 END。
"""

from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.log import logger


async def run_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """执行 SQL 并产出最终问数结果"""

    writer = runtime.stream_writer
    step = "执行SQL"
    writer({"type": "progress", "step": step, "status": "running"})

    try:
        # 这里拿到的可能是 generate_sql 直接通过校验的 SQL，也可能是 correct_sql 覆盖后的 SQL
        sql = state["sql"]
        dw_mysql_repository = runtime.context["dw_mysql_repository"]

        # 真实数据库访问统一封装在仓储层，节点只负责从状态取 SQL 并触发执行
        result = await dw_mysql_repository.run(sql)
        logger.info(f"SQL执行结果：{result}")
        writer({"type": "progress", "step": step, "status": "success"})
        # 空结果标记：零行或全 NULL（聚合函数在空集上返回"一行 NULL"而不是零行）
        # 都意味着"数仓里没有匹配数据"，前端据此展示友好提示而不是一张空表
        is_empty = not result or all(
            all(value is None for value in row.values()) for row in result
        )
        # sql 字段供服务层落库历史会话使用，前端会忽略多余字段
        writer({"type": "result", "data": result, "empty": is_empty, "sql": sql})
        if is_empty:
            logger.info("本次查询未匹配到任何数据")

        # 把本轮执行的 SQL 回填进会话历史（history[-1] 是本轮的原始问题），
        # 下一轮改写追问时能看到上一轮实际执行的 SQL，更容易沿用表和口径。
        # 未挂 checkpointer 的调用（本地调试/评测）没有 history，跳过。
        history = state.get("history") or []
        if history:
            return {"history": history[:-1] + [{**history[-1], "sql": sql}]}
        return None

    except Exception as e:
        logger.error(f"{step} failed: {e}")
        writer({"type": "progress", "step": step, "status": "error"})
        raise
