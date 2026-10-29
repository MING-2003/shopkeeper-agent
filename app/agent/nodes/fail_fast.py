"""
SQL 修正重试超限兜底节点

当修正次数达到上限仍未通过校验时，流程从这里优雅结束：
向前端返回友好的错误消息，而不是抛异常中断整个链路。
"""

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
