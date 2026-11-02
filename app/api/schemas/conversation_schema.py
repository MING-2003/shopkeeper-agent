"""
历史会话接口响应结构

集中声明会话列表和会话消息回放的响应模型，字段校验和 OpenAPI 文档
生成交给 Pydantic 与 FastAPI 完成。
"""

from datetime import datetime

from pydantic import BaseModel


class ConversationSummary(BaseModel):
    """会话列表项：侧边栏一个会话窗口对应一条"""

    id: str
    title: str | None
    created_at: datetime | None
    updated_at: datetime | None


class ConversationMessage(BaseModel):
    """会话内的一轮问答记录：问题 + 执行的 SQL + 查询结果"""

    id: int
    question: str | None
    sql: str | None
    result: list | dict | None
    created_at: datetime | None
