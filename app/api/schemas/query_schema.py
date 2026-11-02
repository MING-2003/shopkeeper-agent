"""
问数接口请求体定义

集中声明 API 层输入输出的数据结构，让路由函数只处理业务流程，
字段校验和 OpenAPI 文档生成交给 Pydantic 与 FastAPI 完成。
"""

from pydantic import BaseModel


class QuerySchema(BaseModel):
    """`/api/query` 请求体，承载用户输入的自然语言问题"""

    # 前端请求体中的 query 字段，例如 {"query": "统计华北地区销售额"}
    query: str
    # 会话 ID：前端每个会话生成一个 UUID，同一会话的追问共享对话记忆（thread_id）；
    # 不传则视为独立的单轮请求（兼容旧调用方）
    conversation_id: str | None = None
