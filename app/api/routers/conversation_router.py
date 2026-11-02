"""
历史会话接口路由

提供两个只读接口：会话列表（前端侧边栏）和指定会话的消息回放
（点进会话窗口时渲染历史提问与回答）。
路由层只处理路径参数和依赖声明，查询逻辑在 ConversationService 中。
"""

from typing import Annotated

from fastapi import APIRouter, Depends

from app.api.dependencies import get_conversation_service
from app.api.schemas.conversation_schema import ConversationMessage, ConversationSummary
from app.services.conversation_service import ConversationService

conversation_router = APIRouter()


@conversation_router.get("/api/conversations")
async def list_conversations(
    conversation_service: Annotated[ConversationService, Depends(get_conversation_service)],
) -> list[ConversationSummary]:
    """返回全部会话，按最后活跃时间倒序"""

    return await conversation_service.list_conversations()


@conversation_router.get("/api/conversations/{conversation_id}/messages")
async def get_conversation_messages(
    conversation_id: str,
    conversation_service: Annotated[ConversationService, Depends(get_conversation_service)],
) -> list[ConversationMessage]:
    """返回指定会话的全部问答记录，按时间正序"""

    return await conversation_service.get_messages(conversation_id)
