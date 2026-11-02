"""
会话仓储

负责会话表和消息表的读写：会话列表、历史消息查询，以及一轮问答的
会话 upsert 和消息插入。"这些写操作是否放在同一笔事务里"由 Service 层决定
"""

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.chat_message import ChatMessageMySQL
from app.models.conversation import ConversationMySQL


class ConversationRepository:
    """负责会话与消息记录在 Meta MySQL 中的持久化与查询"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def upsert_conversation(self, conversation_id: str, title: str):
        """会话不存在则创建（标题取第一轮提问），已存在则仅刷新活跃时间"""
        sql = (
            "INSERT INTO conversation (id, title) VALUES (:id, :title) "
            "ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP"
        )
        await self.session.execute(text(sql), {"id": conversation_id, "title": title})

    def save_message(
        self, conversation_id: str, question: str, sql: str | None, result: list | dict | None
    ):
        """保存一轮成功问答（问题 + SQL + 查询结果）"""
        self.session.add(
            ChatMessageMySQL(
                conversation_id=conversation_id,
                question=question,
                sql=sql,
                result=result,
            )
        )

    async def list_conversations(self) -> list[dict]:
        """按最后活跃时间倒序返回会话列表，供前端侧边栏展示"""
        result = await self.session.execute(
            select(
                ConversationMySQL.id,
                ConversationMySQL.title,
                ConversationMySQL.created_at,
                ConversationMySQL.updated_at,
            ).order_by(ConversationMySQL.updated_at.desc())
        )
        return [
            {
                "id": row.id,
                "title": row.title,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in result.all()
        ]

    async def get_messages(self, conversation_id: str) -> list[dict]:
        """按时间正序返回指定会话的全部问答记录，供前端回放历史"""
        result = await self.session.execute(
            select(
                ChatMessageMySQL.id,
                ChatMessageMySQL.question,
                ChatMessageMySQL.sql,
                ChatMessageMySQL.result,
                ChatMessageMySQL.created_at,
            )
            .where(ChatMessageMySQL.conversation_id == conversation_id)
            .order_by(ChatMessageMySQL.id.asc())
        )
        return [
            {
                "id": row.id,
                "question": row.question,
                "sql": row.sql,
                "result": row.result,
                "created_at": row.created_at,
            }
            for row in result.all()
        ]
