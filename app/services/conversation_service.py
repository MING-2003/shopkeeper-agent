"""
会话服务

负责历史会话的两个读场景（会话列表、单会话消息回放）和一个写场景
（一轮成功问答落库）。落库前把查询结果转成 JSON 安全类型
（数仓数值列是 Decimal、日期列是 date/datetime，直接写 JSON 列会序列化失败）。
"""

from datetime import date, datetime
from decimal import Decimal

from app.repositories.mysql.meta.conversation_repository import ConversationRepository

# 会话标题取第一轮提问，超长截断
_TITLE_MAX_LENGTH = 50


def _json_safe(value):
    """递归把查询结果转成可 JSON 序列化的结构"""
    if isinstance(value, Decimal):
        # 整数值转 int（销量 6 而不是 6.0），小数转 float
        return int(value) if value == value.to_integral() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


class ConversationService:
    """封装历史会话的查询与记录逻辑"""

    def __init__(self, conversation_repository: ConversationRepository):
        self.conversation_repository = conversation_repository

    async def save_turn(
        self,
        conversation_id: str,
        question: str,
        sql: str | None,
        result: list | dict | None,
    ):
        """一轮成功问答落库：会话不存在则创建，消息追加一条

        不能用 session.begin()：同一个 meta Session 在本轮请求里已先被
        问数链路的只读查询用过（SQLAlchemy autobegin 已自动开启事务），
        再 begin() 会报 "A transaction is already begun on this Session"。
        因此直接写库后 commit——autobegin 保证两次写入落在同一事务，原子性不变。
        """

        await self.conversation_repository.upsert_conversation(
            conversation_id, question[:_TITLE_MAX_LENGTH]
        )
        self.conversation_repository.save_message(conversation_id, question, sql, _json_safe(result))
        try:
            await self.conversation_repository.session.commit()
        except Exception:
            # 失败时回滚，避免把半截事务留给同一 Session 的后续使用
            await self.conversation_repository.session.rollback()
            raise

    async def list_conversations(self) -> list[dict]:
        """返回全部会话（按最后活跃时间倒序），供前端侧边栏渲染"""
        return await self.conversation_repository.list_conversations()

    async def get_messages(self, conversation_id: str) -> list[dict]:
        """返回指定会话的全部问答记录，供前端打开历史会话时回放"""
        return await self.conversation_repository.get_messages(conversation_id)
