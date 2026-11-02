"""
`chat_message` ORM 模型

定义元数据库中 chat_message 表对应的 ORM 模型
一轮成功问答（问题 + SQL + 查询结果）存一行，供前端打开历史会话时回放
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from app.models.base import Base


class ChatMessageMySQL(Base):
    """会话消息表对应的 ORM 模型"""

    __tablename__ = "chat_message"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True, comment="消息编号"
    )
    conversation_id: Mapped[str] = mapped_column(
        String(64), comment="所属会话编号"
    )
    question: Mapped[str | None] = mapped_column(Text, comment="用户问题")
    sql: Mapped[str | None] = mapped_column(Text, comment="本轮执行的SQL")
    result: Mapped[list | dict | None] = mapped_column(JSON, comment="查询结果")
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), comment="创建时间"
    )
