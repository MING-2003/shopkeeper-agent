"""
`conversation` ORM 模型

定义元数据库中 conversation 表对应的 ORM 模型
每个会话窗口一行记录：会话编号由前端生成的 UUID 提供
标题取该会话第一轮的用户提问，updated_at 由 MySQL 在每次写入时自动刷新
"""

from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ConversationMySQL(Base):
    """会话表对应的 ORM 模型"""

    __tablename__ = "conversation"

    id: Mapped[str] = mapped_column(String(64), primary_key=True, comment="会话编号")
    title: Mapped[str | None] = mapped_column(String(128), comment="会话标题")
    created_at: Mapped[datetime | None] = mapped_column(
        DateTime, server_default=func.now(), comment="创建时间"
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        comment="最后活跃时间",
    )
