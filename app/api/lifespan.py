"""
FastAPI 应用生命周期管理

负责在服务启动时初始化外部客户端，在服务关闭时释放连接资源。
这些客户端是应用级资源，适合在 lifespan 中创建一次并复用，而不是每个请求
重复初始化。
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from app.agent.graph import build_graph
from app.clients.embedding_client_manager import embedding_client_manager
from app.clients.es_client_manager import es_client_manager
from app.clients.mysql_client_manager import (
    dw_mysql_client_manager,
    meta_mysql_client_manager,
)
from app.clients.qdrant_client_manager import qdrant_client_manager
from app.clients.redis_client_manager import redis_client_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """管理应用启动和关闭两个阶段的外部资源"""

    # 启动阶段：先建立各类外部服务客户端，后续依赖函数会从 manager 中取已初始化对象
    qdrant_client_manager.init()
    embedding_client_manager.init()
    es_client_manager.init()
    meta_mysql_client_manager.init()
    dw_mysql_client_manager.init()
    redis_client_manager.init()

    # 多轮对话记忆：SQLite checkpointer 按 thread_id 持久化每个会话的 state。
    # 存放目录不存在时先创建（sqlite 只建文件不建目录）
    Path("data").mkdir(exist_ok=True)
    async with AsyncSqliteSaver.from_conn_string(
        "data/checkpoints.sqlite"
    ) as checkpointer:
        # 带 checkpointer 的图挂到 app.state，依赖层从这里取（见 dependencies.py）
        app.state.graph = build_graph(checkpointer)

        # yield 之前是启动逻辑，yield 之后是关闭逻辑；中间阶段由 FastAPI 正常处理请求
        yield

    # 关闭阶段：按应用级资源统一释放连接，避免进程退出前留下未关闭的网络连接
    await qdrant_client_manager.close()
    await es_client_manager.close()
    await meta_mysql_client_manager.close()
    await dw_mysql_client_manager.close()
    await redis_client_manager.close()
