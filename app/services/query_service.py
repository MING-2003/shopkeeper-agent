"""
问数查询服务

负责把 API 层传入的自然语言问题转换成一次 LangGraph 工作流执行：
创建初始 State、组装 Runtime Context、消费 graph.astream 的流式输出，
并统一包装成 SSE 文本返回给路由层。
"""

import hashlib
import json

from langchain_huggingface import HuggingFaceEndpointEmbeddings
from langgraph.graph.state import CompiledStateGraph
from redis.asyncio import Redis

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.log import logger
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository
from app.services.conversation_service import ConversationService

# 缓存 TTL（秒）：兜底清理，正常失效靠 schema_version 版本号整体切换
CACHE_TTL_SECONDS = 3600
# schema 版本号键：元数据增量同步（P2-10）发现 schema diff 时自增，旧缓存自然失效
SCHEMA_VERSION_KEY = "dw:schema_version"


def _cache_key(query: str, schema_version: str) -> str:
    """生成缓存键：归一化问题（去空白 + 转小写）取 SHA256 前 32 位，前缀带 schema 版本"""
    normalized = "".join(query.split()).lower()
    digest = hashlib.sha256(normalized.encode()).hexdigest()[:32]
    return f"qa:sse:{schema_version}:{digest}"


class QueryService:
    """封装一次问数查询所需的业务编排逻辑"""

    def __init__(
        self,
        graph: CompiledStateGraph,
        meta_mysql_repository: MetaMySQLRepository,
        embedding_client: HuggingFaceEndpointEmbeddings,
        dw_mysql_repository: DWMySQLRepository,
        column_qdrant_repository: ColumnQdrantRepository,
        metric_qdrant_repository: MetricQdrantRepository,
        value_es_repository: ValueESRepository,
        conversation_service: ConversationService,
        redis_client: Redis,
    ):
        # 带 checkpointer 的图由 lifespan 构建并经依赖层注入（多轮对话记忆用）
        self.graph = graph

        # 会话服务：把每轮成功问答落库，支撑前端历史会话回放
        self.conversation_service = conversation_service

        # MySQL 仓储分别负责元数据补全和真实数仓环境信息读取
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository

        # 召回链路依赖的向量检索、Embedding 和全文检索能力由依赖层注入
        self.embedding_client = embedding_client
        self.column_qdrant_repository = column_qdrant_repository
        self.metric_qdrant_repository = metric_qdrant_repository
        self.value_es_repository = value_es_repository

        # Redis 客户端：分层缓存用，命中后直接重放历史 SSE 事件，不再全链路重跑
        self.redis_client = redis_client

    async def _get_cache(self, key: str) -> str | None:
        """读缓存；缓存是非关键路径，Redis 不可用时降级为全链路执行而不是报错"""
        try:
            return await self.redis_client.get(key)
        except Exception as e:
            logger.warning(f"Redis 缓存读取失败，降级为全链路执行：{e}")
            return None

    async def _set_cache(self, key: str, value: str, ex: int) -> None:
        """写缓存；失败只记日志，不影响问答主流程"""
        try:
            await self.redis_client.set(key, value, ex=ex)
        except Exception as e:
            logger.warning(f"Redis 缓存写入失败：{e}")

    async def query(self, query: str, conversation_id: str | None = None):
        """执行一次问数工作流，并逐段产出 SSE 消息

        conversation_id 是前端每个会话生成的 UUID：传入后作为 LangGraph 的
        thread_id，同一会话的多次请求共享 state（多轮对话记忆）；
        不传则每次调用相互独立，兼容单轮用法。
        """

        # 分层缓存：相同问题（同 schema 版本）命中后按原顺序重放事件，毫秒级返回
        schema_version = await self._get_cache(SCHEMA_VERSION_KEY) or "0"
        key = _cache_key(query, schema_version)
        cached = await self._get_cache(key)
        if cached:
            logger.info(f"缓存命中，直接重放 SSE 事件：{key}")
            # 缓存存的是已序列化的 SSE data 载荷列表，直接重放保证字节级一致；
            # 不重复落库会话（首轮执行时已保存过）
            for chunk_json in json.loads(cached):
                yield f"data: {chunk_json}\n\n"
            return

        # State 只放会被图节点读写和合并的业务数据，外部工具对象不塞进 State；
        # history 由 checkpointer 自动恢复，这里不需要（也不应该）传入旧值
        state = DataAgentState(query=query)
        # Context 保存本次图执行需要复用的外部依赖，节点通过 runtime.context 读取
        context = DataAgentContext(
            column_qdrant_repository=self.column_qdrant_repository,
            embedding_client=self.embedding_client,
            metric_qdrant_repository=self.metric_qdrant_repository,
            value_es_repository=self.value_es_repository,
            meta_mysql_repository=self.meta_mysql_repository,
            dw_mysql_repository=self.dw_mysql_repository,
        )
        # 传了 conversation_id 才挂 thread_id，交给 checkpointer 恢复会话状态
        config = (
            {"configurable": {"thread_id": conversation_id}}
            if conversation_id
            else {}
        )
        # 收集本次流产生的已序列化事件，用于缓存回放
        events: list[str] = []
        has_error = False
        try:
            # stream_mode="custom" 对应节点内部 writer(...) 写出的进度消息
            async for chunk in self.graph.astream(
                input=state, context=context, stream_mode="custom", config=config
            ):
                # SSE 要求每条消息以 data: 开头，并以两个换行符结束
                # ensure_ascii=False 保留中文进度文案，default=str 兜底处理日期等非 JSON 类型
                chunk_json = json.dumps(chunk, ensure_ascii=False, default=str)
                events.append(chunk_json)
                yield f"data: {chunk_json}\n\n"

                # 收到最终结果事件说明本轮 SQL 执行成功，落一条历史会话消息。
                # 只记录成功轮次：失败的半截对话不落库，与 history 只回填成功 SQL 的口径一致
                if (
                    conversation_id
                    and isinstance(chunk, dict)
                    and chunk.get("type") == "result"
                ):
                    try:
                        await self.conversation_service.save_turn(
                            conversation_id=conversation_id,
                            question=query,
                            sql=chunk.get("sql"),
                            result=chunk.get("data"),
                        )
                    except Exception as save_error:
                        # 落库失败不影响问答主流程，只记日志
                        logger.warning(f"历史会话消息落库失败：{save_error}")

                # fail_fast 兜底等节点写出的错误事件：标记本次流不可缓存
                if isinstance(chunk, dict) and chunk.get("type") == "error":
                    has_error = True
        except Exception as e:
            # 流式接口已经开始返回后不能再改 HTTP 状态码，因此把异常也包装成一条 SSE 消息
            error = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(error, ensure_ascii=False, default=str)}\n\n"
            return

        # 含 error 事件的流不缓存：fail_fast 兜底的失败带有随机性（重问一次可能就成功），
        # 缓存 1 小时会让用户失去重试机会
        if not has_error:
            await self._set_cache(
                key, json.dumps(events, ensure_ascii=False), CACHE_TTL_SECONDS
            )
