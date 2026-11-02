"""
问数查询服务

负责把 API 层传入的自然语言问题转换成一次 LangGraph 工作流执行：
创建初始 State、组装 Runtime Context、消费 graph.astream 的流式输出，
并统一包装成 SSE 文本返回给路由层。
"""

import json

from langchain_huggingface import HuggingFaceEndpointEmbeddings
from langgraph.graph.state import CompiledStateGraph

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.core.log import logger
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repositories.qdrant.metric_qdrant_repository import MetricQdrantRepository
from app.services.conversation_service import ConversationService


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

    async def query(self, query: str, conversation_id: str | None = None):
        """执行一次问数工作流，并逐段产出 SSE 消息

        conversation_id 是前端每个会话生成的 UUID：传入后作为 LangGraph 的
        thread_id，同一会话的多次请求共享 state（多轮对话记忆）；
        不传则每次调用相互独立，兼容单轮用法。
        """

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
        try:
            # stream_mode="custom" 对应节点内部 writer(...) 写出的进度消息
            async for chunk in self.graph.astream(
                input=state, context=context, stream_mode="custom", config=config
            ):
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

                # SSE 要求每条消息以 data: 开头，并以两个换行符结束
                # ensure_ascii=False 保留中文进度文案，default=str 兜底处理日期等非 JSON 类型
                yield f"data: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"
        except Exception as e:
            # 流式接口已经开始返回后不能再改 HTTP 状态码，因此把异常也包装成一条 SSE 消息
            error = {"type": "error", "message": str(e)}
            yield f"data: {json.dumps(error, ensure_ascii=False, default=str)}\n\n"
