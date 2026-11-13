"""
Redis 客户端管理器

统一创建和管理 Redis 异步客户端
主要用于分层缓存：相同问题命中缓存直接重放 SSE 事件流，schema 变更可整体失效
"""

from typing import Optional

from redis.asyncio import Redis

from app.conf.app_config import RedisConfig, app_config


class RedisClientManager:
    """管理 Redis 客户端的初始化与关闭"""

    def __init__(self, redis_config: RedisConfig):
        # 保存配置对象，后面初始化客户端时要从这里读取 host 和 port
        self.redis_config = redis_config
        # 先把 client 声明出来，真正初始化放到 init() 中进行
        self.client: Optional[Redis] = None

    def init(self):
        """
        显式初始化 Redis 客户端
        这里不在 __init__ 中直接初始化，是为了和项目的生命周期管理保持一致
        """
        # decode_responses=True 让 get 返回 str 而不是 bytes，省去手动解码
        self.client = Redis(
            host=self.redis_config.host,
            port=self.redis_config.port,
            decode_responses=True,
        )

    async def close(self):
        """关闭 Redis 客户端连接"""
        await self.client.aclose()


# 创建一个全局的管理器对象
# 后续项目中的其他模块都通过它来获取同一套 Redis 客户端
redis_client_manager = RedisClientManager(app_config.redis)
