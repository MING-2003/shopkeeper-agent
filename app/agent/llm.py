"""
电商问数 Agent 使用的大模型实例

集中初始化一个 OpenAI 兼容的 Chat Model，供节点或本地测试直接复用
"""

from langchain.chat_models import init_chat_model

from app.conf.app_config import app_config

# 统一从配置读取模型三件套，节点只复用 llm，不重复初始化模型连接
llm = init_chat_model(
    model=app_config.llm.model_name,
    # 硅基流动等服务兼容 OpenAI 协议时，可以使用 openai provider 接入
    model_provider="openai",
    base_url=app_config.llm.base_url,
    api_key=app_config.llm.api_key,
    # 字段扩展、SQL 生成更看重稳定性，所以这里关闭随机发散
    temperature=0,
    # qwen3 系列思考模型默认开启思考模式，每次调用都先隐性推理一大段：
    # 实测同一个关键词扩展小任务 9.6s -> 1.2s（评测 50 条因此从 ~5 分钟涨到 ~20 分钟）。
    # 该参数对非思考模型（qwen-plus 等）会被服务端忽略，无副作用
    extra_body={"enable_thinking": False},
)

if __name__ == "__main__":
    # 本地快速验证 LLM 配置是否能正常调用
    print(llm.invoke("你好").content)
