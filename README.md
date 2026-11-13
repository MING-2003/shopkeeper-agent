<div align='center'>
  <h1 style="margin-top: 15px;">「电商问数」智能数据分析 Agent</h1>
  <h4><b>shopkeeper-agent</b></h4>
  <p><em>让业务同学用一句话查数仓：混合检索召回 + LangGraph 多阶段推理 + SQL 生成执行 + 多轮对话记忆 + 全链路流式展示</em></p>
</div>

<div align='center'>

![AI](https://img.shields.io/badge/AI-Agent-00c853?style=flat)
![Python](https://img.shields.io/badge/Python-3.14-3776AB.svg?logo=python&logoColor=white)
![LangGraph](https://img.shields.io/badge/LangGraph-Agentic%20Workflow-1C3C3C.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-SSE%20Streaming-009688.svg?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-Vite%20%2B%20Tailwind-61DAFB.svg?logo=react&logoColor=black)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)
![Stars](https://img.shields.io/github/stars/MING-2003/shopkeeper-agent?logo=github&style=flat)

</div>

![电商问数前端首页：样例问题、自然语言输入和智能数据分析 Agent 界面](docs/images/shopkeeper-agent-home.jpg)

## 💡 为什么做这个

真实业务里有个很拧巴的现象：**会提问的人不会写 SQL，懂数仓的人不懂业务**。

把自然语言问题直接丢给大模型？行不通——表选错、字段选错、指标口径理解错、SQL 幻觉，一个都躲不掉。模型缺的不是推理能力，而是**关于这张数仓的记忆**。

所以这个项目的思路很直接：先给模型建一本"数仓字典"，再让它按流程办事，而不是自由发挥。

## 🎯 项目流程

![电商问数系统架构图：前端通过 FastAPI 和 SSE 连接后端，LangGraph 问数智能体基于 Jieba、MySQL、Qdrant、Elasticsearch 和 LLM 完成召回、SQL 生成校验执行与结果返回](docs/images/shopkeeper-agent-system-architecture.svg)

### ① 离线：构建元数据知识库

从电商数仓中抽取表、字段、指标和字段真实取值，为模型建立一本"数仓字典"：

| 产物                 | 存储            | 用途                           |
| -------------------- | --------------- | ------------------------------ |
| 表与字段结构化元数据 | `MySQL`         | 权威口径，问数上下文的最终依据 |
| 字段与指标向量       | `Qdrant`        | 语义召回                       |
| 字段真实取值         | `Elasticsearch` | 关键词与值域检索               |

### ② 在线：自然语言问数链路

```text
追问改写 → 关键词抽取 → 多路召回 → 合并构建 → 过滤补全 → SQL 生成 → 校验修正 → 安全护栏 → 执行 → 流式返回
```

| 阶段       | 做什么                                                                 |
| ---------- | ---------------------------------------------------------------------- |
| 追问改写   | 多轮对话下结合历史消息，把"那上个月呢？"改写成自包含的完整问题         |
| 关键词抽取 | Jieba 分词抽取问题中的关键词                                           |
| 多路召回   | 字段、指标走 Qdrant 语义召回，字段取值走 ES 全文检索，三路并行、批量向量化 |
| 合并构建   | 合并三路召回结果，补齐依赖字段，构建值域上下文                         |
| 过滤补全   | 过滤候选表字段与指标，补齐日期和数据库上下文                           |
| SQL 生成   | 基于整理后的上下文生成 SQL                                             |
| 校验修正   | `EXPLAIN` 校验，失败则携带错误信息进入修正循环（设重试上限）           |
| 安全护栏   | SQL Guard 白名单校验，只放行 SELECT 并拦截危险语句                     |
| 执行返回   | 只读账号查询数仓，空结果给出兜底提示，SSE 实时推送节点进度与最终结果   |

![电商问数查询结果页：LangGraph 执行流程、SQL 校验执行和查询结果表格](docs/images/shopkeeper-agent-query-result.jpg)

## ✨ 项目亮点

### 项目本身的设计

- **检索 + 推理 + 生成，而不是模型直出 SQL**：先把与问题相关的字段、指标、取值召回干净，再组织上下文生成 SQL，链路更稳、更可控
- **字段、指标、取值三类信息协同召回**：比单纯表级/字段级检索更贴近真实的企业分析流程
- **完整可运行闭环**：不停留在 Prompt 设计，真实生成 SQL、校验、执行并以流式方式返回
- **工程化分层架构**：`FastAPI + LangGraph + Repository + Client Manager`，配置、客户端、仓储、服务与智能体各层隔离，换实现不动业务逻辑

### 我做的主要改进

- **多轮对话记忆与历史会话**：LangGraph Checkpoint 持久化会话，追问自动改写，前端支持历史会话管理
- **SQL 安全护栏**：新增 SQL Guard 模块，执行前白名单校验只放行 SELECT，数仓改用只读账号运行
- **稳定性兜底**：SQL 修正设重试上限避免无限循环，查询空结果时返回可解释的兜底提示
- **召回性能优化**：召回节点批量向量化 + 并发检索，降低单次问数延迟
- **评测集驱动回归**：建立 50 条问数评测集，对召回与 SQL 生成效果持续验证

## 🏗️ 技术选型与理由

| 模块       | 技术                              | 为什么用它                                     |
| ---------- | --------------------------------- | ---------------------------------------------- |
| 电商数仓   | `MySQL`                           | 事实表、维度表与分析型查询环境                 |
| 元数据库   | `MySQL` / `SQLAlchemy`            | 保存表、字段、指标、字段指标关系等结构化元数据 |
| 向量检索   | `Qdrant`                          | 字段/指标的语义召回                            |
| 全文检索   | `Elasticsearch`                   | 字段真实取值的关键词与值域检索                 |
| Embedding  | `TEI` / `BAAI/bge-large-zh-v1.5`  | 文本向量化，中文效果稳定                       |
| 智能体编排 | `LangGraph`                       | 多阶段问数工作流，节点可观测、可干预           |
| 模型接入   | `LangChain`                       | 封装 LLM 与 Embedding 调用                     |
| 后端接口   | `FastAPI`                         | 问数 API、依赖注入和生命周期管理               |
| 流式协议   | `SSE`                             | 实时返回节点进度、查询结果和错误消息           |
| 前端       | `React` / `Vite` / `Tailwind CSS` | 聊天式问数界面和流程展示                       |
| 日志追踪   | `ContextVar` / `loguru`           | 并发请求注入 request_id，全链路可追踪          |
| 依赖管理   | `uv` / `pnpm`                     | Python 后端与前端依赖管理                      |

## 📁 项目结构

```text
shopkeeper-agent/
├── app/
│   ├── agent/            # LangGraph 图、状态、上下文和各类节点
│   ├── api/              # FastAPI 路由、依赖注入、生命周期和请求结构
│   ├── clients/          # MySQL、Qdrant、Elasticsearch、Embedding 客户端管理
│   ├── conf/             # 配置 dataclass 与配置加载工具
│   ├── core/             # 日志、request_id 上下文等通用能力
│   ├── entities/         # 更贴近业务语义的数据对象
│   ├── models/           # SQLAlchemy ORM 模型
│   ├── prompt/           # Prompt 加载工具
│   ├── repositories/     # MySQL、Qdrant、Elasticsearch 数据访问层
│   ├── scripts/          # 元数据知识库构建脚本
│   └── services/         # 元数据构建服务和问数查询服务
├── conf/                 # app_config.yaml、meta_config.yaml
├── docker/               # Docker Compose、MySQL 初始化 SQL、ES 插件、Embedding 挂载目录
├── frontend/             # React + Vite + Tailwind CSS 前端项目
├── prompts/              # SQL 生成、修正、过滤等 Prompt 模板
├── main.py               # FastAPI 应用入口
└── pyproject.toml        # Python 项目依赖与工具配置
```

## 🚀 快速开始

### 1. 准备环境

- Python `>= 3.14` + `uv`
- Docker 与 Docker Compose
- Node.js + `pnpm`

### 2. 克隆并安装依赖

```bash
git clone https://github.com/MING-2003/shopkeeper-agent.git
cd shopkeeper-agent
uv sync
```

### 3. 配置大模型 API Key

```bash
cp .env.example .env
```

把 `.env` 中的 `LLM_API_KEY` 替换成真实密钥：

```bash
LLM_API_KEY=your_real_api_key
```

默认使用兼容 OpenAI 接口的硅基流动服务：

```yaml
llm:
    model_name: Pro/zai-org/GLM-5.1
    api_key: ${oc.env:LLM_API_KEY}
    base_url: https://api.siliconflow.cn/v1
```

如需切换其他兼容 OpenAI API 的平台，修改 [conf/app_config.yaml](conf/app_config.yaml) 中的 `model_name` 和 `base_url` 即可。

### 4. 准备 Embedding 模型

项目通过 `TEI` 加载 `BAAI/bge-large-zh-v1.5`，模型文件较大需要先下载到 Docker 挂载目录：

```bash
uv run hf download BAAI/bge-large-zh-v1.5 --local-dir docker/embedding/bge-large-zh-v1.5
```

手动下载的话，解压到 `docker/embedding/bge-large-zh-v1.5` 路径下。

### 5. 启动基础服务

```bash
docker compose -f docker/docker-compose.yaml up -d
```

| 服务          | 端口   |
| ------------- | ------ |
| MySQL         | `3306` |
| Elasticsearch | `9200` |
| Kibana        | `5601` |
| Qdrant        | `6333` |
| Embedding     | `8081` |

> `docker/mysql/meta.sql` 和 `docker/mysql/dw.sql` 会在 MySQL 容器首次启动时自动初始化元数据库和数仓。

### 6. 构建元数据知识库

```bash
uv run python -m app.scripts.build_meta_knowledge -c conf/meta_config.yaml
```

这一步会把表字段元数据写入 MySQL，字段和指标向量写入 Qdrant，字段真实取值写入 Elasticsearch。

### 7. 启动后端

```bash
uv run fastapi dev main.py
```

接口与请求示例：

```text
POST http://127.0.0.1:8000/api/query
```

```json
{
    "query": "统计华北地区的销售总额"
}
```

SSE 消息类型：

| 类型       | 含义         |
| ---------- | ------------ |
| `progress` | 节点执行进度 |
| `result`   | 最终查询结果 |
| `error`    | 全局异常消息 |

### 8. 启动前端

```bash
cd frontend
pnpm install
pnpm dev
```

前端默认通过 Vite 代理把 `/api` 转发到 `http://127.0.0.1:8000`，如需修改：

```bash
cd frontend
cp .env.example .env
# 编辑 .env
VITE_DEV_PROXY_TARGET=http://127.0.0.1:8000
```

## 🗺️ Roadmap

- [x] 多轮对话记忆、追问改写与会话管理
- [x] SQL 安全审计与执行白名单
- [ ] 用户登录、角色权限与数据权限控制
- [ ] SQL 安全审计与执行白名单
- [ ] 查询缓存、限流与性能治理
- [ ] 系统化评测集与自动化回归评测
- [ ] 监控告警、链路追踪平台与灰度发布

## 📄 License

本项目基于 [MIT License](LICENSE) 开源。
