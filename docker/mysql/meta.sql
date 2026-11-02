SET NAMES utf8mb4;
CREATE DATABASE meta DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
GRANT ALL PRIVILEGES ON meta.* TO 'didilili'@'%';

USE meta;

DROP TABLE IF EXISTS table_info;
CREATE TABLE table_info
(
    id          VARCHAR(64) PRIMARY KEY COMMENT '表编号',
    name        VARCHAR(128) COMMENT '表名称',
    role        VARCHAR(32) COMMENT '表类型(fact/dim)',
    description TEXT COMMENT '表描述'
);



DROP TABLE IF EXISTS column_info;
CREATE TABLE column_info
(
    id          VARCHAR(64) PRIMARY KEY COMMENT '列编号',
    name        VARCHAR(128) COMMENT '列名称',
    type        VARCHAR(64) COMMENT '数据类型',
    role        VARCHAR(32) COMMENT '列类型(primary_key,foreign_key,measure,dimension)',
    examples    JSON COMMENT '数据示例',
    description TEXT COMMENT '列描述',
    alias       JSON COMMENT '列别名',
    table_id    VARCHAR(64) COMMENT '所属表编号'
);

DROP TABLE IF EXISTS metric_info;
CREATE TABLE metric_info
(
    id               VARCHAR(64) PRIMARY KEY COMMENT '指标编码',
    name             VARCHAR(128) COMMENT '指标名称',
    description      TEXT COMMENT '指标描述',
    relevant_columns JSON COMMENT '关联的列',
    alias            JSON COMMENT '指标别名'
);


DROP TABLE IF EXISTS column_metric;
CREATE TABLE column_metric
(
    column_id VARCHAR(64) COMMENT '列编号',
    metric_id VARCHAR(64) COMMENT '指标编号',
    PRIMARY KEY (column_id, metric_id)
);

DROP TABLE IF EXISTS conversation;
CREATE TABLE conversation
(
    id         VARCHAR(64) PRIMARY KEY COMMENT '会话编号(前端生成的UUID)',
    title      VARCHAR(128) COMMENT '会话标题(取第一轮提问)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后活跃时间'
);

DROP TABLE IF EXISTS chat_message;
CREATE TABLE chat_message
(
    id              BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '消息编号',
    conversation_id VARCHAR(64) NOT NULL COMMENT '所属会话编号',
    question        TEXT COMMENT '用户问题(原始问法)',
    `sql`           TEXT COMMENT '本轮执行的SQL',
    result          JSON COMMENT '查询结果(截断后)',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_chat_message_conversation (conversation_id, id)
);
