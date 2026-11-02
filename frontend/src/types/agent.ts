/**
 * 智能体类型定义
 * 定义问数智能体前端使用的 SSE 事件、流程步骤和聊天消息类型
 */
export type ProgressStatus = "running" | "success" | "error";

export type ProgressEvent = {
  type: "progress";
  step: string;
  status: ProgressStatus;
};

export type ResultEvent = {
  type: "result";
  data: unknown;
  // 后端标记：结果为零行或全 NULL，表示数仓中没有匹配数据
  empty?: boolean;
};

export type ErrorEvent = {
  type: "error";
  message: string;
};

export type AgentEvent = ProgressEvent | ResultEvent | ErrorEvent;

export type StepState = {
  step: string;
  status: ProgressStatus;
  updatedAt: number;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant";
  content: string;
  createdAt: number;
  status?: "streaming" | "done" | "error";
  steps?: StepState[];
  result?: unknown;
  error?: string;
};

export type ConversationSummary = {
  id: string;
  title: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type ConversationMessage = {
  id: number;
  question: string | null;
  sql: string | null;
  result: unknown;
  created_at: string | null;
};
