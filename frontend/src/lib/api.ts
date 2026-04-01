const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || err.message || "Request failed");
  }
  return res.json();
}

export interface Session {
  session_id: string;
  message: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
  is_primary_key?: boolean;
}

export interface TableColumnsResponse {
  columns: ColumnInfo[] | string[];
  primary_keys: string[];
  foreign_keys: { column: string; references: string }[];
}

export interface GenerateQueryRequest {
  tables: { table: string; schema: string; alias: string }[];
  columns: { table: string; column: string; alias?: string }[];
  conditions: { table: string; column: string; operator: string; value: string }[];
  limit?: number;
  offset?: number;
  order_by?: { column: string; direction: string }[];
  group_by?: string[];
}

export interface GenerateQueryResponse {
  success: boolean;
  query: string;
  metadata?: Record<string, unknown>;
}

export interface ExecuteQueryResponse {
  success: boolean;
  data: Record<string, unknown>[];
  columns: string[];
  row_count: number;
  execution_time: number;
}

export const api = {
  createSession: () => request<Session>("/sessions/create", { method: "POST" }),

  getCategories: () => request<string[]>("/categories"),

  getSchemas: () => request<{ schemas: string[] } | string[]>("/schemas"),

  getTables: (schema: string) =>
    request<{ tables: string[] } | string[]>(`/tables?schema=${encodeURIComponent(schema)}`),

  getTableColumns: (table: string, schema: string) =>
    request<TableColumnsResponse>(`/tables/${encodeURIComponent(table)}/columns?schema=${encodeURIComponent(schema)}`),

  generateQuery: (body: GenerateQueryRequest) =>
    request<GenerateQueryResponse>("/query/generate", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  executeQuery: (sessionId: string, sql: string, validate = true) =>
    request<ExecuteQueryResponse>(`/query/execute?session_id=${encodeURIComponent(sessionId)}`, {
      method: "POST",
      body: JSON.stringify({ sql, validate }),
    }),

  unionQuery: (queries: GenerateQueryRequest[], operation: string, wrapInCte?: string) =>
    request<GenerateQueryResponse>("/query/union", {
      method: "POST",
      body: JSON.stringify({ queries, operation, wrap_in_cte: wrapInCte }),
    }),
};
