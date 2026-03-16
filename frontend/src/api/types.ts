export interface ColumnInfo {
  column_name: string;
  data_type: string;
  is_primary_key: boolean;
  is_foreign_key: boolean;
  is_nullable: boolean;
  references_table?: string;
  references_column?: string;
}

export interface TableInfo {
  name: string;
  columns?: ColumnInfo[];
}

export interface QueryResult {
  success: boolean;
  query?: string;
  data?: Record<string, unknown>[];
  columns?: string[];
  row_count?: number;
  error?: string;
}
