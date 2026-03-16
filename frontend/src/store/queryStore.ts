import { create } from 'zustand';

export interface SelectedTable {
  table: string;
  schema: string;
  alias: string;
}

export interface SelectedColumn {
  table: string;
  column: string;
  alias: string;
}

export interface JoinDef {
  tableA: string;
  tableB: string;
  joinType: 'INNER JOIN' | 'LEFT JOIN' | 'RIGHT JOIN' | 'FULL JOIN';
  condition: string;
}

export interface FilterDef {
  table: string;
  column: string;
  operator: string;
  value: string;
}

export interface CTEStage {
  id: string;
  name: string;
  query: string;
}

export interface TempTable {
  name: string;
  source: string;
}

export interface QueryResult {
  columns: string[];
  rows: Record<string, unknown>[];
  rowCount: number;
  executionTime?: number;
}

export interface QueryHistoryEntry {
  id: string;
  sql: string;
  timestamp: Date;
  rowCount?: number;
  executionTime?: number;
}

export interface Relationship {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
}

interface QueryStore {
  sessionId: string | null;
  setSessionId: (id: string | null) => void;

  selectedTables: SelectedTable[];
  addTable: (t: SelectedTable) => void;
  removeTable: (table: string) => void;

  selectedColumns: SelectedColumn[];
  addColumn: (c: SelectedColumn) => void;
  removeColumn: (table: string, column: string) => void;
  toggleColumn: (table: string, column: string) => void;
  setColumns: (cols: SelectedColumn[]) => void;

  joins: JoinDef[];
  addJoin: (j: JoinDef) => void;
  removeJoin: (idx: number) => void;

  filters: FilterDef[];
  addFilter: (f: FilterDef) => void;
  removeFilter: (idx: number) => void;

  groupBy: string[];
  setGroupBy: (g: string[]) => void;

  orderBy: string[];
  setOrderBy: (o: string[]) => void;

  limit: number | null;
  setLimit: (l: number | null) => void;

  offset: number | null;
  setOffset: (o: number | null) => void;

  cteStages: CTEStage[];
  addCTEStage: (s: CTEStage) => void;
  removeCTEStage: (id: string) => void;
  setCTEStages: (stages: CTEStage[]) => void;

  tempTables: TempTable[];
  setTempTables: (t: TempTable[]) => void;

  generatedSQL: string;
  setGeneratedSQL: (sql: string) => void;

  queryResults: QueryResult | null;
  setQueryResults: (r: QueryResult | null) => void;

  validationErrors: string[];
  setValidationErrors: (e: string[]) => void;

  isHealthy: boolean;
  setIsHealthy: (h: boolean) => void;

  // Query History
  queryHistory: QueryHistoryEntry[];
  addToHistory: (entry: QueryHistoryEntry) => void;
  clearHistory: () => void;

  // Relationships
  relationships: Relationship[];
  setRelationships: (r: Relationship[]) => void;

  reset: () => void;
}

const initialState = {
  sessionId: null,
  selectedTables: [] as SelectedTable[],
  selectedColumns: [] as SelectedColumn[],
  joins: [] as JoinDef[],
  filters: [] as FilterDef[],
  groupBy: [] as string[],
  orderBy: [] as string[],
  limit: null as number | null,
  offset: null as number | null,
  cteStages: [] as CTEStage[],
  tempTables: [] as TempTable[],
  generatedSQL: '',
  queryResults: null as QueryResult | null,
  validationErrors: [] as string[],
  isHealthy: false,
  queryHistory: [] as QueryHistoryEntry[],
  relationships: [] as Relationship[],
};

export const useQueryStore = create<QueryStore>((set) => ({
  ...initialState,
  setSessionId: (id) => set({ sessionId: id }),
  addTable: (t) => set((s) => ({ selectedTables: [...s.selectedTables, t] })),
  removeTable: (table) => set((s) => ({
    selectedTables: s.selectedTables.filter((t) => t.table !== table),
    selectedColumns: s.selectedColumns.filter((c) => c.table !== table),
  })),
  addColumn: (c) => set((s) => {
    if (s.selectedColumns.find((sc) => sc.table === c.table && sc.column === c.column)) return s;
    return { selectedColumns: [...s.selectedColumns, c] };
  }),
  removeColumn: (table, column) => set((s) => ({ selectedColumns: s.selectedColumns.filter((c) => !(c.table === table && c.column === column)) })),
  toggleColumn: (table, column) => set((s) => {
    const exists = s.selectedColumns.find((c) => c.table === table && c.column === column);
    if (exists) return { selectedColumns: s.selectedColumns.filter((c) => !(c.table === table && c.column === column)) };
    return { selectedColumns: [...s.selectedColumns, { table, column, alias: '' }] };
  }),
  setColumns: (cols) => set({ selectedColumns: cols }),
  addJoin: (j) => set((s) => ({ joins: [...s.joins, j] })),
  removeJoin: (idx) => set((s) => ({ joins: s.joins.filter((_, i) => i !== idx) })),
  addFilter: (f) => set((s) => ({ filters: [...s.filters, f] })),
  removeFilter: (idx) => set((s) => ({ filters: s.filters.filter((_, i) => i !== idx) })),
  setGroupBy: (g) => set({ groupBy: g }),
  setOrderBy: (o) => set({ orderBy: o }),
  setLimit: (l) => set({ limit: l }),
  setOffset: (o) => set({ offset: o }),
  addCTEStage: (s) => set((st) => ({ cteStages: [...st.cteStages, s] })),
  removeCTEStage: (id) => set((s) => ({ cteStages: s.cteStages.filter((c) => c.id !== id) })),
  setCTEStages: (stages) => set({ cteStages: stages }),
  setTempTables: (t) => set({ tempTables: t }),
  setGeneratedSQL: (sql) => set({ generatedSQL: sql }),
  setQueryResults: (r) => set({ queryResults: r }),
  setValidationErrors: (e) => set({ validationErrors: e }),
  setIsHealthy: (h) => set({ isHealthy: h }),
  addToHistory: (entry) => set((s) => ({ queryHistory: [entry, ...s.queryHistory].slice(0, 50) })),
  clearHistory: () => set({ queryHistory: [] }),
  setRelationships: (r) => set({ relationships: r }),
  reset: () => set(initialState),
}));
