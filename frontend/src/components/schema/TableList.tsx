import { useState, useEffect, useCallback } from 'react';
import { getSchemasTables, getTableColumns } from '@/api/schema';
import { useQueryStore } from '@/store/queryStore';
import { Search, Table2, ChevronRight, ChevronDown, Plus, Eye, Database } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Checkbox } from '@/components/ui/checkbox';
import { Badge } from '@/components/ui/badge';
import { toast } from 'sonner';

interface TableInfo {
  name: string;
  schema?: string;
  rowCount?: number;
  primaryKey?: string;
}

interface ColumnInfo {
  name: string;
  type?: string;
  isPrimaryKey?: boolean;
}

const DEMO_TABLES: Record<string, TableInfo[]> = {
  public: [
    { name: 'employees', schema: 'public', rowCount: 120000, primaryKey: 'id' },
    { name: 'departments', schema: 'public', rowCount: 45, primaryKey: 'dept_id' },
    { name: 'salaries', schema: 'public', rowCount: 480000, primaryKey: 'id' },
    { name: 'projects', schema: 'public', rowCount: 320, primaryKey: 'id' },
    { name: 'assignments', schema: 'public', rowCount: 95000, primaryKey: 'id' },
  ],
  analytics: [
    { name: 'events', schema: 'analytics', rowCount: 2000000, primaryKey: 'event_id' },
    { name: 'sessions', schema: 'analytics', rowCount: 500000, primaryKey: 'session_id' },
  ],
  staging: [
    { name: 'raw_imports', schema: 'staging', rowCount: 10000, primaryKey: 'id' },
  ],
};

const DEMO_COLUMNS: Record<string, ColumnInfo[]> = {
  employees: [
    { name: 'id', type: 'integer', isPrimaryKey: true },
    { name: 'emp_name', type: 'varchar' },
    { name: 'email', type: 'varchar' },
    { name: 'dept_id', type: 'integer' },
    { name: 'hire_date', type: 'date' },
    { name: 'salary', type: 'decimal' },
    { name: 'created_at', type: 'timestamp' },
  ],
  departments: [
    { name: 'dept_id', type: 'integer', isPrimaryKey: true },
    { name: 'dept_name', type: 'varchar' },
    { name: 'manager_id', type: 'integer' },
    { name: 'budget', type: 'decimal' },
    { name: 'created_at', type: 'timestamp' },
  ],
  salaries: [
    { name: 'id', type: 'integer', isPrimaryKey: true },
    { name: 'emp_id', type: 'integer' },
    { name: 'amount', type: 'decimal' },
    { name: 'effective_date', type: 'date' },
    { name: 'created_at', type: 'timestamp' },
  ],
  projects: [
    { name: 'id', type: 'integer', isPrimaryKey: true },
    { name: 'project_name', type: 'varchar' },
    { name: 'status', type: 'varchar' },
    { name: 'start_date', type: 'date' },
    { name: 'end_date', type: 'date' },
  ],
  assignments: [
    { name: 'id', type: 'integer', isPrimaryKey: true },
    { name: 'emp_id', type: 'integer' },
    { name: 'project_id', type: 'integer' },
    { name: 'role', type: 'varchar' },
    { name: 'assigned_date', type: 'date' },
  ],
  events: [
    { name: 'event_id', type: 'integer', isPrimaryKey: true },
    { name: 'event_type', type: 'varchar' },
    { name: 'payload', type: 'jsonb' },
    { name: 'created_at', type: 'timestamp' },
  ],
  sessions: [
    { name: 'session_id', type: 'integer', isPrimaryKey: true },
    { name: 'user_id', type: 'integer' },
    { name: 'started_at', type: 'timestamp' },
  ],
  raw_imports: [
    { name: 'id', type: 'integer', isPrimaryKey: true },
    { name: 'data', type: 'jsonb' },
    { name: 'imported_at', type: 'timestamp' },
  ],
};

export function TableList() {
  const { selectedSchema, addTable, addColumn, removeColumn, selectedTables, selectedColumns } = useQueryStore();
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [columns, setColumns] = useState<Record<string, ColumnInfo[]>>({});
  const [loading, setLoading] = useState(false);

  // Fetch tables when schema changes
  useEffect(() => {
    if (!selectedSchema) {
      setTables([]);
      setExpanded({});
      setColumns({});
      setSearch('');
      return;
    }
    setLoading(true);
    setExpanded({});
    setColumns({});
    setSearch('');
    getSchemasTables(selectedSchema)
      .then((res) => {
        const raw = res.data?.tables || (Array.isArray(res.data) ? res.data : []);
        const list = raw.map((t: string | TableInfo) =>
          typeof t === 'string' ? { name: t, schema: selectedSchema } : { ...t, schema: t.schema || selectedSchema }
        );
        setTables(list);
      })
      .catch(() => {
        setTables(DEMO_TABLES[selectedSchema] || []);
      })
      .finally(() => setLoading(false));
  }, [selectedSchema]);

  const toggleTable = useCallback(async (tableName: string) => {
    const isOpen = expanded[tableName];
    setExpanded((prev) => ({ ...prev, [tableName]: !isOpen }));
    if (!isOpen && !columns[tableName]) {
      try {
        const res = await getTableColumns(tableName);
        const raw = res.data?.columns || (Array.isArray(res.data) ? res.data : []);
        const cols: ColumnInfo[] = raw.map((c: any) => {
          if (typeof c === 'string') return { name: c };
          return {
            name: c.column_name || c.name,
            type: c.data_type || c.type,
            isPrimaryKey: c.is_primary_key || c.isPrimaryKey || false,
          };
        });
        setColumns((prev) => ({ ...prev, [tableName]: cols }));
      } catch {
        setColumns((prev) => ({
          ...prev,
          [tableName]: DEMO_COLUMNS[tableName] || [
            { name: 'id', type: 'integer', isPrimaryKey: true },
            { name: 'name', type: 'varchar' },
            { name: 'created_at', type: 'timestamp' },
          ],
        }));
      }
    }
  }, [expanded, columns]);

  const handleAddTable = (table: TableInfo) => {
    if (selectedTables.find((t) => t.table === table.name)) {
      toast.info('Table already added');
      return;
    }
    addTable({ table: table.name, schema: table.schema || selectedSchema || 'public', alias: table.name.charAt(0) });
    toast.success(`Added ${table.name}`);
  };

  const isColumnSelected = (tableName: string, colName: string) =>
    selectedColumns.some((c) => c.table === tableName && c.column === colName);

  const handleToggleColumn = (tableName: string, colName: string) => {
    if (isColumnSelected(tableName, colName)) {
      removeColumn(tableName, colName);
    } else {
      if (!selectedTables.find((t) => t.table === tableName)) {
        addTable({ table: tableName, schema: selectedSchema || 'public', alias: tableName.charAt(0) });
      }
      addColumn({ table: tableName, column: colName, alias: '' });
    }
  };

  const formatRowCount = (count?: number) => {
    if (!count) return null;
    if (count >= 1000000) return `${(count / 1000000).toFixed(1)}M`;
    if (count >= 1000) return `${(count / 1000).toFixed(1)}K`;
    return String(count);
  };

  const filtered = tables.filter((t) => t.name.toLowerCase().includes(search.toLowerCase()));

  if (!selectedSchema) {
    return (
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="text-center">
          <Database className="h-8 w-8 text-muted-foreground mx-auto mb-2 opacity-40" />
          <p className="text-xs text-muted-foreground">Select a schema above to browse tables</p>
        </div>
      </div>
    );
  }

  return (
    <>
      {/* Search within selected schema */}
      <div className="p-3 border-b border-sidebar-border">
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <Input
            placeholder={`Search in ${selectedSchema}…`}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-7 pl-7 text-xs bg-secondary border-none"
          />
        </div>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-2">
          {loading && <p className="text-xs text-muted-foreground p-2">Loading tables…</p>}
          {!loading && filtered.length === 0 && (
            <p className="text-xs text-muted-foreground p-2">No tables found</p>
          )}
          {filtered.map((table) => (
            <div key={table.name} className="mb-0.5">
              <div className="flex items-center group">
                <button
                  onClick={() => toggleTable(table.name)}
                  className="flex items-center flex-1 px-2 py-1.5 rounded text-xs hover:bg-sidebar-accent transition-colors"
                >
                  {expanded[table.name] ? (
                    <ChevronDown className="h-3 w-3 mr-1.5 text-muted-foreground shrink-0" />
                  ) : (
                    <ChevronRight className="h-3 w-3 mr-1.5 text-muted-foreground shrink-0" />
                  )}
                  <Table2 className="h-3.5 w-3.5 mr-1.5 text-primary shrink-0" />
                  <span className="text-foreground">{table.name}</span>
                  {table.rowCount && (
                    <span className="ml-auto text-[10px] text-muted-foreground font-mono">
                      {formatRowCount(table.rowCount)} rows
                    </span>
                  )}
                </button>
                <button
                  onClick={() => handleAddTable(table)}
                  className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-muted transition-all"
                  title="Add table to query"
                >
                  <Plus className="h-3 w-3 text-primary" />
                </button>
              </div>

              {expanded[table.name] && columns[table.name] && (
                <div className="ml-3 border-l border-sidebar-border pl-2">
                  {table.primaryKey && (
                    <div className="px-2 py-1 mb-1 flex gap-1 flex-wrap">
                      <Badge variant="secondary" className="text-[9px] px-1 py-0 h-4">
                        PK: {table.primaryKey}
                      </Badge>
                      <Badge variant="secondary" className="text-[9px] px-1 py-0 h-4">
                        {columns[table.name].length} cols
                      </Badge>
                    </div>
                  )}
                  {columns[table.name].map((col) => (
                    <button
                      key={col.name}
                      onClick={() => handleToggleColumn(table.name, col.name)}
                      className="flex items-center w-full px-2 py-1 rounded text-xs hover:bg-sidebar-accent transition-colors group/col gap-1.5"
                    >
                      <Checkbox
                        checked={isColumnSelected(table.name, col.name)}
                        className="h-3 w-3 border-muted-foreground data-[state=checked]:bg-primary data-[state=checked]:border-primary"
                        onClick={(e) => e.stopPropagation()}
                        onCheckedChange={() => handleToggleColumn(table.name, col.name)}
                      />
                      <span className={isColumnSelected(table.name, col.name) ? 'text-primary font-medium' : 'text-sidebar-foreground group-hover/col:text-foreground'}>
                        {col.name}
                      </span>
                      {col.isPrimaryKey && <span className="text-[9px] text-primary">PK</span>}
                      {col.type && (
                        <span className="ml-auto font-mono text-[10px] text-muted-foreground">{col.type}</span>
                      )}
                    </button>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </ScrollArea>
    </>
  );
}
