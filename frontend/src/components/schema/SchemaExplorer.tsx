import { useState, useEffect } from 'react';
import { getTables, getTableColumns } from '@/api/schema';
import { useQueryStore } from '@/store/queryStore';
import { Search, Table2, ChevronRight, ChevronDown, Plus, Eye, Database } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Checkbox } from '@/components/ui/checkbox';
import { toast } from 'sonner';
import { Badge } from '@/components/ui/badge';

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

// Demo relationships for smart join suggestions
const DEMO_RELATIONSHIPS = [
  { fromTable: 'employees', fromColumn: 'dept_id', toTable: 'departments', toColumn: 'dept_id' },
  { fromTable: 'salaries', fromColumn: 'emp_id', toTable: 'employees', toColumn: 'id' },
  { fromTable: 'assignments', fromColumn: 'emp_id', toTable: 'employees', toColumn: 'id' },
  { fromTable: 'assignments', fromColumn: 'project_id', toTable: 'projects', toColumn: 'id' },
];

const DEMO_TABLE_META: Record<string, { rowCount: number; primaryKey: string }> = {
  employees: { rowCount: 120000, primaryKey: 'id' },
  departments: { rowCount: 45, primaryKey: 'dept_id' },
  salaries: { rowCount: 480000, primaryKey: 'id' },
  projects: { rowCount: 320, primaryKey: 'id' },
  assignments: { rowCount: 95000, primaryKey: 'id' },
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
};

export function SchemaExplorer() {
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [columns, setColumns] = useState<Record<string, ColumnInfo[]>>({});
  const [previewTable, setPreviewTable] = useState<string | null>(null);
  const { addColumn, removeColumn, addTable, selectedTables, selectedColumns, setRelationships } = useQueryStore();

  useEffect(() => {
    // Fetch tables
    getTables()
      .then((res) => {
        const raw = res.data?.tables || (Array.isArray(res.data) ? res.data : []);
        const list = raw.map((t: string | TableInfo) => typeof t === 'string' ? { name: t } : t);
        setTables(list);
      })
      .catch(() => {
        setTables([
          { name: 'employees', schema: 'public', rowCount: 120000, primaryKey: 'id' },
          { name: 'departments', schema: 'public', rowCount: 45, primaryKey: 'dept_id' },
          { name: 'salaries', schema: 'public', rowCount: 480000, primaryKey: 'id' },
          { name: 'projects', schema: 'public', rowCount: 320, primaryKey: 'id' },
          { name: 'assignments', schema: 'public', rowCount: 95000, primaryKey: 'id' },
        ]);
      });

    // Fetch relationships
    import('@/api/schema').then(({ getRelationships: fetchRels }) => {
      fetchRels()
        .then((res) => {
          const rels = res.data.relationships || [];
          setRelationships(rels);
        })
        .catch(() => {
          setRelationships(DEMO_RELATIONSHIPS);
        });
    });
  }, [setRelationships]);

  const toggleTable = async (tableName: string) => {
    const isOpen = expanded[tableName];
    setExpanded((prev) => ({ ...prev, [tableName]: !isOpen }));
    if (!isOpen && !columns[tableName]) {
      try {
        const res = await getTableColumns(tableName);
        // Backend returns { columns: [...] } with column_name, data_type, is_primary_key fields
        const raw = res.data?.columns || (Array.isArray(res.data) ? res.data : []);
        const cols = raw.map((c: any) => {
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
  };

  const handleAddTable = (table: TableInfo) => {
    if (selectedTables.find((t) => t.table === table.name)) {
      toast.info('Table already added');
      return;
    }
    addTable({ table: table.name, schema: table.schema || 'public', alias: table.name.charAt(0) });
    toast.success(`Added ${table.name}`);
  };

  const isColumnSelected = (tableName: string, colName: string) =>
    selectedColumns.some((c) => c.table === tableName && c.column === colName);

  const handleToggleColumn = (tableName: string, colName: string) => {
    if (isColumnSelected(tableName, colName)) {
      removeColumn(tableName, colName);
    } else {
      // Auto-add table if not selected
      if (!selectedTables.find((t) => t.table === tableName)) {
        addTable({ table: tableName, schema: 'public', alias: tableName.charAt(0) });
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

  const getMeta = (tableName: string) => DEMO_TABLE_META[tableName];
  const getRelationships = (tableName: string) =>
    DEMO_RELATIONSHIPS.filter((r) => r.fromTable === tableName || r.toTable === tableName);

  const filtered = tables.filter((t) => t.name.toLowerCase().includes(search.toLowerCase()));

  return (
    <aside className="w-72 border-r bg-sidebar flex flex-col h-full">
      <div className="p-3 border-b border-sidebar-border">
        <div className="flex items-center gap-1.5 mb-2">
          <Database className="h-3.5 w-3.5 text-primary" />
          <h2 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Schema Explorer</h2>
        </div>
        <div className="relative">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
          <Input
            placeholder="Search tables..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-7 pl-7 text-xs bg-surface-2 border-none"
          />
        </div>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-2">
          {filtered.map((table) => {
            const meta = getMeta(table.name);
            const rels = getRelationships(table.name);
            return (
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
                    {meta && (
                      <span className="ml-auto text-[10px] text-muted-foreground font-mono">
                        {formatRowCount(meta.rowCount)} rows
                      </span>
                    )}
                  </button>
                  <button
                    onClick={() => handleAddTable(table)}
                    className="opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-surface-3 transition-all"
                    title="Add table to query"
                  >
                    <Plus className="h-3 w-3 text-primary" />
                  </button>
                </div>

                {expanded[table.name] && (
                  <div className="ml-3 border-l border-sidebar-border pl-2">
                    {/* Table metadata */}
                    {meta && (
                      <div className="px-2 py-1 mb-1">
                        <div className="flex gap-1 flex-wrap">
                          <Badge variant="secondary" className="text-[9px] px-1 py-0 h-4">
                            PK: {meta.primaryKey}
                          </Badge>
                          <Badge variant="secondary" className="text-[9px] px-1 py-0 h-4">
                            {columns[table.name]?.length || '...'} cols
                          </Badge>
                        </div>
                      </div>
                    )}

                    {/* Columns with checkboxes */}
                    {columns[table.name]?.map((col) => (
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
                        <span className={`${isColumnSelected(table.name, col.name) ? 'text-primary font-medium' : 'text-sidebar-foreground group-hover/col:text-foreground'}`}>
                          {col.name}
                        </span>
                        {col.isPrimaryKey && <span className="text-[9px] text-primary">PK</span>}
                        {col.type && (
                          <span className="ml-auto font-mono text-[10px] text-muted-foreground">{col.type}</span>
                        )}
                      </button>
                    ))}

                    {/* Relationships */}
                    {rels.length > 0 && (
                      <div className="px-2 py-1.5 mt-1 border-t border-sidebar-border">
                        <span className="text-[10px] text-muted-foreground uppercase tracking-wider font-semibold">Relations</span>
                        {rels.map((r, i) => {
                          const from = r.fromTable === table.name ? r.fromColumn : r.toColumn;
                          const toTbl = r.fromTable === table.name ? r.toTable : r.fromTable;
                          const toCol = r.fromTable === table.name ? r.toColumn : r.fromColumn;
                          return (
                            <div key={i} className="text-[10px] font-mono text-muted-foreground mt-0.5 flex items-center gap-1">
                              <span className="text-primary">{from}</span>
                              <span className="text-cyan-dim">→</span>
                              <span>{toTbl}.{toCol}</span>
                            </div>
                          );
                        })}
                      </div>
                    )}

                    {/* Data preview button */}
                    <button
                      onClick={() => setPreviewTable(previewTable === table.name ? null : table.name)}
                      className="flex items-center gap-1.5 w-full px-2 py-1 mt-0.5 rounded text-[10px] text-muted-foreground hover:bg-sidebar-accent hover:text-foreground transition-colors"
                    >
                      <Eye className="h-3 w-3" />
                      Preview data
                    </button>

                    {previewTable === table.name && (
                      <div className="px-1 py-1 mt-1">
                        <div className="bg-surface-2 rounded p-2 overflow-x-auto">
                          <table className="text-[10px] font-mono">
                            <thead>
                              <tr className="text-muted-foreground">
                                {columns[table.name]?.slice(0, 4).map((c) => (
                                  <th key={c.name} className="pr-3 pb-1 text-left font-medium">{c.name}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody className="text-foreground">
                              {[1, 2, 3].map((row) => (
                                <tr key={row}>
                                  {columns[table.name]?.slice(0, 4).map((c) => (
                                    <td key={c.name} className="pr-3 py-0.5 text-muted-foreground">
                                      {c.type === 'integer' ? String(row * 100 + Math.floor(Math.random() * 100)) :
                                       c.type === 'varchar' ? `sample_${row}` :
                                       c.type === 'decimal' ? (row * 25000 + Math.random() * 10000).toFixed(2) :
                                       c.type === 'date' ? '2024-01-' + String(row).padStart(2, '0') :
                                       '2024-01-01 00:00'}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </ScrollArea>

      {/* Selected columns summary */}
      {selectedColumns.length > 0 && (
        <div className="border-t border-sidebar-border p-2">
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-1">
            Selected ({selectedColumns.length})
          </div>
          <div className="space-y-0.5 max-h-24 overflow-auto">
            {selectedColumns.map((c) => (
              <div key={`${c.table}.${c.column}`} className="text-[10px] font-mono text-foreground flex items-center gap-1">
                <span className="text-muted-foreground">{c.table}.</span>
                <span className="text-primary">{c.column}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </aside>
  );
}
