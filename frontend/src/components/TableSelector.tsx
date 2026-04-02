import { useState, useEffect } from "react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api, type ColumnInfo } from "@/lib/api";
import { Loader2, Plus, X, Database, Table, CheckCircle } from "lucide-react";

export interface SelectedTable {
  id: string;
  schema: string;
  table: string;
  alias: string;
  columns: ColumnInfo[];
  primaryKeys: string[];
  foreignKeys: { column: string; references: string }[];
}

interface TableSelectorProps {
  tables: SelectedTable[];
  onTablesChange: (tables: SelectedTable[]) => void;
  multiTable?: boolean;
}

const TableSelector = ({ tables, onTablesChange, multiTable = false }: TableSelectorProps) => {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [tablesBySchema, setTablesBySchema] = useState<Record<string, string[]>>({});
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [loadingTables, setLoadingTables] = useState<Record<string, boolean>>({});
  const [loadingCols, setLoadingCols] = useState<Record<string, boolean>>({});
  const [curSchema, setCurSchema] = useState("");
  const [curTableList, setCurTableList] = useState<string[]>([]);
  const [tableSearch, setTableSearch] = useState("");

  useEffect(() => {
    setLoadingSchemas(true);
    api.getSchemas()
      .then((res) => {
        const raw = Array.isArray(res) ? res : (res as any).schemas || [];
        // Normalize: handle both string[] and {name, ...}[]
        const list: string[] = raw.map((s: any) =>
          typeof s === "string" ? s : (s?.name ?? String(s))
        );
        setSchemas(list);
      })
      .catch(() => setSchemas(["GM", "HM", "PM", "SI", "SA", "TA"]))
      .finally(() => setLoadingSchemas(false));
  }, []);

  useEffect(() => {
    if (!curSchema) return;
    if (tablesBySchema[curSchema]) {
      setCurTableList(tablesBySchema[curSchema]);
      return;
    }
    setLoadingTables((p) => ({ ...p, [curSchema]: true }));
    api.getTables(curSchema)
      .then((res) => {
        const raw = Array.isArray(res) ? res : (res as any).tables || [];
        // Normalize: handle both string[] and {name, ...}[]
        const list: string[] = raw.map((t: any) =>
          typeof t === "string" ? t : (t?.name ?? String(t))
        );
        setTablesBySchema((p) => ({ ...p, [curSchema]: list }));
        setCurTableList(list);
      })
      .catch(() => setCurTableList([]))
      .finally(() => setLoadingTables((p) => ({ ...p, [curSchema]: false })));
  }, [curSchema]);

  const addTable = (tableName: string) => {
    if (tables.some((t) => t.schema === curSchema && t.table === tableName)) return;
    const id = crypto.randomUUID();

    // Generate unique alias: first letter + suffix if collision
    const base = tableName.slice(0, 1).toLowerCase();
    const existingAliases = new Set(tables.map((t) => t.alias));
    let alias = base;
    let counter = 2;
    while (existingAliases.has(alias)) {
      alias = `${base}${counter}`;
      counter++;
    }
    const newTable: SelectedTable = {
      id, schema: curSchema, table: tableName, alias,
      columns: [], primaryKeys: [], foreignKeys: [],
    };

    setLoadingCols((p) => ({ ...p, [id]: true }));
    api.getTableColumns(tableName, curSchema)
      .then((res) => {
        const cols: ColumnInfo[] = (res.columns || []).map((c: any) =>
          typeof c === "string" ? { name: c, type: "TEXT", is_primary_key: false } : c
        );
        newTable.columns = cols;
        newTable.primaryKeys = res.primary_keys || [];
        newTable.foreignKeys = res.foreign_keys || [];
        if (!multiTable) {
          onTablesChange([newTable]);
        } else {
          onTablesChange([...tables, newTable]);
        }
      })
      .catch(() => {
        if (!multiTable) onTablesChange([newTable]);
        else onTablesChange([...tables, newTable]);
      })
      .finally(() => setLoadingCols((p) => ({ ...p, [id]: false })));
  };

  const removeTable = (id: string) => {
    onTablesChange(tables.filter((t) => t.id !== id));
  };

  const filteredTables = tableSearch
    ? curTableList.filter((t) => t.toLowerCase().includes(tableSearch.toLowerCase()))
    : curTableList;

  return (
    <div className="space-y-3">
      {/* Schema + Table pickers */}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* Schema selector */}
        <div className="space-y-1.5">
          <label className="text-xs font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5">
            <Database className="h-3 w-3" /> Schema
          </label>
          <Select value={curSchema} onValueChange={(v) => { setCurSchema(v); setTableSearch(""); }} disabled={loadingSchemas}>
            <SelectTrigger className="h-10">
              <SelectValue placeholder={loadingSchemas ? "Loading schemas…" : "Choose a schema…"} />
            </SelectTrigger>
            <SelectContent>
              {schemas.map((s) => (
                <SelectItem key={s} value={s}>
                  <span className="font-mono font-semibold">{s}</span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Table selector with search */}
        <div className="space-y-1.5">
          <label className="text-xs font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5">
            <Table className="h-3 w-3" /> Table
            {curTableList.length > 0 && (
              <span className="badge-count">{curTableList.length}</span>
            )}
          </label>
          <div className="space-y-1">
            {curSchema && curTableList.length > 8 && (
              <Input
                value={tableSearch}
                onChange={(e) => setTableSearch(e.target.value)}
                placeholder={`Search ${curTableList.length} tables…`}
                className="h-8 text-xs"
              />
            )}
            <Select
              value=""
              onValueChange={addTable}
              disabled={!curSchema || loadingTables[curSchema]}
            >
              <SelectTrigger className="h-10">
                <SelectValue placeholder={
                  !curSchema ? "Select schema first…" :
                  loadingTables[curSchema] ? "Loading tables…" :
                  "Choose a table…"
                } />
              </SelectTrigger>
              <SelectContent>
                {filteredTables.map((t) => (
                  <SelectItem key={t} value={t}>{t}</SelectItem>
                ))}
                {filteredTables.length === 0 && tableSearch && (
                  <div className="px-3 py-2 text-xs text-muted-foreground">No tables match "{tableSearch}"</div>
                )}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Add more tables button (multi-table mode) */}
      {multiTable && tables.length > 0 && (
        <Button variant="outline" size="sm" className="gap-1.5 text-xs">
          <Plus className="h-3.5 w-3.5" /> Add Another Table
        </Button>
      )}

      {/* Selected tables as cards */}
      {tables.length > 0 && (
        <div className="flex flex-wrap gap-2 pt-1">
          {tables.map((t) => {
            const isLoading = loadingCols[t.id];
            return (
              <div
                key={t.id}
                className="flex items-center gap-2 bg-secondary/10 border border-secondary/30 rounded-xl px-3 py-2 text-sm"
              >
                {isLoading ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin text-secondary" />
                ) : (
                  <CheckCircle className="h-3.5 w-3.5 text-success" />
                )}
                <span className="font-semibold text-secondary">{t.schema}.</span>
                <span className="font-medium">{t.table}</span>
                {t.columns.length > 0 && (
                  <span className="badge-count">{t.columns.length} cols</span>
                )}
                {t.primaryKeys.length > 0 && (
                  <span className="text-[10px] text-amber-600 font-medium">🔑 {t.primaryKeys.join(", ")}</span>
                )}
                <button
                  onClick={() => removeTable(t.id)}
                  className="ml-1 text-muted-foreground hover:text-destructive transition-colors"
                  title="Remove table"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default TableSelector;
