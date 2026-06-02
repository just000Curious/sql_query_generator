import { useState, useEffect } from "react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { api, type ColumnInfo } from "@/lib/api";
import { Loader2, Plus, X, Database, Table, CheckCircle, Pencil } from "lucide-react";

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
  const [editingAliasId, setEditingAliasId] = useState<string | null>(null);
  const [editingAliasVal, setEditingAliasVal] = useState("");

  const renameAlias = (id: string, newAlias: string) => {
    const trimmed = newAlias.trim().replace(/[^a-zA-Z0-9_]/g, "");
    if (!trimmed) return; // don't allow empty alias
    // Check for duplicate aliases
    const isDuplicate = tables.some((t) => t.id !== id && t.alias === trimmed);
    if (isDuplicate) return; // don't allow duplicates
    onTablesChange(
      tables.map((t) => (t.id === id ? { ...t, alias: trimmed } : t))
    );
    setEditingAliasId(null);
  };

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
    // Allow same table multiple times for self-joins — unique alias is generated below
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
                className="h-9 text-sm"
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
              <SelectContent className="max-h-72">
                {filteredTables.length === 0 && tableSearch ? (
                  <div className="px-3 py-2 text-xs text-muted-foreground">No tables match "{tableSearch}"</div>
                ) : tableSearch ? (
                  // When searching, show flat list
                  filteredTables.map((t) => (
                    <SelectItem key={t} value={t}>{t}</SelectItem>
                  ))
                ) : (
                  // When not searching, group by 4-char prefix
                  (() => {
                    const groups: Record<string, string[]> = {};
                    for (const t of filteredTables) {
                      // Extract prefix: letters up to first underscore or first 4 chars
                      const prefix = t.includes("_") ? t.split("_").slice(0, 2).join("_") : t.slice(0, 4);
                      if (!groups[prefix]) groups[prefix] = [];
                      groups[prefix].push(t);
                    }
                    return Object.entries(groups).map(([prefix, items]) => (
                      <div key={prefix}>
                        <div className="px-2 py-1.5 text-[10px] font-bold uppercase tracking-widest border-b border-border/40"
                          style={{ color: "hsl(24 89% 55%)", background: "hsl(24 89% 53% / 0.06)" }}>
                          {prefix}_* &nbsp;<span className="font-normal opacity-60">({items.length})</span>
                        </div>
                        {items.map((t) => (
                          <SelectItem key={t} value={t} className="pl-5 text-sm">{t}</SelectItem>
                        ))}
                      </div>
                    ));
                  })()
                )}
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Add more tables button (multi-table mode) has been removed because users can just use the dropdown again */}

      {/* Selected tables as cards */}
      {tables.length > 0 && (
        <div className="flex flex-wrap gap-2 pt-1">
          {tables.map((t) => {
            const isLoading = loadingCols[t.id];
            const isEditing = editingAliasId === t.id;
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

                {/* Editable alias */}
                {isEditing ? (
                  <input
                    autoFocus
                    value={editingAliasVal}
                    onChange={(e) => setEditingAliasVal(e.target.value)}
                    onBlur={() => renameAlias(t.id, editingAliasVal)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") renameAlias(t.id, editingAliasVal);
                      if (e.key === "Escape") setEditingAliasId(null);
                    }}
                    className="w-14 h-6 px-1.5 text-xs font-mono font-bold rounded border border-primary/50 bg-background text-primary outline-none focus:ring-1 focus:ring-primary/40"
                    maxLength={8}
                  />
                ) : (
                  <span
                    className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[11px] font-mono font-bold bg-primary/10 text-primary cursor-pointer hover:bg-primary/20 transition-colors"
                    title="Click to rename alias"
                    onClick={() => { setEditingAliasId(t.id); setEditingAliasVal(t.alias); }}
                  >
                    {t.alias}
                    <Pencil className="h-2.5 w-2.5 opacity-60" />
                  </span>
                )}

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
