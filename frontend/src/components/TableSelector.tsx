import { useState, useEffect } from "react";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { api, type ColumnInfo } from "@/lib/api";
import { Loader2, Plus, X } from "lucide-react";

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

  // Current selection state for adding
  const [curSchema, setCurSchema] = useState("");
  const [curTableList, setCurTableList] = useState<string[]>([]);

  useEffect(() => {
    setLoadingSchemas(true);
    api.getSchemas()
      .then((res) => {
        const list = Array.isArray(res) ? res : (res as any).schemas || [];
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
        const list = Array.isArray(res) ? res : (res as any).tables || [];
        setTablesBySchema((p) => ({ ...p, [curSchema]: list }));
        setCurTableList(list);
      })
      .catch(() => setCurTableList([]))
      .finally(() => setLoadingTables((p) => ({ ...p, [curSchema]: false })));
  }, [curSchema]);

  const addTable = (tableName: string) => {
    if (tables.some((t) => t.schema === curSchema && t.table === tableName)) return;
    const id = crypto.randomUUID();
    const alias = tableName.charAt(0).toLowerCase() + (tables.length > 0 ? String(tables.length + 1) : "");
    const newTable: SelectedTable = {
      id, schema: curSchema, table: tableName, alias,
      columns: [], primaryKeys: [], foreignKeys: [],
    };

    setLoadingCols((p) => ({ ...p, [id]: true }));
    api.getTableColumns(tableName, curSchema)
      .then((res) => {
        const cols: ColumnInfo[] = (res.columns || []).map((c: any) =>
          typeof c === "string" ? { name: c, type: "VARCHAR", is_primary_key: false } : c
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

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
        <Select value={curSchema} onValueChange={setCurSchema} disabled={loadingSchemas}>
          <SelectTrigger>
            <SelectValue placeholder={loadingSchemas ? "Loading..." : "Select Schema"} />
          </SelectTrigger>
          <SelectContent>
            {schemas.map((s) => <SelectItem key={s} value={s}>{s}</SelectItem>)}
          </SelectContent>
        </Select>

        <Select
          value=""
          onValueChange={addTable}
          disabled={!curSchema || loadingTables[curSchema]}
        >
          <SelectTrigger>
            <SelectValue placeholder={loadingTables[curSchema] ? "Loading..." : "Select Table"} />
          </SelectTrigger>
          <SelectContent>
            {curTableList.map((t) => <SelectItem key={t} value={t}>{t}</SelectItem>)}
          </SelectContent>
        </Select>

        {multiTable && (
          <Button variant="outline" size="sm" className="gap-1.5" disabled={!curSchema}>
            <Plus className="h-3.5 w-3.5" /> Add Table
          </Button>
        )}
      </div>

      {/* Selected tables as tags */}
      {tables.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {tables.map((t) => (
            <span
              key={t.id}
              className="inline-flex items-center gap-1.5 bg-secondary/10 text-secondary border border-secondary/20 rounded-full px-3 py-1 text-sm font-medium"
            >
              {Object.values(loadingCols).some(Boolean) && t.columns.length === 0 ? (
                <Loader2 className="h-3 w-3 animate-spin" />
              ) : null}
              {t.schema}.{t.table}
              <button onClick={() => removeTable(t.id)} className="hover:text-destructive ml-0.5">
                <X className="h-3 w-3" />
              </button>
            </span>
          ))}
        </div>
      )}
    </div>
  );
};

export default TableSelector;
