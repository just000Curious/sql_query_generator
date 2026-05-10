import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, FunctionSquare } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface FunctionColumn {
  id: string;
  category: string;
  func: string;
  column: string;
  param1: string;
  param2: string;
  alias: string;
}

const FUNC_CATEGORIES: Record<string, { func: string; label: string; params: string[] }[]> = {
  "String": [
    { func: "UPPER", label: "UPPER(col)", params: [] },
    { func: "LOWER", label: "LOWER(col)", params: [] },
    { func: "TRIM", label: "TRIM(col)", params: [] },
    { func: "LENGTH", label: "LENGTH(col)", params: [] },
    { func: "SUBSTRING", label: "SUBSTRING(col, start, len)", params: ["start", "length"] },
    { func: "REPLACE", label: "REPLACE(col, old, new)", params: ["old", "new"] },
    { func: "CONCAT", label: "CONCAT(col, val)", params: ["value"] },
    { func: "LEFT", label: "LEFT(col, n)", params: ["n"] },
    { func: "RIGHT", label: "RIGHT(col, n)", params: ["n"] },
  ],
  "Date/Time": [
    { func: "DATE_TRUNC", label: "DATE_TRUNC(precision, col)", params: ["precision"] },
    { func: "EXTRACT", label: "EXTRACT(part FROM col)", params: ["part"] },
    { func: "AGE", label: "AGE(col)", params: [] },
    { func: "TO_CHAR", label: "TO_CHAR(col, format)", params: ["format"] },
    { func: "DATE_PART", label: "DATE_PART(part, col)", params: ["part"] },
  ],
  "Math": [
    { func: "ROUND", label: "ROUND(col, n)", params: ["decimals"] },
    { func: "FLOOR", label: "FLOOR(col)", params: [] },
    { func: "CEIL", label: "CEIL(col)", params: [] },
    { func: "ABS", label: "ABS(col)", params: [] },
    { func: "MOD", label: "MOD(col, divisor)", params: ["divisor"] },
  ],
  "Null Handling": [
    { func: "COALESCE", label: "COALESCE(col, default)", params: ["default"] },
    { func: "NULLIF", label: "NULLIF(col, val)", params: ["value"] },
  ],
  "Type Cast": [
    { func: "CAST", label: "CAST(col AS type)", params: ["type"] },
  ],
};

const ALL_FUNCS = Object.values(FUNC_CATEGORIES).flat();

export const buildFuncSql = (f: FunctionColumn): string => {
  if (!f.func || !f.column) return "";
  const def = ALL_FUNCS.find(d => d.func === f.func);
  let sql = "";
  switch (f.func) {
    case "SUBSTRING": sql = `SUBSTRING(${f.column}, ${f.param1 || 1}, ${f.param2 || 10})`; break;
    case "REPLACE": sql = `REPLACE(${f.column}, '${f.param1}', '${f.param2}')`; break;
    case "CONCAT": sql = `CONCAT(${f.column}, '${f.param1}')`; break;
    case "LEFT": case "RIGHT": sql = `${f.func}(${f.column}, ${f.param1 || 1})`; break;
    case "DATE_TRUNC": sql = `DATE_TRUNC('${f.param1 || "month"}', ${f.column})`; break;
    case "EXTRACT": sql = `EXTRACT(${f.param1 || "YEAR"} FROM ${f.column})`; break;
    case "TO_CHAR": sql = `TO_CHAR(${f.column}, '${f.param1 || "YYYY-MM-DD"}')`; break;
    case "DATE_PART": sql = `DATE_PART('${f.param1 || "year"}', ${f.column})`; break;
    case "ROUND": sql = `ROUND(${f.column}, ${f.param1 || 2})`; break;
    case "MOD": sql = `MOD(${f.column}, ${f.param1 || 2})`; break;
    case "COALESCE": sql = `COALESCE(${f.column}, '${f.param1 || "N/A"}')`; break;
    case "NULLIF": sql = `NULLIF(${f.column}, '${f.param1}')`; break;
    case "CAST": sql = `CAST(${f.column} AS ${f.param1 || "TEXT"})`; break;
    default: sql = `${f.func}(${f.column})`; break;
  }
  if (f.alias) sql += ` AS ${f.alias}`;
  return sql;
};

interface Props {
  tables: SelectedTable[];
  functionColumns: FunctionColumn[];
  onFunctionColumnsChange: (f: FunctionColumn[]) => void;
}

const FunctionBuilder = ({ tables, functionColumns, onFunctionColumnsChange }: Props) => {
  const cols = tables.flatMap(t => t.columns.map(c => ({ key: `${t.alias}.${c.name}`, label: c.name })));

  const add = () => onFunctionColumnsChange([...functionColumns, {
    id: crypto.randomUUID(), category: "String", func: "UPPER", column: "", param1: "", param2: "", alias: "",
  }]);

  const update = (id: string, field: keyof FunctionColumn, val: string) => {
    onFunctionColumnsChange(functionColumns.map(f => {
      if (f.id !== id) return f;
      const updated = { ...f, [field]: val };
      if (field === "category") {
        const firstFunc = FUNC_CATEGORIES[val]?.[0];
        if (firstFunc) updated.func = firstFunc.func;
        updated.param1 = ""; updated.param2 = "";
      }
      if (field === "func") { updated.param1 = ""; updated.param2 = ""; }
      return updated;
    }));
  };

  const remove = (id: string) => onFunctionColumnsChange(functionColumns.filter(f => f.id !== id));

  if (!cols.length) return null;

  return (
    <div className="space-y-3">
      {functionColumns.length === 0 && (
        <p className="text-xs text-muted-foreground italic flex items-center gap-1.5">
          <FunctionSquare className="h-3 w-3 opacity-60" /> No column functions. Add one to transform column values (UPPER, COALESCE, DATE_TRUNC…).
        </p>
      )}
      {functionColumns.map(fc => {
        const catFuncs = FUNC_CATEGORIES[fc.category] || [];
        const funcDef = ALL_FUNCS.find(d => d.func === fc.func);
        const preview = buildFuncSql(fc);
        return (
          <div key={fc.id} className="rounded-xl border border-border bg-muted/20 p-3 space-y-2">
            <div className="flex flex-wrap gap-2 items-center">
              {/* Category */}
              <Select value={fc.category} onValueChange={v => update(fc.id, "category", v)}>
                <SelectTrigger className="w-[120px] h-9 text-xs"><SelectValue /></SelectTrigger>
                <SelectContent>{Object.keys(FUNC_CATEGORIES).map(c => <SelectItem key={c} value={c}>{c}</SelectItem>)}</SelectContent>
              </Select>
              {/* Function */}
              <Select value={fc.func} onValueChange={v => update(fc.id, "func", v)}>
                <SelectTrigger className="w-[180px] h-9 text-xs font-mono"><SelectValue /></SelectTrigger>
                <SelectContent>{catFuncs.map(f => <SelectItem key={f.func} value={f.func}>{f.label}</SelectItem>)}</SelectContent>
              </Select>
              {/* Column */}
              <Select value={fc.column} onValueChange={v => update(fc.id, "column", v)}>
                <SelectTrigger className="flex-1 min-w-[140px] h-9 text-xs"><SelectValue placeholder="Column" /></SelectTrigger>
                <SelectContent>{cols.map(c => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}</SelectContent>
              </Select>
              <Button variant="ghost" size="icon" className="h-9 w-9 text-destructive/70" onClick={() => remove(fc.id)}>
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>
            {/* Params + Alias */}
            <div className="flex flex-wrap gap-2 items-center">
              {funcDef && funcDef.params.length > 0 && (
                <Input value={fc.param1} onChange={e => update(fc.id, "param1", e.target.value)} placeholder={funcDef.params[0]} className="w-28 h-8 text-xs font-mono" />
              )}
              {funcDef && funcDef.params.length > 1 && (
                <Input value={fc.param2} onChange={e => update(fc.id, "param2", e.target.value)} placeholder={funcDef.params[1]} className="w-28 h-8 text-xs font-mono" />
              )}
              <span className="text-xs text-muted-foreground font-bold">AS</span>
              <Input value={fc.alias} onChange={e => update(fc.id, "alias", e.target.value)} placeholder="alias" className="w-32 h-8 text-xs font-mono" />
              {preview && <span className="text-[10px] font-mono text-muted-foreground ml-auto truncate max-w-[250px]">{preview}</span>}
            </div>
          </div>
        );
      })}
      <Button variant="outline" size="default" onClick={add} className="text-sm gap-2">
        <Plus className="h-4 w-4" /> Add Column Function
      </Button>
    </div>
  );
};

export default FunctionBuilder;
