import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Plus, Trash2, BarChart3 } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface WindowFunction {
  id: string;
  func: string;
  column: string;
  param: string;
  partitionBy: string[];
  orderByCol: string;
  orderByDir: "ASC" | "DESC";
  alias: string;
}

const WIN_FUNCS: { value: string; label: string; needsCol: boolean; needsParam: boolean; paramLabel: string }[] = [
  { value: "ROW_NUMBER", label: "ROW_NUMBER()", needsCol: false, needsParam: false, paramLabel: "" },
  { value: "RANK", label: "RANK()", needsCol: false, needsParam: false, paramLabel: "" },
  { value: "DENSE_RANK", label: "DENSE_RANK()", needsCol: false, needsParam: false, paramLabel: "" },
  { value: "NTILE", label: "NTILE(n)", needsCol: false, needsParam: true, paramLabel: "buckets" },
  { value: "LAG", label: "LAG(col, offset)", needsCol: true, needsParam: true, paramLabel: "offset" },
  { value: "LEAD", label: "LEAD(col, offset)", needsCol: true, needsParam: true, paramLabel: "offset" },
  { value: "FIRST_VALUE", label: "FIRST_VALUE(col)", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "LAST_VALUE", label: "LAST_VALUE(col)", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "SUM", label: "SUM(col) OVER", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "COUNT", label: "COUNT(col) OVER", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "AVG", label: "AVG(col) OVER", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "MIN", label: "MIN(col) OVER", needsCol: true, needsParam: false, paramLabel: "" },
  { value: "MAX", label: "MAX(col) OVER", needsCol: true, needsParam: false, paramLabel: "" },
];

export const buildWindowSql = (wf: WindowFunction): string => {
  const def = WIN_FUNCS.find(f => f.value === wf.func);
  if (!def) return "";

  let funcCall = wf.func;
  if (def.needsCol && def.needsParam) {
    funcCall += `(${wf.column || "*"}, ${wf.param || "1"})`;
  } else if (def.needsCol) {
    funcCall += `(${wf.column || "*"})`;
  } else if (def.needsParam) {
    funcCall += `(${wf.param || "4"})`;
  } else {
    funcCall += "()";
  }

  const overParts: string[] = [];
  if (wf.partitionBy.length > 0) overParts.push(`PARTITION BY ${wf.partitionBy.join(", ")}`);
  if (wf.orderByCol && wf.orderByCol.trim()) overParts.push(`ORDER BY ${wf.orderByCol.trim()} ${wf.orderByDir}`);

  let sql = `${funcCall} OVER (${overParts.join(" ")})`;
  if (wf.alias) sql += ` AS ${wf.alias}`;
  return sql;
};

interface Props {
  tables: SelectedTable[];
  windowFunctions: WindowFunction[];
  onWindowFunctionsChange: (w: WindowFunction[]) => void;
}

const WindowFunctionBuilder = ({ tables, windowFunctions, onWindowFunctionsChange }: Props) => {
  const cols = tables.flatMap(t => t.columns.map(c => ({ key: `${t.alias}.${c.name}`, label: c.name })));

  const add = () => onWindowFunctionsChange([...windowFunctions, {
    id: crypto.randomUUID(), func: "ROW_NUMBER", column: "", param: "",
    partitionBy: [], orderByCol: "", orderByDir: "ASC", alias: "rn",
  }]);

  const update = (id: string, field: keyof WindowFunction, val: any) =>
    onWindowFunctionsChange(windowFunctions.map(w => w.id === id ? { ...w, [field]: val } : w));

  const remove = (id: string) => onWindowFunctionsChange(windowFunctions.filter(w => w.id !== id));

  const togglePartition = (id: string, col: string) => {
    const wf = windowFunctions.find(w => w.id === id);
    if (!wf) return;
    const next = wf.partitionBy.includes(col)
      ? wf.partitionBy.filter(c => c !== col)
      : [...wf.partitionBy, col];
    update(id, "partitionBy", next);
  };

  if (!cols.length) return null;

  return (
    <div className="space-y-3">
      {windowFunctions.length === 0 && (
        <p className="text-xs text-muted-foreground italic flex items-center gap-1.5">
          <BarChart3 className="h-3 w-3 opacity-60" /> No window functions. Add one for ROW_NUMBER, RANK, LAG, running totals, etc.
        </p>
      )}
      {windowFunctions.map(wf => {
        const def = WIN_FUNCS.find(f => f.value === wf.func);
        const preview = buildWindowSql(wf);
        return (
          <div key={wf.id} className="rounded-2xl border border-border bg-muted/20 p-4 space-y-3">
            <div className="flex items-center gap-2 mb-1">
              <BarChart3 className="h-4 w-4 text-blue-500" />
              <span className="text-sm font-semibold flex-1">{wf.alias || wf.func}</span>
              <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/70" onClick={() => remove(wf.id)}>
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>
            {/* Row 1: Function + Column + Param + Alias */}
            <div className="flex flex-wrap gap-2 items-center">
              <Select value={wf.func} onValueChange={v => update(wf.id, "func", v)}>
                <SelectTrigger className="w-[180px] h-9 text-xs font-mono"><SelectValue /></SelectTrigger>
                <SelectContent>{WIN_FUNCS.map(f => <SelectItem key={f.value} value={f.value}>{f.label}</SelectItem>)}</SelectContent>
              </Select>
              {def?.needsCol && (
                <Select value={wf.column} onValueChange={v => update(wf.id, "column", v)}>
                  <SelectTrigger className="flex-1 min-w-[140px] h-9 text-xs"><SelectValue placeholder="Column" /></SelectTrigger>
                  <SelectContent>{cols.map(c => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}</SelectContent>
                </Select>
              )}
              {def?.needsParam && (
                <Input value={wf.param} onChange={e => update(wf.id, "param", e.target.value)} placeholder={def.paramLabel} className="w-20 h-9 text-xs font-mono" />
              )}
              <span className="text-xs font-bold text-muted-foreground">AS</span>
              <Input value={wf.alias} onChange={e => update(wf.id, "alias", e.target.value)} placeholder="alias" className="w-28 h-9 text-xs font-mono" />
            </div>
            {/* Row 2: PARTITION BY */}
            <div>
              <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Partition By</p>
              <div className="flex flex-wrap gap-1.5">
                {cols.map(c => (
                  <label key={c.key} className="flex items-center gap-1.5 text-xs bg-muted px-2 py-1 rounded-lg cursor-pointer hover:bg-muted/80">
                    <Checkbox checked={wf.partitionBy.includes(c.key)} onCheckedChange={() => togglePartition(wf.id, c.key)} />
                    {c.label}
                  </label>
                ))}
              </div>
            </div>
            {/* Row 3: ORDER BY inside OVER */}
            <div className="flex gap-2 items-center">
              <p className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider w-16">Order By</p>
              <Select value={wf.orderByCol} onValueChange={v => update(wf.id, "orderByCol", v)}>
                <SelectTrigger className="flex-1 h-9 text-xs"><SelectValue placeholder="Column (optional)" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value=" ">— None —</SelectItem>
                  {cols.map(c => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}
                </SelectContent>
              </Select>
              <Select value={wf.orderByDir} onValueChange={v => update(wf.id, "orderByDir", v as "ASC"|"DESC")}>
                <SelectTrigger className="w-24 h-9 text-xs"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="ASC">ASC ↑</SelectItem>
                  <SelectItem value="DESC">DESC ↓</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {/* Preview */}
            {preview && <div className="p-2 rounded-lg bg-background/80 border border-border/50"><p className="text-[10px] font-mono text-muted-foreground break-all">{preview}</p></div>}
          </div>
        );
      })}
      <Button variant="outline" size="default" onClick={add} className="text-sm gap-2">
        <Plus className="h-4 w-4" /> Add Window Function
      </Button>
    </div>
  );
};

export default WindowFunctionBuilder;
