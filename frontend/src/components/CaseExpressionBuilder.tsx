import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2, GitBranch } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

interface WhenClause { id: string; column: string; operator: string; value: string; then: string; }
export interface CaseExpression { id: string; whenClauses: WhenClause[]; elseValue: string; alias: string; }

const OPS = ["=","!=",">",">=","<","<=","LIKE","IS NULL","IS NOT NULL"];

interface Props {
  tables: SelectedTable[];
  caseExpressions: CaseExpression[];
  onCaseExpressionsChange: (c: CaseExpression[]) => void;
}

export const buildCaseSql = (c: CaseExpression): string => {
  const whens = c.whenClauses.filter(w => w.column && w.then).map(w => {
    const noVal = ["IS NULL","IS NOT NULL"].includes(w.operator);
    return `WHEN ${w.column} ${w.operator}${noVal ? "" : ` ${w.value}`} THEN '${w.then}'`;
  });
  if (!whens.length) return "";
  let sql = `CASE ${whens.join(" ")}`;
  if (c.elseValue) sql += ` ELSE '${c.elseValue}'`;
  sql += " END";
  if (c.alias) sql += ` AS ${c.alias}`;
  return sql;
};

const CaseExpressionBuilder = ({ tables, caseExpressions, onCaseExpressionsChange }: Props) => {
  const cols = tables.flatMap(t => t.columns.map(c => ({ key: `${t.alias}.${c.name}`, label: c.name })));

  const addCase = () => onCaseExpressionsChange([...caseExpressions, {
    id: crypto.randomUUID(), whenClauses: [{ id: crypto.randomUUID(), column: "", operator: "=", value: "", then: "" }], elseValue: "", alias: "",
  }]);

  const removeCase = (id: string) => onCaseExpressionsChange(caseExpressions.filter(c => c.id !== id));

  const updateField = (id: string, field: "elseValue"|"alias", val: string) =>
    onCaseExpressionsChange(caseExpressions.map(c => c.id === id ? { ...c, [field]: val } : c));

  const addWhen = (cid: string) => onCaseExpressionsChange(caseExpressions.map(c =>
    c.id === cid ? { ...c, whenClauses: [...c.whenClauses, { id: crypto.randomUUID(), column: "", operator: "=", value: "", then: "" }] } : c));

  const updateWhen = (cid: string, wid: string, field: keyof WhenClause, val: string) =>
    onCaseExpressionsChange(caseExpressions.map(c => c.id === cid
      ? { ...c, whenClauses: c.whenClauses.map(w => w.id === wid ? { ...w, [field]: val } : w) } : c));

  const removeWhen = (cid: string, wid: string) => onCaseExpressionsChange(caseExpressions.map(c =>
    c.id === cid ? { ...c, whenClauses: c.whenClauses.filter(w => w.id !== wid) } : c));

  if (!cols.length) return null;

  return (
    <div className="space-y-3">
      {caseExpressions.length === 0 && (
        <p className="text-xs text-muted-foreground italic flex items-center gap-1.5">
          <GitBranch className="h-3 w-3 opacity-60" /> No CASE expressions. Add one to create conditional column values.
        </p>
      )}
      {caseExpressions.map(expr => {
        const preview = buildCaseSql(expr);
        return (
          <div key={expr.id} className="rounded-2xl border border-border bg-muted/20 p-4 space-y-3">
            <div className="flex items-center gap-2 mb-1">
              <GitBranch className="h-4 w-4 text-purple-500" />
              <span className="text-sm font-semibold flex-1">{expr.alias || "CASE Expression"}</span>
              <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/70" onClick={() => removeCase(expr.id)}>
                <Trash2 className="h-3.5 w-3.5" />
              </Button>
            </div>
            {expr.whenClauses.map(w => {
              const noVal = ["IS NULL","IS NOT NULL"].includes(w.operator);
              return (
                <div key={w.id} className="flex flex-wrap gap-2 items-center">
                  <span className="text-xs font-bold text-purple-500 w-12">WHEN</span>
                  <Select value={w.column} onValueChange={v => updateWhen(expr.id, w.id, "column", v)}>
                    <SelectTrigger className="flex-1 min-w-[140px] h-9 text-xs"><SelectValue placeholder="Column" /></SelectTrigger>
                    <SelectContent>{cols.map(c => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}</SelectContent>
                  </Select>
                  <Select value={w.operator} onValueChange={v => updateWhen(expr.id, w.id, "operator", v)}>
                    <SelectTrigger className="w-[100px] h-9 text-xs"><SelectValue /></SelectTrigger>
                    <SelectContent>{OPS.map(o => <SelectItem key={o} value={o}>{o}</SelectItem>)}</SelectContent>
                  </Select>
                  {!noVal && <Input value={w.value} onChange={e => updateWhen(expr.id, w.id, "value", e.target.value)} placeholder="value" className="w-28 h-9 text-xs font-mono" />}
                  <span className="text-xs font-bold text-purple-500">THEN</span>
                  <Input value={w.then} onChange={e => updateWhen(expr.id, w.id, "then", e.target.value)} placeholder="result" className="w-28 h-9 text-xs font-mono" />
                  <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive/60" onClick={() => removeWhen(expr.id, w.id)}>
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
              );
            })}
            <Button variant="outline" size="sm" onClick={() => addWhen(expr.id)} className="text-xs gap-1.5">
              <Plus className="h-3 w-3" /> Add WHEN
            </Button>
            <div className="flex gap-3 items-center pt-2 border-t border-border/30">
              <span className="text-xs font-bold text-purple-500 w-12">ELSE</span>
              <Input value={expr.elseValue} onChange={e => updateField(expr.id, "elseValue", e.target.value)} placeholder="default (optional)" className="flex-1 h-9 text-xs font-mono" />
              <span className="text-xs font-bold text-muted-foreground">AS</span>
              <Input value={expr.alias} onChange={e => updateField(expr.id, "alias", e.target.value)} placeholder="column_name" className="w-36 h-9 text-xs font-mono" />
            </div>
            {preview && <div className="p-2 rounded-lg bg-background/80 border border-border/50"><p className="text-[10px] font-mono text-muted-foreground break-all">{preview}</p></div>}
          </div>
        );
      })}
      <Button variant="outline" size="default" onClick={addCase} className="text-sm gap-2">
        <Plus className="h-4 w-4" /> Add CASE Expression
      </Button>
    </div>
  );
};

export default CaseExpressionBuilder;
