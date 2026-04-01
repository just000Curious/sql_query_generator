import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface Condition {
  id: string;
  column: string;
  operator: string;
  value: string;
  logic: "AND" | "OR";
}

const OPERATORS = ["=", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "BETWEEN", "IS NULL", "IS NOT NULL"];

interface ConditionBuilderProps {
  tables: SelectedTable[];
  conditions: Condition[];
  onConditionsChange: (c: Condition[]) => void;
}

const ConditionBuilder = ({ tables, conditions, onConditionsChange }: ConditionBuilderProps) => {
  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({ key: `${t.alias}.${c.name}`, label: `${c.name}`, table: t.alias }))
  );

  const add = () => {
    onConditionsChange([
      ...conditions,
      { id: crypto.randomUUID(), column: "", operator: "=", value: "", logic: "AND" },
    ]);
  };

  const update = (id: string, field: keyof Condition, value: string) => {
    onConditionsChange(conditions.map((c) => (c.id === id ? { ...c, [field]: value } : c)));
  };

  const remove = (id: string) => {
    onConditionsChange(conditions.filter((c) => c.id !== id));
  };

  const noValue = (op: string) => op === "IS NULL" || op === "IS NOT NULL";

  return (
    <div className="space-y-2">
      {conditions.map((cond, i) => (
        <div key={cond.id} className="flex flex-wrap gap-2 items-center">
          {i > 0 && (
            <Select value={cond.logic} onValueChange={(v) => update(cond.id, "logic", v)}>
              <SelectTrigger className="w-20 h-9 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="AND">AND</SelectItem>
                <SelectItem value="OR">OR</SelectItem>
              </SelectContent>
            </Select>
          )}
          <Select value={cond.column} onValueChange={(v) => update(cond.id, "column", v)}>
            <SelectTrigger className="flex-1 min-w-[140px] h-9 text-xs">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {allColumns.map((c) => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={cond.operator} onValueChange={(v) => update(cond.id, "operator", v)}>
            <SelectTrigger className="w-28 h-9 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {OPERATORS.map((op) => <SelectItem key={op} value={op}>{op}</SelectItem>)}
            </SelectContent>
          </Select>
          {!noValue(cond.operator) && (
            <Input
              value={cond.value}
              onChange={(e) => update(cond.id, "value", e.target.value)}
              placeholder="Value"
              className="flex-1 min-w-[120px] h-9 text-xs"
            />
          )}
          <Button variant="ghost" size="icon" className="h-9 w-9 text-destructive flex-shrink-0" onClick={() => remove(cond.id)}>
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
      <Button variant="outline" size="sm" onClick={add} className="text-xs gap-1.5">
        <Plus className="h-3.5 w-3.5" /> Add Condition
      </Button>
    </div>
  );
};

export default ConditionBuilder;
