import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface AggregateConfig {
  id: string;
  func: string;
  column: string;
  alias: string;
}

const AGG_FUNCS = ["COUNT", "SUM", "AVG", "MIN", "MAX"];

interface AggregateBuilderProps {
  tables: SelectedTable[];
  aggregates: AggregateConfig[];
  onAggregatesChange: (a: AggregateConfig[]) => void;
}

const AggregateBuilder = ({ tables, aggregates, onAggregatesChange }: AggregateBuilderProps) => {
  const allColumns = ["*", ...tables.flatMap((t) => t.columns.map((c) => c.name))];

  const add = () => {
    onAggregatesChange([
      ...aggregates,
      { id: crypto.randomUUID(), func: "COUNT", column: "*", alias: "" },
    ]);
  };

  const update = (id: string, field: keyof AggregateConfig, value: string) => {
    onAggregatesChange(aggregates.map((a) => (a.id === id ? { ...a, [field]: value } : a)));
  };

  const remove = (id: string) => {
    onAggregatesChange(aggregates.filter((a) => a.id !== id));
  };

  return (
    <div className="space-y-2">
      {aggregates.map((agg) => (
        <div key={agg.id} className="flex flex-wrap gap-2 items-center">
          <Select value={agg.func} onValueChange={(v) => update(agg.id, "func", v)}>
            <SelectTrigger className="w-28 h-9 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {AGG_FUNCS.map((f) => <SelectItem key={f} value={f}>{f}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={agg.column} onValueChange={(v) => update(agg.id, "column", v)}>
            <SelectTrigger className="flex-1 min-w-[140px] h-9 text-xs">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {allColumns.map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
            </SelectContent>
          </Select>
          <Input
            value={agg.alias}
            onChange={(e) => update(agg.id, "alias", e.target.value)}
            placeholder="Alias (optional)"
            className="flex-1 min-w-[120px] h-9 text-xs"
          />
          <Button variant="ghost" size="icon" className="h-9 w-9 text-destructive" onClick={() => remove(agg.id)}>
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
      <Button variant="outline" size="sm" onClick={add} className="text-xs gap-1.5">
        <Plus className="h-3.5 w-3.5" /> Add Aggregate
      </Button>
    </div>
  );
};

export default AggregateBuilder;
