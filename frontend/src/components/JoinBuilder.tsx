import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Plus, Trash2 } from "lucide-react";
import type { SelectedTable } from "@/components/TableSelector";

export interface JoinConfig {
  id: string;
  fromTable: string;
  fromColumn: string;
  joinType: string;
  toTable: string;
  toColumn: string;
}

const JOIN_TYPES = ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"];

interface JoinBuilderProps {
  tables: SelectedTable[];
  joins: JoinConfig[];
  onJoinsChange: (j: JoinConfig[]) => void;
}

const JoinBuilder = ({ tables, joins, onJoinsChange }: JoinBuilderProps) => {
  const add = () => {
    onJoinsChange([
      ...joins,
      { id: crypto.randomUUID(), fromTable: "", fromColumn: "", joinType: "INNER JOIN", toTable: "", toColumn: "" },
    ]);
  };

  const update = (id: string, field: keyof JoinConfig, value: string) => {
    onJoinsChange(joins.map((j) => (j.id === id ? { ...j, [field]: value } : j)));
  };

  const remove = (id: string) => {
    onJoinsChange(joins.filter((j) => j.id !== id));
  };

  const getColumnsFor = (tableAlias: string) => {
    const t = tables.find((t) => t.alias === tableAlias);
    return t?.columns.map((c) => c.name) || [];
  };

  if (tables.length < 2) {
    return <p className="text-sm text-muted-foreground">Add at least 2 tables above to configure joins.</p>;
  }

  return (
    <div className="space-y-3">
      {joins.map((join) => (
        <div key={join.id} className="flex flex-wrap gap-2 items-center p-3 bg-muted/50 rounded-lg border border-border">
          <Select value={join.fromTable} onValueChange={(v) => update(join.id, "fromTable", v)}>
            <SelectTrigger className="flex-1 min-w-[120px] h-9 text-xs">
              <SelectValue placeholder="From Table" />
            </SelectTrigger>
            <SelectContent>
              {tables.map((t) => <SelectItem key={t.alias} value={t.alias}>{t.table}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={join.fromColumn} onValueChange={(v) => update(join.id, "fromColumn", v)}>
            <SelectTrigger className="flex-1 min-w-[120px] h-9 text-xs">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {getColumnsFor(join.fromTable).map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={join.joinType} onValueChange={(v) => update(join.id, "joinType", v)}>
            <SelectTrigger className="w-[140px] h-9 text-xs font-medium">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {JOIN_TYPES.map((jt) => <SelectItem key={jt} value={jt}>{jt}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={join.toTable} onValueChange={(v) => update(join.id, "toTable", v)}>
            <SelectTrigger className="flex-1 min-w-[120px] h-9 text-xs">
              <SelectValue placeholder="To Table" />
            </SelectTrigger>
            <SelectContent>
              {tables.map((t) => <SelectItem key={t.alias} value={t.alias}>{t.table}</SelectItem>)}
            </SelectContent>
          </Select>
          <Select value={join.toColumn} onValueChange={(v) => update(join.id, "toColumn", v)}>
            <SelectTrigger className="flex-1 min-w-[120px] h-9 text-xs">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {getColumnsFor(join.toTable).map((c) => <SelectItem key={c} value={c}>{c}</SelectItem>)}
            </SelectContent>
          </Select>
          <Button variant="ghost" size="icon" className="h-9 w-9 text-destructive" onClick={() => remove(join.id)}>
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      ))}
      <Button variant="outline" size="sm" onClick={add} className="text-xs gap-1.5">
        <Plus className="h-3.5 w-3.5" /> Add Join Condition
      </Button>
    </div>
  );
};

export default JoinBuilder;
