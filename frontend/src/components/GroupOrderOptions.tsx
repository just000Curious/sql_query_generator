import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Plus, Trash2 } from "lucide-react";

interface OrderByItem {
  column: string;
  direction: "ASC" | "DESC";
}

interface GroupOrderOptionsProps {
  availableColumns: string[];
  groupBy: string[];
  onGroupByChange: (g: string[]) => void;
  orderBy: OrderByItem[];
  onOrderByChange: (o: OrderByItem[]) => void;
  limit: number;
  onLimitChange: (n: number) => void;
  offset: number;
  onOffsetChange: (n: number) => void;
  showGroupBy?: boolean;
}

const GroupOrderOptions = ({
  availableColumns, groupBy, onGroupByChange,
  orderBy, onOrderByChange,
  limit, onLimitChange, offset, onOffsetChange,
  showGroupBy = false,
}: GroupOrderOptionsProps) => {
  const toggleGroupBy = (col: string) => {
    onGroupByChange(
      groupBy.includes(col) ? groupBy.filter((g) => g !== col) : [...groupBy, col]
    );
  };

  const addOrderBy = () => {
    onOrderByChange([...orderBy, { column: "", direction: "ASC" }]);
  };

  const updateOrderBy = (i: number, field: keyof OrderByItem, value: string) => {
    const updated = [...orderBy];
    updated[i] = { ...updated[i], [field]: value };
    onOrderByChange(updated);
  };

  const removeOrderBy = (i: number) => {
    onOrderByChange(orderBy.filter((_, idx) => idx !== i));
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
      {/* GROUP BY */}
      {showGroupBy && (
        <div>
          <Label className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Group By</Label>
          <div className="flex flex-wrap gap-1.5 mt-2">
            {availableColumns.map((c) => {
              const colName = c.includes(".") ? c.split(".").pop()! : c;
              return (
                <label key={c} className="flex items-center gap-1.5 text-xs bg-muted px-2.5 py-1 rounded-md cursor-pointer hover:bg-muted/80">
                  <Checkbox checked={groupBy.includes(c)} onCheckedChange={() => toggleGroupBy(c)} />
                  {colName}
                </label>
              );
            })}
          </div>
        </div>
      )}

      {/* ORDER BY */}
      <div>
        <Label className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">Order By</Label>
        <div className="space-y-2 mt-2">
          {orderBy.map((o, i) => (
            <div key={i} className="flex gap-2 items-center">
              <Select value={o.column} onValueChange={(v) => updateOrderBy(i, "column", v)}>
                <SelectTrigger className="flex-1 h-8 text-xs">
                  <SelectValue placeholder="Column" />
                </SelectTrigger>
                <SelectContent>
                  {availableColumns.map((c) => {
                    const colName = c.includes(".") ? c.split(".").pop()! : c;
                    return <SelectItem key={c} value={c}>{colName}</SelectItem>;
                  })}
                </SelectContent>
              </Select>
              <Select value={o.direction} onValueChange={(v) => updateOrderBy(i, "direction", v)}>
                <SelectTrigger className="w-24 h-8 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="ASC">ASC ↑</SelectItem>
                  <SelectItem value="DESC">DESC ↓</SelectItem>
                </SelectContent>
              </Select>
              <Button variant="ghost" size="icon" className="h-8 w-8 text-destructive" onClick={() => removeOrderBy(i)}>
                <Trash2 className="h-3 w-3" />
              </Button>
            </div>
          ))}
          <Button variant="outline" size="sm" onClick={addOrderBy} className="text-xs gap-1.5 h-7">
            <Plus className="h-3 w-3" /> Add Sort
          </Button>
        </div>
      </div>

      {/* LIMIT & OFFSET (always in second column or below) */}
      <div className={showGroupBy ? "md:col-span-2" : ""}>
        <div className="grid grid-cols-2 gap-4">
          <div>
            <Label className="text-xs text-muted-foreground">Limit</Label>
            <Input type="number" min={0} max={10000} value={limit} onChange={(e) => onLimitChange(Number(e.target.value))} className="h-9 mt-1" />
          </div>
          <div>
            <Label className="text-xs text-muted-foreground">Offset</Label>
            <Input type="number" min={0} value={offset} onChange={(e) => onOffsetChange(Number(e.target.value))} className="h-9 mt-1" />
          </div>
        </div>
      </div>
    </div>
  );
};

export default GroupOrderOptions;
