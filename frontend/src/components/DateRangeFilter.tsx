import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { SelectedTable } from "@/components/TableSelector";

interface DateRangeFilterProps {
  tables: SelectedTable[];
  dateColumn: string;
  onDateColumnChange: (col: string) => void;
  dateFrom: string;
  onDateFromChange: (v: string) => void;
  dateTo: string;
  onDateToChange: (v: string) => void;
}

const DateRangeFilter = ({
  tables, dateColumn, onDateColumnChange,
  dateFrom, onDateFromChange, dateTo, onDateToChange,
}: DateRangeFilterProps) => {
  const dateColumns = tables.flatMap((t) =>
    t.columns
      .filter((c) => /date|time|timestamp/i.test(c.type))
      .map((c) => ({ key: `${t.alias}.${c.name}`, label: c.name }))
  );

  const allColumns = tables.flatMap((t) =>
    t.columns.map((c) => ({ key: `${t.alias}.${c.name}`, label: c.name }))
  );

  const columnsToShow = dateColumns.length > 0 ? dateColumns : allColumns;

  const setPreset = (days: number) => {
    const to = new Date();
    const from = new Date();
    from.setDate(from.getDate() - days);
    onDateFromChange(from.toISOString().split("T")[0]);
    onDateToChange(to.toISOString().split("T")[0]);
  };

  const setMonth = () => {
    const now = new Date();
    onDateFromChange(new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split("T")[0]);
    onDateToChange(now.toISOString().split("T")[0]);
  };

  const setYear = (offset = 0) => {
    const y = new Date().getFullYear() + offset;
    onDateFromChange(`${y}-01-01`);
    onDateToChange(offset === 0 ? new Date().toISOString().split("T")[0] : `${y}-12-31`);
  };

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <div>
          <Label className="text-xs text-muted-foreground">Date Column</Label>
          <Select value={dateColumn} onValueChange={onDateColumnChange}>
            <SelectTrigger className="mt-1 h-9 text-xs">
              <SelectValue placeholder="Select column" />
            </SelectTrigger>
            <SelectContent>
              {columnsToShow.map((c) => <SelectItem key={c.key} value={c.key}>{c.label}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
        <div>
          <Label className="text-xs text-muted-foreground">From</Label>
          <Input type="date" value={dateFrom} onChange={(e) => onDateFromChange(e.target.value)} className="mt-1 h-9 text-xs" />
        </div>
        <div>
          <Label className="text-xs text-muted-foreground">To</Label>
          <Input type="date" value={dateTo} onChange={(e) => onDateToChange(e.target.value)} className="mt-1 h-9 text-xs" />
        </div>
      </div>
      <div className="flex flex-wrap gap-2">
        <Button variant="outline" size="sm" onClick={() => setPreset(7)} className="text-xs h-7">Last 7 Days</Button>
        <Button variant="outline" size="sm" onClick={() => setPreset(30)} className="text-xs h-7">Last 30 Days</Button>
        <Button variant="outline" size="sm" onClick={setMonth} className="text-xs h-7">This Month</Button>
        <Button variant="outline" size="sm" onClick={() => setYear(0)} className="text-xs h-7">This Year</Button>
        <Button variant="outline" size="sm" onClick={() => setYear(-1)} className="text-xs h-7">Last Year</Button>
      </div>
    </div>
  );
};

export default DateRangeFilter;
