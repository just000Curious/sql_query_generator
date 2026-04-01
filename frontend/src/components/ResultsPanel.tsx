import { useState, useMemo } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Download, Search, Clock, Rows3, Columns3, ChevronLeft, ChevronRight, Copy } from "lucide-react";
import { toast } from "sonner";

interface ResultsPanelProps {
  data: Record<string, unknown>[];
  columns: string[];
  rowCount: number;
  executionTime: number;
  hasResults: boolean;
}

const PAGE_SIZES = [10, 25, 50, 100];

const ResultsPanel = ({ data, columns, rowCount, executionTime, hasResults }: ResultsPanelProps) => {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const [sortCol, setSortCol] = useState("");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const filtered = useMemo(() => {
    if (!search) return data;
    const lower = search.toLowerCase();
    return data.filter((row) =>
      Object.values(row).some((v) => String(v).toLowerCase().includes(lower))
    );
  }, [data, search]);

  const sorted = useMemo(() => {
    if (!sortCol) return filtered;
    return [...filtered].sort((a, b) => {
      const va = String(a[sortCol] ?? "");
      const vb = String(b[sortCol] ?? "");
      const cmp = va.localeCompare(vb, undefined, { numeric: true });
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [filtered, sortCol, sortDir]);

  const paginated = sorted.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages = Math.ceil(sorted.length / pageSize);

  const handleSort = (col: string) => {
    if (sortCol === col) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortCol(col); setSortDir("asc"); }
  };

  const exportCsv = () => {
    if (data.length === 0) return;
    const header = columns.join(",");
    const rows = data.map((row) =>
      columns.map((c) => {
        const v = String(row[c] ?? "");
        return v.includes(",") || v.includes('"') ? `"${v.replace(/"/g, '""')}"` : v;
      }).join(",")
    );
    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `results_${Date.now()}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    toast.success("CSV exported");
  };

  const copyResults = () => {
    const text = [columns.join("\t"), ...data.map((row) => columns.map((c) => String(row[c] ?? "")).join("\t"))].join("\n");
    navigator.clipboard.writeText(text);
    toast.success("Results copied");
  };

  if (!hasResults) return null;

  return (
    <div className="space-y-4">
      {/* Metrics bar */}
      <div className="flex flex-wrap items-center gap-4 text-sm">
        <span className="inline-flex items-center gap-1.5 text-success font-medium">
          ✅ Query executed successfully
        </span>
        <span className="inline-flex items-center gap-1 text-muted-foreground">
          <Rows3 className="h-3.5 w-3.5" /> {rowCount} rows
        </span>
        <span className="inline-flex items-center gap-1 text-muted-foreground">
          <Clock className="h-3.5 w-3.5" /> {executionTime.toFixed(3)}s
        </span>
        <span className="inline-flex items-center gap-1 text-muted-foreground">
          <Columns3 className="h-3.5 w-3.5" /> {columns.length} columns
        </span>
        <div className="ml-auto flex gap-2">
          <Button variant="outline" size="sm" onClick={exportCsv} className="text-xs gap-1.5 h-8">
            <Download className="h-3.5 w-3.5" /> Download CSV
          </Button>
          <Button variant="outline" size="sm" onClick={copyResults} className="text-xs gap-1.5 h-8">
            <Copy className="h-3.5 w-3.5" /> Copy Results
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative max-w-sm">
        <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
        <Input
          value={search}
          onChange={(e) => { setSearch(e.target.value); setPage(0); }}
          placeholder="Search results..."
          className="pl-8 h-9 text-sm"
        />
      </div>

      {/* Table */}
      <div className="overflow-auto border rounded-lg max-h-[500px]">
        <Table>
          <TableHeader>
            <TableRow className="bg-muted/50">
              {columns.map((col) => (
                <TableHead
                  key={col}
                  onClick={() => handleSort(col)}
                  className="cursor-pointer hover:bg-muted text-xs font-semibold whitespace-nowrap"
                >
                  {col} {sortCol === col && (sortDir === "asc" ? "↑" : "↓")}
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {paginated.map((row, i) => (
              <TableRow key={i}>
                {columns.map((col) => (
                  <TableCell key={col} className="text-xs whitespace-nowrap max-w-[250px] truncate">
                    {String(row[col] ?? "")}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <div className="flex items-center gap-2">
          <span>Rows per page:</span>
          <Select value={String(pageSize)} onValueChange={(v) => { setPageSize(Number(v)); setPage(0); }}>
            <SelectTrigger className="h-7 w-16 text-xs"><SelectValue /></SelectTrigger>
            <SelectContent>
              {PAGE_SIZES.map((s) => <SelectItem key={s} value={String(s)}>{s}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center gap-1">
          <span>{sorted.length > 0 ? `${page * pageSize + 1}-${Math.min((page + 1) * pageSize, sorted.length)} of ${sorted.length}` : "0 results"}</span>
          <Button variant="ghost" size="icon" className="h-7 w-7" disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
            <ChevronLeft className="h-3.5 w-3.5" />
          </Button>
          <Button variant="ghost" size="icon" className="h-7 w-7" disabled={page >= totalPages - 1} onClick={() => setPage((p) => p + 1)}>
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
    </div>
  );
};

export default ResultsPanel;
