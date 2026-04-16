import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import {
  BookOpen, Zap, Code2, Keyboard, HelpCircle, AlertTriangle,
  Database, ArrowRightLeft, BarChart3, Calendar, PenLine, Layers,
  Copy, Play, Download, RotateCcw, Search, SlidersHorizontal,
} from "lucide-react";

interface HelpModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/* ────────────────────────────── tiny helpers ────────────────────────────── */

const Kbd = ({ children }: { children: React.ReactNode }) => (
  <kbd className="inline-flex items-center px-1.5 py-0.5 bg-muted border border-border rounded text-[11px] font-mono font-semibold text-muted-foreground leading-none">
    {children}
  </kbd>
);

const Tip = ({ children }: { children: React.ReactNode }) => (
  <div className="flex items-start gap-2 p-2.5 rounded-lg bg-primary/5 border border-primary/10 text-xs text-muted-foreground mt-2">
    <span className="text-primary text-sm flex-shrink-0">💡</span>
    <span>{children}</span>
  </div>
);

/* ───────────────────────────── main component ───────────────────────────── */

const HelpModal = ({ open, onOpenChange }: HelpModalProps) => {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl max-h-[85vh] overflow-y-auto p-0">
        <DialogHeader className="px-6 pt-6 pb-2">
          <DialogTitle className="text-xl font-bold text-primary flex items-center gap-2">
            <BookOpen className="h-5 w-5" />
            SQL Query Generator — Help Center
          </DialogTitle>
          <p className="text-xs text-muted-foreground mt-1">
            Everything you need to know to build SQL queries without writing code.
          </p>
        </DialogHeader>

        <Tabs defaultValue="start" className="px-6 pb-6">
          <TabsList className="w-full flex-wrap h-auto gap-1 mb-4">
            <TabsTrigger value="start" className="text-xs gap-1"><Zap className="h-3 w-3" /> Getting Started</TabsTrigger>
            <TabsTrigger value="types" className="text-xs gap-1"><Database className="h-3 w-3" /> Query Types</TabsTrigger>
            <TabsTrigger value="examples" className="text-xs gap-1"><Code2 className="h-3 w-3" /> Examples</TabsTrigger>
            <TabsTrigger value="keys" className="text-xs gap-1"><Keyboard className="h-3 w-3" /> Shortcuts</TabsTrigger>
            <TabsTrigger value="faq" className="text-xs gap-1"><HelpCircle className="h-3 w-3" /> FAQ</TabsTrigger>
            <TabsTrigger value="trouble" className="text-xs gap-1"><AlertTriangle className="h-3 w-3" /> Troubleshooting</TabsTrigger>
          </TabsList>

          {/* ════════════════ GETTING STARTED ════════════════ */}
          <TabsContent value="start" className="space-y-4 text-sm text-foreground">
            <p className="text-muted-foreground">
              Follow these five steps to go from zero to a ready-to-run SQL query:
            </p>

            {[
              {
                step: 1,
                title: "Choose a Query Type",
                icon: <SlidersHorizontal className="h-4 w-4" />,
                desc: "Pick the kind of query you need — Simple SELECT, JOIN, Aggregate, Date Range, UNION, or Raw SQL. Each type reveals different builder sections.",
              },
              {
                step: 2,
                title: "Select Schema, Table & Columns",
                icon: <Database className="h-4 w-4" />,
                desc: "Browse the available schemas (GM, HM, PM, SI, SA, TA), pick a table, and choose the columns you want in your result. Leave columns empty to SELECT *.",
              },
              {
                step: 3,
                title: "Add Filters (WHERE)",
                icon: <Search className="h-4 w-4" />,
                desc: "Optionally narrow your results using conditions like =, !=, >, LIKE, IN, IS NULL, BETWEEN, etc. Combine multiple conditions with AND / OR.",
              },
              {
                step: 4,
                title: "Customize Output",
                icon: <SlidersHorizontal className="h-4 w-4" />,
                desc: "Set LIMIT to control the number of rows, ORDER BY to sort results, and GROUP BY for aggregate summaries. Toggle DISTINCT to remove duplicates.",
              },
              {
                step: 5,
                title: "Generate & Copy",
                icon: <Play className="h-4 w-4" />,
                desc: "Click \"Generate SQL Query\" (or press Ctrl+Enter) to produce the SQL. Copy it to your clipboard and paste it into your PostgreSQL client.",
              },
            ].map((s) => (
              <div key={s.step} className="flex gap-3 items-start">
                <span className="flex-shrink-0 w-8 h-8 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm font-bold">
                  {s.step}
                </span>
                <div>
                  <p className="font-semibold flex items-center gap-1.5">
                    {s.icon} {s.title}
                  </p>
                  <p className="text-muted-foreground text-xs mt-0.5">{s.desc}</p>
                </div>
              </div>
            ))}

            <Tip>
              No schema prefix is needed in your queries — the PostgreSQL <code className="font-mono text-primary">search_path</code> resolves the schema automatically.
            </Tip>
          </TabsContent>

          {/* ════════════════ QUERY TYPES ════════════════ */}
          <TabsContent value="types" className="space-y-4 text-sm">
            <p className="text-muted-foreground">
              The generator supports six query modes. Each unlocks different builder sections:
            </p>

            {[
              {
                icon: <Database className="h-4 w-4 text-blue-400" />,
                name: "Simple SELECT",
                badge: "select",
                desc: "Standard SELECT query from a single table. Best for quick lookups. Supports WHERE, ORDER BY, LIMIT, OFFSET, and DISTINCT.",
              },
              {
                icon: <ArrowRightLeft className="h-4 w-4 text-green-400" />,
                name: "JOIN Query",
                badge: "join",
                desc: "Combine rows from 2+ tables using INNER, LEFT, RIGHT, FULL, or CROSS JOINs. Specify the ON condition columns in the Join Builder step.",
              },
              {
                icon: <BarChart3 className="h-4 w-4 text-amber-400" />,
                name: "Aggregate Query",
                badge: "aggregate",
                desc: "Run aggregate functions — COUNT, SUM, AVG, MIN, MAX — with optional GROUP BY and HAVING clauses for analytical summaries.",
              },
              {
                icon: <Calendar className="h-4 w-4 text-purple-400" />,
                name: "Date Range",
                badge: "date_range",
                desc: "Filter records between two dates. Pick a date column, set the From and To dates, and the generator adds the correct BETWEEN or >= / <= conditions.",
              },
              {
                icon: <Layers className="h-4 w-4 text-pink-400" />,
                name: "UNION",
                badge: "union",
                desc: "Stack results from multiple SELECT queries vertically. Supports UNION (unique rows) and UNION ALL (all rows). Each sub-query has its own independent builder.",
              },
              {
                icon: <PenLine className="h-4 w-4 text-orange-400" />,
                name: "Raw SQL",
                badge: "raw",
                desc: "Write free-form PostgreSQL directly. Useful when you need CTEs, window functions, sub-queries, or anything the visual builder doesn't cover.",
              },
            ].map((qt) => (
              <div key={qt.badge} className="flex gap-3 items-start p-3 rounded-lg bg-muted/40 border border-border/50">
                <span className="flex-shrink-0 mt-0.5">{qt.icon}</span>
                <div className="min-w-0">
                  <p className="font-semibold flex items-center gap-2">
                    {qt.name}
                    <Badge variant="outline" className="text-[10px] font-mono">{qt.badge}</Badge>
                  </p>
                  <p className="text-muted-foreground text-xs mt-0.5">{qt.desc}</p>
                </div>
              </div>
            ))}
          </TabsContent>

          {/* ════════════════ EXAMPLES ════════════════ */}
          <TabsContent value="examples" className="space-y-5 text-sm">
            <p className="text-muted-foreground">
              Real-world examples using the Kokan Railway Corporation database:
            </p>

            {[
              {
                title: "List IT Employees",
                type: "Simple SELECT",
                steps: [
                  "Schema: PM → Table: pmm_employee",
                  "Columns: emp_no, emp_firstname, emp_lastname",
                  "WHERE: emp_dept_cd = 'IT'",
                  "ORDER BY: emp_firstname ASC",
                  "LIMIT: 100",
                ],
                sql: `SELECT emp_no, emp_firstname, emp_lastname\nFROM pmm_employee\nWHERE emp_dept_cd = 'IT'\nORDER BY emp_firstname ASC\nLIMIT 100`,
              },
              {
                title: "Count Complaints by Status",
                type: "Aggregate",
                steps: [
                  "Schema: GM → Table: gmtk_coms_hdr",
                  "Aggregate: COUNT(*) AS total_complaints",
                  "GROUP BY: coms_status",
                  "ORDER BY: total_complaints DESC",
                ],
                sql: `SELECT coms_status, COUNT(*) AS total_complaints\nFROM gmtk_coms_hdr\nGROUP BY coms_status\nORDER BY total_complaints DESC`,
              },
              {
                title: "Employee Complaints via JOIN",
                type: "JOIN",
                steps: [
                  "Table 1 (a): PM → pmm_employee",
                  "Table 2 (b): GM → gmtk_coms_hdr",
                  "JOIN ON: a.emp_no = b.coms_emp_no",
                  "Columns: a.emp_firstname, a.emp_lastname, b.coms_no, b.coms_status",
                ],
                sql: `SELECT a.emp_firstname, a.emp_lastname, b.coms_no, b.coms_status\nFROM pmm_employee a\nINNER JOIN gmtk_coms_hdr b\n  ON a.emp_no = b.coms_emp_no`,
              },
              {
                title: "Complaints in a Date Range",
                type: "Date Range",
                steps: [
                  "Schema: GM → Table: gmtk_coms_hdr",
                  "Date Column: coms_date",
                  "From: 2025-01-01  To: 2025-06-30",
                ],
                sql: `SELECT *\nFROM gmtk_coms_hdr\nWHERE coms_date >= '2025-01-01'\n  AND coms_date <= '2025-06-30'`,
              },
              {
                title: "UNION — Combine Two Departments",
                type: "UNION",
                steps: [
                  "Query 1: pmm_employee WHERE emp_dept_cd = 'IT'",
                  "Query 2: pmm_employee WHERE emp_dept_cd = 'HR'",
                  "Operation: UNION ALL",
                ],
                sql: `SELECT emp_no, emp_firstname, emp_dept_cd\nFROM pmm_employee\nWHERE emp_dept_cd = 'IT'\n\nUNION ALL\n\nSELECT emp_no, emp_firstname, emp_dept_cd\nFROM pmm_employee\nWHERE emp_dept_cd = 'HR'`,
              },
            ].map((ex) => (
              <div key={ex.title} className="rounded-lg border border-border overflow-hidden">
                <div className="bg-muted/50 px-4 py-2.5 flex items-center justify-between">
                  <p className="font-semibold text-sm">{ex.title}</p>
                  <Badge variant="secondary" className="text-[10px]">{ex.type}</Badge>
                </div>
                <div className="px-4 py-3 space-y-2">
                  <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Steps</p>
                  <ol className="text-xs text-muted-foreground space-y-1 list-decimal list-inside">
                    {ex.steps.map((s, i) => <li key={i}>{s}</li>)}
                  </ol>
                  <Separator className="my-2" />
                  <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Generated SQL</p>
                  <pre className="text-xs font-mono bg-muted/60 rounded-md p-3 overflow-x-auto whitespace-pre text-foreground border border-border/40 leading-5">
                    {ex.sql}
                  </pre>
                </div>
              </div>
            ))}
          </TabsContent>

          {/* ════════════════ KEYBOARD SHORTCUTS ════════════════ */}
          <TabsContent value="keys" className="space-y-4 text-sm">
            <p className="text-muted-foreground">
              Power-user shortcuts to speed up your workflow:
            </p>

            <div className="rounded-lg border border-border overflow-hidden">
              {[
                { keys: "Ctrl + Enter", action: "Generate the SQL query", icon: <Play className="h-3.5 w-3.5 text-green-400" /> },
                { keys: "Ctrl + Shift + C", action: "Copy generated SQL to clipboard", icon: <Copy className="h-3.5 w-3.5 text-blue-400" /> },
              ].map((shortcut, i) => (
                <div
                  key={shortcut.keys}
                  className={`flex items-center justify-between px-4 py-3 ${i > 0 ? "border-t border-border" : ""}`}
                >
                  <div className="flex items-center gap-2 text-foreground">
                    {shortcut.icon}
                    <span>{shortcut.action}</span>
                  </div>
                  <Kbd>{shortcut.keys}</Kbd>
                </div>
              ))}
            </div>

            <Separator />

            <p className="font-semibold">Toolbar Actions</p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
              {[
                { icon: <RotateCcw className="h-3.5 w-3.5" />, label: "Reset", desc: "Clears all fields and starts fresh" },
                { icon: <Copy className="h-3.5 w-3.5" />, label: "Copy SQL", desc: "Copies the output to your clipboard" },
                { icon: <Download className="h-3.5 w-3.5" />, label: "Download", desc: "Saves the SQL as a .sql file" },
                { icon: <Play className="h-3.5 w-3.5" />, label: "Validate", desc: "Checks SQL syntax against the in-memory DB" },
              ].map((a) => (
                <div key={a.label} className="flex items-start gap-2 p-2.5 rounded-lg bg-muted/40 border border-border/50">
                  <span className="flex-shrink-0 mt-0.5 text-primary">{a.icon}</span>
                  <div>
                    <p className="font-medium text-xs">{a.label}</p>
                    <p className="text-[11px] text-muted-foreground">{a.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </TabsContent>

          {/* ════════════════ FAQ ════════════════ */}
          <TabsContent value="faq" className="text-sm">
            <p className="text-muted-foreground mb-3">
              Frequently asked questions about the SQL Query Generator:
            </p>

            <Accordion type="multiple" className="w-full">
              {[
                {
                  q: "Do I need to know SQL to use this tool?",
                  a: "No! The visual builder lets you construct SQL queries by clicking through schemas, tables, and columns. The tool writes the SQL for you. However, knowing basic SQL concepts (SELECT, WHERE, JOIN) will help you understand what the output does.",
                },
                {
                  q: "Does this tool execute queries against a live database?",
                  a: "The tool generates SQL text for you to copy and run on your PostgreSQL server. The built-in \"Validate\" button checks syntax against an in-memory SQLite mirror — it does NOT touch your production database. Always review the SQL before executing it.",
                },
                {
                  q: "What schemas are available?",
                  a: "The database is organized into six schemas: GM (General Management — Complaints, Forwarding, DMS), HM (Healthcare Management — Medical Records, Lab Tests), PM (Personnel Management — Employee Data, Payroll, Leave), SI (Stores & Inventory — Materials, Purchase, Tenders), SA (Security & Administration — User Management, Roles), and TA (Traffic & Accounts — Ticketing, Freight, Accounting).",
                },
                {
                  q: "Why don't I see a schema prefix like GM.table_name in the generated SQL?",
                  a: "The PostgreSQL server's search_path setting resolves the correct schema automatically. You only need to use the bare table name (e.g., gmtk_coms_hdr instead of GM.gmtk_coms_hdr). Each table's prefix already indicates its schema.",
                },
                {
                  q: "Can I join tables from different schemas?",
                  a: "Yes! In JOIN mode, add tables from any schema. The builder treats each table independently. Just make sure the ON condition columns actually match (e.g., emp_no from PM and coms_emp_no from GM).",
                },
                {
                  q: "What is the UNION builder?",
                  a: "UNION combines results from two or more SELECT queries into a single result set. Use UNION to get only unique rows, or UNION ALL to include duplicates. Each sub-query in the UNION builder has its own independent table/column/condition selectors.",
                },
                {
                  q: "What does 'Wrap as Temporary Table / CTE' do?",
                  a: "After generating your SQL, you can wrap it inside a CREATE TEMP TABLE statement or a Common Table Expression (WITH ... AS). This is useful when you want to reuse the result in a subsequent query without running the same logic again.",
                },
                {
                  q: "What's the difference between LIMIT and OFFSET?",
                  a: "LIMIT controls how many rows the query returns (e.g., LIMIT 100 = first 100 rows). OFFSET skips that many rows before starting to return results (e.g., OFFSET 200 LIMIT 100 = rows 201–300). Together they enable pagination.",
                },
                {
                  q: "Can I write my own SQL instead of using the builder?",
                  a: "Absolutely. Switch to Raw SQL mode and type or paste any valid PostgreSQL query. The builder steps are hidden in this mode — you control the full SQL text.",
                },
                {
                  q: "How do I view my past queries?",
                  a: "Click the History (🕒) button in the header. It stores your recently generated / validated queries in your browser's local storage. Click any entry to reload it into the Raw SQL editor.",
                },
                {
                  q: "What operators are supported in WHERE conditions?",
                  a: "The tool supports: = (equals), != / <> (not equals), >, >=, <, <= (comparisons), LIKE / NOT LIKE (pattern matching with % and _ wildcards), IN / NOT IN (value lists), IS NULL / IS NOT NULL (null checks), and BETWEEN (range filtering).",
                },
                {
                  q: "Is my data safe?",
                  a: "This tool only generates read-only SELECT queries. It cannot INSERT, UPDATE, DELETE, or DROP anything. The API runs an in-memory SQLite database with table structures only (no real data). Your production database is never touched by this application.",
                },
              ].map((item, i) => (
                <AccordionItem key={i} value={`faq-${i}`}>
                  <AccordionTrigger className="text-sm text-left font-medium hover:no-underline">
                    {item.q}
                  </AccordionTrigger>
                  <AccordionContent className="text-xs text-muted-foreground leading-relaxed">
                    {item.a}
                  </AccordionContent>
                </AccordionItem>
              ))}
            </Accordion>
          </TabsContent>

          {/* ════════════════ TROUBLESHOOTING ════════════════ */}
          <TabsContent value="trouble" className="space-y-4 text-sm">
            <p className="text-muted-foreground">
              Common issues and how to resolve them:
            </p>

            {[
              {
                icon: "🔴",
                title: "\"API Offline\" banner appears",
                solution: (
                  <>
                    The backend FastAPI server isn't running.
                    Open a terminal in the project root and run:
                    <pre className="mt-1.5 bg-muted/60 p-2 rounded-md font-mono text-xs border border-border/40">python api.py</pre>
                    The server will start at <code className="font-mono text-primary">http://localhost:8000</code>. The status pill in the header should turn green within 15 seconds.
                  </>
                ),
              },
              {
                icon: "⚠️",
                title: "\"No tables found\" or empty schema list",
                solution: (
                  <>
                    The <code className="font-mono text-primary">db_files/metadata.json</code> file may be missing or malformed.
                    Make sure the file exists and contains valid JSON with the schema structure.
                    Restart the backend after fixing the file.
                  </>
                ),
              },
              {
                icon: "🟡",
                title: "Generated SQL works in the tool but fails in pgAdmin / DBeaver",
                solution: (
                  <>
                    Double-check that your PostgreSQL <code className="font-mono text-primary">search_path</code> includes the target schema.
                    You can set it at the start of your session:
                    <pre className="mt-1.5 bg-muted/60 p-2 rounded-md font-mono text-xs border border-border/40">SET search_path TO gm, hm, pm, si, sa, ta, public;</pre>
                  </>
                ),
              },
              {
                icon: "🔗",
                title: "JOIN produces a CROSS JOIN warning",
                solution: (
                  <>
                    You've selected multiple tables but haven't defined an ON condition in the Join Builder step.
                    Go to <strong>Step 3 — Join Tables</strong> and add at least one condition linking the tables on a shared column (e.g., <code className="font-mono text-primary">a.emp_no = b.coms_emp_no</code>).
                  </>
                ),
              },
              {
                icon: "❌",
                title: "Validation error: column does not exist",
                solution: (
                  <>
                    A selected column doesn't match the metadata for that table. This usually happens when you switch tables but the old column selection remains.
                    Click <strong>Reset</strong> in the header or re-select your columns from the updated column list.
                  </>
                ),
              },
              {
                icon: "🖥️",
                title: "Frontend won't start (npm run dev fails)",
                solution: (
                  <>
                    Make sure you've installed dependencies:
                    <pre className="mt-1.5 bg-muted/60 p-2 rounded-md font-mono text-xs border border-border/40">cd frontend{"\n"}npm install{"\n"}npm run dev</pre>
                    The dev server starts at <code className="font-mono text-primary">http://localhost:5173</code> by default.
                  </>
                ),
              },
            ].map((issue) => (
              <div key={issue.title} className="rounded-lg border border-border overflow-hidden">
                <div className="bg-muted/50 px-4 py-2.5 flex items-center gap-2">
                  <span>{issue.icon}</span>
                  <p className="font-semibold text-sm">{issue.title}</p>
                </div>
                <div className="px-4 py-3 text-xs text-muted-foreground leading-relaxed">
                  {issue.solution}
                </div>
              </div>
            ))}

            <Tip>
              Still stuck? Check the browser console (<Kbd>F12</Kbd>) and the backend terminal for error messages.
            </Tip>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
};

export default HelpModal;
