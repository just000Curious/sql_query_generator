import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { BookOpen, Zap, Code2, Keyboard, HelpCircle, AlertTriangle } from "lucide-react";

interface HelpModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const Kbd = ({ k }: { k: string }) => (
  <kbd className="inline-flex items-center px-1.5 py-0.5 bg-muted border border-border rounded text-[11px] font-mono font-semibold text-muted-foreground">{k}</kbd>
);

const Step = ({ n, title, desc }: { n: number; title: string; desc: string }) => (
  <div className="flex gap-3 items-start">
    <span className="flex-shrink-0 w-7 h-7 rounded-full flex items-center justify-center text-sm font-bold text-white" style={{ background: "hsl(24 89% 53%)" }}>{n}</span>
    <div>
      <p className="font-semibold text-sm">{title}</p>
      <p className="text-xs text-muted-foreground mt-0.5">{desc}</p>
    </div>
  </div>
);

const Tag = ({ children }: { children: React.ReactNode }) => (
  <code className="px-1.5 py-0.5 rounded bg-muted text-xs font-mono">{children}</code>
);

const Row = ({ op, eg, desc }: { op: string; eg: string; desc: string }) => (
  <tr className="border-b border-border/40 last:border-0">
    <td className="py-1.5 pr-3 font-mono text-xs font-bold" style={{ color: "hsl(24 89% 55%)" }}>{op}</td>
    <td className="py-1.5 pr-3"><Tag>{eg}</Tag></td>
    <td className="py-1.5 text-xs text-muted-foreground">{desc}</td>
  </tr>
);

const HelpModal = ({ open, onOpenChange }: HelpModalProps) => (
  <Dialog open={open} onOpenChange={onOpenChange}>
    <DialogContent className="max-w-3xl max-h-[88vh] overflow-y-auto p-0">
      <DialogHeader className="px-6 pt-5 pb-3" style={{ borderBottom: "1px solid hsl(24 89% 53% / 0.2)" }}>
        <DialogTitle className="text-lg font-bold flex items-center gap-2" style={{ color: "hsl(24 89% 55%)" }}>
          <BookOpen className="h-5 w-5" /> SQL Query Generator — Help Center
        </DialogTitle>
        <p className="text-xs text-muted-foreground">Konkan Railway Corporation · Build SQL queries visually, no coding needed.</p>
      </DialogHeader>

      <Tabs defaultValue="start" className="px-6 pb-6">
        <TabsList className="w-full flex-wrap h-auto gap-1 my-4">
          <TabsTrigger value="start" className="text-xs gap-1"><Zap className="h-3 w-3" />Getting Started</TabsTrigger>
          <TabsTrigger value="types" className="text-xs gap-1"><Code2 className="h-3 w-3" />Query Types</TabsTrigger>
          <TabsTrigger value="ops" className="text-xs gap-1"><HelpCircle className="h-3 w-3" />Operators</TabsTrigger>
          <TabsTrigger value="keys" className="text-xs gap-1"><Keyboard className="h-3 w-3" />Shortcuts</TabsTrigger>
          <TabsTrigger value="faq" className="text-xs gap-1"><AlertTriangle className="h-3 w-3" />FAQ & Fixes</TabsTrigger>
        </TabsList>

        {/* ── GETTING STARTED ── */}
        <TabsContent value="start" className="space-y-5 mt-0">
          <div className="rounded-xl p-4 border space-y-4" style={{ background: "hsl(24 89% 53% / 0.05)", borderColor: "hsl(24 89% 53% / 0.2)" }}>
            <p className="text-sm font-semibold" style={{ color: "hsl(24 89% 55%)" }}>Follow these 5 steps to build your first query:</p>
            <div className="space-y-3">
              <Step n={1} title="Choose a Query Type" desc="Pick Simple SELECT for basic queries, JOIN to combine tables, Aggregate for COUNT/SUM/AVG, Date Range for time filters, or Raw SQL to write your own." />
              <Step n={2} title="Select Schema → Table → Columns" desc="Choose a schema (GM, HM, PM…), pick a table from the grouped dropdown, then tick the columns you want. Leave all unticked to SELECT *." />
              <Step n={3} title="Add WHERE Filters (optional)" desc="Click 'Add WHERE Condition', choose a column, pick an operator (=, LIKE, IN, BETWEEN, IS NULL, EXISTS…) and enter a value. Combine with AND / OR." />
              <Step n={4} title="Sort, Group & Limit" desc="Add ORDER BY columns, set GROUP BY for aggregates, enter LIMIT (default 100) and OFFSET for pagination." />
              <Step n={5} title="Generate → Copy or Run" desc="Click 'Generate SQL Query' (or Ctrl+Enter). Then Copy SQL to paste into your client, or click 'Run & Preview Results' to execute inline." />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 text-xs">
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">💡 Schema prefix tip</p>
              <p className="text-muted-foreground">No schema prefix is needed in your queries — the PostgreSQL <Tag>search_path</Tag> resolves it automatically.</p>
            </div>
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">🔑 Primary Key indicator</p>
              <p className="text-muted-foreground">Columns marked <Tag>PK</Tag> are primary keys. <Tag>FK</Tag> are foreign keys. Always include PK in JOIN ON conditions.</p>
            </div>
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">📋 DISTINCT toggle</p>
              <p className="text-muted-foreground">Enable DISTINCT in the column selector to remove duplicate rows from results.</p>
            </div>
            <div className="rounded-lg p-3 border border-border/60 bg-muted/30">
              <p className="font-semibold mb-1">🗂️ Wrap as CTE / Temp Table</p>
              <p className="text-muted-foreground">After generating, use the Wrap card to convert your query into a <Tag>WITH cte AS (…)</Tag> or <Tag>CREATE TEMP TABLE</Tag> block.</p>
            </div>
          </div>
        </TabsContent>

        {/* ── QUERY TYPES ── */}
        <TabsContent value="types" className="space-y-4 mt-0">
          {[
            { icon: "📋", name: "Simple SELECT", desc: "Fetch rows from a single table. Choose columns, add WHERE filters, sort and limit. Best for straightforward lookups.", eg: "SELECT emp_no, emp_name FROM pmm_employee WHERE dept = 'TE' LIMIT 50" },
            { icon: "🔗", name: "JOIN", desc: "Combine two or more tables using INNER, LEFT, RIGHT, or FULL joins. Select both tables, then define the ON condition (usually matching primary ↔ foreign keys).", eg: "SELECT e.emp_no, s.salary FROM pmm_employee e INNER JOIN pmm_salary s ON e.emp_no = s.emp_no" },
            { icon: "📊", name: "Aggregate", desc: "Summarise data with COUNT, SUM, AVG, MIN, MAX. Choose the aggregate function and the column, add GROUP BY to segment, and HAVING to filter groups.", eg: "SELECT dept, COUNT(*) AS total FROM pmm_employee GROUP BY dept HAVING COUNT(*) > 10" },
            { icon: "📅", name: "Date Range", desc: "Filter a date/timestamp column between two dates. Pick the date column, set From and To dates — the tool generates >= and <= conditions automatically.", eg: "SELECT * FROM gmtk_fwd_hdr WHERE time_stamp >= '2024-01-01' AND time_stamp <= '2024-12-31'" },
            { icon: "✍️", name: "Raw SQL", desc: "Write your own PostgreSQL SELECT statement directly. Useful for complex subqueries or when you already know the SQL. The system validates and highlights syntax.", eg: "SELECT * FROM (SELECT emp_no, RANK() OVER (ORDER BY salary DESC) AS rnk FROM pmm_salary) t WHERE rnk <= 10" },
          ].map(({ icon, name, desc, eg }) => (
            <div key={name} className="rounded-xl border border-border/60 overflow-hidden">
              <div className="flex items-center gap-2 px-4 py-2.5 font-semibold text-sm" style={{ background: "hsl(24 89% 53% / 0.08)" }}>
                <span>{icon}</span><span>{name}</span>
              </div>
              <div className="px-4 py-3 space-y-2">
                <p className="text-xs text-muted-foreground">{desc}</p>
                <pre className="text-[11px] font-mono p-2 rounded bg-muted/60 whitespace-pre-wrap break-all">{eg}</pre>
              </div>
            </div>
          ))}
        </TabsContent>

        {/* ── OPERATORS ── */}
        <TabsContent value="ops" className="mt-0 space-y-4">
          <p className="text-xs text-muted-foreground">All operators available in the WHERE condition builder:</p>
          <table className="w-full text-left">
            <thead>
              <tr className="text-[10px] uppercase tracking-wider text-muted-foreground border-b border-border">
                <th className="pb-2 pr-3">Operator</th>
                <th className="pb-2 pr-3">Example value</th>
                <th className="pb-2">When to use</th>
              </tr>
            </thead>
            <tbody>
              <Row op="= / !=" eg="'TE'" desc="Exact match or exclusion. Works on text, numbers, dates." />
              <Row op="> / >= / < / <=" eg="50000" desc="Numeric or date comparisons." />
              <Row op="LIKE" eg="%Singh%" desc="Case-sensitive pattern. % = any chars, _ = one char." />
              <Row op="ILIKE" eg="%singh%" desc="Case-insensitive pattern (PostgreSQL only). Use instead of LIKE when case doesn't matter." />
              <Row op="NOT LIKE / NOT ILIKE" eg="%temp%" desc="Rows where the pattern does NOT match." />
              <Row op="IN" eg="'TE','CE','ME'" desc="Match any of a comma-separated list of values." />
              <Row op="NOT IN" eg="'X','Y'" desc="Exclude any of the listed values." />
              <Row op="BETWEEN" eg="10000 AND 50000" desc="Inclusive range. Enter as: start AND end." />
              <Row op="IS NULL" eg="—" desc="Column has no value (empty). No value needed." />
              <Row op="IS NOT NULL" eg="—" desc="Column has any value. No value needed." />
              <Row op="EXISTS" eg="SELECT 1 FROM …" desc="True if the subquery returns any row. Enter the inner SELECT in the expanded text area." />
              <Row op="NOT EXISTS" eg="SELECT 1 FROM …" desc="True if the subquery returns NO rows." />
            </tbody>
          </table>
          <div className="rounded-lg p-3 border text-xs space-y-1" style={{ borderColor: "hsl(24 89% 53% / 0.25)", background: "hsl(24 89% 53% / 0.05)" }}>
            <p className="font-semibold" style={{ color: "hsl(24 89% 55%)" }}>Grouping conditions with ( )</p>
            <p className="text-muted-foreground">Each condition row has small <code className="bg-muted px-1 rounded">( )</code> buttons on the right. Click <strong>(</strong> to open a parenthesis before that condition and <strong>)</strong> to close after it. This lets you build logic like <code className="bg-muted px-1 rounded">(A OR B) AND C</code>.</p>
          </div>
        </TabsContent>

        {/* ── SHORTCUTS ── */}
        <TabsContent value="keys" className="mt-0 space-y-3">
          <p className="text-xs text-muted-foreground mb-3">Keyboard shortcuts to speed up your workflow:</p>
          {[
            { keys: ["Ctrl", "Enter"], action: "Generate SQL Query — the main action, works from anywhere on the page." },
            { keys: ["Ctrl", "Shift", "C"], action: "Copy the generated SQL to clipboard without clicking the Copy button." },
            { keys: ["Ctrl", "Shift", "R"], action: "(Browser) Hard refresh — clears cached files and reloads the latest build." },
          ].map(({ keys, action }) => (
            <div key={action} className="flex items-start gap-4 p-3 rounded-lg border border-border/60 bg-muted/20">
              <div className="flex items-center gap-1 flex-shrink-0">
                {keys.map((k, i) => (
                  <span key={k} className="flex items-center gap-1">
                    <Kbd k={k} />
                    {i < keys.length - 1 && <span className="text-muted-foreground text-xs">+</span>}
                  </span>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">{action}</p>
            </div>
          ))}
          <div className="mt-4 rounded-xl p-4 border border-border/60 bg-muted/20 text-xs space-y-2">
            <p className="font-semibold">Mouse tips</p>
            <ul className="space-y-1 text-muted-foreground list-disc list-inside">
              <li>Click a column chip to select/deselect it. Click <strong>All</strong> to select all columns.</li>
              <li>Click <strong>Keys Only</strong> to quickly select only primary and foreign key columns.</li>
              <li>Click the history icon (top-right) to see past queries and reload them.</li>
              <li>Click the moon/sun icon to toggle dark/light mode.</li>
              <li>Click <strong>Reset</strong> to clear all fields and start fresh.</li>
            </ul>
          </div>
        </TabsContent>

        {/* ── FAQ & FIXES ── */}
        <TabsContent value="faq" className="mt-0 space-y-3">
          {[
            {
              q: "The Generate button is greyed out — why?",
              a: "There is a validation error shown in red below the builder. Common causes: no table selected, a WHERE condition is missing a value, or a JOIN condition has no ON column. Fix the highlighted error and the button will activate."
            },
            {
              q: "My query runs but returns 0 rows.",
              a: "Check your WHERE values — text values must match exactly (case-sensitive by default). Try removing conditions one at a time, or switch LIKE to ILIKE for case-insensitive matching. Also verify the LIMIT isn't 0."
            },
            {
              q: "I see a 'CROSS JOIN' warning in the SQL.",
              a: "This appears when you have multiple tables in JOIN mode but no ON condition defined. Go to the JOIN section, make sure both left and right columns are selected for every join."
            },
            {
              q: "'API Offline' banner is showing.",
              a: "The Python backend is not running. Open a terminal, navigate to the project folder, and run: python api.py. The banner will disappear within 15 seconds once the API is online."
            },
            {
              q: "How do I do a subquery with EXISTS?",
              a: "In the WHERE section, click 'Add WHERE Condition', set the operator to 'EXISTS (subquery)', then click 'Enter subquery'. A text area appears — type the inner SELECT statement there (e.g. SELECT 1 FROM orders o WHERE o.emp_no = e.emp_no)."
            },
            {
              q: "Can I save my query to a file?",
              a: "Yes — click 'Save .sql' button below the generated query. It downloads a timestamped .sql file you can open in pgAdmin, DBeaver, or any text editor."
            },
            {
              q: "How do I reuse a query I built earlier?",
              a: "Click the History icon in the top-right corner. All previously generated queries are listed. Click any one to load it back into the Raw SQL editor."
            },
            {
              q: "Window functions / CASE expressions are not visible.",
              a: "These are in the Advanced sections that appear when you scroll down past the main builder. Look for 'Window Functions', 'CASE Expressions', and 'Custom Functions' cards."
            },
          ].map(({ q, a }) => (
            <div key={q} className="rounded-xl border border-border/60 overflow-hidden">
              <div className="px-4 py-2.5 text-sm font-semibold" style={{ background: "hsl(24 89% 53% / 0.07)", color: "hsl(24 89% 55%)" }}>
                ❓ {q}
              </div>
              <div className="px-4 py-3 text-xs text-muted-foreground leading-relaxed">{a}</div>
            </div>
          ))}
        </TabsContent>
      </Tabs>
    </DialogContent>
  </Dialog>
);

export default HelpModal;
