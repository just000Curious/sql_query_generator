import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Separator } from "@/components/ui/separator";

interface HelpModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const HelpModal = ({ open, onOpenChange }: HelpModalProps) => {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="text-xl font-bold text-primary">
            How to Use the SQL Query Generator
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-5 text-sm text-foreground">
          {[
            { step: 1, title: "Select Query Type", desc: "Choose from Simple SELECT, JOIN, Aggregate, UNION, Date Range, or Raw SQL." },
            { step: 2, title: "Choose Your Data", desc: "Select a schema (GM, HM, PM, etc.), then pick a table and the columns you want." },
            { step: 3, title: "Filter Your Data (Optional)", desc: "Add WHERE conditions with operators like =, >, LIKE. Combine multiple conditions." },
            { step: 4, title: "Customize Output", desc: "Set LIMIT to control rows, ORDER BY to sort, GROUP BY for summaries." },
            { step: 5, title: "Generate & Execute", desc: 'Click "Generate SQL Query" to see the SQL, then "Execute" to run it. Download results as CSV.' },
          ].map((s) => (
            <div key={s.step} className="flex gap-3">
              <span className="flex-shrink-0 w-8 h-8 rounded-full bg-primary text-primary-foreground flex items-center justify-center text-sm font-bold">
                {s.step}
              </span>
              <div>
                <p className="font-semibold">{s.title}</p>
                <p className="text-muted-foreground">{s.desc}</p>
              </div>
            </div>
          ))}

          <Separator />

          <div>
            <p className="font-semibold mb-2">⌨️ Keyboard Shortcuts</p>
            <div className="grid grid-cols-2 gap-2 text-muted-foreground">
              <span><kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">Ctrl+Enter</kbd> Execute query</span>
              <span><kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">Ctrl+Shift+C</kbd> Copy SQL</span>
            </div>
          </div>

          <Separator />

          <div>
            <p className="font-semibold mb-2">📝 Common Examples</p>
            <ul className="space-y-2 text-muted-foreground">
              <li>• <strong>Get IT employees:</strong> Table: pmm_employee → WHERE emp_dept_cd = 'IT'</li>
              <li>• <strong>Count complaints:</strong> Aggregate → GROUP BY status → COUNT(*)</li>
              <li>• <strong>Employee complaints:</strong> JOIN → pmm_employee + gmtk_coms_hdr ON emp_no</li>
            </ul>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default HelpModal;
