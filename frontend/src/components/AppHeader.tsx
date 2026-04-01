import { Train, HelpCircle, Wifi, WifiOff, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface AppHeaderProps {
  sessionId: string | null;
  onHelpOpen: () => void;
  onClearAll: () => void;
}

const AppHeader = ({ sessionId, onHelpOpen, onClearAll }: AppHeaderProps) => {
  return (
    <header className="bg-primary text-primary-foreground shadow-lg sticky top-0 z-50">
      <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="bg-accent rounded-lg p-2">
            <Train className="h-6 w-6 text-accent-foreground" />
          </div>
          <div className="hidden sm:block">
            <h1 className="text-sm font-bold tracking-tight">SQL Query Generator</h1>
            <p className="text-[10px] text-primary-foreground/70 font-medium tracking-wider uppercase">Kokan Railway Corporation</p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <div className="hidden md:flex items-center gap-1.5 text-xs text-primary-foreground/70 bg-primary-foreground/10 rounded-full px-3 py-1.5">
            {sessionId ? (
              <>
                <Wifi className="h-3 w-3 text-green-400" />
                <span className="font-mono">{sessionId.slice(0, 8)}…</span>
              </>
            ) : (
              <>
                <WifiOff className="h-3 w-3 text-red-400" />
                <span>No session</span>
              </>
            )}
          </div>
          <Button variant="ghost" size="sm" onClick={onClearAll} className="text-primary-foreground hover:bg-primary-foreground/10 text-xs gap-1.5">
            <RotateCcw className="h-3.5 w-3.5" /> Clear All
          </Button>
          <Button variant="ghost" size="icon" onClick={onHelpOpen} className="text-primary-foreground hover:bg-primary-foreground/10">
            <HelpCircle className="h-5 w-5" />
          </Button>
        </div>
      </div>
    </header>
  );
};

export default AppHeader;
