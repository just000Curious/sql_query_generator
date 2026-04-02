import { useState, useEffect } from "react";
import { Train, HelpCircle, RotateCcw, Wifi, WifiOff, CheckCircle, XCircle, Moon, Sun, History } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/components/theme-provider";

interface AppHeaderProps {
  sessionId: string | null;
  onHelpOpen: () => void;
  onClearAll: () => void;
  onHistoryOpen: () => void;
}

const AppHeader = ({ sessionId, onHelpOpen, onClearAll, onHistoryOpen }: AppHeaderProps) => {
  const [apiStatus, setApiStatus] = useState<"checking" | "online" | "offline">("checking");
  const { theme, setTheme } = useTheme();

  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
    const check = async () => {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 3000);
      try {
        const res = await fetch(`${API_BASE}/health`, { signal: controller.signal });
        clearTimeout(timer);
        setApiStatus(res.ok ? "online" : "offline");
      } catch {
        clearTimeout(timer);
        setApiStatus("offline");
      }
    };
    check();
    const interval = setInterval(check, 15000);
    return () => clearInterval(interval);
  }, []);

  return (
    <header
      className="shadow-lg sticky top-0 z-50 transition-colors"
      style={{
        background: theme === "dark" 
          ? "linear-gradient(135deg, hsl(220,85%,14%) 0%, hsl(215,75%,22%) 100%)"
          : "linear-gradient(135deg, hsl(220,85%,40%) 0%, hsl(215,75%,50%) 100%)",
        borderBottom: theme === "dark" ? "1px solid hsl(220,60%,22%)" : "1px solid hsl(220,60%,40%)",
      }}
    >
      <div className="max-w-7xl mx-auto px-4 py-3 flex items-center justify-between gap-3">
        {/* Logo + Title */}
        <div className="flex items-center gap-3 min-w-0">
          <div
            className="rounded-xl p-2 flex-shrink-0"
            style={{ background: "hsl(38,95%,52%)" }}
          >
            <Train className="h-5 w-5" style={{ color: "hsl(220,85%,18%)" }} />
          </div>
          <div className="hidden sm:block min-w-0">
            <h1 className="text-sm font-bold text-white tracking-tight truncate">
              SQL Query Generator
            </h1>
            <p className="text-[10px] font-semibold tracking-widest uppercase" style={{ color: "hsl(210,60%,90%)" }}>
              Kokan Railway Corporation · No SQL skills needed
            </p>
          </div>
        </div>

        {/* Status + Actions */}
        <div className="flex items-center gap-2 flex-shrink-0">
          {/* API Status pill */}
          <div
            className="hidden md:flex items-center gap-1.5 text-xs rounded-full px-3 py-1.5 font-medium"
            style={{ background: theme === "dark" ? "hsl(220,65%,22%)" : "hsl(220,65%,35%)" }}
          >
            {apiStatus === "checking" && (
              <>
                <Wifi className="h-3 w-3 animate-pulse" style={{ color: "hsl(210,60%,85%)" }} />
                <span style={{ color: "hsl(210,60%,90%)" }}>Connecting…</span>
              </>
            )}
            {apiStatus === "online" && (
              <>
                <CheckCircle className="h-3 w-3" style={{ color: "hsl(145,60%,65%)" }} />
                <span style={{ color: "hsl(145,60%,85%)" }}>API Online</span>
              </>
            )}
            {apiStatus === "offline" && (
              <>
                <XCircle className="h-3 w-3 text-red-300" />
                <span className="text-red-300">API Offline</span>
              </>
            )}
          </div>

          {/* Session badge */}
          {sessionId && (
            <div
              className="hidden lg:flex items-center gap-1.5 text-xs rounded-full px-2.5 py-1.5"
              style={{ background: theme === "dark" ? "hsl(220,65%,22%)" : "hsl(220,65%,35%)", color: "hsl(210,50%,90%)" }}
            >
              <Wifi className="h-3 w-3" style={{ color: "hsl(145,55%,65%)" }} />
              <span className="font-mono">{sessionId.slice(0, 8)}…</span>
            </div>
          )}

          <Button
            variant="ghost"
            size="sm"
            onClick={onClearAll}
            className="text-xs gap-1.5 font-medium text-white hover:bg-white/10 hover:text-white"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Reset
          </Button>

          {/* Theme Toggle */}
          <Button
            variant="ghost"
            size="icon"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            title="Toggle theme"
            className="text-white hover:bg-white/10 hover:text-white"
          >
            {theme === "dark" ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </Button>

          {/* History Button */}
          <Button
            variant="ghost"
            size="icon"
            onClick={onHistoryOpen}
            title="History"
            className="text-white hover:bg-white/10 hover:text-white"
          >
            <History className="h-5 w-5" />
          </Button>

          {/* Help Button */}
          <Button
            variant="ghost"
            size="icon"
            onClick={onHelpOpen}
            title="Help"
            className="text-white hover:bg-white/10 hover:text-white"
          >
            <HelpCircle className="h-5 w-5" />
          </Button>
        </div>
      </div>

      {/* Offline warning banner */}
      {apiStatus === "offline" && (
        <div className="bg-red-900/60 border-t border-red-700/50 px-4 py-2 text-center text-xs text-red-200">
          ⚠️ Backend API is offline. Run <code className="bg-red-950/60 px-1.5 py-0.5 rounded font-mono">python api.py</code> in your terminal to start it.
        </div>
      )}
    </header>
  );
};

export default AppHeader;
