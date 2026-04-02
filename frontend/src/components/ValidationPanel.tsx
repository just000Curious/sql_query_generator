import { AlertTriangle, XCircle, CheckCircle2, Info } from "lucide-react";

export interface ValidationResult {
  errors: string[];
  warnings: string[];
}

interface ValidationPanelProps {
  result: ValidationResult;
}

const ValidationPanel = ({ result }: ValidationPanelProps) => {
  const { errors, warnings } = result;
  if (errors.length === 0 && warnings.length === 0) return null;

  return (
    <div className="space-y-2">
      {errors.length > 0 && (
        <div className="rounded-xl border border-destructive/40 bg-destructive/8 p-4">
          <div className="flex items-center gap-2 mb-2">
            <XCircle className="h-4 w-4 text-destructive flex-shrink-0" />
            <p className="text-sm font-semibold text-destructive">
              {errors.length} error{errors.length > 1 ? "s" : ""} — fix before generating
            </p>
          </div>
          <ul className="space-y-1">
            {errors.map((e, i) => (
              <li key={i} className="flex items-start gap-2 text-xs text-destructive/90">
                <span className="mt-0.5 flex-shrink-0">•</span>
                {e}
              </li>
            ))}
          </ul>
        </div>
      )}

      {warnings.length > 0 && (
        <div className="rounded-xl border border-amber-500/40 bg-amber-500/8 p-4">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle className="h-4 w-4 text-amber-600 flex-shrink-0" />
            <p className="text-sm font-semibold text-amber-700">
              {warnings.length} warning{warnings.length > 1 ? "s" : ""} — query may be incorrect
            </p>
          </div>
          <ul className="space-y-1">
            {warnings.map((w, i) => (
              <li key={i} className="flex items-start gap-2 text-xs text-amber-800/90">
                <span className="mt-0.5 flex-shrink-0">•</span>
                {w}
              </li>
            ))}
          </ul>
        </div>
      )}

      {errors.length === 0 && warnings.length === 0 && (
        <div className="flex items-center gap-2 text-xs text-emerald-600 p-2">
          <CheckCircle2 className="h-4 w-4" />
          Query looks valid
        </div>
      )}
    </div>
  );
};

export const emptyValidation = (): ValidationResult => ({ errors: [], warnings: [] });

export default ValidationPanel;
