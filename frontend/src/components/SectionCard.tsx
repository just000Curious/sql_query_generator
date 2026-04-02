import { ReactNode } from "react";

interface SectionCardProps {
  title: string;
  icon: string;
  children: ReactNode;
  className?: string;
  visible?: boolean;
  badge?: string | number;
  hint?: string;
  stepNum?: number;
  done?: boolean;
}

const SectionCard = ({
  title, icon, children, className = "",
  visible = true, badge, hint, stepNum, done = false
}: SectionCardProps) => {
  if (!visible) return null;
  return (
    <section className={`railway-card ${className}`}>
      <div className="section-header">
        <h3 className="section-title">
          {stepNum !== undefined && (
            <span className={done ? "step-indicator-done" : "step-indicator"}>
              {done ? "✓" : stepNum}
            </span>
          )}
          {!stepNum && <span>{icon}</span>}
          {title}
          {badge !== undefined && (
            <span className="badge-count">{badge}</span>
          )}
        </h3>
        {hint && (
          <span className="text-[11px] text-muted-foreground italic hidden sm:block">{hint}</span>
        )}
      </div>
      {children}
    </section>
  );
};

export default SectionCard;
