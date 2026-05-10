type QueryType = "select" | "join" | "aggregate" | "date_range" | "raw";

interface QueryTypeToggleProps {
  value: QueryType;
  onChange: (v: QueryType) => void;
}

const TYPES: { value: QueryType; label: string; icon: string; desc: string }[] = [
  { value: "select",     label: "Simple SELECT", icon: "📋", desc: "Fetch rows from one table" },
  { value: "join",       label: "JOIN",          icon: "🔗", desc: "Combine two or more tables" },
  { value: "aggregate",  label: "Aggregate",     icon: "📊", desc: "COUNT, SUM, AVG, MIN, MAX" },
  { value: "date_range", label: "Date Range",    icon: "📅", desc: "Filter by date period" },
  { value: "raw",        label: "Raw SQL",       icon: "✍️",  desc: "Write your own SQL" },
];

const QueryTypeToggle = ({ value, onChange }: QueryTypeToggleProps) => {
  return (
    <div className="flex flex-wrap gap-3">
      {TYPES.map((t) => {
        const isActive = value === t.value;
        return (
          <button
            key={t.value}
            onClick={() => onChange(t.value)}
            title={t.desc}
            className={`query-type-btn ${isActive ? "query-type-btn-active" : "query-type-btn-inactive"}`}
          >
            <span className="text-2xl">{t.icon}</span>
            <span className="font-semibold text-sm">{t.label}</span>
            <span
              className="text-xs leading-tight"
              style={{ color: isActive ? "rgba(255,255,255,0.8)" : undefined }}
              // inactive text uses muted-foreground from CSS
            >
              {t.desc}
            </span>
          </button>
        );
      })}
    </div>
  );
};

export default QueryTypeToggle;
export type { QueryType };
