type QueryType = "select" | "join" | "aggregate" | "union" | "date_range" | "raw";

interface QueryTypeToggleProps {
  value: QueryType;
  onChange: (v: QueryType) => void;
}

const TYPES: { value: QueryType; label: string; icon: string; desc: string }[] = [
  { value: "select",     label: "Simple SELECT", icon: "📋", desc: "Fetch rows from one table" },
  { value: "join",       label: "JOIN",          icon: "🔗", desc: "Combine two or more tables" },
  { value: "aggregate",  label: "Aggregate",     icon: "📊", desc: "COUNT, SUM, AVG, MIN, MAX" },
  { value: "union",      label: "UNION",         icon: "🔄", desc: "Stack results from multiple tables" },
  { value: "date_range", label: "Date Range",    icon: "📅", desc: "Filter by date period" },
  { value: "raw",        label: "Raw SQL",       icon: "✍️", desc: "Write your own SQL" },
];

const QueryTypeToggle = ({ value, onChange }: QueryTypeToggleProps) => {
  return (
    <div className="flex flex-wrap gap-2">
      {TYPES.map((t) => (
        <button
          key={t.value}
          onClick={() => onChange(t.value)}
          title={t.desc}
          className={`query-type-btn ${value === t.value ? "query-type-btn-active" : "query-type-btn-inactive"}`}
        >
          <span className="text-xl">{t.icon}</span>
          <span className="font-semibold text-xs">{t.label}</span>
          <span className={`text-[10px] leading-tight ${value === t.value ? "text-primary-foreground/70" : "text-muted-foreground"}`}>
            {t.desc}
          </span>
        </button>
      ))}
    </div>
  );
};

export default QueryTypeToggle;
export type { QueryType };
