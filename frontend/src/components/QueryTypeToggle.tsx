type QueryType = "select" | "join" | "aggregate" | "union" | "date_range" | "raw";

interface QueryTypeToggleProps {
  value: QueryType;
  onChange: (v: QueryType) => void;
}

const TYPES: { value: QueryType; label: string; icon: string }[] = [
  { value: "select", label: "Simple SELECT", icon: "📋" },
  { value: "join", label: "JOIN Query", icon: "🔗" },
  { value: "aggregate", label: "Aggregate", icon: "📊" },
  { value: "union", label: "UNION", icon: "🔄" },
  { value: "date_range", label: "Date Range", icon: "📅" },
  { value: "raw", label: "Raw SQL", icon: "✍️" },
];

const QueryTypeToggle = ({ value, onChange }: QueryTypeToggleProps) => {
  return (
    <div className="flex flex-wrap gap-2">
      {TYPES.map((t) => (
        <button
          key={t.value}
          onClick={() => onChange(t.value)}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-all border ${
            value === t.value
              ? "bg-primary text-primary-foreground border-primary shadow-md"
              : "bg-card text-foreground border-border hover:border-secondary hover:bg-muted"
          }`}
        >
          {t.icon} {t.label}
        </button>
      ))}
    </div>
  );
};

export default QueryTypeToggle;
export type { QueryType };
