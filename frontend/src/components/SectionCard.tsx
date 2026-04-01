import { ReactNode } from "react";

interface SectionCardProps {
  title: string;
  icon: string;
  children: ReactNode;
  className?: string;
  visible?: boolean;
}

const SectionCard = ({ title, icon, children, className = "", visible = true }: SectionCardProps) => {
  if (!visible) return null;
  return (
    <section className={`railway-card ${className}`}>
      <h3 className="text-sm font-bold text-primary flex items-center gap-2 mb-4 uppercase tracking-wide">
        <span>{icon}</span> {title}
      </h3>
      {children}
    </section>
  );
};

export default SectionCard;
