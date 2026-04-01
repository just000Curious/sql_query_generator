export interface QueryHistoryItem {
  id: string;
  sql: string;
  timestamp: number;
  favorite: boolean;
}

const STORAGE_KEY = "kokan_query_history";
const MAX_ITEMS = 10;

export function getHistory(): QueryHistoryItem[] {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

export function addToHistory(sql: string) {
  const history = getHistory();
  const item: QueryHistoryItem = {
    id: crypto.randomUUID(),
    sql,
    timestamp: Date.now(),
    favorite: false,
  };
  const updated = [item, ...history.filter((h) => h.sql !== sql)].slice(0, MAX_ITEMS);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
}

export function toggleFavorite(id: string) {
  const history = getHistory();
  const item = history.find((h) => h.id === id);
  if (item) item.favorite = !item.favorite;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(history));
}

export function clearHistory() {
  localStorage.removeItem(STORAGE_KEY);
}
