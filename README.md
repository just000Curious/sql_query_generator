# 🚂 SQL Query Generator

> **Visual SQL query builder for Kokan Railway Corporation's PostgreSQL databases.**
> Build complex queries through an intuitive UI — no SQL expertise required.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115+-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)
![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?logo=typescript&logoColor=white)
![Vite](https://img.shields.io/badge/Vite-5-646CFF?logo=vite&logoColor=white)
![TailwindCSS](https://img.shields.io/badge/Tailwind_CSS-3.4-06B6D4?logo=tailwindcss&logoColor=white)

---

## 📋 Table of Contents

- [Features](#-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
  - [1. Clone the Repository](#1-clone-the-repository)
  - [2. Backend Setup (Python / FastAPI)](#2-backend-setup-python--fastapi)
  - [3. Frontend Setup (React / Vite)](#3-frontend-setup-react--vite)
- [Project Structure](#-project-structure)
- [Available Scripts](#-available-scripts)
- [API Endpoints](#-api-endpoints)
- [Database Schemas](#-database-schemas)
- [Environment Variables](#-environment-variables)
- [Troubleshooting](#-troubleshooting)
- [Contributing](#-contributing)
- [License](#-license)

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **Visual Query Builder** | Point-and-click interface for SELECT, JOIN, Aggregate, Date Range, and UNION queries |
| **6 Query Modes** | Simple SELECT · JOIN · Aggregate · Date Range · UNION · Raw SQL |
| **Schema Browser** | Navigate GM, HM, PM, SI, SA, TA schemas with full table/column metadata |
| **Live Validation** | Real-time error and warning checks before query generation |
| **Temp Table / CTE Wrapper** | Optionally wrap output as a `CREATE TEMP TABLE` or `WITH ... AS` CTE |
| **Query History** | Browser-stored history of generated queries with one-click reload |
| **Dark / Light Mode** | Toggle between themes with a single click |
| **Keyboard Shortcuts** | `Ctrl+Enter` to generate, `Ctrl+Shift+C` to copy |
| **Offline Fallback** | Generates SQL locally if the backend API is unavailable |
| **No Data Exposure** | The tool generates read-only SELECT statements — never touches production data |

---

## 🏗️ Architecture

```
┌──────────────────────┐       HTTP/REST        ┌──────────────────────┐
│                      │ ◄───────────────────── │                      │
│   React Frontend     │                        │   FastAPI Backend     │
│   (Vite + TS)        │ ──────────────────────►│   (Python 3.10+)     │
│   Port: 5173         │                        │   Port: 8000         │
│                      │                        │                      │
│  • Query Builder UI  │                        │  • Query Generation  │
│  • SQL Preview       │                        │  • Schema Metadata   │
│  • History / Theme   │                        │  • Validation Engine │
│  • Local Fallback    │                        │  • In-Memory SQLite  │
└──────────────────────┘                        └──────────┬───────────┘
                                                           │
                                                           ▼
                                                ┌──────────────────────┐
                                                │  db_files/           │
                                                │  metadata.json       │
                                                │  (Schema Definitions)│
                                                └──────────────────────┘
```

---

## 🔧 Tech Stack

### Backend
- **Python 3.10+** — Core runtime
- **FastAPI** — High-performance async web framework
- **Uvicorn** — ASGI server
- **Pydantic v2** — Data validation and settings
- **SQLite** (in-memory) — Schema validation mirror
- **Pandas** — Data processing

### Frontend
- **React 18** — Component-based UI library
- **TypeScript 5** — Type safety
- **Vite 5** — Lightning-fast dev server and bundler
- **Tailwind CSS 3.4** — Utility-first CSS framework
- **shadcn/ui + Radix UI** — Accessible, composable component primitives
- **Lucide React** — Icon library
- **TanStack React Query** — Server-state management
- **Sonner** — Toast notifications
- **React Router v6** — Client-side routing

---

## 📦 Prerequisites

Before you begin, make sure you have the following installed:

| Tool | Minimum Version | Check |
|------|:---------------:|-------|
| **Python** | 3.10+ | `python --version` |
| **pip** | latest | `pip --version` |
| **Node.js** | 18+ | `node --version` |
| **npm** | 9+ | `npm --version` |
| **Git** | any | `git --version` |

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/sql-query-generator.git
cd sql-query-generator
```

---

### 2. Backend Setup (Python / FastAPI)

#### a) Create a Virtual Environment

```bash
# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Windows (CMD)
python -m venv .venv
.\.venv\Scripts\activate.bat

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

> **Tip:** You'll see `(.venv)` in your terminal prompt when the venv is active.

#### b) Install Python Dependencies

```bash
pip install -r requirements-api.txt
```

This installs:
- `fastapi` — Web framework
- `uvicorn` — ASGI server
- `pydantic` — Data validation
- `python-multipart` — Form data support
- `pandas` — Data processing
- `openpyxl` — Excel file support

If you need the query engine utilities (pypika, sqlparse, etc.):

```bash
pip install -r requirements-core.txt
```

#### c) Start the Backend Server

```bash
python api.py
```

You should see:

```
============================================================
🚀 Starting SQL Query Generator API v5.0
============================================================
📂 Loading schema from JSON...
✅ Loaded 6 schemas with <N> tables
🗄️ Creating database tables...
✅ Database initialized
✅ Server ready at http://127.0.0.1:8000
✅ API docs at http://127.0.0.1:8000/docs
============================================================
```

- **API Base:** [http://localhost:8000](http://localhost:8000)
- **Swagger Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc:** [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

### 3. Frontend Setup (React / Vite)

Open a **new terminal** (keep the backend running in the first one).

#### a) Navigate to the Frontend Directory

```bash
cd frontend
```

#### b) Install Node Dependencies

```bash
npm install
```

#### c) Start the Development Server

```bash
npm run dev
```

Your browser should open (or navigate to):

```
http://localhost:5173
```

> **Note:** The frontend auto-connects to the backend at `http://localhost:8000`. If the API is offline, a red banner appears at the top, and queries are generated locally as a fallback.

---

## 📁 Project Structure

```
sql-query-generator/
├── api.py                  # FastAPI backend — main entry point
├── db_information.py       # Database introspection utilities
├── query_engine.py         # SQL generation engine
├── pypika_query_engine.py  # PyPika-based query builder
├── query_assembler.py      # Multi-step query assembly
├── query_validator.py      # Query validation and safety checks
├── join_builder.py         # JOIN clause construction
├── union_builder.py        # UNION query logic
├── cte_builder.py          # CTE (Common Table Expression) builder
├── filter_templates.py     # Reusable filter templates
├── temporary_table.py      # Temporary table wrapper
├── requirements-api.txt    # Python deps for the API server
├── requirements-core.txt   # Python deps for core query utilities
├── db_files/
│   └── metadata.json       # Schema/table/column definitions (auto-loaded)
│
├── frontend/
│   ├── package.json        # Node project config & dependencies
│   ├── vite.config.ts      # Vite bundler configuration
│   ├── tsconfig.json       # TypeScript compiler options
│   ├── tailwind.config.ts  # Tailwind CSS configuration
│   ├── postcss.config.js   # PostCSS plugins
│   ├── index.html          # HTML entry point
│   └── src/
│       ├── main.tsx        # React entry point
│       ├── App.tsx         # Root component with routing & providers
│       ├── index.css       # Global styles & design tokens
│       ├── pages/
│       │   ├── Index.tsx   # Main query builder page
│       │   └── NotFound.tsx
│       ├── components/
│       │   ├── AppHeader.tsx         # Header with status, theme toggle
│       │   ├── QueryTypeToggle.tsx   # Query mode selector
│       │   ├── TableSelector.tsx     # Schema → Table picker
│       │   ├── ColumnSelector.tsx    # Column multi-select
│       │   ├── ConditionBuilder.tsx  # WHERE clause builder
│       │   ├── JoinBuilder.tsx       # JOIN condition editor
│       │   ├── AggregateBuilder.tsx  # Aggregate function picker
│       │   ├── DateRangeFilter.tsx   # Date range inputs
│       │   ├── GroupOrderOptions.tsx # GROUP BY, ORDER BY, LIMIT
│       │   ├── UnionBuilder.tsx      # UNION query composer
│       │   ├── SqlPreview.tsx        # SQL output with copy/download
│       │   ├── TempTableOptions.tsx  # Temp table / CTE wrapper
│       │   ├── ValidationPanel.tsx   # Live error/warning display
│       │   ├── HelpModal.tsx         # Comprehensive help center
│       │   ├── HistoryPanel.tsx      # Query history sidebar
│       │   ├── SectionCard.tsx       # Collapsible step card
│       │   ├── theme-provider.tsx    # Dark/light theme context
│       │   └── ui/                   # shadcn/ui primitives (50+ components)
│       ├── hooks/
│       └── lib/
│           ├── api.ts               # API client functions
│           ├── query-history.ts     # Local storage history management
│           └── utils.ts             # Utility helpers (cn, etc.)
└── README.md
```

---

## 📜 Available Scripts

### Backend

| Command | Description |
|---------|-------------|
| `python api.py` | Start the FastAPI server on port 8000 |

### Frontend (run from `frontend/` directory)

| Command | Description |
|---------|-------------|
| `npm run dev` | Start Vite dev server with HMR (`localhost:5173`) |
| `npm run build` | Create production build in `dist/` |
| `npm run preview` | Preview the production build locally |
| `npm run lint` | Run ESLint across the codebase |
| `npm run test` | Run unit tests with Vitest |
| `npm run test:watch` | Run tests in watch mode |

---

## 🔌 API Endpoints

| Method | Endpoint | Description |
|:------:|----------|-------------|
| `GET` | `/health` | Health check (API status) |
| `GET` | `/schemas` | List all database schemas |
| `GET` | `/schemas/{name}/tables` | List tables in a schema |
| `GET` | `/schemas/{name}/tables/{table}` | Get table details (columns, keys) |
| `GET` | `/tables/all` | Get all tables grouped by schema |
| `GET` | `/search/tables?q=...` | Search tables by name |
| `GET` | `/search/columns?q=...` | Search columns across all tables |
| `GET` | `/stats` | Database statistics |
| `POST` | `/session/create` | Create a new session |
| `POST` | `/query/generate` | Generate SQL from builder parameters |
| `POST` | `/query/union` | Generate UNION SQL from multiple queries |
| `POST` | `/query/execute` | Validate SQL against the in-memory database |

> **Interactive Docs:** Visit [http://localhost:8000/docs](http://localhost:8000/docs) for full Swagger UI.

---

## 🗄️ Database Schemas

| Code | Name | Description |
|:----:|------|-------------|
| **GM** | General Management | Complaints, forwarding, document management |
| **HM** | Healthcare Management | Medical records, lab tests, certificates |
| **PM** | Personnel Management | Employee data, payroll, leave management |
| **SI** | Stores & Inventory | Materials, purchases, tenders |
| **SA** | Security & Administration | User management, roles, access control |
| **TA** | Traffic & Accounts | Ticketing, freight, accounting |

Schema metadata is loaded from `db_files/metadata.json` at server startup.

---

## 🔐 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_API_BASE_URL` | `http://localhost:8000` | Backend API URL (frontend) |

To override, create a `.env` file in the `frontend/` directory:

```env
VITE_API_BASE_URL=http://your-server:8000
```

---

## 🔥 Troubleshooting

### Backend Issues

<details>
<summary><strong>❌ <code>ModuleNotFoundError: No module named 'fastapi'</code></strong></summary>

Your virtual environment isn't active or dependencies are missing.

```bash
# Activate venv first
.\.venv\Scripts\Activate.ps1   # PowerShell
source .venv/bin/activate       # macOS/Linux

# Then install
pip install -r requirements-api.txt
```
</details>

<details>
<summary><strong>❌ <code>Address already in use</code> on port 8000</strong></summary>

Another process is using port 8000. Find and kill it:

```bash
# Windows
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# macOS/Linux
lsof -i :8000
kill -9 <PID>
```
</details>

<details>
<summary><strong>❌ <code>JSON file not found</code> warning at startup</strong></summary>

The `db_files/metadata.json` file is missing. Ensure the file exists in the `db_files/` directory at the project root with the correct schema structure.
</details>

### Frontend Issues

<details>
<summary><strong>❌ <code>npm install</code> fails</strong></summary>

Try clearing the npm cache and retrying:

```bash
npm cache clean --force
rm -rf node_modules package-lock.json
npm install
```
</details>

<details>
<summary><strong>❌ Blank page or module errors after <code>npm run dev</code></strong></summary>

Clear the Vite cache:

```bash
rm -rf node_modules/.vite
npm run dev
```
</details>

<details>
<summary><strong>❌ "API Offline" banner in the UI</strong></summary>

The backend isn't running. Open a separate terminal, activate the venv, and run `python api.py`. The frontend will auto-detect the API within 15 seconds.
</details>

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'feat: add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

---

## 📄 License

This project is proprietary to **Kokan Railway Corporation**. All rights reserved.

---

<p align="center">
  Built with ❤️ for Kokan Railway Corporation
</p>
