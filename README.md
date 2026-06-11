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

## 📸 Screenshots

### 🖥️ Interactive Visual Query Builder
![Interactive Visual Query Builder](screenshots/homepage.png)

### ⚙️ Database Connection Settings
![Database Connection Settings](screenshots/db_connection_modal.png)

---

## 📋 Table of Contents

- [Features](#-features)
- [How the Project Works](#-how-the-project-works)
- [Architecture](#-architecture)
- [How the EXE Was Created](#-how-the-exe-was-created)
- [What is app.py (Streamlit Testing UI)](#-what-is-apppy-streamlit-testing-ui)
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
| **Schema Hot-Reloading** | Upload new database schemas instantly via the UI without restarting the server |
| **Schema Rollback System** | Automatically keeps 3 past schemas and allows 1-click restore/delete |
| **Standalone Executable** | Packaged into a zero-dependency `.exe` file via PyInstaller |
| **Live Validation** | Real-time error and warning checks before query generation |
| **Temp Table / CTE Wrapper** | Optionally wrap output as a `CREATE TEMP TABLE` or `WITH ... AS` CTE |
| **Query History** | Browser-stored history of generated queries with one-click reload |
| **Dark / Light Mode** | Toggle between themes with a single click |
| **Offline Fallback** | Generates SQL locally if the backend API is unavailable |

---

## 🔄 How the Project Works

This section explains the **end-to-end flow** of the SQL Query Generator — from user interaction to SQL output.

### Overview

The project is a **client-server application** where:
- A **React frontend** (the visual query builder) runs in the browser.
- A **Python/FastAPI backend** runs a REST API that generates SQL queries.
- The two communicate over HTTP on `localhost`.

### Step-by-Step Flow

```
 User opens browser                     Backend starts
      │                                      │
      ▼                                      ▼
 React UI loads on                     FastAPI reads db_files/metadata.json
 http://localhost:5173                 and builds an in-memory SQLite mirror
      │                                      │
      ▼                                      ▼
 User selects Schema ──► GET /schemas ──► Returns list of schemas (GM, PM, etc.)
      │
      ▼
 User selects Table ──► GET /schemas/{name}/tables ──► Returns tables & columns
      │
      ▼
 User picks columns, adds WHERE
 conditions, JOINs, aggregates, etc.
      │
      ▼
 User clicks "Generate" ──► POST /query/generate ──► Backend assembles SQL
      │                                                    │
      ▼                                                    ▼
 SQL Preview panel shows                            Validates columns exist,
 the generated query                                checks operator correctness,
      │                                             returns formatted SQL string
      ▼
 User can copy, download, or
 wrap in CTE / TEMP TABLE
```

### Key Backend Concepts

1. **Schema Metadata (`db_files/metadata.json`)**
   - This JSON file contains the full definition of every database schema, table, column, primary key, and foreign key from the Konkan Railway Corporation's PostgreSQL databases.
   - At startup, `api.py` reads this file and loads it into memory. It also creates a lightweight **in-memory SQLite mirror** so that SQL queries can be syntax-validated without needing access to the real PostgreSQL server.

2. **Query Generation Engine**
   - When the frontend sends a `POST /query/generate` request with the user's selections (tables, columns, conditions, joins, etc.), the backend's `SchemaQueryGenerator` class builds a properly formatted SQL string.
   - The engine handles all SQL clauses: `SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `DISTINCT`, computed columns, and `CAST` expressions.
   - Values are properly quoted based on column types (numeric values are unquoted, strings are escaped and quoted, dates are formatted as `'YYYY-MM-DD'` literals).

3. **Validation**
   - Before returning SQL, the backend validates the request against the schema metadata — checking that referenced columns actually exist in their tables, join conditions are complete, and operators are valid.
   - Errors and warnings are returned to the frontend, which displays them in a real-time **Validation Panel**.

4. **Live PostgreSQL Connection (Optional)**
   - Users can optionally connect the tool to a **live PostgreSQL server** via the Database Connection modal in the UI.
   - When connected, the tool fetches the live schema directly from `information_schema` and replaces the static `metadata.json` data, ensuring the column list is always up-to-date.

5. **Offline Fallback**
   - If the backend API is unreachable, the React frontend switches to a **local fallback mode** where it generates basic SQL queries entirely in the browser using JavaScript. A red "API Offline" banner appears to indicate this mode.

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

## 📦 How the EXE Was Created

The standalone `SQL_Query_Generator.exe` in the `dist/` folder is a **single-file executable** (~64 MB) that bundles the **entire application** — Python backend, React frontend, and all dependencies — into one double-clickable file. No Python, Node.js, or any other software needs to be installed on the target machine.

### Tool Used: PyInstaller

[PyInstaller](https://pyinstaller.org/) is used to freeze the Python backend into a standalone executable. It works by:
1. Analyzing all Python `import` statements starting from `api.py`.
2. Collecting every Python module, C extension, and shared library into a single archive.
3. Embedding a Python interpreter so the exe runs without Python being installed.

### What Gets Bundled Inside the EXE

| Bundled Asset | Source Location | Packed Into |
|---|---|---|
| **Python runtime** | System Python 3.10+ | Embedded interpreter |
| **FastAPI + Uvicorn** | `requirements-api.txt` | Frozen Python packages |
| **React frontend build** | `frontend/dist/` | `frontend/dist/` inside exe |
| **Schema metadata** | `db_files/metadata.json` | `db_files/` inside exe |
| **All Python modules** | `api.py`, `query_engine.py`, etc. | Frozen bytecode |

### How the Build Works

There are **two ways** to build the exe:

#### Option 1: Using `package_app.py` (Recommended)

This is a helper script that automates the entire process:

```bash
python package_app.py
```

It performs three steps:
1. **Pre-flight checks** — Verifies that `db_files/metadata.json` and `frontend/dist/` (the React production build) exist. If the frontend hasn't been built yet, it tells you to run `cd frontend && npm install && npm run build` first.
2. **Runs PyInstaller** — Executes the PyInstaller command with all the correct flags (`--onefile`, `--noconsole`, `--add-data`, and `--hidden-import` for uvicorn/FastAPI internals).
3. **Reports success** — Shows the exe path and file size.

#### Option 2: Using the `.spec` file directly

```bash
python -m PyInstaller SQL_Query_Generator.spec --clean
```

The `SQL_Query_Generator.spec` file is a PyInstaller configuration that defines:
- **Entry point**: `api.py`
- **Data files**: `frontend/dist` (React build) and `db_files` (schema metadata)
- **Icon**: `frontend/public/krc-logo.png`
- **Mode**: `--onefile` (single exe) + `--noconsole` (no terminal window)

### What Happens When You Run the EXE

1. PyInstaller extracts the bundled files to a temporary directory (`sys._MEIPASS`).
2. `api.py` detects it's running inside PyInstaller and uses `get_resource_path()` to resolve paths relative to that temp directory.
3. The FastAPI/Uvicorn server starts on `http://127.0.0.1:8000`.
4. The browser auto-opens to `http://127.0.0.1:8000` where the React frontend is served.
5. Crash logs are written to `%USERPROFILE%\SQL_Query_Generator_logs\app.log`.

> **Note:** The `.exe` is a Windows-only binary. It cannot run on Linux or macOS. To create an executable for another OS, you must run PyInstaller on that OS.

### Prerequisites for Building

Before building the exe, you need:

```bash
# 1. Install PyInstaller
pip install pyinstaller

# 2. Build the React frontend (creates frontend/dist/)
cd frontend
npm install
npm run build
cd ..

# 3. Run the packager
python package_app.py
```

---

## 🧪 What is `app.py` (Streamlit Testing UI)

`app.py` is a **Streamlit-based testing interface** that was created during early development to quickly test and debug the backend API. It is **not** the main frontend — the main frontend is the React application in the `frontend/` directory.

### Purpose

- **API Testing Tool** — Provides a simple browser UI (via Streamlit) to call the backend endpoints and inspect responses without needing the full React frontend running.
- **Development Aid** — Used during development to verify that query generation, schema loading, and SQL execution work correctly.
- **Standalone** — Can be run independently to debug the API.

### What It Does

| Tab | Functionality |
|-----|---------------|
| **Query Builder** | A simplified form to select a table, pick columns, add WHERE conditions, GROUP BY, ORDER BY, and LIMIT — then generate SQL via the API. |
| **SQL Editor** | A raw SQL text area where you can type any SQL query and execute it against the in-memory database. |
| **Sample Queries** | Pre-built example queries that can be run with one click. |
| **Analytics** | Shows database statistics (table counts, column counts) and provides quick analytics queries. |

### How to Run

```bash
# Install Streamlit (if not already installed)
pip install streamlit pandas plotly requests

# Make sure the backend API is running first
python api.py

# Then in a separate terminal:
streamlit run app.py
```

This opens a browser at `http://localhost:8501` with the Streamlit testing UI.

### When to Use

| Scenario | Use This |
|----------|----------|
| Normal day-to-day use | **React frontend** (`npm run dev` or the `.exe`) |
| Testing/debugging the API | **`app.py`** (Streamlit) |
| Quick API verification | **`http://localhost:8000/docs`** (Swagger UI) |

> **Note:** `app.py` is not included in the `.exe` build. It's a development-only tool.

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
├── api.py                  # FastAPI backend — main entry point & query engine
├── app.py                  # Streamlit testing UI (development/debugging tool)
├── package_app.py          # PyInstaller build script (creates the .exe)
├── SQL_Query_Generator.spec # PyInstaller configuration file
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
├── dist/
│   └── SQL_Query_Generator.exe  # Standalone executable (~64 MB)
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
| `streamlit run app.py` | Launch the Streamlit testing UI (dev only) |

### Frontend (run from `frontend/` directory)

| Command | Description |
|---------|-------------|
| `npm run dev` | Start Vite dev server with HMR (`localhost:5173`) |
| `npm run build` | Create production build in `dist/` |
| `npm run preview` | Preview the production build locally |
| `npm run lint` | Run ESLint across the codebase |
| `npm run test` | Run unit tests with Vitest |
### Executable Build (PyInstaller)

| Command | Description |
|---------|-------------|
| `python package_app.py` | Build the standalone `.exe` (recommended — handles pre-flight checks) |
| `python -m PyInstaller SQL_Query_Generator.spec --clean` | Build using the `.spec` file directly |

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
