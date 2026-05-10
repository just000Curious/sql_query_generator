# Implementation Plan - Portable Windows Executable (.exe)

This plan details how to bundle the **SQL Query Generator** (FastAPI + React) into a single standalone Windows executable.

## User Review Required

> [!IMPORTANT]
> The application will run a local web server in the background and automatically open your default web browser (Chrome, Edge, etc.) to display the interface.

> [!NOTE]
> All database metadata (`metadata.json`) and frontend assets will be embedded inside the single `.exe` file.

## Proposed Changes

### 1. Frontend: Production Build
- **Action**: Build the React application for production.
- **Commands**: 
  ```bash
  cd frontend
  npm install
  npm run build
  ```
- **Result**: Creates a `frontend/dist` folder containing optimized HTML, CSS, and JS.

---

### 2. Backend: Integration & Portability
#### [MODIFY] [api.py](file:///g:/sql%20query%20generator/api.py)
- **Asset Path Handling**: Implement a helper to find files inside the PyInstaller temporary bundle (`_MEIPASS`).
- **Static File Serving**: 
  - Use `fastapi.staticfiles.StaticFiles` to serve the `frontend/dist` folder.
  - Add a route to serve `index.html` at the root `/`.
- **Automatic Launch**: Use the `webbrowser` module to open `http://127.0.0.1:8000` automatically when the server starts.
- **Production Mode**: Ensure `reload=False` and `debug=False` are set for the bundled version.

---

### 3. Packaging Process
#### [NEW] [package_app.py](file:///g:/sql%20query%20generator/package_app.py)
- Create a script to run PyInstaller with the following flags:
  - `--onefile`: Bundle everything into a single `.exe`.
  - `--noconsole`: Hide the command prompt window when the app runs.
  - `--add-data`: Include `db_files/metadata.json` and `frontend/dist`.
  - `--name`: Set the output name to `SQL_Query_Generator.exe`.

## Verification Plan

### Automated Steps
1. Execute `python package_app.py`.
2. Verify the existence of `dist/SQL_Query_Generator.exe`.

### Manual Testing
1. Launch the generated `.exe`.
2. Confirm the browser opens to the correct page.
3. Test schema selection and query generation to ensure internal paths (to `metadata.json`) are working.
