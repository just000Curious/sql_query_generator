"""
package_app.py
--------------
Builds SQL_Query_Generator.exe using PyInstaller.

Usage:
    python package_app.py

The script must be run from the project root (same directory as api.py).
Output: dist/SQL_Query_Generator.exe
"""

import subprocess
import sys
import os

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT_DIR     = os.path.dirname(os.path.abspath(__file__))
DIST_DIR     = os.path.join(ROOT_DIR, "dist")
METADATA_SRC = os.path.join(ROOT_DIR, "db_files", "metadata.json")
FRONTEND_SRC = os.path.join(ROOT_DIR, "frontend", "dist")
ENTRY_POINT  = os.path.join(ROOT_DIR, "api.py")
EXE_NAME     = "SQL_Query_Generator"


# ── Pre-flight checks ──────────────────────────────────────────────────────────
def check_prerequisites():
    errors = []

    if not os.path.isfile(METADATA_SRC):
        errors.append("  MISSING: db_files/metadata.json -> " + METADATA_SRC)

    if not os.path.isdir(FRONTEND_SRC):
        errors.append(
            "  MISSING: frontend/dist -> " + FRONTEND_SRC +
            "\n  Run:  cd frontend && npm install && npm run build"
        )
    elif not os.path.isfile(os.path.join(FRONTEND_SRC, "index.html")):
        errors.append("  MISSING: frontend/dist/index.html  -- rebuild the frontend")

    if errors:
        print("FAILED - Pre-flight checks:\n" + "\n".join(errors))
        sys.exit(1)

    print("OK - Pre-flight checks passed")


# ── Build ──────────────────────────────────────────────────────────────────────
def build():
    # On Windows the separator for --add-data is semicolon (;)
    sep = ";" if sys.platform == "win32" else ":"

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onefile",
        "--noconsole",
        "--name", EXE_NAME,
        # Bundle metadata.json -> db_files/ inside the exe
        "--add-data", "{}{}db_files".format(METADATA_SRC, sep),
        # Bundle the entire React build -> frontend/dist/ inside the exe
        "--add-data", "{}{}frontend/dist".format(FRONTEND_SRC, sep),
        # Hidden imports that PyInstaller sometimes misses with FastAPI/uvicorn
        "--hidden-import", "uvicorn.logging",
        "--hidden-import", "uvicorn.loops",
        "--hidden-import", "uvicorn.loops.auto",
        "--hidden-import", "uvicorn.protocols",
        "--hidden-import", "uvicorn.protocols.http",
        "--hidden-import", "uvicorn.protocols.http.auto",
        "--hidden-import", "uvicorn.protocols.websockets",
        "--hidden-import", "uvicorn.protocols.websockets.auto",
        "--hidden-import", "uvicorn.lifespan",
        "--hidden-import", "uvicorn.lifespan.on",
        "--hidden-import", "fastapi",
        "--hidden-import", "starlette",
        "--hidden-import", "anyio",
        "--hidden-import", "anyio._backends._asyncio",
        ENTRY_POINT,
    ]

    print("\nRunning PyInstaller ...")
    print("Command: " + " ".join(cmd))
    print()

    result = subprocess.run(cmd, cwd=ROOT_DIR)

    if result.returncode != 0:
        print("\nFAILED - PyInstaller failed (see output above)")
        sys.exit(result.returncode)


# ── Report ─────────────────────────────────────────────────────────────────────
def report():
    exe_path = os.path.join(DIST_DIR, "{}.exe".format(EXE_NAME))
    if os.path.isfile(exe_path):
        size_mb = os.path.getsize(exe_path) / (1024 * 1024)
        print("\n" + "=" * 60)
        print("BUILD SUCCESSFUL!")
        print("  Executable : " + exe_path)
        print("  Size       : {:.1f} MB".format(size_mb))
        print("=" * 60)
        print("\nDouble-click the .exe to launch.")
        print("Your browser will open automatically at http://127.0.0.1:8000")
        print("Crash logs : %USERPROFILE%\\SQL_Query_Generator_logs\\app.log\n")
    else:
        print("\nFAILED - Expected exe not found: " + exe_path)
        sys.exit(1)


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  SQL Query Generator -- PyInstaller Packager")
    print("=" * 60)

    check_prerequisites()
    build()
    report()
