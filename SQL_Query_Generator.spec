# -*- mode: python ; coding: utf-8 -*-
# SQL Query Generator — PyInstaller Spec
# Rebuild with: pyinstaller SQL_Query_Generator.spec --clean

block_cipher = None

a = Analysis(
    ['H:\\sql query generator\\api.py'],
    pathex=['H:\\sql query generator'],
    binaries=[],
    datas=[
        ('H:\\sql query generator\\db_files\\metadata.json', 'db_files'),
        ('H:\\sql query generator\\frontend\\dist',          'frontend/dist'),
    ],
    hiddenimports=[
        # --- uvicorn internals ---
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.http.h11_impl',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        'uvicorn.lifespan.off',
        # --- fastapi / starlette ---
        'fastapi',
        'starlette',
        'starlette.routing',
        'starlette.middleware',
        'starlette.staticfiles',
        'starlette.responses',
        # --- anyio (async backend) ---
        'anyio',
        'anyio._backends._asyncio',
        'anyio._backends._trio',
        # --- psycopg2 (optional live DB feed) ---
        'psycopg2',
        'psycopg2._psycopg',
        'psycopg2.extensions',
        'psycopg2.extras',
        # --- cryptography (credential encryption) ---
        'cryptography',
        'cryptography.fernet',
        'cryptography.hazmat.primitives',
        'cryptography.hazmat.primitives.ciphers',
        'cryptography.hazmat.primitives.ciphers.algorithms',
        'cryptography.hazmat.primitives.ciphers.modes',
        'cryptography.hazmat.primitives.hashes',
        'cryptography.hazmat.backends',
        'cryptography.hazmat.backends.openssl',
        # --- Windows multiprocessing support ---
        'multiprocessing.util',
        'multiprocessing.spawn',
        # --- email (used by starlette internals) ---
        'email.mime.multipart',
        'email.mime.text',
        'email.mime.base',
        # --- pydantic ---
        'pydantic',
        'pydantic.v1',
        # --- h11 HTTP parser ---
        'h11',
        # --- stdlib modules that PyInstaller may miss ---
        'logging.config',
        'logging.handlers',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='SQL_Query_Generator',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,          # Keep True so startup errors are visible; set False for silent production
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
