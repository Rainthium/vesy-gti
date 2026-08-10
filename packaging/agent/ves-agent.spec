# Спецификация PyInstaller для ves-agent.exe (Windows).
#
# Режим onedir (папка app/ с exe и _internal/): служба стартует сразу,
# без распаковки onefile во %TEMP% — под системной учёткой nssm это
# источник проблем. Обновление — заменой папки целиком (update.bat).
#
# Сборка (из корня репозитория):
#   uv run --with pyinstaller pyinstaller packaging/agent/ves-agent.spec --noconfirm
#
# Ресурсы кладутся по тем же относительным путям, по которым модули ищут
# их через Path(__file__).parent — agent/web/app.py и agent/cameras/overlay.py.

from pathlib import Path

ROOT = Path(SPECPATH).resolve().parents[1]  # noqa: F821 — SPECPATH даёт PyInstaller

a = Analysis(  # noqa: F821
    [str(ROOT / "packaging" / "agent" / "entry.py")],
    pathex=[str(ROOT)],
    datas=[
        (str(ROOT / "agent" / "web" / "templates"), "agent/web/templates"),
        (str(ROOT / "agent" / "web" / "static"), "agent/web/static"),
        (str(ROOT / "agent" / "cameras" / "fonts"), "agent/cameras/fonts"),
    ],
    # uvicorn выбирает реализации динамически (loops/protocols/lifespan) —
    # перечисляем явно, чтобы не зависеть от полноты hook'ов contrib
    hiddenimports=[
        "uvicorn.logging",
        "uvicorn.loops",
        "uvicorn.loops.auto",
        "uvicorn.loops.asyncio",
        "uvicorn.protocols",
        "uvicorn.protocols.http",
        "uvicorn.protocols.http.auto",
        "uvicorn.protocols.http.h11_impl",
        "uvicorn.protocols.http.httptools_impl",
        "uvicorn.protocols.websockets",
        "uvicorn.protocols.websockets.auto",
        "uvicorn.protocols.websockets.websockets_impl",
        "uvicorn.lifespan",
        "uvicorn.lifespan.on",
        "uvicorn.lifespan.off",
    ],
    hookspath=[],
    # изолированный режим замороженного Python игнорирует PYTHONUTF8 —
    # UTF-8 для stdout/stderr включает runtime-хук
    runtime_hooks=[str(ROOT / "packaging" / "agent" / "rthook_utf8.py")],
    # центр и его зависимости в агенте не нужны (Analysis их и так не тянет,
    # excludes — страховка от случайного захвата через общие импорты)
    excludes=["center", "psycopg", "alembic"],
    noarchive=False,
)

pyz = PYZ(a.pure)  # noqa: F821

exe = EXE(  # noqa: F821
    pyz,
    a.scripts,
    exclude_binaries=True,
    name="ves-agent",
    console=True,  # консольное приложение: логи в stdout (nssm пишет в файл), getpass для add-operator
    icon=None,
)

coll = COLLECT(  # noqa: F821
    exe,
    a.binaries,
    a.datas,
    name="ves-agent",
)
