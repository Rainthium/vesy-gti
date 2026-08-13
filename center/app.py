"""Сборка приложения центра: WS-сервер агентов + совместимый API v1.

Веб-панель диспетчера подключится сюда же (следующая задача).

Запуск в разработке:
    uv run uvicorn center.app:create_app --factory --port 8080
(нужен запущенный dev-postgres: docker compose up -d postgres,
и применённые миграции: uv run alembic upgrade head)
"""

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from center.agents_ws.hub import AgentHub
from center.agents_ws.router import create_agents_router
from center.api_v1.router import ApiV1Config, create_api_v1_router
from center.db.session import make_engine, make_session_factory
from center.photos.router import PhotosConfig, create_photos_router
from center.releases_router import create_releases_router
from center.web.router import create_panel_router

# dev-значения, с которыми боевой центр стартовать отказывается (правило №7)
_DEV_DEFAULTS = {
    "PANEL_SECRET": "dev-only-panel-secret",
    "AIS_PHOTO_TOKEN": "dev-only-ais-token",
}


def _check_production_env() -> None:
    """CENTER_ENV=production: секреты обязаны быть заданы явно.

    V1_USERNAME/V1_PASSWORD могут остаться admin/admin (совместимый приём
    запросов АИС, правило №7-исключение), но выбор должен быть явным —
    переменная обязана присутствовать в окружении.
    """
    problems = []
    if not os.environ.get("DATABASE_URL"):
        problems.append("DATABASE_URL не задан")
    for name, dev_value in _DEV_DEFAULTS.items():
        value = os.environ.get(name)
        if not value:
            problems.append(f"{name} не задан")
        elif value == dev_value:
            problems.append(f"{name} совпадает с dev-дефолтом")
    for name in ("V1_USERNAME", "V1_PASSWORD"):
        if not os.environ.get(name):
            problems.append(f"{name} не задан (admin допустим, но только явно)")
    if problems:
        raise RuntimeError("Отказ старта в проде (CENTER_ENV=production): " + "; ".join(problems))


def create_app() -> FastAPI:
    """Фабрика приложения центра (конфигурация — из переменных окружения)."""
    if os.environ.get("CENTER_ENV") == "production":
        _check_production_env()
    engine = make_engine()
    session_factory = make_session_factory(engine)
    hub = AgentHub()

    api_v1_config = ApiV1Config(
        legacy_username=os.environ.get("V1_USERNAME", "admin"),
        legacy_password=os.environ.get("V1_PASSWORD", "admin"),
        weigh_timeout_s=float(os.environ.get("V1_WEIGH_TIMEOUT_S", "120")),
    )

    # сервисные токены интеграторов (правило №7: значения только из env);
    # dev-значение по умолчанию — чтобы локальный стенд работал из коробки
    ais_token = os.environ.get("AIS_PHOTO_TOKEN", "dev-only-ais-token")
    allowed = os.environ.get("AIS_ALLOWED_IPS", "")
    photos_config = PhotosConfig(
        photos_dir=Path(os.environ.get("PHOTOS_DIR", "./photos_data")),
        service_tokens={ais_token: "ais-svh"},
        allowed_ips=frozenset(ip.strip() for ip in allowed.split(",") if ip.strip()) or None,
    )

    app = FastAPI(title="Весовая система — центр", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Живость процесса для healthcheck'ов compose и nginx (без похода в БД)."""
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        """Корень домена ведёт в панель (человек, набравший vesy.gti.kg)."""
        return RedirectResponse("/panel/", status_code=307)

    # сессии панели (подписанная cookie); секрет — из env, dev-дефолт для стенда.
    # lax, не strict: strict не отдаёт cookie при переходе по ссылке с другого
    # сайта (закладки/чаты/письма) — «выбивало на вход» (боевой урок 13.08.2026,
    # особенно Safari); POST-мутации lax по-прежнему не шлёт кросс-сайтово.
    # PANEL_COOKIE_SECURE=1 вешает флаг Secure — включать, только когда панель
    # доступна ИСКЛЮЧИТЕЛЬНО по https: вход по http (внутренний IP, dev-стенд)
    # с Secure-cookie перестаёт работать
    app.add_middleware(
        SessionMiddleware,
        secret_key=os.environ.get("PANEL_SECRET", "dev-only-panel-secret"),
        session_cookie="ves_center_session",
        same_site="lax",
        https_only=os.environ.get("PANEL_COOKIE_SECURE") == "1",
    )
    static_dir = Path(__file__).parent / "web" / "static"
    app.mount("/panel/static", StaticFiles(directory=str(static_dir)), name="panel-static")
    # каталог релизов агента (автообновление): архивы кладутся на ВМ
    releases_dir = Path(os.environ.get("AGENT_RELEASES_DIR", "./releases_data"))
    app.include_router(create_agents_router(hub, session_factory))
    app.include_router(create_api_v1_router(hub, session_factory, api_v1_config))
    app.include_router(create_photos_router(session_factory, photos_config))
    app.include_router(create_releases_router(session_factory, releases_dir))
    app.include_router(
        create_panel_router(
            session_factory, hub, photos_dir=photos_config.photos_dir, releases_dir=releases_dir
        )
    )
    # хаб доступен другим слоям (панель, сквозные тесты)
    app.state.hub = hub
    app.state.session_factory = session_factory
    return app
