"""Сборка приложения центра: WS-сервер агентов + совместимый API v1.

Веб-панель диспетчера подключится сюда же (следующая задача).

Запуск в разработке:
    uv run uvicorn center.app:create_app --factory --port 8080
(нужен запущенный dev-postgres: docker compose up -d postgres,
и применённые миграции: uv run alembic upgrade head)
"""

import os

from fastapi import FastAPI

from center.agents_ws.hub import AgentHub
from center.agents_ws.router import create_agents_router
from center.api_v1.router import ApiV1Config, create_api_v1_router
from center.db.session import make_engine, make_session_factory


def create_app() -> FastAPI:
    """Фабрика приложения центра (конфигурация — из переменных окружения)."""
    engine = make_engine()
    session_factory = make_session_factory(engine)
    hub = AgentHub()

    api_v1_config = ApiV1Config(
        legacy_username=os.environ.get("V1_USERNAME", "admin"),
        legacy_password=os.environ.get("V1_PASSWORD", "admin"),
        weigh_timeout_s=float(os.environ.get("V1_WEIGH_TIMEOUT_S", "120")),
    )

    app = FastAPI(title="Весовая система — центр", docs_url=None, redoc_url=None)
    app.include_router(create_agents_router(hub, session_factory))
    app.include_router(create_api_v1_router(hub, session_factory, api_v1_config))
    # хаб доступен другим слоям (панель, сквозные тесты)
    app.state.hub = hub
    app.state.session_factory = session_factory
    return app
