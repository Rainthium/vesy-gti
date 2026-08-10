"""HTTP-раздача релизов агентам (автообновление, решение 10.08.2026).

Скачивание — только с токеном агента (тот же Bearer, что у загрузки
фото): архив сборки не публичный. Имена файлов жёстко валидируются
(center/releases.py) — обход путей невозможен.
"""

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from center.db import repo
from center.releases import release_by_filename

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]


def create_releases_router(session_factory: SessionFactory, releases_dir: Path) -> APIRouter:
    router = APIRouter()

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    @router.get("/agents/releases/{filename}")
    async def download_release(filename: str, request: Request) -> FileResponse:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        agent = None
        if token:
            agent = await asyncio.to_thread(_db, lambda s: repo.authenticate_agent(s, token))
        if agent is None:
            raise HTTPException(status_code=401, detail="нужен токен агента")
        release = await asyncio.to_thread(release_by_filename, releases_dir, filename)
        if release is None:
            raise HTTPException(status_code=404, detail="релиз не найден")
        logger.info("агент весов %d скачивает релиз %s", agent.scale_id, filename)
        return FileResponse(release.path, media_type="application/zip", filename=filename)

    return router
