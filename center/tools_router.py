"""HTTP-раздача инструментов агентам (0.4.33): GET /agents/tools/<файл>.

Только с токеном агента — тот же Bearer, что у релизов и загрузки фото
(center/releases_router.py). Контрольная сумма и размер уходят заголовками
``X-Sha256`` / ``X-Size-Bytes``: агент сверяет их, прежде чем положить файл
в корень установки (agent/ffmpeg_tool.py). Имена файлов жёстко валидируются
(center/tools.py) — обход путей невозможен.
"""

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from center.db import repo
from center.tools import tool_by_filename

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]


def create_tools_router(session_factory: SessionFactory, tools_dir: Path) -> APIRouter:
    router = APIRouter()

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    @router.get("/agents/tools/{filename}")
    async def download_tool(filename: str, request: Request) -> FileResponse:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        agent = None
        if token:
            agent = await asyncio.to_thread(_db, lambda s: repo.authenticate_agent(s, token))
        if agent is None:
            raise HTTPException(status_code=401, detail="нужен токен агента")
        tool = await asyncio.to_thread(tool_by_filename, tools_dir, filename)
        if tool is None:
            raise HTTPException(status_code=404, detail="инструмент не найден")
        logger.info(
            "агент весов %d скачивает инструмент %s (%d МБ)",
            agent.scale_id,
            filename,
            tool.size_bytes >> 20,
        )
        return FileResponse(
            tool.path,
            media_type="application/octet-stream",
            filename=filename,
            headers={"X-Sha256": tool.sha256, "X-Size-Bytes": str(tool.size_bytes)},
        )

    return router
