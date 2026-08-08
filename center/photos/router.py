"""Приём и раздача фото взвешиваний (architecture §7 «Хранение фото»).

Приём (от агентов): ``POST /agents/photos/{weighing_uuid}/{role}`` —
токен агента, тело = JPEG как есть. Центр сверяет sha256 с метаданными,
зафиксированными при записи операции (контрольная сумма!), кладёт файл
по каноническому пути и один раз генерирует миниатюру для журнала.
Файл после сохранения неизменен (правило №2) — повторная загрузка
с тем же хешем идемпотентна, с другим — отвергается.

Раздача (интеграторам): ``GET /vesy/...`` — сервисный токен в
``Authorization`` + IP-allowlist; скачивания журналируются (audit_log).
В проде путь закрывается nginx'ом, этот маршрут — источник истины.
"""

import asyncio
import hashlib
import io
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Request, Response
from fastapi.responses import FileResponse
from PIL import Image
from sqlalchemy import select
from sqlalchemy.orm import Session

from center.db import repo
from center.db.models import AuditLog, Weighing, WeighingPhoto
from shared.enums import CameraRole

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]

THUMB_MAX_SIDE = 320  # ~30–50 КБ для списков журнала (architecture §7)
THUMB_QUALITY = 70
THUMB_SUFFIX = "_thumb"


@dataclass(frozen=True)
class PhotosConfig:
    """Хранилище и доступ интеграторов (значения из env, правило №7)."""

    photos_dir: Path
    # сервисные токены интеграторов: токен → имя (АИС, таможня, ...)
    service_tokens: dict[str, str] = field(default_factory=dict)
    # None — allowlist выключен (dev); в проде задаётся списком IP серверов
    allowed_ips: frozenset[str] | None = None


def create_photos_router(session_factory: SessionFactory, config: PhotosConfig) -> APIRouter:
    router = APIRouter()
    config.photos_dir.mkdir(parents=True, exist_ok=True)

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    def _file_path(db_path: str) -> Path:
        """Файловая система: /vesy/... → PHOTOS_DIR/vesy/... (без выхода наружу)."""
        relative = db_path.lstrip("/")
        full = (config.photos_dir / relative).resolve()
        if not full.is_relative_to(config.photos_dir.resolve()):
            raise ValueError(f"путь вне хранилища: {db_path}")
        return full

    def _find_photo(
        session: Session, weighing_uuid: UUID, role: CameraRole
    ) -> WeighingPhoto | None:
        return session.execute(
            select(WeighingPhoto)
            .join(Weighing, Weighing.id == WeighingPhoto.weighing_id)
            .where(Weighing.uuid == weighing_uuid)
            .where(WeighingPhoto.role == role)
        ).scalar_one_or_none()

    def _write_thumbnail(original: bytes, target: Path) -> None:
        """Миниатюра генерируется один раз при приёме (пересжатий оригинала нет)."""
        image: Image.Image = Image.open(io.BytesIO(original))
        image.thumbnail((THUMB_MAX_SIDE, THUMB_MAX_SIDE))
        if image.mode != "RGB":
            image = image.convert("RGB")
        image.save(target, "JPEG", quality=THUMB_QUALITY)

    # --- приём от агентов ---

    @router.post("/agents/photos/{weighing_uuid}/{role}")
    async def upload_photo(weighing_uuid: UUID, role: str, request: Request) -> Response:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        agent = None
        if token:
            agent = await asyncio.to_thread(_db, lambda s: repo.authenticate_agent(s, token))
        if agent is None:
            return Response(status_code=401)
        try:
            camera_role = CameraRole(role)
        except ValueError:
            return Response(status_code=404)

        photo = await asyncio.to_thread(_db, lambda s: _find_photo(s, weighing_uuid, camera_role))
        if photo is None:
            # запись ещё не дослана (или неизвестна) — агент повторит позже
            return Response(status_code=404)

        body = await request.body()
        digest = hashlib.sha256(body).hexdigest()
        if digest != photo.sha256:
            logger.error(
                "фото %s/%s отвергнуто: хеш не совпал с зафиксированным",
                weighing_uuid,
                role,
            )
            return Response(status_code=409)

        try:
            target = _file_path(photo.path)
        except ValueError:
            # путь в БД повреждён — данные требуют вмешательства администратора
            logger.error("недопустимый путь фото в БД: %s", photo.path)
            return Response(status_code=500)
        if target.exists():
            # идемпотентность со сверкой: битый файл (крах посреди записи)
            # лечится повторной загрузкой, целый — не трогается
            existing = hashlib.sha256(target.read_bytes()).hexdigest()
            if existing == photo.sha256:
                return Response(status_code=204)
            logger.warning("файл %s повреждён на диске — перезаписываем", photo.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # атомарно: временный файл + rename, частичных записей не остаётся
        tmp_target = target.with_name(target.name + ".part")
        tmp_target.write_bytes(body)  # JPEG как есть, байт-в-байт (правило №2)
        tmp_target.replace(target)
        try:
            thumb = target.with_name(target.stem + THUMB_SUFFIX + target.suffix)
            await asyncio.to_thread(_write_thumbnail, body, thumb)
        except Exception:
            logger.exception("не удалось построить миниатюру для %s", photo.path)
        logger.info("фото принято: %s (%d байт)", photo.path, len(body))
        return Response(status_code=204)

    # --- раздача интеграторам ---

    @router.get("/vesy/{file_path:path}")
    async def serve_photo(file_path: str, request: Request) -> Response:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        integrator = config.service_tokens.get(token) if token else None
        if integrator is None:
            return Response(status_code=401)
        client_ip = request.client.host if request.client else ""
        if config.allowed_ips is not None and client_ip not in config.allowed_ips:
            logger.warning("фото: запрос с непозволенного IP %s (%s)", client_ip, integrator)
            return Response(status_code=403)

        try:
            full = _file_path(f"/vesy/{file_path}")
        except ValueError:
            return Response(status_code=404)
        if not full.is_file():
            return Response(status_code=404)

        def audit(session: Session) -> None:
            session.add(
                AuditLog(
                    actor=f"integrator:{integrator}",
                    action="photo_download",
                    details={"path": f"/vesy/{file_path}", "ip": client_ip},
                )
            )
            session.commit()

        await asyncio.to_thread(_db, audit)
        return FileResponse(full, media_type="image/jpeg")

    return router
