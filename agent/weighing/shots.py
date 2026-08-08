"""Сохранение снимков камер файлами на диске агента.

Общий кирпич ручного (manual.py) и автоматического (auto.py) режимов:
удачные кадры пишутся байт-в-байт (правило №2 — после сохранения
не пересжимаются, sha256 фиксируется в записи), неудачные камеры
собираются списком ошибок для кода ERR_CAMERA.
"""

import hashlib
from datetime import datetime
from pathlib import Path
from uuid import UUID

from agent.cameras.capture import CameraShot
from agent.sync.storage import StoredPhoto


def store_shots(
    photos_dir: Path,
    record_uuid: UUID,
    weighed_at: datetime,
    shots: list[CameraShot],
) -> tuple[list[StoredPhoto], list[str]]:
    """Сохранить удачные снимки файлами (байты как есть), собрать ошибки.

    Структура каталогов — ГГГГ/ММ/ДД, имена ``<uuid.hex>_photoN.jpeg``
    (нумерация по порядку камер; канонические пути в центре всё равно
    формирует центр — см. decisions 08.08.2026).
    """
    photos: list[StoredPhoto] = []
    errors: list[str] = []
    day_dir = photos_dir / weighed_at.strftime("%Y/%m/%d")
    for index, shot in enumerate(shots, start=1):
        if not shot.ok or shot.jpeg is None:
            errors.append(shot.error or "камера недоступна")
            continue
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"{record_uuid.hex}_photo{index}.jpeg"
        path.write_bytes(shot.jpeg)
        photos.append(
            StoredPhoto(
                role=shot.role,
                path=str(path),
                sha256=hashlib.sha256(shot.jpeg).hexdigest(),
                size_bytes=len(shot.jpeg),
            )
        )
    return photos, errors
