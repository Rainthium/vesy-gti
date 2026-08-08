"""Сохранение снимков камер файлами на диске агента.

Общий кирпич ручного (manual.py) и автоматического (auto.py) режимов:
на удачные кадры прожигается оверлей (камера, дата/время, вес — как OSD
UniServer, см. cameras/overlay.py), затем байты пишутся на диск и
хешируются. Оверлей — однократно ЗДЕСЬ, до расчёта sha256: после
сохранения фото не пересжимается никогда (правило №2). Неудачные камеры
собираются списком ошибок (вызывающий код при них операцию отклоняет).
"""

import hashlib
from datetime import datetime
from pathlib import Path
from uuid import UUID

from agent.cameras.capture import CameraShot
from agent.cameras.overlay import OverlayInfo, burn_overlay
from agent.sync.storage import StoredPhoto


def store_shots(
    photos_dir: Path,
    record_uuid: UUID,
    weighed_at: datetime,
    shots: list[CameraShot],
    *,
    weight_kg: float | None = None,
) -> tuple[list[StoredPhoto], list[str]]:
    """Прожечь оверлей, сохранить удачные снимки файлами, собрать ошибки.

    ``weight_kg`` — зафиксированный вес для плашки (None — плашка без веса).
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
        jpeg = burn_overlay(
            shot.jpeg,
            OverlayInfo(role=shot.role, moment=weighed_at, weight_kg=weight_kg),
        )
        day_dir.mkdir(parents=True, exist_ok=True)
        path = day_dir / f"{record_uuid.hex}_photo{index}.jpeg"
        path.write_bytes(jpeg)
        photos.append(
            StoredPhoto(
                role=shot.role,
                path=str(path),
                sha256=hashlib.sha256(jpeg).hexdigest(),
                size_bytes=len(jpeg),
            )
        )
    return photos, errors
