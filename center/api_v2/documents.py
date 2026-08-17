"""Документ операции — контракт v2 с АИС «СВХ», раздел 5.

Один и тот же JSON уходит в ответе на команду, в событии RabbitMQ
``weighing.completed`` и по GET-запросам: документ строится ИЗ ЗАПИСИ
журнала (запись неизменяема — правило №2), центр ничего не досчитывает
поверх неё. Позже заполняются только сопутствующие поля: доступность
файлов фото, номер документа АИС у офлайн-операций (обратная связь) и,
как следствие, ``tare.ais_ref``.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from center.api_v1.schemas import bishkek_iso
from center.db import repo
from center.db.models import Scale, Site, Weighing, WeighingPhoto
from shared.card import card_number
from shared.enums import CameraRole, Operation
from shared.tare import three_months_before

PHOTO_KEY_BY_ROLE = {CameraRole.FRONT: "front", CameraRole.REAR: "rear"}


def _as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)


def _photo_available(photos_dir: Path, db_path: str) -> bool:
    """Файл снимка уже доставлен в центр (агент догружает его после записи)."""
    relative = db_path.lstrip("/")
    full = (photos_dir / relative).resolve()
    if not full.is_relative_to(photos_dir.resolve()):
        return False
    return full.is_file()


def _taring_block(
    session: Session, taring: Weighing | None, status: str, massa: float
) -> dict[str, Any]:
    """Вложение ``tare``: тарирование сцепки со статусом (контракт 5.3)."""
    ais_ref = None
    if taring is not None:
        ais_ref = repo.ais_refs_for(session, [taring.id]).get(taring.id)
    weighed_at = taring.weighed_at if taring is not None else None
    return {
        "status": status,
        "id": str(taring.uuid) if taring is not None else None,
        "card_number": (
            card_number(Operation.TARING, weighed_at) if weighed_at is not None else None
        ),
        "ais_ref": ais_ref,
        "tared_at": bishkek_iso(weighed_at),
        "massa": massa,
    }


def tare_block(session: Session, weighing: Weighing) -> dict[str, Any] | None:
    """Что система знает о таре сцепки на момент взвешивания (5.3).

    ``applied`` — тара из самой записи (по ней считали нетто; не меняется
    задним числом); иначе — последнее тарирование сцепки не позже момента
    взвешивания из журнала: ``expired``, если истекло к тому моменту (граница
    правила №4 — те же 3 календарных месяца, что печатает карта), либо
    ``not_applied`` (действовало, но не подставлено — офлайн-взвешивание с
    отставшей репликой реестра). Тарирований не было → None.
    """
    if weighing.operation is not Operation.WEIGHING:
        return None
    if weighing.tare_value is not None:
        applied = (
            session.get(Weighing, weighing.tare_weighing_id) if weighing.tare_weighing_id else None
        )
        return _taring_block(session, applied, "applied", weighing.tare_value)
    if not weighing.vehicle_number or weighing.weighed_at is None:
        return None
    latest = repo.latest_taring_as_of(
        session, weighing.vehicle_number, weighing.trailer_number, weighing.weighed_at
    )
    if latest is None or latest.weighed_at is None or latest.massa is None:
        return None
    expired = _as_utc(latest.weighed_at) < three_months_before(_as_utc(weighing.weighed_at))
    return _taring_block(session, latest, "expired" if expired else "not_applied", latest.massa)


def _verification(scale: Scale) -> dict[str, Any] | None:
    if not scale.verif_number:
        return None
    return {
        "number": scale.verif_number,
        "date": scale.verif_date.isoformat() if scale.verif_date else None,
        "valid_until": scale.verif_until.isoformat() if scale.verif_until else None,
    }


def build_document(session: Session, weighing: Weighing, *, photos_dir: Path) -> dict[str, Any]:
    """Документ операции по записи журнала (контракт v2, раздел 5)."""
    scale = session.get(Scale, weighing.scale_id)
    site = session.get(Site, scale.site_id) if scale is not None else None
    photos: dict[str, dict[str, Any] | None] = {"front": None, "rear": None}
    for photo in session.execute(
        select(WeighingPhoto).where(WeighingPhoto.weighing_id == weighing.id)
    ).scalars():
        key = PHOTO_KEY_BY_ROLE.get(photo.role)
        if key is None:
            continue
        photos[key] = {
            "url": photo.path,
            "sha256": photo.sha256,
            "available": _photo_available(photos_dir, photo.path),
        }
    ais_ref = repo.ais_refs_for(session, [weighing.id]).get(weighing.id)
    return {
        "id": str(weighing.uuid),
        "card_number": (
            card_number(weighing.operation, weighing.weighed_at)
            if weighing.weighed_at is not None
            else None
        ),
        "operation": weighing.operation.value,
        "source": weighing.source.value,
        "ais_ref": ais_ref,
        "site": (
            {"code": site.code, "name": site.name, "ais_object": scale.ais_object}
            if site is not None and scale is not None
            else None
        ),
        "scale": (
            {
                "id": scale.id,
                "no": scale.ais_scale_no,
                "name": scale.name,
                "verification": _verification(scale),
            }
            if scale is not None
            else None
        ),
        "weighed_at": bishkek_iso(weighing.weighed_at),
        "recorded_at": bishkek_iso(weighing.created_at),
        "vehicle_number": weighing.vehicle_number,
        "trailer_number": weighing.trailer_number,
        "operator": weighing.operator,
        "unit": weighing.unit,
        "massa": weighing.massa,
        "tare": tare_block(session, weighing),
        "netto": weighing.netto,
        "photos": photos,
        "checksum": weighing.checksum,
    }
