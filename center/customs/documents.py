"""Документы для таможенной системы — контракт customs-events-v1, раздел 5.

Документ операции = документ контракта v2 с АИС «СВХ» плюс три поля,
нужные только таможне: ``site.customs_code``, ``scale.kind`` и
``weighed_at_utc``. Документ для АИС (``center.api_v2.documents``) при этом
не меняется ни на байт — поля добавляются поверх его копии здесь.

Тело отказа (``rejection``, раздел 5.5) — отклонённое тарирование: не
операция, строится из ``tare_rejections``.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from center.api_v1.schemas import bishkek_iso
from center.api_v2.documents import _verification, build_document
from center.db.models import Scale, ScaleKind, Site, TareRejection, Weighing
from shared.enums import Operation, WeighingSource


def weighed_at_utc(moment: datetime | None) -> str | None:
    """Момент фиксации веса строкой, входящей в ``checksum`` (приложение Б).

    Ровно то же выражение, что в ``weighing_checksum``: UTC, isoformat —
    микросекунды присутствуют, если они не нулевые. ``weighed_at`` документа
    округлён до секунды, поэтому без этой строки сумму не пересчитать.
    """
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat()


def _inserted_after(source: dict[str, Any], anchor: str, key: str, value: object) -> dict[str, Any]:
    """Копия словаря с новым ключом сразу после ``anchor`` (порядок полей контракта)."""
    result: dict[str, Any] = {}
    for name, item in source.items():
        result[name] = item
        if name == anchor:
            result[key] = value
    if key not in result:
        result[key] = value
    return result


def scale_kind(scale: Scale) -> str:
    """Вид весов по контракту: ``dynamic`` — поосные, всё остальное — ``static``.

    В справочнике центра есть ещё ``platform`` — для таможенной системы это
    те же статические весы.
    """
    return "dynamic" if scale.kind is ScaleKind.DYNAMIC else "static"


def scale_block(scale: Scale) -> dict[str, Any]:
    """Блок весов: как в документе v2 плюс вид весов (один на документ и отказ)."""
    return {
        "id": scale.id,
        "no": scale.ais_scale_no,
        "kind": scale_kind(scale),
        "name": scale.name,
        "verification": _verification(scale),
    }


def site_block(site: Site, scale: Scale) -> dict[str, Any]:
    """Блок объекта: как в документе v2 плюс код таможенного органа."""
    return {
        "code": site.code,
        "name": site.name,
        "customs_code": site.customs_code,
        "ais_object": scale.ais_object,
    }


def build_customs_document(
    session: Session, weighing: Weighing, *, photos_dir: Path
) -> dict[str, Any]:
    """Документ операции для таможенной системы (разделы 5.1–5.4)."""
    document = build_document(session, weighing, photos_dir=photos_dir)
    scale = session.get(Scale, weighing.scale_id)
    site = session.get(Site, scale.site_id) if scale is not None else None
    if site is not None and scale is not None:
        document["site"] = site_block(site, scale)
    if scale is not None and document.get("scale") is not None:
        document["scale"] = _inserted_after(document["scale"], "no", "kind", scale_kind(scale))
    return _inserted_after(
        document, "weighed_at", "weighed_at_utc", weighed_at_utc(weighing.weighed_at)
    )


def build_rejection(session: Session, rejection: TareRejection) -> dict[str, Any]:
    """Тело события ``weighing.rejected`` — отклонённое тарирование (5.5)."""
    scale = session.get(Scale, rejection.scale_id)
    site = session.get(Site, scale.site_id) if scale is not None else None
    return {
        "id": str(rejection.uuid),
        "operation": Operation.TARING.value,
        "code": rejection.code,
        "message": rejection.message,
        "rejected_at": bishkek_iso(rejection.rejected_at),
        "source": WeighingSource.AIS.value,
        "ais_ref": rejection.ais_ref,
        "site": site_block(site, scale) if site is not None and scale is not None else None,
        "scale": scale_block(scale) if scale is not None else None,
        "vehicle_number": rejection.vehicle_number,
        "trailer_number": rejection.trailer_number,
        "operator": rejection.operator,
        "tare_limit": rejection.tare_limit_kg,
        "massa": rejection.massa_kg,
    }
