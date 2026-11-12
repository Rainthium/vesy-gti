"""События таможенной системе: идентификаторы, ключи и конверт (раздел 4).

Здесь только сборка сообщения по строке outbox ``customs_events``; транспорт
(подключение к брокеру Таргет-центра, публикатор) — отдельным модулем: у
учётной записи центра на том брокере нет права объявлять топологию.
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from center.api_v1.schemas import bishkek_iso
from center.customs.documents import build_customs_document, build_rejection, scale_kind
from center.db.models import CustomsEvent, Scale, Site, TareRejection, Weighing
from center.db.repo import CUSTOMS_WEIGHING_REJECTED
from center.events import OutgoingEvent

EVENT_VERSION = 1
PRODUCER = "vesy-center"
# своё пространство имён: event_id события таможне не совпадает с event_id
# события АИС «СВХ» по той же операции — потоки независимы
EVENT_NAMESPACE = uuid.UUID("b3e1d7c4-52a9-4f0b-8c6d-7a1e9f2b4d60")


def event_id_for(subject_uuid: uuid.UUID, event_type: str) -> str:
    """Идентификатор события: детерминирован по операции (или отказу) и типу.

    Повторная публикация того же события несёт тот же ``event_id`` — ключ
    дедупликации у потребителя (контракт, раздел 2).
    """
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{subject_uuid}:{event_type}"))


def routing_key_for(event_type: str, site_code: str) -> str:
    """``<type>.<site.code>`` — поток делится по объектам без разбора тела."""
    return f"{event_type}.{site_code}"


def build_customs_event(
    session: Session, outbox: CustomsEvent, *, photos_dir: Path, now: datetime
) -> OutgoingEvent | None:
    """Собрать сообщение по строке outbox; None — предмета события уже нет."""
    body_key: str
    body: dict[str, Any]
    if outbox.event_type == CUSTOMS_WEIGHING_REJECTED:
        rejection = session.get(TareRejection, outbox.rejection_id) if outbox.rejection_id else None
        if rejection is None:
            return None
        subject_uuid, scale_id = rejection.uuid, rejection.scale_id
        body_key, body = "rejection", build_rejection(session, rejection)
    else:
        weighing = session.get(Weighing, outbox.weighing_id) if outbox.weighing_id else None
        if weighing is None:
            return None
        subject_uuid, scale_id = weighing.uuid, weighing.scale_id
        body_key, body = (
            "weighing",
            build_customs_document(session, weighing, photos_dir=photos_dir),
        )
    scale = session.get(Scale, scale_id)
    site = session.get(Site, scale.site_id) if scale is not None else None
    if scale is None or site is None:
        return None
    event_id = event_id_for(subject_uuid, outbox.event_type)
    return OutgoingEvent(
        outbox_id=outbox.id,
        event_id=event_id,
        event_type=outbox.event_type,
        routing_key=routing_key_for(outbox.event_type, site.code),
        body={
            "event_id": event_id,
            "type": outbox.event_type,
            "version": EVENT_VERSION,
            "published_at": bishkek_iso(now),
            "producer": PRODUCER,
            "site": {"code": site.code, "name": site.name, "customs_code": site.customs_code},
            "scale_kind": scale_kind(scale),
            body_key: body,
        },
    )
