"""Операции БД для серверной части центра (WS-сервер, позже API v1).

Все функции синхронные (SQLAlchemy Session); асинхронный код вызывает их
через ``asyncio.to_thread`` — объёмы малы (сотни строк в день).
"""

import hashlib
import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from center.db.models import (
    Agent,
    AgentStatus,
    Scale,
    TareRegistry,
    User,
    UserRole,
    Weighing,
    WeighingPhoto,
    weighing_checksum,
)
from shared.enums import CameraRole, ErrorCode, Operation
from shared.messages import OperatorRecord, PhotoMeta, TareRecord, WeighingRecord
from shared.tare import three_months_before

logger = logging.getLogger(__name__)


PHOTO_INDEX_BY_ROLE = {CameraRole.FRONT: 1, CameraRole.REAR: 2}


def canonical_photo_path(record: WeighingRecord, role: CameraRole) -> str:
    """Постоянный путь фото (architecture §4.1): /vesy/ГГГГ/ММ/ДД/<uuid>_photoN.jpeg.

    Дата — из weighed_at (UTC-момент записи); front → photo1, rear → photo2,
    как в UniServer.
    """
    moment = record.weighed_at or datetime.now(UTC)
    day = moment.astimezone(UTC).strftime("%Y/%m/%d")
    return f"/vesy/{day}/{record.uuid.hex}_photo{PHOTO_INDEX_BY_ROLE[role]}.jpeg"


def hash_agent_token(token: str) -> str:
    """Токен агента хранится только так (правило №7)."""
    return hashlib.sha256(token.encode()).hexdigest()


def authenticate_agent(session: Session, token: str) -> Agent | None:
    """Найти агента по токену; None — токен неизвестен."""
    return session.execute(
        select(Agent).where(Agent.token_hash == hash_agent_token(token))
    ).scalar_one_or_none()


def set_agent_status(
    session: Session, agent_id: int, status: AgentStatus, *, version: str | None = None
) -> None:
    """Обновить статус связи и время последней активности агента."""
    agent = session.get(Agent, agent_id)
    if agent is None:
        return
    agent.status = status
    agent.last_seen_at = datetime.now(UTC)
    if version is not None:
        agent.version = version
    session.commit()


def save_weighing_record(
    session: Session,
    scale_id: int,
    record: WeighingRecord,
    photos: list[PhotoMeta] | None = None,
    *,
    request_payload: dict[str, object] | None = None,
) -> bool:
    """Записать операцию в журнал центра; вернуть True, если запись новая.

    Идемпотентно по uuid: повтор досылки той же записи — не ошибка
    (False). Запись после вставки неизменяема (правило №2).

    Отказы (code != OK) НЕ сохраняются (решение Игоря 10.08.2026):
    с семантикой авторежима v0.2.0 неуспешная операция агентом просто
    не выполняется — веса и фото у отказа нет, код доходит до АИС живым
    ответом, а журнал состоит только из состоявшихся операций.
    """
    if record.code is not ErrorCode.OK:
        return False
    existing = session.execute(
        select(Weighing.id).where(Weighing.uuid == record.uuid)
    ).scalar_one_or_none()
    if existing is not None:
        return False

    tare_weighing_id = None
    if record.tare_weighing_uuid is not None:
        tare_weighing_id = session.execute(
            select(Weighing.id).where(Weighing.uuid == record.tare_weighing_uuid)
        ).scalar_one_or_none()

    photos = photos or []
    photo_paths = {photo.role: canonical_photo_path(record, photo.role) for photo in photos}
    checksum = weighing_checksum(
        uuid=record.uuid,
        operation=record.operation.value,
        code=record.code.value,
        massa=record.massa,
        weighed_at=record.weighed_at,
        vehicle_number=record.vehicle_number,
        source=record.source.value,
        photo_sha256s=[photo.sha256 for photo in photos],
    )
    row = Weighing(
        uuid=record.uuid,
        scale_id=scale_id,
        operation=record.operation,
        code=record.code,
        massa=record.massa,
        unit=record.unit,
        stable=record.stable,
        weighed_at=record.weighed_at,
        vehicle_number=record.vehicle_number,
        trailer_number=record.trailer_number,
        tare_weighing_id=tare_weighing_id,
        tare_value=record.tare_value,
        netto=record.netto,
        source=record.source,
        operator=record.operator,
        message=record.message,
        request_payload=request_payload,
        checksum=checksum,
    )
    session.add(row)
    session.flush()
    for photo in photos:
        session.add(
            WeighingPhoto(
                weighing_id=row.id,
                role=photo.role,
                # канонический путь формирует центр; имя файла агента не используется
                path=photo_paths[photo.role],
                sha256=photo.sha256,
                size_bytes=photo.size_bytes,
            )
        )
    # успешное тарирование обновляет единый реестр активных тар
    # (сюда доходят только code == OK — отказы отсеяны выше)
    if record.operation is Operation.TARING and record.vehicle_number and record.massa is not None:
        _upsert_tare(session, row, record)
    session.commit()
    return True


def _upsert_tare(session: Session, row: Weighing, record: WeighingRecord) -> None:
    """Обновить активную тару СЦЕПКИ (реестр — снимок, обновляем на месте).

    Ключ — пара голова+прицеп (решение 09.08.2026). Более раннее тарирование
    не затирает более позднее (досылка офлайн-пачек может идти не по порядку).
    """
    tared_at = record.weighed_at or datetime.now(UTC)
    statement = (
        pg_insert(TareRegistry)
        .values(
            vehicle_number=record.vehicle_number,
            trailer_number=record.trailer_number or "",
            weighing_id=row.id,
            tare_value=record.massa,
            tared_at=tared_at,
        )
        .on_conflict_do_update(
            index_elements=[TareRegistry.vehicle_number, TareRegistry.trailer_number],
            set_={"weighing_id": row.id, "tare_value": record.massa, "tared_at": tared_at},
            where=(TareRegistry.tared_at <= tared_at),
        )
    )
    session.execute(statement)


def load_tare_registry(session: Session, *, now: datetime | None = None) -> list[TareRecord]:
    """Снимок реестра действующих тар для репликации агентам (правило №4).

    Просроченные записи (старше 3 календарных месяцев) не реплицируются.
    """
    moment = now or datetime.now(UTC)
    threshold = three_months_before(moment)
    rows = session.execute(
        select(TareRegistry, Weighing.uuid)
        .join(Weighing, Weighing.id == TareRegistry.weighing_id)
        .where(TareRegistry.tared_at >= threshold)
    ).all()
    return [
        TareRecord(
            vehicle_number=tare.vehicle_number,
            trailer_number=tare.trailer_number or None,
            tare_value=tare.tare_value,
            tared_at=tare.tared_at,
            weighing_uuid=weighing_uuid,
        )
        for tare, weighing_uuid in rows
    ]


def load_operators_for_scale(session: Session, scale_id: int) -> list[OperatorRecord]:
    """Снимок операторов для реплики на агента весов (решение Игоря 10.08.2026).

    Операторы — учётки users с ролью operator: привязанные к объекту этих
    весов или без привязки (site_id NULL — работают на всех объектах).
    Отключённые учётки входят в снимок с is_active=False, чтобы агент
    заблокировал и офлайн-вход. Пароли — только хешами (правило №7).
    """
    scale = session.get(Scale, scale_id)
    if scale is None:
        return []
    rows = session.execute(
        select(User)
        .where(User.role == UserRole.OPERATOR)
        .where((User.site_id.is_(None)) | (User.site_id == scale.site_id))
        .order_by(User.login)
    ).scalars()
    return [
        OperatorRecord(
            login=user.login,
            pw_hash=user.pw_hash,
            full_name=user.full_name,
            is_active=user.is_active,
        )
        for user in rows
    ]


def find_active_tare(
    session: Session,
    vehicle_number: str,
    trailer_number: str | None = None,
    *,
    now: datetime | None = None,
) -> TareRecord | None:
    """Действующая тара СЦЕПКИ голова+прицеп (для расчёта нетто в API v1).

    Тара подставляется только при совпадении ОБОИХ номеров (решение
    09.08.2026); тарирование без прицепа действует только для машины
    без прицепа.
    """
    moment = now or datetime.now(UTC)
    row = session.execute(
        select(TareRegistry, Weighing.uuid)
        .join(Weighing, Weighing.id == TareRegistry.weighing_id)
        .where(TareRegistry.vehicle_number == vehicle_number)
        .where(TareRegistry.trailer_number == (trailer_number or ""))
        .where(TareRegistry.tared_at >= three_months_before(moment))
    ).one_or_none()
    if row is None:
        return None
    tare, weighing_uuid = row
    return TareRecord(
        vehicle_number=tare.vehicle_number,
        trailer_number=tare.trailer_number or None,
        tare_value=tare.tare_value,
        tared_at=tare.tared_at,
        weighing_uuid=weighing_uuid,
    )


__all__ = [
    "CameraRole",
    "authenticate_agent",
    "find_active_tare",
    "hash_agent_token",
    "load_tare_registry",
    "save_weighing_record",
    "set_agent_status",
]
