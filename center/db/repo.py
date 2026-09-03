"""Операции БД для серверной части центра (WS-сервер, позже API v1).

Все функции синхронные (SQLAlchemy Session); асинхронный код вызывает их
через ``asyncio.to_thread`` — объёмы малы (сотни строк в день).
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from center.db.models import (
    Agent,
    AgentOperator,
    AgentStatus,
    Camera,
    MonitoringEvent,
    MonitoringSeverity,
    Scale,
    Site,
    TareRegistry,
    User,
    UserRole,
    Weighing,
    WeighingAisRef,
    WeighingEvent,
    WeighingPhoto,
    weighing_checksum,
)
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import (
    AgentOperatorInfo,
    CameraSettings,
    CycleSettings,
    OperatorRecord,
    PhotoMeta,
    ScaleSettingsPayload,
    TareRecord,
    VerificationInfo,
    WeighingRecord,
)
from shared.tare import three_months_before

logger = logging.getLogger(__name__)


PHOTO_INDEX_BY_ROLE = {CameraRole.FRONT: 1, CameraRole.REAR: 2}
# событие контракта v2 (раздел 7): состоявшаяся операция → АИС
EVENT_WEIGHING_COMPLETED = "weighing.completed"


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
    ais_ref: str | None = None,
) -> bool:
    """Записать операцию в журнал центра; вернуть True, если запись новая.

    Идемпотентно по uuid: повтор досылки той же записи — не ошибка
    (False). Запись после вставки неизменяема (правило №2).

    ``ais_ref`` — номер документа АИС из команды v2: пишется в
    ``weighing_ais_refs`` той же транзакцией, что и запись, — чтобы повтор
    команды АИС нашёл состоявшуюся операцию, даже если центр упадёт между
    сохранением и ответом (контракт v2, идемпотентность 4.5).

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
    if ais_ref:
        # номер уже закреплён за другой операцией (повтор АИС проскочил в окно
        # между тайм-аутом и поздним результатом) — запись сохраняем БЕЗ связки,
        # чтобы уникальный индекс не откатил транзакцию и не уронил цикл агента
        taken = session.execute(
            select(WeighingAisRef.weighing_id).where(WeighingAisRef.ais_ref == ais_ref)
        ).scalar_one_or_none()
        if taken is None:
            session.add(WeighingAisRef(weighing_id=row.id, ais_ref=ais_ref, origin="command"))
        else:
            logger.warning(
                "запись %s: номер АИС %s уже закреплён за операцией id=%s — сохраняем без связки",
                record.uuid,
                ais_ref,
                taken,
            )
    # успешное тарирование обновляет единый реестр активных тар
    # (сюда доходят только code == OK — отказы отсеяны выше)
    if record.operation is Operation.TARING and record.vehicle_number and record.massa is not None:
        _upsert_tare(session, row, record)
    # офлайн-операция выполнена без АИС — единственный путь доставить её в АИС
    # событие (контракт v2, раздел 7): outbox в той же транзакции, что и запись.
    # Событие ставится ТОЛЬКО весам с привязкой АИС (ais_object): поток
    # weighing.completed.* принадлежит АИС «СВХ», события непривязанных весов
    # им не публикуются (вопрос Игоря 20.08.2026; будущая таможенная
    # интеграция получит СВОЙ event_type со своими правилами — см. decisions)
    if record.source is WeighingSource.LOCAL_OFFLINE:
        scale = session.get(Scale, scale_id)
        if scale is not None and scale.ais_object:
            session.add(WeighingEvent(weighing_id=row.id, event_type=EVENT_WEIGHING_COMPLETED))
        else:
            logger.info(
                "запись %s: весы id=%d не привязаны к АИС — событие weighing.completed не ставится",
                record.uuid,
                scale_id,
            )
    session.commit()
    return True


def scale_title(session: Session, scale_id: int) -> str:
    """«Объект · весы» для сообщений о весах (или «весы N», если не нашлись)."""
    scale = session.get(Scale, scale_id)
    site = session.get(Site, scale.site_id) if scale is not None else None
    if site is not None and scale is not None:
        return f"{site.name} · {scale.name}"
    return f"весы {scale_id}"


def record_update_event(
    session: Session,
    scale_id: int,
    message: str,
    *,
    severity: MonitoringSeverity = MonitoringSeverity.WARNING,
    kind: str = "update_failed",
    commit: bool = True,
) -> None:
    """Событие мониторинга об автообновлении агента (без антидребезга):
    отказ/откат — warning, успешная установка — ok. Видно в «Событиях» и
    уходит в Telegram; раньше исход жил только в логах."""
    session.add(
        MonitoringEvent(
            scale_id=scale_id,
            kind=kind,
            severity=severity,
            message=f"{scale_title(session, scale_id)}: {message}",
        )
    )
    if commit:
        session.commit()


def record_update_failure(session: Session, scale_id: int, version: str, error: str) -> None:
    """Отказ автообновления агента → событие мониторинга (warning).

    Успех обновления виден по смене версии; отказ раньше жил только в логе
    центра и agent.log — теперь он в «Событиях» и уходит в Telegram.
    """
    record_update_event(
        session, scale_id, f"автообновление агента до {version} не выполнено — {error}"
    )


# --- контракт v2 с АИС «СВХ» (согласован 17.08.2026) ---


def enqueue_weighing_event(
    session: Session, weighing: Weighing, event_type: str = EVENT_WEIGHING_COMPLETED
) -> WeighingEvent:
    """Поставить событие операции в outbox повторно (кнопка «переотправить»).

    Не коммитит: вызывающий добавляет запись аудита и коммитит одной
    транзакцией.
    """
    event = WeighingEvent(weighing_id=weighing.id, event_type=event_type)
    session.add(event)
    session.flush()
    return event


def pending_weighing_events(session: Session, limit: int = 50) -> list[WeighingEvent]:
    """Неотправленные события в порядке постановки (для публикатора)."""
    return list(
        session.execute(
            select(WeighingEvent)
            .where(WeighingEvent.published_at.is_(None))
            .order_by(WeighingEvent.id)
            .limit(limit)
        ).scalars()
    )


def mark_weighing_event_published(
    session: Session, event_id: int, at: datetime, *, note: str | None = None
) -> None:
    """Отметить отправку; ``note`` — причина закрытия без отправки (остаётся в last_error)."""
    event = session.get(WeighingEvent, event_id)
    if event is None:
        return
    event.published_at = at
    event.attempts += 1
    event.last_error = note
    session.commit()


def mark_weighing_event_failed(session: Session, event_id: int, error: str) -> None:
    event = session.get(WeighingEvent, event_id)
    if event is None:
        return
    event.attempts += 1
    event.last_error = error[:500]
    session.commit()


def pending_event_stats(session: Session) -> dict[int, tuple[int, datetime]]:
    """Неотправленные события по весам: {scale_id: (сколько, самое старое created_at)}.

    Для счётчика дашборда и детектора мониторинга «события в АИС не уходят».
    """
    rows = session.execute(
        select(
            Weighing.scale_id,
            func.count(WeighingEvent.id),
            func.min(WeighingEvent.created_at),
        )
        .join(Weighing, Weighing.id == WeighingEvent.weighing_id)
        .where(WeighingEvent.published_at.is_(None))
        .group_by(Weighing.scale_id)
    ).all()
    return {scale_id: (int(count), oldest) for scale_id, count, oldest in rows}


def latest_weighing_event(session: Session, weighing_id: int) -> WeighingEvent | None:
    """Последнее событие операции (для страницы записи: отправлено / в очереди / ошибка)."""
    return session.execute(
        select(WeighingEvent)
        .where(WeighingEvent.weighing_id == weighing_id)
        .order_by(WeighingEvent.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def find_scale_by_ais_route(session: Session, ais_object: str, scale_no: int) -> Scale | None:
    """Весы по паре «Специальный идентификатор СВХ + № весов на объекте».

    Привязка живёт в справочнике центра (правится в панели); непривязанная
    пара → 404 ERR_UNKNOWN_SCALE у API v2.
    """
    return session.execute(
        select(Scale).where(Scale.ais_object == ais_object, Scale.ais_scale_no == scale_no)
    ).scalar_one_or_none()


def weighing_by_ais_ref(session: Session, ais_ref: str) -> Weighing | None:
    """Операция, за которой закреплён номер документа АИС (или None)."""
    return session.execute(
        select(Weighing)
        .join(WeighingAisRef, WeighingAisRef.weighing_id == Weighing.id)
        .where(WeighingAisRef.ais_ref == ais_ref)
    ).scalar_one_or_none()


def ais_refs_for(session: Session, weighing_ids: list[int]) -> dict[int, str]:
    """Номера документов АИС для набора операций: {weighing_id: ais_ref}."""
    if not weighing_ids:
        return {}
    rows = session.execute(
        select(WeighingAisRef.weighing_id, WeighingAisRef.ais_ref).where(
            WeighingAisRef.weighing_id.in_(weighing_ids)
        )
    ).all()
    return {weighing_id: ref for weighing_id, ref in rows}


def link_ais_ref(session: Session, weighing: Weighing, ais_ref: str, *, origin: str) -> str:
    """Закрепить номер документа АИС за операцией (обратная связь 7.5).

    Возвращает ``"linked"`` (новая связь), ``"same"`` (уже этот номер) или
    ``"conflict"`` (у операции другой номер либо номер занят другой
    операцией) — 409 ERR_ALREADY_LINKED у API v2. Запись при этом не
    меняется: связь хранится рядом с ней (правило №2).
    """
    existing = session.get(WeighingAisRef, weighing.id)
    if existing is not None:
        return "same" if existing.ais_ref == ais_ref else "conflict"
    taken = session.execute(
        select(WeighingAisRef.weighing_id).where(WeighingAisRef.ais_ref == ais_ref)
    ).scalar_one_or_none()
    if taken is not None:
        return "conflict"
    session.add(WeighingAisRef(weighing_id=weighing.id, ais_ref=ais_ref, origin=origin))
    try:
        session.commit()
    except IntegrityError:  # два одновременных обратных вызова с одним номером
        session.rollback()
        return "conflict"
    return "linked"


def latest_taring_as_of(
    session: Session, vehicle_number: str, trailer_number: str | None, moment: datetime
) -> Weighing | None:
    """Последнее состоявшееся тарирование СЦЕПКИ не позже момента ``moment``.

    Для документа операции v2 (вложение ``tare`` со статусом): что система
    знала о таре сцепки на момент взвешивания. Ищется по журналу, а не по
    реестру (реестр хранит только последнее тарирование вообще — после
    перетарирования он уже не скажет, какая тара действовала тогда).
    """
    trailer = (trailer_number or "").strip().upper() or None
    query = (
        select(Weighing)
        .where(
            Weighing.operation == Operation.TARING,
            Weighing.code == ErrorCode.OK,
            Weighing.vehicle_number == vehicle_number,
            Weighing.weighed_at.is_not(None),
            Weighing.weighed_at <= moment,
        )
        .order_by(Weighing.weighed_at.desc(), Weighing.id.desc())
        .limit(1)
    )
    if trailer is None:
        # «без прицепа» в журнале — NULL (агент нормализует пустую строку в None);
        # пустую строку тоже принимаем, чтобы не зависеть от источника записи
        query = query.where(or_(Weighing.trailer_number.is_(None), Weighing.trailer_number == ""))
    else:
        query = query.where(Weighing.trailer_number == trailer)
    return session.execute(query).scalar_one_or_none()


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


def load_tare_registry(session: Session) -> list[TareRecord]:
    """Снимок реестра тар для репликации агентам — ЦЕЛИКОМ, без фильтра срока.

    Правило №4 держит читающая сторона (find_active_tare агента проверяет
    границу на каждой подстановке), а устаревшие строки нужны агенту для
    примечаний «почему нет нетто» на карте и подсказок оператору (14.08.2026):
    с фильтром агентская печать теряла дату устаревшего тарирования после
    первого же обновления реплики. Реестр — одна строка на сцепку, размер
    ограничен парком машин.
    """
    rows = session.execute(
        select(TareRegistry, Weighing.uuid).join(Weighing, Weighing.id == TareRegistry.weighing_id)
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


def agent_versions(session: Session) -> dict[int, str | None]:
    """Версии агентов по scale_id (для гейта снимков с секретами)."""
    return {agent.scale_id: agent.version for agent in session.execute(select(Agent)).scalars()}


def replace_agent_operators(
    session: Session, scale_id: int, records: list[AgentOperatorInfo]
) -> None:
    """Заменить снимок учёток весового ПК целиком (operators_report).

    Отчёт — полный список, поэтому пропавшие логины удаляются вместе
    со старым снимком. Длины срезаются до колонок: логины CLI агента
    не проходят белый список панели и бывают любыми.
    """
    now = datetime.now(UTC)
    session.execute(delete(AgentOperator).where(AgentOperator.scale_id == scale_id))
    for record in records:
        session.add(
            AgentOperator(
                scale_id=scale_id,
                login=record.login[:128],
                full_name=record.full_name[:200],
                is_active=record.is_active,
                from_center=record.from_center,
                reported_at=now,
            )
        )
    session.commit()


def load_scale_settings(session: Session, scale_id: int) -> ScaleSettingsPayload | None:
    """Снимок настроек весов для доставки агенту (решение Игоря 10.08.2026).

    Источники: scales.thresholds (параметры цикла, полный набор — пишется
    страницей настроек), scales.port_cfg ({"port", "baudrate"}), камеры
    из справочника. None — в центре ничего не задано, агент живёт
    по локальному конфигу.
    """
    scale = session.get(Scale, scale_id)
    if scale is None:
        return None
    cycle = None
    if scale.thresholds:
        try:
            cycle = CycleSettings.model_validate(scale.thresholds)
        except ValueError:
            logger.warning("весы %d: thresholds в БД не разбираются — пропущены", scale_id)
    port = None
    baudrate = None
    if scale.port_cfg:
        raw_port = scale.port_cfg.get("port")
        raw_baudrate = scale.port_cfg.get("baudrate")
        port = str(raw_port) if raw_port else None
        baudrate = int(raw_baudrate) if isinstance(raw_baudrate, int) else None
    camera_rows = list(
        session.execute(
            select(Camera).where(Camera.scale_id == scale_id).order_by(Camera.role)
        ).scalars()
    )
    cameras = [
        CameraSettings(
            role=c.role,
            snapshot_url=c.snapshot_url,
            rtsp_url=c.rtsp_url,
            preview_url=c.preview_url,
        )
        for c in camera_rows
        if c.snapshot_url or c.rtsp_url
    ] or None
    # поверка — для офлайн-печати весовой карточки на агенте
    verification = None
    if scale.verif_number:
        verification = VerificationInfo(
            number=scale.verif_number,
            verified_on=scale.verif_date,
            valid_until=scale.verif_until,
        )
    indicator_model = scale.indicator_model or None
    # срок хранения локальных фото (0.4.25): 0 — «не убирать», это тоже
    # управление; NULL — локальный конфиг агента
    photo_retention_days = scale.photo_retention_days
    # ручной режим при связи (0.4.28): в снимок едет ВСЕГДА (False тоже
    # управление — снимает разрешение); один лишь True — уже снимок.
    # Оговорка: у весов вообще без настроек снятый флаг снимка не даёт и
    # до агента не доедет — через панель недостижимо (форма всегда пишет
    # пороги цикла), при правке БД руками — пересохранить настройки в панели
    manual_allowed = bool(scale.manual_allowed)
    if (
        cycle is None
        and port is None
        and cameras is None
        and verification is None
        and indicator_model is None
        and photo_retention_days is None
        and not manual_allowed
    ):
        return None
    return ScaleSettingsPayload(
        cycle=cycle,
        cameras=cameras,
        scale_port=port,
        baudrate=baudrate,
        verification=verification,
        indicator_model=indicator_model,
        photo_retention_days=photo_retention_days,
        manual_allowed=manual_allowed,
    )


@dataclass(frozen=True)
class TareHint:
    """Подсказка агенту: искал ли центр тару и что нашёл (WeighRequest.tare*)."""

    tare: TareRecord | None
    resolved: bool


def resolve_tare_hint(
    session: Session, operation: Operation, vehicle_number: str | None, trailer_number: str | None
) -> TareHint:
    """Действующая тара сцепки для команды агенту (17.08.2026).

    Реестр центра авторитетен, реплика на весовом ПК могла отстать: агент
    0.4.17 при ``resolved`` берёт тару из команды (в том числе «нет тары»),
    старые агенты поле не знают и подставляют по реплике, как раньше.
    Тарированию тара не нужна — ``resolved`` False.
    """
    if operation is not Operation.WEIGHING or not vehicle_number:
        return TareHint(tare=None, resolved=False)
    return TareHint(tare=find_active_tare(session, vehicle_number, trailer_number), resolved=True)


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
