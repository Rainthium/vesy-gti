"""Модели БД центра (PostgreSQL, architecture §5).

Ключевые принципы:
- **Неизменяемость** (правило №2): weighings и weighing_photos не
  редактируются и не удаляются — на уровне БД это дополнительно
  закреплено триггерами в миграции (сторнирование — новой записью
  со ссылкой ``storno_of``).
- **Время** (правило №4а): все метки времени — TIMESTAMPTZ, хранение
  в UTC; бишкекское время — забота слоя представления.
- **Контрольная сумма**: sha256 канонической строки полей записи
  (см. ``weighing_checksum``) — фиксируется при вставке, связывает
  запись с фото и служит доказательством неизменности.

Схема «одни весы = один агент»: агент привязан к весам (scale_id,
уникально), а не к объекту — см. docs/decisions.md.
"""

import enum
import hashlib
from datetime import UTC, date, datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    return datetime.now(UTC)


class ScaleKind(enum.StrEnum):
    """Тип весов (architecture §5)."""

    STATIC = "static"
    DYNAMIC = "dynamic"
    PLATFORM = "platform"


class UserRole(enum.StrEnum):
    """Роли пользователей панели (architecture §4.3)."""

    ADMIN = "admin"
    DISPATCHER = "dispatcher"
    OPERATOR = "operator"


class AgentStatus(enum.StrEnum):
    """Состояние связи с агентом (для мониторинга)."""

    ONLINE = "online"
    OFFLINE = "offline"


class ReleaseChannel(enum.StrEnum):
    """Каналы раскатки агентов (architecture §7а)."""

    PILOT = "pilot"
    STABLE = "stable"


def _str_enum(enum_cls: type[enum.StrEnum], name: str) -> Enum:
    """Enum-колонка, хранящая строковые значения (а не имена констант)."""
    return Enum(
        enum_cls,
        name=name,
        values_callable=lambda cls: [member.value for member in cls],
        native_enum=False,
        length=32,
    )


class Site(Base):
    """Объект (СВХ/ПП): на одном объекте может быть несколько весов."""

    __tablename__ = "sites"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True)  # напр. "kyzyl-kyia"
    name: Mapped[str] = mapped_column(String(200))  # СВХ «Кызыл-Кыя»
    timezone: Mapped[str] = mapped_column(String(64), default="Asia/Bishkek")


class Scale(Base):
    """Весы: одни весы = один ПК = один агент (decisions 06.08.2026)."""

    __tablename__ = "scales"

    id: Mapped[int] = mapped_column(primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id"))
    name: Mapped[str] = mapped_column(String(200))  # «Весы SCS-80»
    kind: Mapped[ScaleKind] = mapped_column(_str_enum(ScaleKind, "scale_kind"))
    driver: Mapped[str] = mapped_column(String(32))  # "cas22", ...
    port_cfg: Mapped[dict[str, object] | None] = mapped_column(
        JSONB, default=None
    )  # порт/скорость и пр.
    thresholds: Mapped[dict[str, object] | None] = mapped_column(
        JSONB, default=None
    )  # пороги/таймауты
    # маршрутизация совместимого API v1: старый адрес UniServer → эти весы
    legacy_ip: Mapped[str | None] = mapped_column(String(45), default=None)
    legacy_port: Mapped[int | None] = mapped_column(default=None)
    legacy_autoscale: Mapped[int | None] = mapped_column(default=None)
    # свидетельство о поверке (одно на весы; печатается на весовой карточке
    # и реплицируется агенту в снимке настроек — офлайн-печать)
    verif_number: Mapped[str | None] = mapped_column(String(64), default=None)
    verif_date: Mapped[date | None] = mapped_column(Date, default=None)
    verif_until: Mapped[date | None] = mapped_column(Date, default=None)
    # маршрутизация контракта v2 (согласован 17.08.2026): «Специальный
    # идентификатор СВХ» из справочника АИС (строка с ведущими нулями, «0014»)
    # + номер весов на объекте («Авто весы 1/2») → эти весы
    ais_object: Mapped[str | None] = mapped_column(String(16), default=None)
    ais_scale_no: Mapped[int | None] = mapped_column(default=None)

    __table_args__ = (
        # уникальность legacy-маршрута только для заполненных маршрутов:
        # UNIQUE с NULLS DISTINCT пропускал бы дубли при autoscale IS NULL
        Index(
            "uq_scales_legacy_route",
            "legacy_ip",
            "legacy_port",
            "legacy_autoscale",
            unique=True,
            postgresql_where=text("legacy_ip IS NOT NULL"),
            postgresql_nulls_not_distinct=True,
        ),
        # одна пара «объект АИС + номер весов» — одни весы (маршрут v2)
        Index(
            "uq_scales_ais_route",
            "ais_object",
            "ais_scale_no",
            unique=True,
            postgresql_where=text("ais_object IS NOT NULL"),
        ),
    )


class Camera(Base):
    """Камера весов (front/rear)."""

    __tablename__ = "cameras"

    id: Mapped[int] = mapped_column(primary_key=True)
    scale_id: Mapped[int] = mapped_column(ForeignKey("scales.id"))
    role: Mapped[CameraRole] = mapped_column(_str_enum(CameraRole, "camera_role"))
    snapshot_url: Mapped[str | None] = mapped_column(Text, default=None)
    rtsp_url: Mapped[str | None] = mapped_column(Text, default=None)

    __table_args__ = (UniqueConstraint("scale_id", "role", name="uq_cameras_scale_role"),)


class Agent(Base):
    """Агент весового ПК; токен хранится только хешем."""

    __tablename__ = "agents"

    id: Mapped[int] = mapped_column(primary_key=True)
    scale_id: Mapped[int] = mapped_column(ForeignKey("scales.id"), unique=True)
    token_hash: Mapped[str] = mapped_column(String(128))  # sha256 токена
    version: Mapped[str | None] = mapped_column(String(32), default=None)
    channel: Mapped[ReleaseChannel] = mapped_column(
        _str_enum(ReleaseChannel, "release_channel"), default=ReleaseChannel.PILOT
    )
    status: Mapped[AgentStatus] = mapped_column(
        _str_enum(AgentStatus, "agent_status"), default=AgentStatus.OFFLINE
    )
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)


class AgentOperator(Base):
    """Снимок учётки весового ПК — как её видит сам агент (обратный канал).

    Заполняется целиком из ``operators_report`` (агент 0.4.14, запрос
    Игоря 14.08.2026): центр видит и учётки, заведённые на месте вручную
    (``from_center=False`` — CLI add-operator, аварийный доступ). Хеш
    пароля сюда не попадает никогда (правило №7).
    """

    __tablename__ = "agent_operators"

    scale_id: Mapped[int] = mapped_column(ForeignKey("scales.id"), primary_key=True)
    login: Mapped[str] = mapped_column(String(128), primary_key=True)
    full_name: Mapped[str] = mapped_column(String(200), default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    from_center: Mapped[bool] = mapped_column(Boolean, default=True)
    reported_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class User(Base):
    """Пользователь панели центра (и синхронизация операторов на агентов)."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    login: Mapped[str] = mapped_column(String(64), unique=True)
    pw_hash: Mapped[str] = mapped_column(String(256))
    full_name: Mapped[str] = mapped_column(String(200), default="")
    role: Mapped[UserRole] = mapped_column(_str_enum(UserRole, "user_role"))
    site_id: Mapped[int | None] = mapped_column(ForeignKey("sites.id"), default=None)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Weighing(Base):
    """Запись операции взвешивания/тарирования — неизменяема (правило №2).

    Сторнирование — новой записью с ``storno_of`` = id исходной.
    """

    __tablename__ = "weighings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    uuid: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), unique=True)
    scale_id: Mapped[int] = mapped_column(ForeignKey("scales.id"))
    operation: Mapped[Operation] = mapped_column(_str_enum(Operation, "operation"))
    code: Mapped[ErrorCode] = mapped_column(_str_enum(ErrorCode, "error_code"))
    massa: Mapped[float | None] = mapped_column(Float, default=None)
    unit: Mapped[str] = mapped_column(String(8), default="kg")
    stable: Mapped[bool] = mapped_column(Boolean, default=False)
    weighed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    vehicle_number: Mapped[str | None] = mapped_column(String(32), default=None)
    trailer_number: Mapped[str | None] = mapped_column(String(32), default=None)
    tare_weighing_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("weighings.id"), default=None
    )
    tare_value: Mapped[float | None] = mapped_column(Float, default=None)
    netto: Mapped[float | None] = mapped_column(Float, default=None)
    source: Mapped[WeighingSource] = mapped_column(_str_enum(WeighingSource, "weighing_source"))
    operator: Mapped[str | None] = mapped_column(String(200), default=None)
    message: Mapped[str | None] = mapped_column(Text, default=None)
    request_payload: Mapped[dict[str, object] | None] = mapped_column(
        JSONB, default=None
    )  # запрос АИС
    storno_of: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("weighings.id"), default=None
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    checksum: Mapped[str] = mapped_column(String(64))  # sha256, см. weighing_checksum()

    __table_args__ = (
        Index("ix_weighings_scale_created", "scale_id", "created_at"),
        Index("ix_weighings_vehicle", "vehicle_number"),
    )


class WeighingPhoto(Base):
    """Фото операции; файл на диске неизменен, sha256 связан с записью."""

    __tablename__ = "weighing_photos"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    weighing_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("weighings.id"))
    role: Mapped[CameraRole] = mapped_column(_str_enum(CameraRole, "camera_role"))
    path: Mapped[str] = mapped_column(Text)  # /vesy/ГГГГ/ММ/ДД/<uuid>_photoN.jpeg
    sha256: Mapped[str] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(BigInteger)

    __table_args__ = (UniqueConstraint("weighing_id", "role", name="uq_weighing_photos_role"),)


class WeighingAisRef(Base):
    """Номер документа АИС «СВХ» (``WEI…``/``TAR…``) у операции журнала.

    Контракт v2 (согласован 17.08.2026): номер — ключ идемпотентности команд
    (один документ АИС = одна операция) и обратная связь по офлайн-операциям
    (АИС заводит документ по событию и сообщает номер). Отдельная таблица,
    потому что запись ``weighings`` неизменяема (правило №2), а номер у
    офлайн-операции появляется позже неё.

    ``origin``: ``command`` — из команды v2 (пишется в одной транзакции с
    записью), ``callback`` — из обратного вызова АИС.
    """

    __tablename__ = "weighing_ais_refs"

    weighing_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("weighings.id"), primary_key=True
    )
    ais_ref: Mapped[str] = mapped_column(String(32), unique=True)
    origin: Mapped[str] = mapped_column(String(16))
    linked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )


class WeighingEvent(Base):
    """Outbox события в АИС «СВХ» (контракт v2, раздел 7).

    Строка появляется в одной транзакции с записью офлайн-операции; фоновый
    публикатор отправляет её в RabbitMQ с подтверждением брокера и ставит
    ``published_at``. Неотправленные строки видны панели и мониторингу;
    повторная публикация по кнопке — новая строка с тем же ``event_id``
    (он детерминирован по операции и типу события).
    """

    __tablename__ = "weighing_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    weighing_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("weighings.id"))
    event_type: Mapped[str] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)

    __table_args__ = (
        Index("ix_weighing_events_pending", "id", postgresql_where=text("published_at IS NULL")),
        Index("ix_weighing_events_weighing", "weighing_id"),
    )


class TareRegistry(Base):
    """Активная тара СЦЕПКИ — единый реестр, реплицируется агентам.

    Ключ — пара (голова, прицеп): смена прицепа не подставляет тару старой
    сцепки (решение Игоря 09.08.2026). Пустая строка = без прицепа
    (NULL в первичном ключе невозможен)."""

    __tablename__ = "tare_registry"

    vehicle_number: Mapped[str] = mapped_column(String(32), primary_key=True)
    trailer_number: Mapped[str] = mapped_column(
        String(32), primary_key=True, default="", server_default=""
    )
    weighing_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("weighings.id"))
    tare_value: Mapped[float] = mapped_column(Float)
    tared_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AuditLog(Base):
    """Журналирование команд и действий (architecture §7)."""

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    actor: Mapped[str] = mapped_column(String(200))  # пользователь/агент/сервис-токен
    action: Mapped[str] = mapped_column(String(64))
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    details: Mapped[dict[str, object] | None] = mapped_column(JSONB, default=None)


class MonitoringSeverity(enum.StrEnum):
    """Важность события мониторинга; OK — восстановление после проблемы."""

    DANGER = "danger"  # объект не работает (агент офлайн, индикатор молчит)
    WARNING = "warning"  # работает, но требует внимания (камера, очереди, диск)
    OK = "ok"  # проблема закрылась


class MonitoringEvent(Base):
    """Журнал переходов мониторинга: проблема появилась / закрылась.

    Пишется детекторами MonitoringService на ПЕРЕХОДАХ состояния (не
    каждый тик), читается экраном «События» панели и рассылается в
    Telegram (notified_at — отметка доставки; NULL у события старше
    окна доставки означает «уже не шлём»).
    """

    __tablename__ = "monitoring_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    scale_id: Mapped[int] = mapped_column(ForeignKey("scales.id"))
    kind: Mapped[str] = mapped_column(String(32))  # offline/no_data/camera/...
    severity: Mapped[MonitoringSeverity] = mapped_column(
        _str_enum(MonitoringSeverity, "monitoring_severity")
    )
    # полный текст с именами объекта и весов: событие самодостаточно
    # и в Telegram, и в журнале (имена на момент события, не сегодняшние)
    message: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), default=None)

    __table_args__ = (
        Index("ix_monitoring_events_created", "created_at"),
        # выборка недоставленных уведомлений (нотификатор опрашивает часто)
        Index(
            "ix_monitoring_events_unnotified",
            "id",
            postgresql_where=text("notified_at IS NULL"),
        ),
    )


class AgentRelease(Base):
    """Релизы агентов для автообновления (architecture §7а, этап 2)."""

    __tablename__ = "agent_releases"

    id: Mapped[int] = mapped_column(primary_key=True)
    version: Mapped[str] = mapped_column(String(32), unique=True)
    channel: Mapped[ReleaseChannel] = mapped_column(_str_enum(ReleaseChannel, "release_channel"))
    file_path: Mapped[str] = mapped_column(Text)
    sha256: Mapped[str] = mapped_column(String(64))
    released_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )


def weighing_checksum(
    *,
    uuid: UUID,
    operation: str,
    code: str,
    massa: float | None,
    weighed_at: datetime | None,
    vehicle_number: str | None,
    source: str,
    photo_sha256s: list[str],
) -> str:
    """Контрольная сумма записи: sha256 канонической строки ключевых полей.

    Связывает запись с фото (их sha256 входят в строку в порядке ролей).
    Формула фиксирована; менять её нельзя — проверка старых записей сломается.
    """
    if weighed_at is not None and weighed_at.tzinfo is None:
        # naive-время машинозависимо: один момент дал бы разные хеши
        # на агенте и в центре — проверка неизменности обесценилась бы
        raise ValueError("weighed_at должен быть timezone-aware")
    canonical = "|".join(
        [
            str(uuid),
            operation,
            code,
            "" if massa is None else f"{massa:.3f}",
            "" if weighed_at is None else weighed_at.astimezone(UTC).isoformat(),
            vehicle_number or "",
            source,
            *sorted(photo_sha256s),
        ]
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
