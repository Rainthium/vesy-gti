"""Модели сообщений протокола агент↔центр (WebSocket, architecture §4.2).

Каждое сообщение — JSON-объект с обязательным полем ``type`` (дискриминатор).
Фото в сообщениях не передаются — только метаданные; сами файлы уходят
отдельно (бинарные кадры либо HTTP-загрузка на центр).

Направления:
- агент → центр: hello, heartbeat, weigh_result, offline_sync;
- центр → агент: weigh_request, offline_sync_ack, tare_registry.

Состав полей — черновик v1 (этап 0); уточняется по мере реализации,
но существующие поля не переименовываются без записи в docs/decisions.md.
"""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, TypeAdapter

from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource

PROTOCOL_VERSION = 1


class CameraStatus(BaseModel):
    """Доступность одной камеры в самодиагностике."""

    role: CameraRole
    available: bool
    last_snapshot_at: datetime | None = None


class EquipmentStatus(BaseModel):
    """Самодиагностика агента: индикатор, камеры, очередь досылки (architecture §3.1)."""

    scale_status: ScaleStatus
    last_packet_at: datetime | None = None  # время последнего пакета с индикатора
    current_weight: float | None = None  # текущий вес, кг (None — нет данных)
    stable: bool | None = None  # флаг стабильности (None — нет данных)
    cameras: list[CameraStatus] = Field(default_factory=list)
    pending_sync_count: int = 0  # накоплено недосланных офлайн-записей


class PhotoMeta(BaseModel):
    """Метаданные снимка; сам файл передаётся отдельно."""

    role: CameraRole
    filename: str
    sha256: str
    size_bytes: int


class WeighingRecord(BaseModel):
    """Запись завершённой операции (взвешивание/тарирование).

    Используется в weigh_result и offline_sync. Запись неизменяема:
    после сохранения не редактируется (правило проекта №2).
    """

    uuid: UUID
    operation: Operation
    code: ErrorCode
    massa: float | None = None  # брутто или тара, кг; None — операция не дошла до фиксации
    unit: str = "kg"
    stable: bool = False
    weighed_at: datetime | None = None
    vehicle_number: str | None = None
    trailer_number: str | None = None
    tare_value: float | None = None  # подставленная тара, кг
    tare_weighing_uuid: UUID | None = None  # ссылка на операцию тарирования
    netto: float | None = None  # брутто − тара; None — нет действующей тары
    source: WeighingSource
    operator: str | None = None  # логин оператора при ручном режиме
    message: str | None = None  # детали при code != OK
    # метаданные снимков: едут с записью и в weigh_result, и в offline_sync;
    # сами файлы агент загружает отдельно по HTTP (см. decisions 08.08.2026)
    photos: list[PhotoMeta] = Field(default_factory=list)


class TareRecord(BaseModel):
    """Строка реестра тарирований — активная тара СЦЕПКИ (решение 09.08.2026:
    тара привязана к паре голова+прицеп; None — тарирование без прицепа)."""

    vehicle_number: str
    trailer_number: str | None = None
    tare_value: float
    tared_at: datetime
    weighing_uuid: UUID


# --- агент → центр ---


class Hello(BaseModel):
    """Первое сообщение после подключения: кто я и в каком состоянии."""

    type: Literal["hello"] = "hello"
    agent_id: str
    version: str  # версия агента
    protocol_version: int = PROTOCOL_VERSION
    driver: str  # имя драйвера индикатора (например, "cas22")
    equipment: EquipmentStatus


class Heartbeat(BaseModel):
    """Периодический статус оборудования; центр строит по нему мониторинг."""

    type: Literal["heartbeat"] = "heartbeat"
    agent_id: str
    sent_at: datetime
    equipment: EquipmentStatus


class WeighResult(BaseModel):
    """Результат операции по команде центра (фото — в record.photos)."""

    type: Literal["weigh_result"] = "weigh_result"
    request_id: UUID
    record: WeighingRecord


class OfflineSync(BaseModel):
    """Пакетная досылка операций, накопленных без связи с центром."""

    type: Literal["offline_sync"] = "offline_sync"
    agent_id: str
    records: list[WeighingRecord]


# --- центр → агент ---


class WeighRequest(BaseModel):
    """Команда провести операцию взвешивания/тарирования."""

    type: Literal["weigh_request"] = "weigh_request"
    request_id: UUID
    operation: Operation
    vehicle_number: str | None = None  # номер ТС, если известен инициатору
    trailer_number: str | None = None
    timeout_s: float | None = None  # тайм-аут всей операции; None — из конфига агента


class OfflineSyncAck(BaseModel):
    """Подтверждение приёма досылки: агент помечает записи synced."""

    type: Literal["offline_sync_ack"] = "offline_sync_ack"
    accepted_uuids: list[UUID]


class TareRegistryUpdate(BaseModel):
    """Полный снимок реестра тарирований (реплицируется целиком, architecture §3.3а)."""

    type: Literal["tare_registry"] = "tare_registry"
    records: list[TareRecord]


# --- дискриминированные объединения и разбор ---

AgentMessage = Annotated[
    Hello | Heartbeat | WeighResult | OfflineSync,
    Field(discriminator="type"),
]
CenterMessage = Annotated[
    WeighRequest | OfflineSyncAck | TareRegistryUpdate,
    Field(discriminator="type"),
]

_AGENT_ADAPTER: TypeAdapter[AgentMessage] = TypeAdapter(AgentMessage)
_CENTER_ADAPTER: TypeAdapter[CenterMessage] = TypeAdapter(CenterMessage)


def parse_agent_message(raw: str | bytes) -> AgentMessage:
    """Разобрать JSON-сообщение, пришедшее от агента."""
    return _AGENT_ADAPTER.validate_json(raw)


def parse_center_message(raw: str | bytes) -> CenterMessage:
    """Разобрать JSON-сообщение, пришедшее от центра."""
    return _CENTER_ADAPTER.validate_json(raw)
