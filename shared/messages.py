"""Модели сообщений протокола агент↔центр (WebSocket, architecture §4.2).

Каждое сообщение — JSON-объект с обязательным полем ``type`` (дискриминатор).
Фото в сообщениях не передаются — только метаданные; сами файлы уходят
отдельно (бинарные кадры либо HTTP-загрузка на центр).

Направления:
- агент → центр: hello, heartbeat, weigh_result, offline_sync, update_status,
  config_status, operators_report;
- центр → агент: weigh_request, offline_sync_ack, tare_registry,
  operators_registry, scale_config, update_command.

Состав полей — черновик v1 (этап 0); уточняется по мере реализации,
но существующие поля не переименовываются без записи в docs/decisions.md.
"""

from datetime import date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, TypeAdapter

from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource

PROTOCOL_VERSION = 1

# Потолок строк в одном ответе с журналом: сообщение WS не должно
# распухать (строка лога бывает длинной)
MAX_LOG_TAIL_LINES = 500

# Минимальная версия агента, понимающая снимки с секретами
# (operators_registry с pw_hash, scale_config с URL камер). Более старым
# агентам центр их НЕ шлёт: старый ws_client логирует незнакомые сообщения
# первыми 200 символами — секреты попали бы в лог весового ПК (правило №7).
SECURE_SYNC_MIN_VERSION = (0, 4, 0)


# Минимальная версия агента, умеющая присылать хвост своего журнала
# по запросу центра (удалённая диагностика без захода в сеть объекта).
LOG_TAIL_MIN_VERSION = (0, 4, 5)


def _version_tuple(version: str | None) -> tuple[int, ...] | None:
    if not version:
        return None
    try:
        return tuple(int(p) for p in version.split(".")[:3])
    except ValueError:
        return None


def supports_secure_sync(version: str | None) -> bool:
    """Можно ли агенту этой версии слать снимки с секретами."""
    parts = _version_tuple(version)
    return parts is not None and parts >= SECURE_SYNC_MIN_VERSION


def supports_log_tail(version: str | None) -> bool:
    """Умеет ли агент этой версии отдавать хвост журнала по запросу."""
    parts = _version_tuple(version)
    return parts is not None and parts >= LOG_TAIL_MIN_VERSION


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
    # метрики мониторинга (13.08.2026, агент 0.4.13). None — агент старой
    # версии их не шлёт: центр отличает «нет данных» от нуля и детекторы
    # по таким весам просто молчат
    pending_photos_count: int | None = None  # снимки, ещё не загруженные на центр
    disk_free_mb: int | None = None  # свободно на диске с фото весового ПК, МБ


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
    # номер документа АИС «СВХ» (WEI…/TAR…) из команды v2 (агент 0.4.17): едет
    # с записью любым путём — weigh_result, offline_sync после разрыва, после
    # рестарта центра, — центр закрепляет его за записью одной транзакцией
    # (идемпотентность контракта v2). У офлайн-операций и команд v1 — None
    ais_ref: str | None = None


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


class UpdateStatus(BaseModel):
    """Отчёт агента о ходе автообновления (для логов и панели центра)."""

    type: Literal["update_status"] = "update_status"
    agent_id: str
    version: str  # целевая версия из команды
    ok: bool  # True — обновление запущено (перезапуск службы пошёл)
    error: str | None = None


class AgentOperatorInfo(BaseModel):
    """Учётка на весовом ПК — как её видит сам агент (обратный канал).

    Хеша пароля здесь НЕТ намеренно: центру он не нужен, а гонять
    секреты лишний раз нельзя (правило №7). ``from_center=False`` —
    учётка заведена на месте вручную (CLI add-operator, аварийный
    доступ); в центре такой учётки нет.
    """

    login: str
    full_name: str = ""
    is_active: bool = True
    from_center: bool = True


class OperatorsReport(BaseModel):
    """Агент → центр: полный снимок учёток на весовом ПК (агент 0.4.14).

    Запрос Игоря 14.08.2026: центр должен видеть ВСЕ учётки агентов,
    включая заведённые на месте вручную. Шлётся при подключении, после
    применения operators_registry и периодически (ловит правки CLI,
    сделанные при работающей службе другим процессом).
    """

    type: Literal["operators_report"] = "operators_report"
    agent_id: str
    records: list[AgentOperatorInfo] = Field(default_factory=list)


# --- центр → агент ---


class WeighRequest(BaseModel):
    """Команда провести операцию взвешивания/тарирования."""

    type: Literal["weigh_request"] = "weigh_request"
    request_id: UUID
    operation: Operation
    vehicle_number: str | None = None  # номер ТС, если известен инициатору
    trailer_number: str | None = None
    # ФИО оператора весового контроля из запроса АИС: агент штампует его
    # в запись — печатается на весовой карточке с обеих сторон
    operator: str | None = None
    timeout_s: float | None = None  # тайм-аут всей операции; None — из конфига агента
    # номер документа АИС (контракт v2): агент 0.4.17 сохраняет его в записи и
    # возвращает в weigh_result/offline_sync; старые агенты поле не знают —
    # центр тогда связывает номер по памяти хаба (request_id)
    ais_ref: str | None = None
    # действующая тара сцепки по авторитетному реестру центра (агент 0.4.17):
    # ``tare_resolved`` = центр искал тару, результат в ``tare`` (может быть
    # None — «действующей тары нет»); агент тогда не ходит в свою реплику,
    # которая могла отстать. Старые агенты подставляют по реплике, как раньше
    tare: TareRecord | None = None
    tare_resolved: bool = False


class UpdateCommand(BaseModel):
    """Команда автообновления: скачать сборку с центра и перезапуститься.

    Агент проверяет sha256 и размер скачанного архива; несовпадение —
    отказ от обновления (архив повреждён или подменён).
    """

    type: Literal["update_command"] = "update_command"
    version: str  # версия сборки (сравнивается с agent.__version__)
    url_path: str  # путь скачивания на центре, например /agents/releases/<файл>
    sha256: str
    size_bytes: int


class OfflineSyncAck(BaseModel):
    """Подтверждение приёма досылки: агент помечает записи synced."""

    type: Literal["offline_sync_ack"] = "offline_sync_ack"
    accepted_uuids: list[UUID]


class TareRegistryUpdate(BaseModel):
    """Полный снимок реестра тарирований (реплицируется целиком, architecture §3.3а)."""

    type: Literal["tare_registry"] = "tare_registry"
    records: list[TareRecord]


class OperatorRecord(BaseModel):
    """Учётка оператора весов, реплицируемая на агента.

    Пароль передаётся ХЕШЕМ (pbkdf2$..., формат shared.passwords —
    общий для центра и агента); сам пароль по каналу не ходит никогда.
    Отключённые учётки реплицируются с ``is_active=False``, чтобы агент
    заблокировал и офлайн-вход.
    """

    login: str
    # repr=False — страховка от попадания хеша в логи через repr модели
    # (хеш офлайн-подбираем, правило №7); в JSON-сериализацию поле входит
    pw_hash: str = Field(repr=False)
    full_name: str = ""
    is_active: bool = True


class CycleSettings(BaseModel):
    """Параметры цикла взвешивания, задаваемые в центре (страница весов).

    Все поля обязательны: центр шлёт полный набор (частичных обновлений
    нет — семантика полного снимка, как у реестров).
    """

    zero_threshold_kg: float
    vehicle_threshold_kg: float
    zero_timeout_s: float
    vehicle_timeout_s: float
    stable_duration_s: float
    stable_timeout_s: float
    no_data_timeout_s: float


class CameraSettings(BaseModel):
    """Камера весов из справочника центра (URL содержат пароли — секретны)."""

    role: CameraRole
    snapshot_url: str | None = Field(default=None, repr=False)
    rtsp_url: str | None = Field(default=None, repr=False)


class VerificationInfo(BaseModel):
    """Свидетельство о поверке весов (справочник центра).

    На работу агента не влияет — печатается на весовой карточке. Агент
    хранит копию в снимке настроек, поэтому карточка печатается и без
    связи с центром. Одно свидетельство на весы: на карточку идёт
    свидетельство тех весов, где прошла операция.
    """

    number: str
    verified_on: date | None = None  # дата поверки
    valid_until: date | None = None  # срок действия


class ScaleSettingsPayload(BaseModel):
    """Снимок настроек весов из центра (решение Игоря 10.08.2026: настраивать
    объекты без AnyDesk — включая камеры и COM-порт).

    None в поле — «в центре не задано, агент живёт по локальному конфигу».
    Токен агента через канал НЕ передаётся (ключ самого канала).
    """

    cycle: CycleSettings | None = None
    cameras: list[CameraSettings] | None = None
    scale_port: str | None = None  # «COM11» либо pyserial-URL
    baudrate: int | None = None
    # агенты до 0.4.9 поля не знают и молча отбрасывают (extra=ignore)
    verification: VerificationInfo | None = None


class ScaleConfigUpdate(BaseModel):
    """Центр → агент: применить настройки весов (при hello и после правки)."""

    type: Literal["scale_config"] = "scale_config"
    settings: ScaleSettingsPayload


class ConfigStatus(BaseModel):
    """Агент → центр: отчёт о применении настроек."""

    type: Literal["config_status"] = "config_status"
    ok: bool
    error: str | None = None
    # смена COM-порта, после которой индикатор замолчал, откатывается
    rolled_back: bool = False


class HeartbeatAck(BaseModel):
    """Центр → агент: ответ на hello/heartbeat с временем центра.

    Агент считает смещение своих часов и ставит в записи время центра
    (вопрос Игоря 10.08.2026: часы весовых ПК уходят, часы ВМ — NTP).
    """

    type: Literal["heartbeat_ack"] = "heartbeat_ack"
    server_time: datetime


class OperatorsRegistryUpdate(BaseModel):
    """Полный снимок операторов весов (решение Игоря 10.08.2026: учётки
    операторов заводятся и блокируются в центре, агент хранит реплику
    для офлайн-входа)."""

    type: Literal["operators_registry"] = "operators_registry"
    records: list[OperatorRecord]


class LogTailRequest(BaseModel):
    """Центр → агент: пришли последние строки журнала службы.

    Удалённая диагностика (вопрос Игоря 10.08.2026): чтобы разбирать сбой
    объекта, не заходя в его сеть. Тот же хвост оператор видит на экране
    «Диагностика» самого агента.
    """

    type: Literal["log_tail_request"] = "log_tail_request"
    request_id: UUID
    lines: int = Field(default=200, ge=1, le=MAX_LOG_TAIL_LINES)


class LogTailResponse(BaseModel):
    """Агент → центр: хвост журнала службы (пусто — файл недоступен)."""

    type: Literal["log_tail_response"] = "log_tail_response"
    request_id: UUID
    agent_id: str
    # потолок и на приёме: центр не обязан верить размеру ответа клиента
    lines: list[str] = Field(default_factory=list, max_length=MAX_LOG_TAIL_LINES)
    location: str = ""  # где лежит файл на весовом ПК — подсказка диспетчеру


# --- дискриминированные объединения и разбор ---

AgentMessage = Annotated[
    Hello
    | Heartbeat
    | WeighResult
    | OfflineSync
    | UpdateStatus
    | ConfigStatus
    | LogTailResponse
    | OperatorsReport,
    Field(discriminator="type"),
]
CenterMessage = Annotated[
    WeighRequest
    | OfflineSyncAck
    | TareRegistryUpdate
    | OperatorsRegistryUpdate
    | ScaleConfigUpdate
    | HeartbeatAck
    | UpdateCommand
    | LogTailRequest,
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
