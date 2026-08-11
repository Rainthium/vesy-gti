"""WebSocket-клиент агента к центру (architecture §4.2).

Агент сам открывает исходящее соединение и держит его (центр на объекты
не стучится — принцип §2 п.1). Аутентификация — токен агента в заголовке
``Authorization``.

Обязанности клиента:
- ``hello`` сразу после подключения, ``heartbeat`` с периодом из конфига;
- приём ``weigh_request`` → вызов обработчика → отправка ``weigh_result``;
- досылка офлайн-записей порциями (``offline_sync`` → ``offline_sync_ack``
  → пометка synced в локальной БД → следующая порция);
- приём ``tare_registry`` и ``operators_registry`` → полная замена
  локальных реплик (тары и учётки операторов из центра);
- бесконечный реконнект с экспоненциальным backoff — потеря связи не
  роняет агента, офлайн-записи копятся в локальной БД (правило §2 п.2).

Свойство ``connected`` — источник истины для правила режимов (№3):
пока связь с центром есть, локальное взвешивание в интерфейсе оператора
заблокировано.

Обращения к SQLite здесь синхронные: объёмы малы (сотни строк), а очередь
событий блокируется на миллисекунды; при росте объёмов — run_in_executor.
"""

import asyncio
import contextlib
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

import websockets

from agent.sync.storage import AgentStorage
from shared.enums import ErrorCode, WeighingSource
from shared.messages import (
    ConfigStatus,
    EquipmentStatus,
    Heartbeat,
    HeartbeatAck,
    Hello,
    OfflineSync,
    OfflineSyncAck,
    OperatorsRegistryUpdate,
    ScaleConfigUpdate,
    TareRegistryUpdate,
    UpdateCommand,
    UpdateStatus,
    WeighingRecord,
    WeighRequest,
    WeighResult,
    parse_center_message,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClientConfig:
    """Параметры подключения к центру (конфиг агента)."""

    url: str  # например, wss://center.gti.local/agents/ws
    # токен агента (выдаёт центр, хранится вне git); repr=False — чтобы
    # случайный лог конфига не засветил секрет
    token: str = field(repr=False)
    agent_id: str
    version: str  # версия агента для hello
    driver: str  # имя драйвера индикатора для hello
    heartbeat_interval_s: float = 5.0
    reconnect_min_s: float = 1.0  # первая пауза реконнекта
    reconnect_max_s: float = 30.0  # потолок экспоненциального backoff
    sync_batch_size: int = 100  # размер порции досылки офлайн-записей


class CenterClient:
    """Клиент соединения агент → центр.

    ``equipment_status`` — колбэк самодиагностики (текущий вес, камеры,
    очередь досылки) для hello/heartbeat.
    ``on_weigh_request`` — асинхронный обработчик команды взвешивания;
    его результат уходит центру как ``weigh_result``. Обработчик сам
    отвечает за ERR_BUSY при параллельной команде.
    """

    def __init__(
        self,
        config: ClientConfig,
        storage: AgentStorage,
        *,
        equipment_status: Callable[[], EquipmentStatus],
        on_weigh_request: Callable[[WeighRequest], Awaitable[WeighResult]],
        on_update_command: Callable[[UpdateCommand], Awaitable[UpdateStatus]] | None = None,
        on_scale_config: Callable[[ScaleConfigUpdate], Awaitable[ConfigStatus]] | None = None,
        on_server_time: Callable[[datetime], None] | None = None,
    ) -> None:
        self._config = config
        self._storage = storage
        self._equipment_status = equipment_status
        self._on_weigh_request = on_weigh_request
        self._on_update_command = on_update_command
        self._on_scale_config = on_scale_config
        self._on_server_time = on_server_time
        self._connected = asyncio.Event()
        self._stopping = False
        self._request_tasks: set[asyncio.Task[None]] = set()
        # порция досылки уже отправлена и ждёт ack — повторно не шлём
        self._sync_in_flight = False
        self._session_connected = False  # для сброса backoff в run()

    @property
    def connected(self) -> bool:
        """Есть ли живое соединение с центром (правило режимов №3)."""
        return self._connected.is_set()

    async def run(self) -> None:
        """Главный цикл: подключение → работа → реконнект с backoff.

        Работает до отмены задачи (asyncio.CancelledError) — это штатный
        способ остановки.
        """
        attempt = 0
        while not self._stopping:
            self._session_connected = False
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("связь с центром потеряна: %s", exc)
            finally:
                self._connected.clear()
            # сессия дошла до рабочего состояния — отсчёт backoff заново
            # (сессия почти всегда завершается исключением, поэтому сброс
            # по флагу подключения, а не по «нормальному» возврату)
            attempt = 0 if self._session_connected else attempt + 1
            delay = min(
                self._config.reconnect_max_s,
                self._config.reconnect_min_s * (2**attempt),
            )
            await asyncio.sleep(delay)

    async def _session(self) -> None:
        """Одно живое соединение: hello, затем heartbeat и приём сообщений."""
        headers = {"Authorization": f"Bearer {self._config.token}"}
        async with websockets.connect(self._config.url, additional_headers=headers) as connection:
            logger.info("соединение с центром установлено: %s", self._config.url)
            await connection.send(self._hello().model_dump_json())
            self._connected.set()
            self._session_connected = True
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(self._heartbeat_loop(connection))
                    group.create_task(self._receive_loop(connection))
                    group.create_task(self._send_pending(connection))
            finally:
                self._connected.clear()
                self._sync_in_flight = False  # неподтверждённая порция уйдёт заново
                # обработчики команд не должны переживать соединение:
                # их результат отправить уже некуда
                for task in self._request_tasks:
                    task.cancel()

    def _hello(self) -> Hello:
        return Hello(
            agent_id=self._config.agent_id,
            version=self._config.version,
            driver=self._config.driver,
            equipment=self._equipment_status(),
        )

    async def _heartbeat_loop(self, connection: websockets.ClientConnection) -> None:
        while True:
            await asyncio.sleep(self._config.heartbeat_interval_s)
            heartbeat = Heartbeat(
                agent_id=self._config.agent_id,
                sent_at=datetime.now(UTC),
                equipment=self._equipment_status(),
            )
            await connection.send(heartbeat.model_dump_json())
            # записи, появившиеся при живом соединении (например, после
            # ручного режима) досылаются без ожидания
            # реконнекта — подталкиваем очередь вместе с heartbeat
            await self._send_pending(connection)

    async def _receive_loop(self, connection: websockets.ClientConnection) -> None:
        async for raw in connection:
            try:
                message = parse_center_message(raw)
            except ValueError:
                # тело не печатаем: в снимках настроек едут URL камер с
                # паролями, а лог теперь виден оператору на «Диагностике»
                logger.warning("непонятное сообщение от центра (%d символов)", len(raw))
                continue
            if isinstance(message, WeighRequest):
                self._spawn_request_handler(connection, message)
            elif isinstance(message, OfflineSyncAck):
                self._sync_in_flight = False
                self._storage.mark_synced(message.accepted_uuids)
                # порция подтверждена — если очередь не пуста, шлём следующую
                await self._send_pending(connection)
            elif isinstance(message, TareRegistryUpdate):
                count = self._storage.replace_tare_registry(message.records)
                logger.info("реплика реестра тарирований обновлена: %d записей", count)
            elif isinstance(message, OperatorsRegistryUpdate):
                count = self._storage.replace_center_operators(message.records)
                logger.info("реплика операторов обновлена: %d учёток", count)
            elif isinstance(message, HeartbeatAck):
                if self._on_server_time is not None:
                    try:
                        self._on_server_time(message.server_time)
                    except Exception:
                        # часы пишут смещение в SQLite: локальная ошибка БД
                        # не должна стоить разрыва соединения (ack придёт
                        # со следующим heartbeat)
                        logger.exception("обновление смещения часов упало")
            elif isinstance(message, ScaleConfigUpdate):
                self._spawn_config_handler(connection, message)
            elif isinstance(message, UpdateCommand):
                self._spawn_update_handler(connection, message)

    def _spawn_config_handler(
        self, connection: websockets.ClientConnection, update: ScaleConfigUpdate
    ) -> None:
        """Настройки центра — отдельной задачей: проверка COM-порта с
        возможным откатом длится секунды, heartbeat замирать не должен."""
        handler = self._on_scale_config

        async def handle() -> None:
            if handler is None:
                logger.warning("scale_config получен, но обработчик не настроен")
                return
            status = await handler(update)
            with contextlib.suppress(Exception):
                await connection.send(status.model_dump_json())

        task = asyncio.create_task(handle(), name="scale-config")
        self._request_tasks.add(task)
        task.add_done_callback(self._request_tasks.discard)

    def _spawn_update_handler(
        self, connection: websockets.ClientConnection, command: UpdateCommand
    ) -> None:
        """Автообновление — отдельной задачей: скачивание длится минуты,
        heartbeat и операции не должны замирать."""
        handler = self._on_update_command

        async def handle() -> None:
            if handler is None:
                logger.warning("команда автообновления получена, но обработчик не настроен")
                return
            status = await handler(command)
            with contextlib.suppress(Exception):
                await connection.send(status.model_dump_json())

        task = asyncio.create_task(handle(), name=f"update-{command.version}")
        self._request_tasks.add(task)
        task.add_done_callback(self._request_tasks.discard)

    def _spawn_request_handler(
        self, connection: websockets.ClientConnection, request: WeighRequest
    ) -> None:
        """Команда обрабатывается отдельной задачей: приём сообщений не блокируется.

        Цикл взвешивания длится десятки секунд — heartbeat и ack должны
        ходить всё это время.
        """

        async def handle() -> None:
            try:
                result = await self._on_weigh_request(request)
            except asyncio.CancelledError:
                raise
            except Exception:
                # центр не должен ждать до таймаута — отвечаем ERR_INTERNAL
                logger.exception("ошибка обработчика команды %s", request.request_id)
                result = WeighResult(
                    request_id=request.request_id,
                    record=WeighingRecord(
                        uuid=uuid4(),
                        operation=request.operation,
                        code=ErrorCode.ERR_INTERNAL,
                        source=WeighingSource.AIS,
                        message="внутренняя ошибка агента при выполнении команды",
                    ),
                )
            try:
                await connection.send(result.model_dump_json())
            except websockets.ConnectionClosed:
                # соединение умерло между взвешиванием и отправкой — запись
                # уже в локальной БД у обработчика и уйдёт досылкой
                logger.warning("соединение закрылось до отправки результата %s", request.request_id)

        task = asyncio.create_task(handle())
        self._request_tasks.add(task)
        task.add_done_callback(self._request_tasks.discard)

    async def _send_pending(self, connection: websockets.ClientConnection) -> None:
        """Отправить одну порцию недосланных офлайн-записей (если есть).

        Пока порция ждёт ack, новая не отправляется — иначе одни и те же
        записи ушли бы дважды.
        """
        if self._sync_in_flight:
            return
        try:
            records = self._storage.pending_records(limit=self._config.sync_batch_size)
        except sqlite3.Error as exc:
            # локальная ошибка БД не должна стоить разрыва соединения с центром
            logger.warning("ошибка чтения очереди досылки: %s", exc)
            return
        if not records:
            return
        self._sync_in_flight = True
        message = OfflineSync(agent_id=self._config.agent_id, records=records)
        await connection.send(message.model_dump_json())
        logger.info("досылка офлайн-записей: отправлено %d", len(records))


async def run_forever(client: CenterClient) -> None:
    """Удобный запуск: работать до отмены, гасить CancelledError снаружи."""
    with contextlib.suppress(asyncio.CancelledError):
        await client.run()
