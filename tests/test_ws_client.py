"""Тесты WebSocket-клиента агента (agent/sync/ws_client.py).

Покрытие:
- подключение: hello первым сообщением (agent_id/version/driver/protocol_version,
  статус оборудования), заголовок Authorization с токеном;
- heartbeat: периодичность, заполненный sent_at, свежий вызов колбэка
  самодиагностики на каждый heartbeat;
- свойство connected: False до старта, True после подключения, False после
  падения сервера;
- команды взвешивания: разбор weigh_request → обработчик → weigh_result с тем же
  request_id; долгий обработчик не блокирует heartbeat; две команды подряд;
  исключение в обработчике не роняет клиента;
- досылка офлайн-записей: автоматическая отправка при подключении, порционность
  (2+2+1 при batch=2), пометка synced после ack, частичный ack, пустая очередь,
  записи, добавленные во время сессии (фиксация фактического поведения);
- tare_registry: полная замена реплики реестра тарирований;
- operators_registry: обновление реплики операторов (вход по новой учётке,
  снятый оператор исчезает, заблокированный не входит офлайн);
- heartbeat_ack (время от центра): server_time доходит до колбэка
  on_server_time на каждый ack; без колбэка клиент жив; исключение
  колбэка гасится на месте — сессия НЕ рвётся (локальная ошибка БД
  смещения не должна стоить соединения);
- scale_config: снимок настроек → обработчик → config_status центру (включая
  отчёт об откате порта); долгий обработчик не блокирует heartbeat; без
  обработчика клиент жив и отчёт не шлётся;
- реконнект: сервер недоступен → клиент жив и подключается позже; разрыв
  соединения → повторный hello и досылка накопленного; мусорные сообщения
  не роняют клиента; отмена run()/run_forever — штатная остановка.

pytest-asyncio не используется: каждый тест — синхронная обёртка,
внутри asyncio.run(...) с общим лимитом времени на сценарий.
"""

import asyncio
import contextlib
import socket
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from agent.sync.storage import AgentStorage
from agent.sync.ws_client import CenterClient, ClientConfig, run_forever
from shared.enums import ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import (
    PROTOCOL_VERSION,
    AgentMessage,
    ConfigStatus,
    CycleSettings,
    EquipmentStatus,
    Heartbeat,
    HeartbeatAck,
    Hello,
    LogTailRequest,
    LogTailResponse,
    OfflineSync,
    OfflineSyncAck,
    OperatorRecord,
    OperatorsRegistryUpdate,
    ScaleConfigUpdate,
    ScaleSettingsPayload,
    TareRecord,
    TareRegistryUpdate,
    WeighingRecord,
    WeighRequest,
    WeighResult,
    parse_agent_message,
)
from shared.passwords import hash_password

# Общий лимит на один сценарий: тесты событийные, реальное время много меньше.
SCENARIO_TIMEOUT_S = 15.0
# Таймаут ожидания одного события/сообщения внутри сценария.
STEP_TIMEOUT_S = 5.0

TOKEN = "test-token-123"

# Тип конкретного сообщения агента для типизированного ожидания в FakeCenter.expect
MessageT = TypeVar("MessageT", bound=BaseModel)


def run_scenario(coro: Any) -> None:
    """Запустить асинхронный сценарий с общим лимитом времени."""
    asyncio.run(asyncio.wait_for(coro, timeout=SCENARIO_TIMEOUT_S))


def free_tcp_port() -> int:
    """Свободный TCP-порт: биндимся на 0 и отпускаем (для теста «сервер недоступен»)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def make_equipment(pending: int = 0, weight: float | None = None) -> EquipmentStatus:
    """Типичный статус самодиагностики для hello/heartbeat."""
    return EquipmentStatus(
        scale_status=ScaleStatus.OK,
        current_weight=weight,
        stable=True,
        pending_sync_count=pending,
    )


def make_record(**overrides: Any) -> WeighingRecord:
    """Типичная запись завершённой операции; overrides — точечные замены полей."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 12340.0,
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 10, 0, 0, tzinfo=UTC),
        "vehicle_number": "01KG123ABC",
        "source": WeighingSource.LOCAL_OFFLINE,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


async def echo_weigh_handler(request: WeighRequest) -> WeighResult:
    """Обработчик по умолчанию: сразу возвращает успешный результат."""
    return WeighResult(
        request_id=request.request_id,
        record=make_record(
            operation=request.operation,
            vehicle_number=request.vehicle_number,
            source=WeighingSource.AIS,
        ),
    )


class FakeCenter:
    """Поддельный центр: принимает подключения агента, складывает сообщения в очередь.

    Разбор входящих — общим parse_agent_message: заодно проверяется, что клиент
    шлёт валидные по протоколу сообщения.
    """

    def __init__(self) -> None:
        self.server: Server | None = None
        self.inbox: asyncio.Queue[AgentMessage] = asyncio.Queue()
        self.connections: list[ServerConnection] = []
        self.auth_headers: list[str | None] = []

    async def start(self, port: int = 0) -> None:
        self.server = await serve(self._handler, "127.0.0.1", port)

    async def stop(self) -> None:
        # close() по умолчанию рвёт и уже открытые соединения
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    @property
    def port(self) -> int:
        assert self.server is not None
        return int(self.server.sockets[0].getsockname()[1])

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    @property
    def connection(self) -> ServerConnection:
        """Последнее (текущее) соединение с агентом."""
        return self.connections[-1]

    async def _handler(self, connection: ServerConnection) -> None:
        assert connection.request is not None  # рукопожатие завершено — запрос есть
        self.auth_headers.append(connection.request.headers.get("Authorization"))
        self.connections.append(connection)
        with contextlib.suppress(ConnectionClosed):
            async for raw in connection:
                await self.inbox.put(parse_agent_message(raw))

    async def next_message(self, timeout: float = STEP_TIMEOUT_S) -> AgentMessage:
        return await asyncio.wait_for(self.inbox.get(), timeout=timeout)

    async def expect(
        self, message_type: type[MessageT], timeout: float = STEP_TIMEOUT_S
    ) -> MessageT:
        """Ждать сообщение заданного типа, пропуская остальные (например, heartbeat)."""

        async def wait() -> MessageT:
            while True:
                message = await self.inbox.get()
                if isinstance(message, message_type):
                    return message

        return await asyncio.wait_for(wait(), timeout=timeout)

    async def assert_no_message(self, message_type: type[BaseModel], during_s: float) -> None:
        """Убедиться, что сообщений заданного типа нет в течение during_s."""
        deadline = asyncio.get_running_loop().time() + during_s
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return
            try:
                message = await asyncio.wait_for(self.inbox.get(), timeout=remaining)
            except TimeoutError:
                return
            assert not isinstance(message, message_type), f"неожиданное сообщение: {message!r}"


def make_config(url: str, **overrides: Any) -> ClientConfig:
    """Конфиг с маленькими таймаутами, чтобы тесты бежали быстро."""
    fields: dict[str, Any] = {
        "url": url,
        "token": TOKEN,
        "agent_id": "agent-test",
        "version": "0.1-test",
        "driver": "cas22",
        "heartbeat_interval_s": 0.1,
        "reconnect_min_s": 0.05,
        "reconnect_max_s": 0.2,
        "sync_batch_size": 100,
    }
    fields.update(overrides)
    return ClientConfig(**fields)


class Scene:
    """Сцена теста: поддельный центр + клиент + фоновая задача run().

    Использование: async with Scene(...) as scene — по выходу задача клиента
    отменяется, сервер и хранилище закрываются.
    """

    def __init__(
        self,
        *,
        storage: AgentStorage | None = None,
        equipment: Any = None,
        weigh_handler: Any = None,
        start_server: bool = True,
        port: int = 0,
        autostart_client: bool = True,
        **config_overrides: Any,
    ) -> None:
        self.center = FakeCenter()
        self.storage = storage if storage is not None else AgentStorage(":memory:")
        self._equipment = equipment if equipment is not None else make_equipment
        self._weigh_handler = weigh_handler if weigh_handler is not None else echo_weigh_handler
        self._start_server = start_server
        self._port = port
        self._autostart_client = autostart_client
        self._config_overrides = config_overrides
        self.client: CenterClient
        self.run_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> "Scene":
        if self._start_server:
            await self.center.start(self._port)
            url = self.center.url
        else:
            url = f"ws://127.0.0.1:{self._port}"
        self.client = CenterClient(
            make_config(url, **self._config_overrides),
            self.storage,
            equipment_status=self._equipment,
            on_weigh_request=self._weigh_handler,
        )
        if self._autostart_client:
            self.start_client()
        return self

    def start_client(self) -> None:
        self.run_task = asyncio.create_task(self.client.run())

    async def __aexit__(self, *exc_info: object) -> None:
        if self.run_task is not None and not self.run_task.done():
            self.run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.run_task
        await self.center.stop()
        self.storage.close()


async def wait_until(predicate: Any, timeout: float = STEP_TIMEOUT_S, step: float = 0.005) -> None:
    """Поллинг условия с таймаутом (для событий без явного сигнала)."""

    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(step)

    await asyncio.wait_for(poll(), timeout=timeout)


# --- подключение и протокол ---


def test_hello_is_first_message_with_identity_and_auth_header() -> None:
    """hello приходит первым, с полными реквизитами агента; токен — в Authorization."""

    async def scenario() -> None:
        async with Scene(equipment=lambda: make_equipment(pending=7, weight=250.0)) as scene:
            first = await scene.center.next_message()
            # именно hello, и именно первым — до любых heartbeat
            assert isinstance(first, Hello)
            assert first.agent_id == "agent-test"
            assert first.version == "0.1-test"
            assert first.driver == "cas22"
            assert first.protocol_version == PROTOCOL_VERSION
            # статус оборудования взят из колбэка самодиагностики
            assert first.equipment.pending_sync_count == 7
            assert first.equipment.current_weight == 250.0
            assert first.equipment.scale_status is ScaleStatus.OK
            # аутентификация: токен агента в заголовке Authorization
            assert scene.center.auth_headers == [f"Bearer {TOKEN}"]

    run_scenario(scenario())


def test_heartbeats_are_periodic_and_refresh_equipment() -> None:
    """heartbeat идут периодически; каждый заново вызывает колбэк самодиагностики."""
    calls = {"n": 0}

    def equipment() -> EquipmentStatus:
        # каждый вызов — новое значение: по нему видно, что колбэк зовётся заново
        calls["n"] += 1
        return make_equipment(pending=calls["n"])

    async def scenario() -> None:
        async with Scene(equipment=equipment) as scene:
            hello = await scene.center.next_message()
            assert isinstance(hello, Hello)
            started = datetime.now(UTC)
            heartbeats = [await scene.center.expect(Heartbeat) for _ in range(3)]
            for heartbeat in heartbeats:
                assert heartbeat.agent_id == "agent-test"
                # sent_at заполнен и осмыслен (не из прошлого/будущего)
                assert heartbeat.sent_at is not None
                assert started - timedelta(seconds=5) <= heartbeat.sent_at
                assert heartbeat.sent_at <= datetime.now(UTC) + timedelta(seconds=5)
            # значения самодиагностики различаются: колбэк вызван на каждый heartbeat
            pendings = [hb.equipment.pending_sync_count for hb in heartbeats]
            assert len(set(pendings)) == 3
            assert pendings == sorted(pendings)  # в порядке вызовов

    run_scenario(scenario())


def test_connected_flag_lifecycle() -> None:
    """connected: False до старта, True после подключения, False после падения сервера."""

    async def scenario() -> None:
        async with Scene(autostart_client=False) as scene:
            # до старта задачи run() соединения нет
            assert scene.client.connected is False
            scene.start_client()
            await wait_until(lambda: scene.client.connected)
            await scene.center.expect(Hello)
            # сервер падает целиком (рвёт соединение и перестаёт слушать)
            await scene.center.stop()
            await wait_until(lambda: not scene.client.connected)
            # клиент жив и продолжает попытки реконнекта
            assert scene.run_task is not None and not scene.run_task.done()
            await asyncio.sleep(0.3)
            assert scene.client.connected is False

    run_scenario(scenario())


# --- команды взвешивания ---


def test_weigh_request_dispatched_and_result_returned() -> None:
    """weigh_request разбирается, обработчик получает запрос, weigh_result уходит центру."""
    received: list[WeighRequest] = []

    async def handler(request: WeighRequest) -> WeighResult:
        received.append(request)
        return await echo_weigh_handler(request)

    async def scenario() -> None:
        async with Scene(weigh_handler=handler) as scene:
            await scene.center.expect(Hello)
            request = WeighRequest(
                request_id=uuid4(),
                operation=Operation.TARING,
                vehicle_number="01KG777XYZ",
                timeout_s=30.0,
            )
            await scene.center.connection.send(request.model_dump_json())
            result = await scene.center.expect(WeighResult)
            # обработчик получил разобранный запрос со всеми полями
            assert len(received) == 1
            assert received[0].request_id == request.request_id
            assert received[0].operation is Operation.TARING
            assert received[0].vehicle_number == "01KG777XYZ"
            assert received[0].timeout_s == 30.0
            # результат привязан к тому же request_id
            assert result.request_id == request.request_id
            assert result.record.vehicle_number == "01KG777XYZ"

    run_scenario(scenario())


def test_slow_weigh_handler_does_not_block_heartbeat() -> None:
    """Долгая операция взвешивания не мешает heartbeat (цикл длится десятки секунд)."""

    async def slow_handler(request: WeighRequest) -> WeighResult:
        await asyncio.sleep(0.5)  # имитация долгого цикла взвешивания
        return await echo_weigh_handler(request)

    async def scenario() -> None:
        async with Scene(weigh_handler=slow_handler) as scene:
            await scene.center.expect(Hello)
            request = WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING)
            await scene.center.connection.send(request.model_dump_json())
            # пока обработчик спит, heartbeat продолжают приходить
            heartbeats_during = 0
            while True:
                message = await scene.center.next_message()
                if isinstance(message, Heartbeat):
                    heartbeats_during += 1
                elif isinstance(message, WeighResult):
                    assert message.request_id == request.request_id
                    break
            assert heartbeats_during >= 2, "heartbeat заблокированы обработчиком команды"

    run_scenario(scenario())


def test_two_requests_in_a_row_both_get_results() -> None:
    """Две команды подряд: обе обрабатываются, обе получают результат."""

    async def scenario() -> None:
        async with Scene() as scene:
            await scene.center.expect(Hello)
            first = WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING)
            second = WeighRequest(request_id=uuid4(), operation=Operation.TARING)
            await scene.center.connection.send(first.model_dump_json())
            await scene.center.connection.send(second.model_dump_json())
            results = {(await scene.center.expect(WeighResult)).request_id for _ in range(2)}
            assert results == {first.request_id, second.request_id}

    run_scenario(scenario())


def test_weigh_handler_exception_returns_err_internal() -> None:
    """Исключение в обработчике: центру уходит ERR_INTERNAL, клиент жив."""

    async def broken_handler(request: WeighRequest) -> WeighResult:
        raise RuntimeError("обработчик сломался")

    async def scenario() -> None:
        async with Scene(weigh_handler=broken_handler) as scene:
            await scene.center.expect(Hello)
            request = WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING)
            await scene.center.connection.send(request.model_dump_json())
            # центр не ждёт до таймаута: приходит результат с ERR_INTERNAL
            result = await scene.center.expect(WeighResult)
            assert result.request_id == request.request_id
            assert result.record.code is ErrorCode.ERR_INTERNAL
            assert result.record.message
            # соединение живо: heartbeat продолжают приходить после падения задачи
            for _ in range(2):
                await scene.center.expect(Heartbeat)
            assert len(scene.center.connections) == 1
            assert scene.client.connected is True

    run_scenario(scenario())


# --- досылка офлайн-записей ---


def test_offline_sync_batches_until_queue_empty() -> None:
    """5 записей при batch=2 досылаются порциями 2+2+1; после всех ack очередь пуста."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        records = [make_record() for _ in range(5)]
        for record in records:
            storage.save_weighing(record)
        async with Scene(storage=storage, sync_batch_size=2) as scene:
            await scene.center.expect(Hello)
            batch_sizes: list[int] = []
            acked: set[UUID] = set()
            for _ in range(3):
                sync = await scene.center.expect(OfflineSync)
                assert sync.agent_id == "agent-test"
                batch_sizes.append(len(sync.records))
                uuids = [r.uuid for r in sync.records]
                # порции не пересекаются: каждая запись досылается один раз
                assert not (set(uuids) & acked)
                acked.update(uuids)
                ack = OfflineSyncAck(accepted_uuids=uuids)
                await scene.center.connection.send(ack.model_dump_json())
            assert batch_sizes == [2, 2, 1]
            assert acked == {r.uuid for r in records}
            await wait_until(lambda: storage.pending_count() == 0)
            # очередь пуста — новых offline_sync больше нет
            await scene.center.assert_no_message(OfflineSync, during_s=0.25)

    run_scenario(scenario())


def test_offline_sync_not_sent_when_queue_empty() -> None:
    """Пустая очередь досылки: offline_sync не отправляется вовсе."""

    async def scenario() -> None:
        async with Scene() as scene:
            await scene.center.expect(Hello)
            # ждём несколько периодов heartbeat — offline_sync так и не приходит
            await scene.center.assert_no_message(OfflineSync, during_s=0.35)

    run_scenario(scenario())


def test_partial_ack_keeps_unaccepted_records_pending() -> None:
    """Частичный ack: подтверждённые записи synced, неподтверждённые досылаются снова."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        records = [make_record() for _ in range(3)]
        for record in records:
            storage.save_weighing(record)
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            sync = await scene.center.expect(OfflineSync)
            assert len(sync.records) == 3
            accepted = [sync.records[0].uuid, sync.records[1].uuid]
            rejected_uuid = sync.records[2].uuid
            ack = OfflineSyncAck(accepted_uuids=accepted)
            await scene.center.connection.send(ack.model_dump_json())
            # подтверждённые помечены synced, неподтверждённая осталась в очереди
            await wait_until(lambda: storage.pending_count() == 1)
            assert [r.uuid for r in storage.pending_records()] == [rejected_uuid]
            # после ack клиент сам шлёт следующую порцию — только с остатком
            retry = await scene.center.expect(OfflineSync)
            assert [r.uuid for r in retry.records] == [rejected_uuid]
            await scene.center.connection.send(
                OfflineSyncAck(accepted_uuids=[rejected_uuid]).model_dump_json()
            )
            await wait_until(lambda: storage.pending_count() == 0)

    run_scenario(scenario())


def test_record_added_while_ack_cycle_runs_is_picked_up() -> None:
    """Запись, добавленная во время ack-цикла, уходит следующей порцией после ack."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        first = make_record()
        storage.save_weighing(first)
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            sync = await scene.center.expect(OfflineSync)
            assert [r.uuid for r in sync.records] == [first.uuid]
            # пока порция не подтверждена, появляется новая запись
            late = make_record()
            storage.save_weighing(late)
            await scene.center.connection.send(
                OfflineSyncAck(accepted_uuids=[first.uuid]).model_dump_json()
            )
            # ack запускает следующую порцию — поздняя запись досылается
            retry = await scene.center.expect(OfflineSync)
            assert [r.uuid for r in retry.records] == [late.uuid]

    run_scenario(scenario())


def test_record_added_mid_session_is_pushed_with_heartbeat() -> None:
    """Запись, добавленная при пустой очереди во время живой сессии,
    досылается вместе с ближайшим heartbeat — без ожидания реконнекта."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            # очередь пуста, сессия живёт; добавляем запись «в тишине»
            late = make_record()
            storage.save_weighing(late)
            # ближайший heartbeat подталкивает очередь — досылка в той же сессии
            sync = await scene.center.expect(OfflineSync)
            assert [r.uuid for r in sync.records] == [late.uuid]
            await scene.center.connection.send(
                OfflineSyncAck(accepted_uuids=[late.uuid]).model_dump_json()
            )
            await wait_until(lambda: storage.pending_count() == 0)

    run_scenario(scenario())


# --- реестр тарирований ---


def test_tare_registry_replaces_replica_entirely() -> None:
    """tare_registry от центра: реплика заменяется целиком, старые записи исчезают."""

    async def scenario() -> None:
        now = datetime.now(UTC)
        storage = AgentStorage(":memory:")
        # старая реплика: тара другого номера ТС
        storage.replace_tare_registry(
            [
                TareRecord(
                    vehicle_number="OLD111AAA",
                    tare_value=6000.0,
                    tared_at=now - timedelta(days=1),
                    weighing_uuid=uuid4(),
                )
            ]
        )
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            new_tare = TareRecord(
                vehicle_number="NEW222BBB",
                tare_value=7250.0,
                tared_at=now - timedelta(days=2),
                weighing_uuid=uuid4(),
            )
            update = TareRegistryUpdate(records=[new_tare])
            await scene.center.connection.send(update.model_dump_json())
            await wait_until(lambda: storage.find_active_tare("NEW222BBB", now) is not None)
            # старой записи больше нет — замена была полной
            assert storage.find_active_tare("OLD111AAA", now) is None
            assert storage.tare_registry_size() == 1
            found = storage.find_active_tare("NEW222BBB", now)
            assert found is not None
            assert found.tare_value == 7250.0
            assert found.weighing_uuid == new_tare.weighing_uuid

    run_scenario(scenario())


def test_operators_registry_replaces_center_replica() -> None:
    """operators_registry от центра: реплика операторов обновляется, вход
    по новой учётке работает, заблокированная — отклоняется офлайн."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        # старый центровый оператор, которого сняли с объекта
        storage.replace_center_operators(
            [OperatorRecord(login="old.operator", pw_hash=hash_password("old-pass-123"))]
        )
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            update = OperatorsRegistryUpdate(
                records=[
                    OperatorRecord(
                        login="a.aliev",
                        pw_hash=hash_password("new-pass-123"),
                        full_name="Алиев А.",
                    ),
                    OperatorRecord(
                        login="blocked.op",
                        pw_hash=hash_password("blocked-pass-1"),
                        is_active=False,
                    ),
                ]
            )
            await scene.center.connection.send(update.model_dump_json())
            await wait_until(lambda: storage.verify_operator("a.aliev", "new-pass-123") is not None)
            assert storage.verify_operator("a.aliev", "new-pass-123") == "Алиев А."
            # снятый оператор исчез, заблокированный не входит даже с верным паролем
            assert storage.verify_operator("old.operator", "old-pass-123") is None
            assert storage.verify_operator("blocked.op", "blocked-pass-1") is None

    run_scenario(scenario())


# --- реконнект и устойчивость ---


def test_client_survives_unreachable_server_and_connects_later() -> None:
    """Порт никто не слушает: клиент жив, connected False; сервер поднялся — подключился."""

    async def scenario() -> None:
        port = free_tcp_port()
        async with Scene(start_server=False, port=port) as scene:
            # несколько неудачных попыток с backoff — клиент не падает
            await asyncio.sleep(0.3)
            assert scene.run_task is not None and not scene.run_task.done()
            assert scene.client.connected is False
            # сервер появился на том же порту — клиент сам подключается
            await scene.center.start(port)
            hello = await scene.center.expect(Hello)
            assert hello.agent_id == "agent-test"
            await wait_until(lambda: scene.client.connected)

    run_scenario(scenario())


def test_reconnect_after_drop_sends_new_hello_and_pending_records() -> None:
    """Разрыв соединения сервером: клиент переподключается, шлёт hello и досылает записи."""

    async def scenario() -> None:
        storage = AgentStorage(":memory:")
        async with Scene(storage=storage) as scene:
            await scene.center.expect(Hello)
            # запись накапливается, затем сервер рвёт соединение
            record = make_record()
            storage.save_weighing(record)
            await scene.center.connection.close()
            await wait_until(lambda: not scene.client.connected)
            # клиент сам переподключается: новый hello на новом соединении
            hello = await scene.center.expect(Hello)
            assert hello.agent_id == "agent-test"
            assert len(scene.center.connections) == 2
            await wait_until(lambda: scene.client.connected)
            # накопленное досылается при новом подключении
            sync = await scene.center.expect(OfflineSync)
            assert [r.uuid for r in sync.records] == [record.uuid]

    run_scenario(scenario())


def test_garbage_and_unknown_messages_do_not_kill_client() -> None:
    """Мусорный JSON и неизвестный type от центра: клиент жив, соединение то же."""

    async def scenario() -> None:
        async with Scene() as scene:
            await scene.center.expect(Hello)
            connection = scene.center.connection
            await connection.send("это вообще не JSON {{{")
            await connection.send('{"type": "alien_message", "payload": 1}')
            await connection.send('{"no_type_field": true}')
            await connection.send(b"\x00\xff\xfe garbage bytes")
            # после мусора клиент продолжает обслуживать команды
            request = WeighRequest(request_id=uuid4(), operation=Operation.WEIGHING)
            await connection.send(request.model_dump_json())
            result = await scene.center.expect(WeighResult)
            assert result.request_id == request.request_id
            # реконнекта не было: соединение всё то же
            assert len(scene.center.connections) == 1
            assert scene.client.connected is True

    run_scenario(scenario())


def test_run_task_cancellation_is_clean_stop() -> None:
    """Отмена задачи run(): CancelledError доходит наружу, connected сбрасывается."""

    async def scenario() -> None:
        async with Scene() as scene:
            await scene.center.expect(Hello)
            await wait_until(lambda: scene.client.connected)
            assert scene.run_task is not None
            scene.run_task.cancel()
            # штатная остановка run() — именно отменой: CancelledError ожидаема
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(scene.run_task, timeout=2.0)
            assert scene.run_task.done()
            assert scene.client.connected is False

    run_scenario(scenario())


def test_run_forever_suppresses_cancellation() -> None:
    """run_forever: отмена гасится, задача завершается без исключения."""

    async def scenario() -> None:
        async with Scene(autostart_client=False) as scene:
            task = asyncio.create_task(run_forever(scene.client))
            scene.run_task = task  # чтобы Scene корректно прибрала задачу
            await scene.center.expect(Hello)
            await wait_until(lambda: scene.client.connected)
            task.cancel()
            # CancelledError погашен внутри run_forever: await завершается без исключения
            await asyncio.wait_for(task, timeout=2.0)
            assert task.done() and not task.cancelled()
            assert scene.client.connected is False

    run_scenario(scenario())


def test_run_forever_cancel_while_reconnecting() -> None:
    """run_forever можно отменить и в фазе реконнекта (сервер недоступен)."""

    async def scenario() -> None:
        port = free_tcp_port()
        async with Scene(start_server=False, port=port, autostart_client=False) as scene:
            task = asyncio.create_task(run_forever(scene.client))
            scene.run_task = task
            await asyncio.sleep(0.15)  # пара неудачных попыток подключения
            assert not task.done()
            task.cancel()
            # отмена в фазе backoff-паузы: run_forever гасит её и завершается штатно
            await asyncio.wait_for(task, timeout=2.0)
            assert task.done() and not task.cancelled()

    run_scenario(scenario())


# --- настройки весов из центра (scale_config) ---


class ConfigScene(Scene):
    """Сцена с обработчиком scale_config (Scene его не пробрасывает)."""

    def __init__(self, config_handler: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._config_handler = config_handler

    async def __aenter__(self) -> "ConfigScene":
        await super().__aenter__()
        # клиент пересобирается с обработчиком настроек (сигнатура CenterClient)
        if self.run_task is not None and not self.run_task.done():
            self.run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.run_task
        self.client = CenterClient(
            make_config(self.center.url),
            self.storage,
            equipment_status=make_equipment,
            on_weigh_request=echo_weigh_handler,
            on_scale_config=self._config_handler,
        )
        self.start_client()
        return self


def make_scale_config(**overrides: Any) -> ScaleConfigUpdate:
    """Типичный снимок настроек: полный цикл + COM-порт."""
    fields: dict[str, Any] = {
        "cycle": CycleSettings(
            zero_threshold_kg=150.0,
            vehicle_threshold_kg=600.0,
            zero_timeout_s=10.0,
            vehicle_timeout_s=90.0,
            stable_duration_s=5.0,
            stable_timeout_s=30.0,
            no_data_timeout_s=5.0,
        ),
        "scale_port": "COM11",
        "baudrate": 19200,
    }
    fields.update(overrides)
    return ScaleConfigUpdate(settings=ScaleSettingsPayload(**fields))


def test_scale_config_dispatched_and_status_sent_to_center() -> None:
    """scale_config от центра: обработчик получает разобранный снимок,
    его ConfigStatus уходит центру."""
    received: list[ScaleConfigUpdate] = []

    async def config_handler(update: ScaleConfigUpdate) -> ConfigStatus:
        received.append(update)
        return ConfigStatus(ok=True)

    async def scenario() -> None:
        async with ConfigScene(config_handler) as scene:
            await scene.center.expect(Hello)
            update = make_scale_config()
            await scene.center.connection.send(update.model_dump_json())
            status = await scene.center.expect(ConfigStatus)
            # отчёт обработчика дошёл центру как config_status
            assert status.ok is True
            assert status.rolled_back is False
            # обработчик получил снимок со всеми полями
            assert len(received) == 1
            settings = received[0].settings
            assert settings.scale_port == "COM11"
            assert settings.baudrate == 19200
            assert settings.cycle is not None
            assert settings.cycle.vehicle_threshold_kg == 600.0
            # соединение живо: применение настроек его не рвёт
            assert scene.client.connected is True
            assert len(scene.center.connections) == 1

    run_scenario(scenario())


def test_scale_config_rollback_status_reaches_center() -> None:
    """Откат COM-порта на агенте: центр получает ok=False + rolled_back=True."""

    async def config_handler(update: ScaleConfigUpdate) -> ConfigStatus:
        return ConfigStatus(ok=False, rolled_back=True, error="индикатор молчит на порту COM11")

    async def scenario() -> None:
        async with ConfigScene(config_handler) as scene:
            await scene.center.expect(Hello)
            await scene.center.connection.send(make_scale_config().model_dump_json())
            status = await scene.center.expect(ConfigStatus)
            assert status.ok is False
            assert status.rolled_back is True
            assert status.error and "COM11" in status.error

    run_scenario(scenario())


def test_slow_scale_config_handler_does_not_block_heartbeat() -> None:
    """Проверка порта длится секунды (до 12 с) — heartbeat не замирает."""

    async def slow_handler(update: ScaleConfigUpdate) -> ConfigStatus:
        await asyncio.sleep(0.5)  # имитация ожидания «индикатор ожил»
        return ConfigStatus(ok=True)

    async def scenario() -> None:
        async with ConfigScene(slow_handler) as scene:
            await scene.center.expect(Hello)
            await scene.center.connection.send(make_scale_config().model_dump_json())
            heartbeats_during = 0
            while True:
                message = await scene.center.next_message()
                if isinstance(message, Heartbeat):
                    heartbeats_during += 1
                elif isinstance(message, ConfigStatus):
                    assert message.ok is True
                    break
            assert heartbeats_during >= 2, "heartbeat заблокированы применением настроек"

    run_scenario(scenario())


def test_scale_config_without_handler_does_not_kill_client() -> None:
    """Обработчик не настроен: scale_config логируется и пропускается,
    клиент жив, config_status центру не уходит."""

    async def scenario() -> None:
        async with Scene() as scene:  # обычная сцена — без on_scale_config
            await scene.center.expect(Hello)
            await scene.center.connection.send(make_scale_config().model_dump_json())
            # ответа нет, но соединение живо и heartbeat продолжаются
            await scene.center.assert_no_message(ConfigStatus, during_s=0.3)
            await scene.center.expect(Heartbeat)
            assert scene.client.connected is True
            assert len(scene.center.connections) == 1

    run_scenario(scenario())


# --- время от центра (heartbeat_ack → on_server_time) ---


class ClockScene(Scene):
    """Сцена с колбэком времени центра (Scene его не пробрасывает)."""

    def __init__(self, on_server_time: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._on_server_time = on_server_time

    async def __aenter__(self) -> "ClockScene":
        await super().__aenter__()
        # клиент пересобирается с колбэком времени (сигнатура CenterClient)
        if self.run_task is not None and not self.run_task.done():
            self.run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.run_task
        self.client = CenterClient(
            make_config(self.center.url),
            self.storage,
            equipment_status=make_equipment,
            on_weigh_request=echo_weigh_handler,
            on_server_time=self._on_server_time,
        )
        self.start_client()
        return self


def test_heartbeat_ack_delivers_server_time_to_callback() -> None:
    """heartbeat_ack от центра: колбэк on_server_time получает тот самый
    server_time (aware datetime); каждый ack вызывает колбэк заново."""
    received: list[datetime] = []

    async def scenario() -> None:
        async with ClockScene(received.append) as scene:
            await scene.center.expect(Hello)
            server_time = datetime(2026, 8, 10, 6, 0, 0, tzinfo=UTC)
            ack = HeartbeatAck(server_time=server_time)
            await scene.center.connection.send(ack.model_dump_json())
            await wait_until(lambda: len(received) == 1)
            # дата дошла без искажений и осталась aware (для вычитания с now(UTC))
            assert received[0] == server_time
            assert received[0].tzinfo is not None
            # второй ack (следующий heartbeat) → колбэк вызван снова
            later = HeartbeatAck(server_time=server_time + timedelta(seconds=30))
            await scene.center.connection.send(later.model_dump_json())
            await wait_until(lambda: len(received) == 2)
            assert received[1] == server_time + timedelta(seconds=30)
            # приём ack не рвёт соединение
            assert scene.client.connected is True
            assert len(scene.center.connections) == 1

    run_scenario(scenario())


def test_heartbeat_ack_without_callback_keeps_client_alive() -> None:
    """Колбэк не настроен (on_server_time=None): ack молча пропускается,
    клиент жив, heartbeat продолжаются на том же соединении."""

    async def scenario() -> None:
        async with Scene() as scene:  # обычная сцена — без on_server_time
            await scene.center.expect(Hello)
            ack = HeartbeatAck(server_time=datetime.now(UTC))
            await scene.center.connection.send(ack.model_dump_json())
            await scene.center.expect(Heartbeat)
            assert scene.client.connected is True
            assert len(scene.center.connections) == 1

    run_scenario(scenario())


def test_server_time_callback_exception_keeps_session_alive() -> None:
    """Исключение в on_server_time (например, sqlite3.Error при записи
    смещения в БД) гасится на месте: сессия НЕ рвётся — как локальные
    ошибки БД в _send_pending. Следующий ack снова зовёт колбэк."""
    calls = {"n": 0}

    def broken_callback(server_time: datetime) -> None:
        calls["n"] += 1
        raise RuntimeError("БД смещения недоступна")

    async def scenario() -> None:
        async with ClockScene(broken_callback) as scene:
            await scene.center.expect(Hello)
            for expected in (1, 2):
                ack = HeartbeatAck(server_time=datetime.now(UTC))
                await scene.center.connection.send(ack.model_dump_json())
                await wait_until(lambda n=expected: calls["n"] == n)
            # соединение то же самое, heartbeat продолжают идти
            await scene.center.expect(Heartbeat)
            assert len(scene.center.connections) == 1
            assert scene.client.connected

    run_scenario(scenario())


# ---------------------------------------------------------------------------
# Хвост журнала по запросу центра (удалённая диагностика, 11.08.2026)
# ---------------------------------------------------------------------------


class LogTailScene(Scene):
    """Сцена с обработчиком журнала (сигнатура CenterClient)."""

    def __init__(self, handler: Callable[[int], tuple[list[str], str]]) -> None:
        super().__init__()
        self._handler = handler

    async def __aenter__(self) -> "LogTailScene":
        await super().__aenter__()
        if self.run_task is not None and not self.run_task.done():
            self.run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.run_task
        self.client = CenterClient(
            make_config(self.center.url),
            self.storage,
            equipment_status=make_equipment,
            on_weigh_request=echo_weigh_handler,
            on_log_tail=self._handler,
        )
        self.start_client()
        return self


def test_log_tail_request_answered() -> None:
    """Запрос центра → агент отвечает своим хвостом журнала и request_id."""
    calls: list[int] = []

    def handler(lines: int) -> tuple[list[str], str]:
        calls.append(lines)
        return (["первая", "вторая"], "C:/vesy-agent/logs/agent.log")

    async def scenario() -> None:
        async with LogTailScene(handler) as scene:
            await scene.center.expect(Hello)
            request = LogTailRequest(request_id=uuid4(), lines=42)
            await scene.center.connection.send(request.model_dump_json())
            response = await scene.center.expect(LogTailResponse)
            assert response.request_id == request.request_id
            assert response.lines == ["первая", "вторая"]
            assert response.location.endswith("agent.log")
            assert response.agent_id == "agent-test"
            assert calls == [42], "число строк из запроса не дошло до обработчика"
            assert scene.client.connected is True

    run_scenario(scenario())


def test_log_tail_without_handler_answers_empty() -> None:
    """Обработчик не настроен: центр получает пустой ответ, а не тишину —
    иначе панель ждала бы тайм-аут."""

    async def scenario() -> None:
        async with Scene() as scene:  # обычная сцена, без on_log_tail
            await scene.center.expect(Hello)
            request = LogTailRequest(request_id=uuid4())
            await scene.center.connection.send(request.model_dump_json())
            response = await scene.center.expect(LogTailResponse)
            assert response.lines == []
            assert scene.client.connected is True

    run_scenario(scenario())


def test_log_tail_handler_failure_keeps_session() -> None:
    """Чтение журнала упало (файл занят, нет прав) — пустой ответ, сессия жива."""

    def broken(lines: int) -> tuple[list[str], str]:
        raise OSError("файл занят")

    async def scenario() -> None:
        async with LogTailScene(broken) as scene:
            await scene.center.expect(Hello)
            await scene.center.connection.send(LogTailRequest(request_id=uuid4()).model_dump_json())
            response = await scene.center.expect(LogTailResponse)
            assert response.lines == []
            # соединение живо: следующий heartbeat приходит на том же канале
            await scene.center.expect(Heartbeat)
            assert len(scene.center.connections) == 1

    run_scenario(scenario())
