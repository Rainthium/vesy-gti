"""Тесты драйвера «CAS 22 byte» (agent/drivers/cas22.py).

Пакеты и потоки генерируются эмулятором tools/cas22_emulator.py
(PacketBuilder и сценарии) — он покрыт своими тестами и сверен
с эталонным парсером прототипа cas22_reader.py. Здесь проверяется
именно драйвер, три слоя:

1. ``parse_packet`` — чистый разбор одного 22-байтового пакета;
2. ``PacketAssembler`` — сборка пакетов из потока с ресинхронизацией;
3. ``Cas22Driver`` — фоновый поток чтения через pyserial-URL
   ``socket://`` с TCP-сервером эмулятора (интеграционно, без железа),
   включая смену порта на лету (``set_port``, настройки из центра).
"""

import asyncio
import contextlib
import socket
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from agent.drivers.base import ScaleState
from agent.drivers.cas22 import (
    PACKET_LEN,
    Cas22Driver,
    Cas22Packet,
    PacketAssembler,
    parse_packet,
)
from shared.enums import ScaleStatus
from tools.cas22_emulator import (
    PacketBuilder,
    ScenarioFactory,
    Step,
    drive_on,
    garbage,
    make_scenario,
    serve,
    stable_weight,
)

TARGET_KG = 12500.0

# Ускоренные тайминги драйвера — чтобы весь файл пробегал быстро
FAST_READ_TIMEOUT_S = 0.05
FAST_RX_ERROR_TIMEOUT_S = 0.5
FAST_REOPEN_DELAY_S = 0.15
RATE = 40.0  # пакетов в секунду в тестовых сценариях


@pytest.fixture
def builder() -> PacketBuilder:
    """Сборщик пакетов эмулятора с дискретностью по умолчанию (10 кг)."""
    return PacketBuilder()


def parse_ok(pkt: bytes) -> Cas22Packet:
    """Разобрать пакет, который обязан быть валидным."""
    packet = parse_packet(pkt)
    assert packet is not None, f"валидный пакет отвергнут: {pkt!r}"
    return packet


def patch_mass(pkt: bytes, mass: bytes) -> bytes:
    """Заменить поле массы (байты 9–16) в готовом пакете эмулятора."""
    assert len(mass) == 8
    return pkt[:9] + mass + pkt[17:]


def stream_of(steps: Iterator[Step]) -> bytes:
    """Склеить полезную нагрузку шагов сценария в один байтовый поток."""
    return b"".join(step.payload for step in steps)


# ---------------------------------------------------------------------------
# parse_packet: разбор одного пакета
# ---------------------------------------------------------------------------


class TestParsePacketValid:
    """Корректные пакеты всех флагов и режимов."""

    def test_stable_gross(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(12500, stable=True))
        assert packet == Cas22Packet(weight_kg=12500.0, stable=True, overload=False, gross=True)

    def test_unstable_gross(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(340, stable=False))
        assert packet == Cas22Packet(weight_kg=340.0, stable=False, overload=False, gross=True)

    def test_stable_net(self, builder: PacketBuilder) -> None:
        # Режим NT (нетто) → gross=False
        packet = parse_ok(builder.build(500, stable=True, mode=b"NT"))
        assert packet == Cas22Packet(weight_kg=500.0, stable=True, overload=False, gross=False)

    def test_unstable_net(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(500, stable=False, mode=b"NT"))
        assert packet.gross is False
        assert packet.stable is False

    def test_zero_weight(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(0, stable=True))
        assert packet.weight_kg == 0.0
        assert packet.stable is True
        assert packet.overload is False

    def test_space_inside_mass(self, builder: PacketBuilder) -> None:
        # Причуда реального индикатора: пробел-разделитель тысяч внутри числа
        pkt = builder.build(1460, space_in_mass=True)
        assert pkt[9:17] == b"   1 460"  # пробел действительно внутри
        assert parse_ok(pkt).weight_kg == 1460.0

    def test_plus_sign_inside_mass_stripped(self, builder: PacketBuilder) -> None:
        # Знак «+» внутри поля массы вычищается
        pkt = patch_mass(builder.build(12500), b"  +12500")
        assert parse_ok(pkt).weight_kg == 12500.0


class TestParsePacketNegative:
    """Отрицательный вес: знак в поле 6–7 и/или внутри поля массы."""

    def test_sign_in_status_field(self, builder: PacketBuilder) -> None:
        # Знак в поле знака/статуса (байты 6–7), поле массы без минуса
        pkt = builder.build(-30)
        assert pkt[6:8] == b" -"
        assert b"-" not in pkt[9:17]
        assert parse_ok(pkt).weight_kg == -30.0

    def test_sign_inside_mass_field(self, builder: PacketBuilder) -> None:
        # Минус внутри поля массы при «пустом» поле знака
        pkt = patch_mass(builder.build(30), b"     -30")
        assert pkt[6:8] == b"  "
        assert parse_ok(pkt).weight_kg == -30.0

    def test_sign_in_both_places(self, builder: PacketBuilder) -> None:
        # Минус и в поле знака, и в поле массы — не даёт «минус на минус»
        pkt = patch_mass(builder.build(-30), b"     -30")
        assert parse_ok(pkt).weight_kg == -30.0

    def test_negative_with_space_in_mass(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(-1460, space_in_mass=True))
        assert packet.weight_kg == -1460.0


class TestParsePacketOverload:
    """Перегруз OL: веса нет, стабильности нет."""

    def test_overload_gross(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(99990, overload=True))
        assert packet.weight_kg is None
        assert packet.overload is True
        assert packet.stable is False
        assert packet.gross is True

    def test_overload_net(self, builder: PacketBuilder) -> None:
        packet = parse_ok(builder.build(99990, overload=True, mode=b"NT"))
        assert packet.weight_kg is None
        assert packet.overload is True
        assert packet.gross is False


class TestParsePacketRejects:
    """Отбраковка мусора: неверная структура → None, без исключений."""

    def test_wrong_length_short(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(100)[:21]) is None

    def test_wrong_length_long(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(100) + b"X") is None

    def test_empty_bytes(self) -> None:
        assert parse_packet(b"") is None

    @pytest.mark.parametrize("comma_pos", [2, 5, 8])
    def test_comma_out_of_place(self, builder: PacketBuilder, comma_pos: int) -> None:
        # Каждая из трёх запятых-разделителей обязательна
        pkt = builder.build(12500)
        broken = pkt[:comma_pos] + b";" + pkt[comma_pos + 1 :]
        assert parse_packet(broken) is None

    def test_missing_crlf(self, builder: PacketBuilder) -> None:
        pkt = builder.build(12500)
        assert parse_packet(pkt[:20] + b"XX") is None

    def test_swapped_crlf(self, builder: PacketBuilder) -> None:
        pkt = builder.build(12500)
        assert parse_packet(pkt[:20] + b"\n\r") is None

    @pytest.mark.parametrize("flag", [b"XX", b"st", b"  ", b"S,"])
    def test_unknown_flag(self, builder: PacketBuilder, flag: bytes) -> None:
        pkt = builder.build(12500)
        assert parse_packet(flag + pkt[2:]) is None

    def test_non_ascii_mass(self, builder: PacketBuilder) -> None:
        assert parse_packet(patch_mass(builder.build(100), b"\xff" * 8)) is None

    def test_non_ascii_byte_inside_digits(self, builder: PacketBuilder) -> None:
        assert parse_packet(patch_mass(builder.build(100), b"   12\xff00")) is None

    def test_non_numeric_ascii_mass(self, builder: PacketBuilder) -> None:
        assert parse_packet(patch_mass(builder.build(100), b"  ABCDEF")) is None

    def test_blank_mass_is_zero(self, builder: PacketBuilder) -> None:
        # ФАКТ (совпадает с эталонным прототипом): пустое поле массы
        # (одни пробелы) трактуется как вес 0.0, пакет не отбраковывается
        packet = parse_ok(patch_mass(builder.build(100), b"        "))
        assert packet.weight_kg == 0.0

    def test_lone_minus_mass_is_zero(self, builder: PacketBuilder) -> None:
        # ФАКТ: поле массы из одного минуса → 0.0 (знак на ноль не влияет)
        packet = parse_ok(patch_mass(builder.build(100), b"       -"))
        assert packet.weight_kg == 0.0


# ---------------------------------------------------------------------------
# PacketAssembler: сборка потока и ресинхронизация
# ---------------------------------------------------------------------------


class TestPacketAssembler:
    """Сборка пакетов из потока: целые куски, разрезы, мусор."""

    def test_whole_packets_stream(self, builder: PacketBuilder) -> None:
        # Поток из целых пакетов одним куском → все разобраны по порядку
        stream = stream_of(stable_weight(builder, TARGET_KG, duration_s=2.0, rate=8.0))
        packets = PacketAssembler().feed(stream)
        assert len(packets) == 16
        assert all(
            p == Cas22Packet(weight_kg=TARGET_KG, stable=True, overload=False, gross=True)
            for p in packets
        )

    def test_mixed_flags_and_modes(self, builder: PacketBuilder) -> None:
        # Смена флагов ST/US/OL и режимов GS/NT в одном потоке
        stream = (
            builder.build(0)
            + builder.build(4000, stable=False)
            + builder.build(-30)
            + builder.build(99990, overload=True)
            + builder.build(200, mode=b"NT")
        )
        packets = PacketAssembler().feed(stream)
        assert packets == [
            Cas22Packet(weight_kg=0.0, stable=True, overload=False, gross=True),
            Cas22Packet(weight_kg=4000.0, stable=False, overload=False, gross=True),
            Cas22Packet(weight_kg=-30.0, stable=True, overload=False, gross=True),
            Cas22Packet(weight_kg=None, stable=False, overload=True, gross=True),
            Cas22Packet(weight_kg=200.0, stable=True, overload=False, gross=False),
        ]

    @pytest.mark.parametrize("chunk_size", [1, 5, 7])
    def test_packets_split_across_reads(self, builder: PacketBuilder, chunk_size: int) -> None:
        # Пакеты, разрезанные по границе чтения порта, собираются без потерь
        stream = stream_of(stable_weight(builder, TARGET_KG, duration_s=2.0, rate=8.0))
        assembler = PacketAssembler()
        packets: list[Cas22Packet] = []
        for i in range(0, len(stream), chunk_size):
            packets.extend(assembler.feed(stream[i : i + chunk_size]))
        assert len(packets) == 16
        assert all(p.weight_kg == TARGET_KG and p.stable for p in packets)

    @pytest.mark.parametrize("seed", range(10))
    def test_garbage_yields_no_false_packets(self, seed: int) -> None:
        # Сценарий garbage содержит ложный CR LF — ни одного ложного пакета
        assembler = PacketAssembler()
        packets = [p for step in garbage(seed=seed) for p in assembler.feed(step.payload)]
        assert packets == []

    def test_resync_after_garbage_scenario(self, builder: PacketBuilder) -> None:
        # После мусора настоящие пакеты разбираются все до единого
        steps = make_scenario("garbage", builder, TARGET_KG, 8.0)()
        assembler = PacketAssembler()
        packets = [p for step in steps for p in assembler.feed(step.payload)]
        assert len(packets) == 16
        assert all(p.weight_kg == TARGET_KG and p.stable for p in packets)

    def test_garbage_between_packets(self, builder: PacketBuilder) -> None:
        # Мусор до/между/после пакетов, с ложным CR LF и обрывком пакета
        junk = b"\x01\x02NOISE\r\nUS,GS" + b"\xf0" * 7
        pkt = builder.build(TARGET_KG)
        packets = PacketAssembler().feed(junk + pkt + junk + pkt + junk)
        assert len(packets) == 2
        assert all(p.weight_kg == TARGET_KG for p in packets)

    def test_buffer_is_bounded_on_endless_junk(self) -> None:
        # Сплошной мусор без CR LF не раздувает буфер (ограничение 4×22)
        assembler = PacketAssembler()
        for _ in range(50):
            assert assembler.feed(b"\x00\x01\x02garbage-without-crlf") == []
            assert len(assembler._buffer) <= 4 * PACKET_LEN

    def test_resync_after_big_junk_burst(self, builder: PacketBuilder) -> None:
        # Большой кусок мусора (больше лимита буфера), затем настоящие пакеты
        assembler = PacketAssembler()
        assert assembler.feed(b"g" * 500) == []
        packets = assembler.feed(builder.build(100) + builder.build(200))
        assert [p.weight_kg for p in packets] == [100.0, 200.0]

    def test_short_prefix_before_crlf(self, builder: PacketBuilder) -> None:
        # CR LF раньше, чем накопилось 22 байта, — не падает, пакет не теряется
        packets = PacketAssembler().feed(b"abc\r\n" + builder.build(300))
        assert [p.weight_kg for p in packets] == [300.0]


# ---------------------------------------------------------------------------
# Cas22Driver: интеграционные тесты через socket:// и TCP-сервер эмулятора
# ---------------------------------------------------------------------------


def free_port() -> int:
    """Взять свободный TCP-порт у ОС (bind на порт 0)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def wait_until(predicate: Callable[[], bool], timeout_s: float, poll_s: float = 0.005) -> bool:
    """Ждать выполнения условия с опросом; вернуть успел ли predicate."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll_s)
    return predicate()


class EmulatorServer:
    """asyncio-сервер эмулятора ``serve()`` в отдельном потоке.

    Останавливается детерминированно: отмена задачи сервера и добивание
    задач-обработчиков клиентов, чтобы тесты не текли потоками/сокетами.
    """

    def __init__(
        self,
        factory: ScenarioFactory,
        *,
        port: int | None = None,
        loop_forever: bool = True,
    ) -> None:
        self.port: int = port if port is not None else free_port()
        self._factory = factory
        self._loop_forever = loop_forever
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Запустить сервер и дождаться, пока он начнёт принимать клиентов."""
        self._thread = threading.Thread(target=self._run, name=f"emu:{self.port}", daemon=True)
        self._thread.start()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                    return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError(f"эмулятор не начал слушать порт {self.port}")

    def stop(self) -> None:
        loop, task = self._loop, self._task
        if loop is not None and task is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(task.cancel)
        if self._thread is not None:
            self._thread.join(5.0)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._task = loop.create_task(self._serve_until_cancelled())
        try:
            loop.run_until_complete(self._task)
        finally:
            pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    async def _serve_until_cancelled(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await serve("127.0.0.1", self.port, self._factory, loop_forever=self._loop_forever)


@contextlib.contextmanager
def running_driver(
    port: int, *, rx_error_timeout_s: float = FAST_RX_ERROR_TIMEOUT_S
) -> Iterator[Cas22Driver]:
    """Запущенный драйвер с ускоренными таймингами; stop() гарантирован."""
    driver = Cas22Driver(
        f"socket://127.0.0.1:{port}",
        read_timeout_s=FAST_READ_TIMEOUT_S,
        rx_error_timeout_s=rx_error_timeout_s,
        reopen_delay_s=FAST_REOPEN_DELAY_S,
    )
    driver.start()
    try:
        yield driver
    finally:
        driver.stop()


def driver_thread_names(port: int) -> list[str]:
    """Имена живых фоновых потоков драйвера для данного порта."""
    name = f"cas22:socket://127.0.0.1:{port}"
    return [t.name for t in threading.enumerate() if t.name == name]


@pytest.fixture(scope="module")
def steady_server() -> Iterator[EmulatorServer]:
    """Сервер с бесконечным стабильным весом TARGET_KG (нормальный поток)."""
    steady_builder = PacketBuilder()

    def factory() -> Iterator[Step]:
        return stable_weight(steady_builder, TARGET_KG, duration_s=30.0, rate=RATE)

    server = EmulatorServer(factory)
    server.start()
    yield server
    server.stop()


class TestDriverNormalStream:
    """Нормальный поток пакетов: статус OK, вес и стабильность обновляются."""

    def test_status_ok_with_weight(self, steady_server: EmulatorServer) -> None:
        with running_driver(steady_server.port) as driver:
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
            state = driver.state
            assert state.weight_kg == TARGET_KG
            assert state.stable is True
            assert state.overload is False
            assert state.last_packet_at is not None

    def test_last_packet_at_advances(self, steady_server: EmulatorServer) -> None:
        with running_driver(steady_server.port) as driver:
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
            first = driver.state.last_packet_at
            assert first is not None
            # свежие пакеты сдвигают отметку времени вперёд
            assert wait_until(lambda: (driver.state.last_packet_at or 0.0) > first, 2.0)

    def test_start_twice_does_not_spawn_threads(self, steady_server: EmulatorServer) -> None:
        with running_driver(steady_server.port) as driver:
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
            driver.start()  # повторный вызов при живом потоке — безвреден
            assert len(driver_thread_names(steady_server.port)) == 1
            assert driver.state.status is ScaleStatus.OK

    def test_stop_then_restart(self, steady_server: EmulatorServer) -> None:
        driver = Cas22Driver(
            f"socket://127.0.0.1:{steady_server.port}",
            read_timeout_s=FAST_READ_TIMEOUT_S,
            rx_error_timeout_s=FAST_RX_ERROR_TIMEOUT_S,
            reopen_delay_s=FAST_REOPEN_DELAY_S,
        )
        driver.start()
        try:
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
            driver.stop()
            # поток действительно завершён
            assert driver_thread_names(steady_server.port) == []
            # повторный start() снова читает поток
            driver.start()
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
        finally:
            driver.stop()

    def test_zero_is_not_supported(self) -> None:
        # Протокол только на чтение: zero() всегда False, без обращений к порту
        driver = Cas22Driver("socket://127.0.0.1:1")
        assert driver.zero() is False


class TestDriverCycle:
    """Сценарий цикла: драйвер видит нестабильную фазу и стабильную фиксацию."""

    def test_unstable_then_stable_target(self) -> None:
        cycle_builder = PacketBuilder()

        def factory() -> Iterator[Step]:
            return iter(
                (
                    *drive_on(cycle_builder, TARGET_KG, duration_s=0.8, rate=RATE),
                    *stable_weight(cycle_builder, TARGET_KG, duration_s=10.0, rate=RATE),
                )
            )

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port) as driver:
                snapshots: list[ScaleState] = []
                deadline = time.monotonic() + 6.0
                while time.monotonic() < deadline:
                    state = driver.state
                    snapshots.append(state)
                    if state.status is ScaleStatus.OK and state.stable:
                        break
                    time.sleep(0.003)
                final = snapshots[-1]
                # фаза фиксации: стабильный целевой вес
                assert final.status is ScaleStatus.OK
                assert final.stable is True
                assert final.weight_kg == TARGET_KG
                # фаза заезда наблюдалась: нестабильный ненулевой вес
                unstable = [
                    s
                    for s in snapshots
                    if s.status is ScaleStatus.OK and not s.stable and s.weight_kg is not None
                ]
                assert unstable, "нестабильная фаза заезда не наблюдалась"
                assert any(s.weight_kg is not None and s.weight_kg > 0 for s in unstable)
        finally:
            server.stop()


class TestDriverDropout:
    """Обрыв потока: соединение живо, но пакетов нет дольше таймаута."""

    def test_no_data_then_recovery(self) -> None:
        dropout_builder = PacketBuilder()

        def factory() -> Iterator[Step]:
            # 0.6 с пакетов → 1.4 с тишины; сервер повторяет сценарий по кругу
            return iter(
                (
                    *stable_weight(dropout_builder, 500.0, duration_s=0.6, rate=RATE),
                    Step(b"", 1.4),
                )
            )

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port, rx_error_timeout_s=0.4) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)

                seen_no_data: list[float | None] = []

                def saw_no_data() -> bool:
                    state = driver.state
                    if state.status is ScaleStatus.NO_DATA:
                        seen_no_data.append(state.last_packet_at)
                        return True
                    return False

                # тишина дольше rx_error_timeout_s → «нет данных»
                assert wait_until(saw_no_data, 3.0)
                # отметка последнего пакета сохранена (пакеты были до обрыва)
                assert seen_no_data[0] is not None
                # возобновление пакетов → снова OK с весом
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK and driver.state.weight_kg == 500.0
                    ),
                    3.0,
                )
        finally:
            server.stop()


class TestDriverGarbageOnly:
    """Сплошной мусор в потоке приравнивается к отсутствию данных."""

    def test_garbage_stream_leads_to_no_data_not_ok(self) -> None:
        def factory() -> Iterator[Step]:
            # только мусор (с ложными CR LF), ни одного валидного пакета
            return garbage(n_bytes=64, seed=3, rate=RATE)

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port, rx_error_timeout_s=0.4) as driver:
                seen_ok: list[ScaleState] = []

                def saw_no_data() -> bool:
                    state = driver.state
                    if state.status is ScaleStatus.OK:
                        seen_ok.append(state)
                    return state.status is ScaleStatus.NO_DATA

                # мусор дольше rx_error_timeout_s → «нет данных с индикатора»
                assert wait_until(saw_no_data, 3.0)
                # ни один кусок мусора не был принят за валидный пакет
                assert seen_ok == []
        finally:
            server.stop()


class TestDriverPortErrors:
    """Ошибки порта: недоступный сервер, разрыв соединения, автопереоткрытие."""

    def test_port_error_then_auto_reopen(self) -> None:
        port = free_port()  # сервер ещё не запущен — подключение отвергается
        reopen_builder = PacketBuilder()

        def factory() -> Iterator[Step]:
            return stable_weight(reopen_builder, TARGET_KG, duration_s=10.0, rate=RATE)

        with running_driver(port) as driver:

            def saw_port_error() -> bool:
                state = driver.state
                return state.status is ScaleStatus.PORT_ERROR and bool(state.error)

            assert wait_until(saw_port_error, 2.0)
            # драйвер жив и крутится в цикле переподключения
            assert driver_thread_names(port) != []

            server = EmulatorServer(factory, port=port)
            server.start()
            try:
                # автопереоткрытие: в течение пары reopen_delay_s статус OK
                assert wait_until(
                    lambda: driver.state.status is ScaleStatus.OK,
                    5 * FAST_REOPEN_DELAY_S + 2.0,
                )
                assert driver.state.weight_kg == TARGET_KG
            finally:
                server.stop()

    def test_server_disconnect_then_reconnect(self) -> None:
        drop_builder = PacketBuilder()

        def factory() -> Iterator[Step]:
            return stable_weight(drop_builder, TARGET_KG, duration_s=0.3, rate=RATE)

        # loop_forever=False: сервер закрывает соединение после сценария,
        # драйвер обязан переподключиться и продолжить чтение
        server = EmulatorServer(factory, loop_forever=False)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                # сервер закрыл соединение → драйвер фиксирует ошибку порта
                assert wait_until(lambda: driver.state.status is ScaleStatus.PORT_ERROR, 3.0)
                # и сам переподключается: поток снова читается
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                assert driver_thread_names(server.port) != []
        finally:
            server.stop()


class TestDriverSetPort:
    """Смена порта на лету (set_port, настройки из центра): драйвер
    останавливает чтение старого порта и продолжает на новом."""

    WEIGHT_A = 1000.0
    WEIGHT_B = 2000.0

    def _server(self, weight: float) -> EmulatorServer:
        builder = PacketBuilder()

        def factory() -> Iterator[Step]:
            return stable_weight(builder, weight, duration_s=30.0, rate=RATE)

        server = EmulatorServer(factory)
        server.start()
        return server

    def test_set_port_switches_between_two_emulators(self) -> None:
        """Драйвер читает эмулятор A; set_port на эмулятор B — статус OK
        с весом B, свойства port_url/baudrate обновлены, поток чтения один."""
        server_a = self._server(self.WEIGHT_A)
        server_b = self._server(self.WEIGHT_B)
        try:
            with running_driver(server_a.port) as driver:
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK
                        and driver.state.weight_kg == self.WEIGHT_A
                    ),
                    3.0,
                )
                new_url = f"socket://127.0.0.1:{server_b.port}"
                driver.set_port(new_url)
                # адрес сменился сразу, baudrate остался прежним (None = не менять)
                assert driver.port_url == new_url
                assert driver.baudrate == 9600
                # чтение продолжается уже с нового эмулятора
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK
                        and driver.state.weight_kg == self.WEIGHT_B
                    ),
                    3.0,
                )
                # старый поток чтения остановлен, новый — единственный
                assert driver_thread_names(server_a.port) == []
                assert len(driver_thread_names(server_b.port)) == 1
        finally:
            server_a.stop()
            server_b.stop()

    def test_set_port_resets_state_until_new_data(self) -> None:
        """set_port на мёртвый порт: старый вес не «залипает» — состояние
        сброшено в NO_DATA/PORT_ERROR (вызывающий код видит молчание и
        откатывает порт)."""
        server_a = self._server(self.WEIGHT_A)
        dead_port = free_port()  # никто не слушает
        try:
            with running_driver(server_a.port) as driver:
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK
                        and driver.state.weight_kg == self.WEIGHT_A
                    ),
                    3.0,
                )
                driver.set_port(f"socket://127.0.0.1:{dead_port}", 19200)
                assert driver.baudrate == 19200
                # вес старого порта не показывается как живой
                state = driver.state
                assert state.status is not ScaleStatus.OK
                assert wait_until(
                    lambda: driver.state.status in (ScaleStatus.NO_DATA, ScaleStatus.PORT_ERROR),
                    2.0,
                )
                # и OK больше не появляется (данных нет)
                assert not wait_until(lambda: driver.state.status is ScaleStatus.OK, 0.6)
                # откат обратно (как это делает SettingsManager) — чтение оживает
                driver.set_port(f"socket://127.0.0.1:{server_a.port}")
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK
                        and driver.state.weight_kg == self.WEIGHT_A
                    ),
                    3.0,
                )
        finally:
            server_a.stop()
