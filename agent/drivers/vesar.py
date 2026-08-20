"""Драйвер протокола «VESAR» (индикатор весов SCS-80, СВХ «Кара-Суу»).

Спецификация — docs/protocols/vesar.md; протокол вскрыт по живым дампам
20.08.2026 (пустые весы и оператор на платформе), эталоны — в выгрузке
объекта (docs/objects/kara-suu.md).

Протокол: непрерывный поток 12-байтовых пакетов ~12,5 пакетов/с,
9600 8-N-1: STX 0x02, знак '+'/'-', масса 7 цифр ASCII с лидирующими
нулями (младший разряд — ДЕСЯТЫЕ кг), контрольная сумма XOR байтов
[1..8] двумя ASCII-hex цифрами, ETX 0x03. Команд к весам нет.

Отличия от cas22, важные для логики:
- флага стабильности в пакете нет — вычисляется программно, как в
  UniServer: амплитуда показаний за окно успокоения не больше трёх
  дискрет (настройки «Автостабилизация 3 дискрет / 1 с» с объекта);
- индикатор шлёт вес точнее табло (85,0 кг в порту при «80» на табло,
  дискретность 10 кг) — драйвер приводит вес к дискрете усечением
  по модулю вниз, как табло; правило подтверждается сверкой
  с UniServer на реальных машинах (docs/objects/kara-suu.md).

Надёжность (правило проекта №6): порт переоткрывается сам, DTR/RTS
не поднимаются, тишина дольше 3 с — статус «нет данных».
"""

import contextlib
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING

import serial

from agent.drivers.base import ScaleState
from shared.enums import ScaleStatus

if TYPE_CHECKING:
    from agent.drivers.base import ScaleDriver

PACKET_LEN = 12
STX = 0x02
ETX = 0x03

# дискретность индикатора: в порт идут десятые кг, табло показывает
# кратно 10 кг (85,0 в потоке = «80» на табло, подтверждено 20.08.2026).
# Появится второй vesar-объект с другой дискретой — вынести в конфиг.
DISCRETE_KG = 10.0

# программная стабильность — настройки UniServer с объекта:
# «Автостабилизация 3 дискрет / 1 с»
STABILITY_WINDOW_S = 1.0
STABILITY_AMPLITUDE_KG = 3 * DISCRETE_KG

DEFAULT_BAUDRATE = 9600
DEFAULT_READ_TIMEOUT_S = 0.2
DEFAULT_RX_ERROR_TIMEOUT_S = 3.0  # RxErrorTimeOut в UniServer — 3000 мс
DEFAULT_REOPEN_DELAY_S = 2.0


@dataclass(frozen=True)
class VesarPacket:
    """Разобранный пакет: вес в кг с точностью протокола (десятые)."""

    raw_weight_kg: float


def parse_packet(pkt: bytes) -> VesarPacket | None:
    """Разобрать один 12-байтовый пакет; None — пакет не прошёл проверку.

    Проверяются рамки STX/ETX, знак, 7 цифр массы и контрольная сумма
    (XOR байтов знака и массы, две ASCII-hex цифры в верхнем регистре —
    подтверждена на всех уникальных пакетах живых дампов 20.08.2026).
    """
    if len(pkt) != PACKET_LEN or pkt[0] != STX or pkt[11] != ETX:
        return None
    sign = pkt[1:2]
    if sign not in (b"+", b"-"):
        return None
    digits = pkt[2:9]
    if not digits.isdigit():
        return None
    checksum = 0
    for byte in pkt[1:9]:
        checksum ^= byte
    try:
        declared = int(pkt[9:11].decode("ascii"), 16)
    except (UnicodeDecodeError, ValueError):
        return None
    if checksum != declared:
        return None
    value = int(digits) / 10.0  # младший разряд — десятые кг
    if sign == b"-":
        value = -value
    return VesarPacket(raw_weight_kg=value)


def quantize(raw_weight_kg: float) -> float:
    """Привести вес к дискрете индикатора усечением по модулю вниз.

    Поведение табло, зафиксированное 20.08.2026: 85,0 в порту → «80»
    на табло. Знак сохраняется: −85,0 → −80.
    """
    sign = -1.0 if raw_weight_kg < 0 else 1.0
    return sign * (abs(raw_weight_kg) // DISCRETE_KG) * DISCRETE_KG


class PacketAssembler:
    """Сборка пакетов из потока с ресинхронизацией по STX/ETX.

    Мусор до STX отбрасывается; кандидат без ETX на своём месте —
    ложный STX, разбор сдвигается на байт. Буфер ограничен, чтобы
    сплошной мусор без STX не копился бесконечно.
    """

    def __init__(self) -> None:
        self._buffer = b""

    def feed(self, chunk: bytes) -> list[VesarPacket]:
        """Добавить прочитанные байты, вернуть все собравшиеся пакеты."""
        self._buffer += chunk
        packets: list[VesarPacket] = []
        while True:
            idx = self._buffer.find(bytes([STX]))
            if idx < 0:
                self._buffer = b""
                break
            self._buffer = self._buffer[idx:]
            if len(self._buffer) < PACKET_LEN:
                break
            packet = parse_packet(self._buffer[:PACKET_LEN])
            if packet is not None:
                packets.append(packet)
                self._buffer = self._buffer[PACKET_LEN:]
            else:
                # ложный STX (мусор или рассинхрон) — ищем следующий
                self._buffer = self._buffer[1:]
        return packets


class StabilityTracker:
    """Программный флаг стабильности: амплитуда показаний за окно.

    Как «Автостабилизация» UniServer: вес стабилен, когда окно набрано
    целиком и разброс сырых показаний в нём не превышает трёх дискрет.
    До накопления окна (старт потока, обрыв) вес считается нестабильным.
    """

    def __init__(
        self,
        *,
        window_s: float = STABILITY_WINDOW_S,
        amplitude_kg: float = STABILITY_AMPLITUDE_KG,
    ) -> None:
        self._window_s = window_s
        self._amplitude_kg = amplitude_kg
        self._samples: deque[tuple[float, float]] = deque()

    def update(self, raw_weight_kg: float, now: float) -> bool:
        """Добавить показание, вернуть стабильность на текущий момент."""
        self._samples.append((now, raw_weight_kg))
        floor = now - self._window_s
        while self._samples and self._samples[0][0] < floor:
            self._samples.popleft()
        if not self._samples or self._samples[0][0] > floor + 0.2 * self._window_s:
            return False  # окно ещё не набрано
        weights = [weight for _, weight in self._samples]
        return max(weights) - min(weights) <= self._amplitude_kg

    def reset(self) -> None:
        """Сбросить окно (переоткрытие порта: старые показания не в счёт)."""
        self._samples.clear()


class VesarDriver:
    """Драйвер VESAR: фоновое чтение порта с автопереоткрытием.

    ``port_url`` — имя порта («COM4») либо pyserial-URL
    («socket://127.0.0.1:4001» — ретрансляция UniServer или эмулятор
    tools/vesar_emulator.py).
    """

    def __init__(
        self,
        port_url: str,
        *,
        baudrate: int = DEFAULT_BAUDRATE,
        read_timeout_s: float = DEFAULT_READ_TIMEOUT_S,
        rx_error_timeout_s: float = DEFAULT_RX_ERROR_TIMEOUT_S,
        reopen_delay_s: float = DEFAULT_REOPEN_DELAY_S,
    ) -> None:
        self._port_url = port_url
        self._baudrate = baudrate
        self._read_timeout_s = read_timeout_s
        self._rx_error_timeout_s = rx_error_timeout_s
        self._reopen_delay_s = reopen_delay_s

        self._lock = threading.Lock()
        # отдельный лок жизненного цикла: state-лок нельзя держать во время
        # join() — читающий поток обновляет state и получился бы дедлок
        self._lifecycle_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = ScaleState(status=ScaleStatus.NO_DATA)

    # --- публичный интерфейс ScaleDriver ---

    def start(self) -> None:
        """Запустить фоновый поток чтения (повторный вызов безвреден).

        У каждого запуска свой stop-event — как в cas22 (зависший
        в блокирующем read() поток не мешает новому запуску).
        """
        with self._lifecycle_lock:
            if (
                self._thread is not None
                and self._thread.is_alive()
                and not self._stop_event.is_set()
            ):
                return
            self._stop_event = threading.Event()
            self._thread = threading.Thread(
                target=self._run,
                args=(self._stop_event,),
                name=f"vesar:{self._port_url}",
                daemon=True,
            )
            self._thread.start()

    @property
    def port_url(self) -> str:
        return self._port_url

    @property
    def baudrate(self) -> int:
        return self._baudrate

    def set_port(self, port_url: str, baudrate: int | None = None) -> None:
        """Переключить порт/скорость (настройки из центра) — как в cas22."""
        self.stop()
        self._port_url = port_url
        if baudrate is not None:
            self._baudrate = baudrate
        self._set_state(ScaleState(status=ScaleStatus.NO_DATA))
        self.start()

    def stop(self) -> None:
        """Остановить чтение; блокирует до завершения потока (не дольше 5 с)."""
        with self._lifecycle_lock:
            self._stop_event.set()
            if self._thread is not None:
                self._thread.join(timeout=5.0)
                if self._thread.is_alive():
                    return  # зависший read(); start() заведёт новый поток
                self._thread = None

    @property
    def state(self) -> ScaleState:
        with self._lock:
            return self._state

    def zero(self) -> bool:
        """Команд в протоколе нет — обнуление кнопкой на индикаторе."""
        return False

    # --- внутреннее ---

    def _set_state(self, state: ScaleState) -> None:
        with self._lock:
            self._state = state

    def _run(self, stop_event: threading.Event) -> None:
        """Открыть порт → читать; при сбое — переоткрыть (правило №6)."""
        while not stop_event.is_set():
            port: serial.Serial | None = None
            try:
                port = self._open_port()
                self._read_loop(port, stop_event)
            except (serial.SerialException, OSError) as exc:
                self._set_state(ScaleState(status=ScaleStatus.PORT_ERROR, error=str(exc)))
            finally:
                if port is not None:
                    with contextlib.suppress(serial.SerialException, OSError):
                        port.close()
            stop_event.wait(self._reopen_delay_s)

    def _open_port(self) -> serial.Serial:
        """Открыть порт, НЕ поднимая DTR/RTS (правило проекта №6)."""
        port = serial.serial_for_url(self._port_url, do_not_open=True)
        port.baudrate = self._baudrate
        port.bytesize = serial.EIGHTBITS
        port.stopbits = serial.STOPBITS_ONE
        port.parity = serial.PARITY_NONE
        port.timeout = self._read_timeout_s
        port.dtr = False
        port.rts = False
        port.open()
        return port

    def _read_loop(self, port: serial.Serial, stop_event: threading.Event) -> None:
        """Читать поток, разбирать пакеты, обновлять состояние."""
        assembler = PacketAssembler()
        stability = StabilityTracker()
        last_packet_at: float | None = None
        started_at = time.monotonic()

        while not stop_event.is_set():
            chunk: bytes = port.read(64)
            now = time.monotonic()

            if chunk:
                packets = assembler.feed(chunk)
                if packets:
                    last_packet_at = now
                    # стабильность видят ВСЕ пакеты чанка: качание веса,
                    # у которого последним в каждом чанке оказывается одно
                    # и то же значение, не должно считаться ровным
                    # (находка qa-tester 20.08.2026)
                    stable = False
                    for packet in packets:
                        stable = stability.update(packet.raw_weight_kg, now)
                    self._set_state(
                        ScaleState(
                            status=ScaleStatus.OK,
                            weight_kg=quantize(packets[-1].raw_weight_kg),
                            stable=stable,
                            overload=False,
                            last_packet_at=now,
                        )
                    )

            silence_from = last_packet_at if last_packet_at is not None else started_at
            if now - silence_from > self._rx_error_timeout_s:
                stability.reset()
                self._set_state(
                    ScaleState(status=ScaleStatus.NO_DATA, last_packet_at=last_packet_at)
                )


if TYPE_CHECKING:
    # статическая проверка mypy: VesarDriver соответствует протоколу ScaleDriver
    _contract_check: "ScaleDriver" = VesarDriver("")
