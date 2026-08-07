"""Драйвер протокола «CAS 22 byte» (индикаторы CAS CI-серии, напр. CI-201A).

Спецификация — docs/protocols/cas22.md; основа — прототип cas22_reader.py,
проверенный на реальном индикаторе (СВХ «Кызыл-Кыя», 06.08.2026).

Протокол: непрерывный поток 22-байтовых пакетов, 9600 8-N-1, без CRC.
Команд к весам нет — только чтение; обнуление — кнопкой >0< на индикаторе.

Надёжность (правило проекта №6):
- DTR/RTS не поднимаются (драйверы CH340 падают с ошибкой 31);
- порт автоматически переоткрывается при любом сбое, процесс не падает;
- нет валидных пакетов дольше 3 с — статус «нет данных с индикатора».
"""

import contextlib
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import serial

from agent.drivers.base import ScaleState
from shared.enums import ScaleStatus

if TYPE_CHECKING:
    from agent.drivers.base import ScaleDriver

PACKET_LEN = 22
FLAGS = (b"ST", b"US", b"OL")

DEFAULT_BAUDRATE = 9600
DEFAULT_READ_TIMEOUT_S = 0.2  # ReadyRxTimeOut в UniServer — 100 мс
DEFAULT_RX_ERROR_TIMEOUT_S = 3.0  # RxErrorTimeOut в UniServer — 3000 мс
DEFAULT_REOPEN_DELAY_S = 2.0  # пауза перед переоткрытием порта


@dataclass(frozen=True)
class Cas22Packet:
    """Разобранный пакет индикатора. ``weight_kg is None`` — перегруз."""

    weight_kg: float | None
    stable: bool
    overload: bool
    gross: bool  # True — брутто (GS), False — нетто (NT)


def parse_packet(pkt: bytes) -> Cas22Packet | None:
    """Разобрать один 22-байтовый пакет; None — пакет не прошёл проверку.

    Правила из спецификации: запятые на позициях 2/5/8, CR LF в конце,
    известный флаг состояния; пробелы и «+» ВНУТРИ поля массы удаляются
    (причуда реального индикатора, валившая первый прототип).
    """
    if len(pkt) != PACKET_LEN:
        return None
    if pkt[2:3] != b"," or pkt[5:6] != b"," or pkt[8:9] != b"," or pkt[20:22] != b"\r\n":
        return None
    flag = pkt[0:2]
    if flag not in FLAGS:  # дополнительная защита от случайного мусора
        return None
    mode = pkt[3:5]
    sign = pkt[6:8]
    mass_raw = pkt[9:17]

    if flag == b"OL":
        return Cas22Packet(weight_kg=None, stable=False, overload=True, gross=mode == b"GS")

    try:
        text = mass_raw.decode("ascii").replace(" ", "").replace("+", "")
        value = float(text) if text not in ("", "-") else 0.0
    except (UnicodeDecodeError, ValueError):
        return None
    if b"-" in sign or b"-" in mass_raw:
        value = -abs(value)

    return Cas22Packet(weight_kg=value, stable=flag == b"ST", overload=False, gross=mode == b"GS")


class PacketAssembler:
    """Сборка пакетов из непрерывного потока с ресинхронизацией.

    Алгоритм из спецификации: найти CR LF, взять 22 байта до него
    включительно, проверить структуру; мусор и обрывки отбрасываются,
    разбор продолжается со следующего CR LF.
    """

    def __init__(self) -> None:
        self._buffer = b""

    def feed(self, chunk: bytes) -> list[Cas22Packet]:
        """Добавить прочитанные байты, вернуть все собравшиеся пакеты."""
        self._buffer += chunk
        packets: list[Cas22Packet] = []
        while True:
            idx = self._buffer.find(b"\r\n")
            if idx < 0:
                # без CR LF пакета нет; не даём буферу расти на сплошном мусоре
                if len(self._buffer) > 4 * PACKET_LEN:
                    self._buffer = self._buffer[-PACKET_LEN:]
                break
            start = idx + 2 - PACKET_LEN
            candidate = self._buffer[start : idx + 2] if start >= 0 else b""
            self._buffer = self._buffer[idx + 2 :]
            packet = parse_packet(candidate)
            if packet is not None:
                packets.append(packet)
        return packets


class Cas22Driver:
    """Драйвер CAS 22 byte: фоновое чтение порта с автопереоткрытием.

    ``port_url`` — имя порта («COM5», «/dev/ttyUSB0») либо pyserial-URL
    («socket://localhost:4001» для эмулятора tools/cas22_emulator.py).
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
        """Запустить фоновый поток чтения (повторный вызов безвреден)."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run, name=f"cas22:{self._port_url}", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        """Остановить чтение; блокирует до завершения потока (не дольше 5 с)."""
        with self._lifecycle_lock:
            self._stop_event.set()
            if self._thread is not None:
                self._thread.join(timeout=5.0)
                if self._thread.is_alive():
                    # поток завис в блокирующем read() (битый CH340) — ссылку
                    # сохраняем, чтобы start() не породил второй поток чтения
                    return
                self._thread = None

    @property
    def state(self) -> ScaleState:
        with self._lock:
            return self._state

    def zero(self) -> bool:
        """Протокол не поддерживает команды — обнуление кнопкой >0< на индикаторе."""
        return False

    # --- внутреннее ---

    def _set_state(self, state: ScaleState) -> None:
        with self._lock:
            self._state = state

    def _run(self) -> None:
        """Главный цикл потока: открыть порт → читать; при сбое — переоткрыть.

        Цикл бесконечен до stop(): любое исключение порта переводит статус
        в PORT_ERROR и приводит к новой попытке через reopen_delay_s.
        """
        while not self._stop_event.is_set():
            port: serial.Serial | None = None
            try:
                port = self._open_port()
                self._read_loop(port)
            except (serial.SerialException, OSError) as exc:
                # включая ошибку 31 подвисшего CH340 — просто переоткроемся
                self._set_state(ScaleState(status=ScaleStatus.PORT_ERROR, error=str(exc)))
            finally:
                if port is not None:
                    with contextlib.suppress(serial.SerialException, OSError):
                        port.close()
            self._stop_event.wait(self._reopen_delay_s)

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

    def _read_loop(self, port: serial.Serial) -> None:
        """Читать поток, разбирать пакеты, обновлять состояние."""
        assembler = PacketAssembler()
        last_packet_at: float | None = None
        started_at = time.monotonic()

        while not self._stop_event.is_set():
            chunk: bytes = port.read(64)
            now = time.monotonic()

            if chunk:
                packets = assembler.feed(chunk)
                if packets:
                    last_packet_at = now
                    self._apply_packet(packets[-1], now)

            # тишина или сплошной мусор дольше таймаута — «нет данных»
            silence_from = last_packet_at if last_packet_at is not None else started_at
            if now - silence_from > self._rx_error_timeout_s:
                self._set_state(
                    ScaleState(status=ScaleStatus.NO_DATA, last_packet_at=last_packet_at)
                )

    def _apply_packet(self, packet: Cas22Packet, now: float) -> None:
        """Перенести разобранный пакет в публичное состояние."""
        self._set_state(
            ScaleState(
                status=ScaleStatus.OK,
                weight_kg=packet.weight_kg,
                stable=packet.stable,
                overload=packet.overload,
                last_packet_at=now,
            )
        )


if TYPE_CHECKING:
    # статическая проверка mypy: Cas22Driver соответствует протоколу ScaleDriver
    _contract_check: "ScaleDriver" = Cas22Driver("")
