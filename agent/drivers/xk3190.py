"""Драйвер семейства Yaohua XK3190 (DS3 — Кокчо-Коз, A9 — Кербен, D2+ — Беловодское).

У семейства ДВА формата непрерывной передачи, драйвер понимает оба и
различает их сам, по первому байту кадра, а после трёх верных кадров подряд
закрепляет формат потока (docs/protocols/xk3190.md):

1. Кадр VESAR (DS3, A9): STX, знак, 7 цифр ASCII, XOR-КС двумя
   hex-символами, ETX. Подтверждено буфером «Менеджера сервера весов АВТО»
   на Кокчо-Козе 03.09.2026 — ``02 2B 30 30 30 30 30 30 30 31 42 03``
   (пустые весы; XOR «+» и семи нулей = 0x1B). Цифры — ДЕСЯТЫЕ кг (урок
   первого запуска 03.09.2026: человек 80 кг показался как 800 при
   делителе 1; в 0.4.27 умолчание было 1, исправлено в 0.4.28), дискрета
   табло 20 кг. Оба параметра переопределяются в конфиге агента
   (``[scale] weight_divisor`` / ``discrete_kg``).

2. Кадр «=» (D2+, Беловодское, дамп 17.11.2026, 1200 бод, ~2 кадра/с):
   ``=`` и 8 символов — показание табло, записанное ЗАДОМ НАПЕРЁД
   (``=07000000`` — 70 кг, ``=00000000`` — пусто), контрольной суммы нет.
   Это вывод «на выносное табло»: число уже такое, как на индикаторе,
   поэтому ``weight_divisor`` к нему не применяется и к дискрете оно не
   приводится; ``discrete_kg`` задаёт только допуск стабильности.
   Десятичная точка и минус в живом дампе не встречались — разбор их
   допускает по описанию семейства (точка на своём месте, минус —
   старший символ), подтвердить на объекте.
"""

import re

from agent.drivers.vesar import (
    PACKET_LEN,
    STX,
    PacketAssembler,
    StabilityTracker,
    VesarDriver,
    VesarPacket,
    parse_packet,
)

DISPLAY_MARK = 0x3D  # «=»
DISPLAY_BODY_LEN = 8
DISPLAY_FRAME_LEN = 1 + DISPLAY_BODY_LEN
# верных кадров одного формата подряд до закрепления формата потока
FORMAT_LOCK_FRAMES = 3


# после разворота: гашёные старшие разряды (пробелы), необязательный минус,
# снова возможные пробелы, затем цифры с не более чем одной точкой — БЕЗ
# пробелов и минусов внутри числа: контрольной суммы у кадра нет, и разбор —
# единственный рубеж против искажённого символа (находки qa-tester 17.11.2026)
_DISPLAY_RE = re.compile(r"^ *(-?) *(\d+(?:\.\d+)?)$")


def parse_display_frame(body: bytes) -> VesarPacket | None:
    """Разобрать 8 символов кадра «=»; None — это не показание табло.

    Символы идут от младшего разряда к старшему: ``07000000`` → «00000070».
    Минус и пробелы-заполнители допустимы только перед числом (в старших
    разрядах); пробел или минус между цифрами — искажение, кадр отвергается.
    """
    if len(body) != DISPLAY_BODY_LEN:
        return None
    try:
        text = body[::-1].decode("ascii")
    except UnicodeDecodeError:
        return None
    match = _DISPLAY_RE.fullmatch(text)
    if match is None:
        return None
    value = float(match.group(2))
    if match.group(1) and value != 0:  # «минус ноль» — просто ноль
        value = -value
    return VesarPacket(raw_weight_kg=value, display=True)


class Xk3190Assembler(PacketAssembler):
    """Сборка кадров обоих форматов семейства из одного потока.

    Кадр начинается с STX (формат VESAR) или «=» (показание табло). Целые
    кадры форматов не путаются, но искажённый кадр VESAR может выглядеть
    кадром «=»: знак «-» (0x2D) от «=» (0x3D) отличает один бит, и следом
    как раз идут 8 цифр — получился бы «вес» больше 10 000 т (находка
    reviewer 17.11.2026). Поэтому после FORMAT_LOCK_FRAMES верных кадров
    одного формата подряд сборщик закрепляет формат и начало другого больше
    не ищет: индикатор формат на ходу не меняет, а новый сборщик создаётся
    при каждом открытии порта. До закрепления (доли секунды после старта)
    программная стабильность ещё не набрана, так что случайный кадр
    записан быть не может.
    """

    def __init__(self, *, divisor: float) -> None:
        super().__init__(divisor=divisor)
        self._locked_display: bool | None = None  # None — формат ещё не закреплён
        self._streak_display: bool | None = None
        self._streak = 0

    def _find_start(self) -> int:
        """Индекс ближайшего начала кадра в буфере; −1 — начала нет."""
        if self._locked_display is True:
            return self._buffer.find(DISPLAY_MARK)
        if self._locked_display is False:
            return self._buffer.find(STX)
        starts = [i for i in (self._buffer.find(STX), self._buffer.find(DISPLAY_MARK)) if i >= 0]
        return min(starts) if starts else -1

    def _count_valid(self, is_display: bool) -> None:
        """Учесть верный кадр; закрепить формат после серии одного вида."""
        if self._locked_display is not None:
            return
        if self._streak_display is is_display:
            self._streak += 1
        else:
            self._streak_display = is_display
            self._streak = 1
        if self._streak >= FORMAT_LOCK_FRAMES:
            self._locked_display = is_display

    def feed(self, chunk: bytes) -> list[VesarPacket]:
        """Добавить прочитанные байты, вернуть все собравшиеся кадры.

        Кандидат, не прошедший разбор, — мусор или рассинхрон: сдвиг на
        байт и поиск следующего начала.
        """
        self._buffer += chunk
        packets: list[VesarPacket] = []
        while True:
            start = self._find_start()
            if start < 0:
                self._buffer = b""
                break
            self._buffer = self._buffer[start:]
            is_display = self._buffer[0] == DISPLAY_MARK
            frame_len = DISPLAY_FRAME_LEN if is_display else PACKET_LEN
            if len(self._buffer) < frame_len:
                break
            candidate = self._buffer[:frame_len]
            packet = (
                parse_display_frame(candidate[1:])
                if is_display
                else parse_packet(candidate, divisor=self._divisor)
            )
            if packet is not None:
                packets.append(packet)
                self._count_valid(is_display)
                self._buffer = self._buffer[frame_len:]
            else:
                self._buffer = self._buffer[1:]
        return packets


class Xk3190Driver(VesarDriver):
    """XK3190: кадр VESAR (десятые кг, дискрета 20) либо кадр «=» (как табло)."""

    DEFAULT_WEIGHT_DIVISOR: float = 10.0
    DEFAULT_DISCRETE_KG: float = 20.0

    def _make_assembler(self) -> PacketAssembler:
        return Xk3190Assembler(divisor=self._weight_divisor)

    def _make_stability(self) -> StabilityTracker:
        # опорное показание — ради потока «=» с шагом кадров ~0,5 с; частым
        # потокам DS3/A9 (6–7 кадров/с) оно добавляет к окну не больше шага кадра
        return StabilityTracker(amplitude_kg=3 * self._discrete_kg, keep_anchor=True)
