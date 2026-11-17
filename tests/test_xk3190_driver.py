"""Драйвер xk3190: ДВА формата потока семейства Yaohua XK3190 в одном драйвере.

1. Кадр VESAR (DS3 — Кокчо-Коз, A9 — Кербен; 0.4.27/0.4.28): STX, знак,
   7 цифр, XOR-КС, ETX; цифры — десятые кг (делитель 10 — урок первого
   запуска 03.09.2026: при делителе 1 человек 80 кг показался как 800),
   дискрета 20. Проверяется: разбор кадра с явным делителем, квантование
   к 20 кг, умолчания класса против vesar, переопределение из конфига и
   сквозной проход кадра Кокчо-Коза через цикл чтения драйвера.

2. Кадр «=» (D2+ — Беловодское, живой дамп 17.11.2026, 1200 бод,
   ~2 кадра/с): «=» и 8 символов показания табло задом наперёд, без
   контрольной суммы. Проверяется: разбор тела (parse_display_frame) и
   отказ на мусор, сборщик Xk3190Assembler (оба формата из одного потока,
   ресинхронизация, потерянные байты, ограниченность буфера, живой дамп
   tests/data/xk3190_d2plus_person_70kg.bin), StabilityTracker с опорным
   показанием (keep_anchor) на редком потоке и сквозной проход через
   цикл чтения: вес как на табло — без делителя и без квантования.

Находки первого прогона (17.11.2026) исправлены в драйвере и закреплены
здесь: минус и пробел внутри числа отвергаются, «минус ноль» — просто
ноль, опорное показание стабильности не старше двух окон.
"""

import math
import random
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import agent.drivers.vesar as vesar_module
from agent.config import ScaleSection
from agent.drivers import create_driver
from agent.drivers.base import ScaleState
from agent.drivers.vesar import (
    PacketAssembler,
    StabilityTracker,
    VesarDriver,
    VesarPacket,
    parse_packet,
    quantize,
)
from agent.drivers.xk3190 import (
    DISPLAY_FRAME_LEN,
    FORMAT_LOCK_FRAMES,
    Xk3190Assembler,
    Xk3190Driver,
    parse_display_frame,
)
from shared.enums import ScaleStatus

# живой дамп XK3190-D2+ (Беловодское, 17.11.2026, 1200 бод): пустые весы →
# один кадр 60 → 16 кадров 70; в конце — оборванный кадр (один байт «=»)
D2PLUS_DUMP = Path(__file__).parent / "data" / "xk3190_d2plus_person_70kg.bin"

# буфер «Менеджера сервера весов АВТО» на Кокчо-Козе 03.09.2026 (пустые весы)
KOKCHO_EMPTY = bytes.fromhex("02 2B 30 30 30 30 30 30 30 31 42 03")


def frame(sign: bytes, digits: str) -> bytes:
    """Собрать кадр с верной контрольной суммой (XOR знака и цифр)."""
    body = sign + digits.encode("ascii")
    checksum = 0
    for byte in body:
        checksum ^= byte
    return b"\x02" + body + f"{checksum:02X}".encode("ascii") + b"\x03"


class TestParseWithDivisor:
    def test_kokcho_buffer_is_zero_and_checksum_matches(self) -> None:
        assert frame(b"+", "0000000") == KOKCHO_EMPTY
        packet = parse_packet(KOKCHO_EMPTY, divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == 0.0

    def test_digits_are_whole_kilograms_with_divisor_one(self) -> None:
        packet = parse_packet(frame(b"+", "0037000"), divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == 37000.0

    def test_default_divisor_stays_tenths_for_vesar(self) -> None:
        packet = parse_packet(frame(b"+", "0037000"))
        assert packet is not None and packet.raw_weight_kg == 3700.0

    def test_negative_sign_keeps_divisor(self) -> None:
        packet = parse_packet(frame(b"-", "0000080"), divisor=1.0)
        assert packet is not None and packet.raw_weight_kg == -80.0

    def test_assembler_passes_divisor_through(self) -> None:
        assembler = PacketAssembler(divisor=1.0)
        packets = assembler.feed(b"\xff" + frame(b"+", "0000080") + KOKCHO_EMPTY)
        assert [p.raw_weight_kg for p in packets] == [80.0, 0.0]


class TestQuantizeDiscrete20:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [(37010.0, 37000.0), (37000.0, 37000.0), (19.9, 0.0), (20.0, 20.0), (-85.0, -80.0)],
    )
    def test_truncates_to_twenty(self, raw: float, expected: float) -> None:
        assert quantize(raw, 20.0) == expected

    def test_default_discrete_is_ten_for_vesar(self) -> None:
        assert quantize(85.0) == 80.0


class TestDefaultsAndRegistry:
    def test_xk3190_defaults(self) -> None:
        """Десятые кг (как VESAR) и дискрета 20 — по живому весу Кокчо-Коза."""
        driver = create_driver("xk3190", "loop://", baudrate=1200)
        assert isinstance(driver, Xk3190Driver)
        assert driver.weight_divisor == 10.0
        assert driver.discrete_kg == 20.0
        assert driver.baudrate == 1200

    def test_vesar_defaults_unchanged(self) -> None:
        driver = create_driver("vesar", "loop://", baudrate=9600)
        assert type(driver) is VesarDriver
        assert driver.weight_divisor == 10.0
        assert driver.discrete_kg == 10.0

    def test_config_overrides_class_defaults(self) -> None:
        driver = create_driver("xk3190", "loop://", baudrate=9600, weight_divisor=1, discrete_kg=10)
        assert isinstance(driver, Xk3190Driver)
        assert driver.weight_divisor == 1.0
        assert driver.discrete_kg == 10.0

    def test_cas22_rejects_frame_options(self) -> None:
        with pytest.raises(ValueError, match="cas22"):
            create_driver("cas22", "loop://", baudrate=9600, discrete_kg=20)


class TestScaleSection:
    def test_accepts_xk3190_with_options(self) -> None:
        section = ScaleSection(driver="xk3190", port="COM7", baudrate=1200, discrete_kg=20)
        assert section.driver == "xk3190"
        assert section.weight_divisor is None
        assert section.discrete_kg == 20.0

    def test_options_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            ScaleSection(driver="xk3190", port="COM7", discrete_kg=0)

    def test_unknown_driver_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ScaleSection(driver="xk3191", port="COM7")  # type: ignore[arg-type]


class _FakePort:
    """Порт, отдающий кадры по одному и гасящий цикл, когда они кончились."""

    def __init__(self, frames: list[bytes], stop_event: threading.Event) -> None:
        self._frames = list(frames)
        self._stop_event = stop_event

    def read(self, _size: int) -> bytes:
        if self._frames:
            return self._frames.pop(0)
        self._stop_event.set()
        return b""


def test_read_loop_reports_tenths_quantized_to_twenty() -> None:
    """«+0000800» — человек 80 кг (десятые), «+0370100» — 37 010 кг → 37 000."""
    driver = Xk3190Driver("loop://", baudrate=1200)
    stop_event = threading.Event()
    port = _FakePort([KOKCHO_EMPTY, frame(b"+", "0000800")], stop_event)
    driver._read_loop(port, stop_event)  # type: ignore[arg-type]
    assert driver.state.status is ScaleStatus.OK
    assert driver.state.weight_kg == 80.0

    stop_event = threading.Event()
    port = _FakePort([frame(b"+", "0370100")], stop_event)
    driver._read_loop(port, stop_event)  # type: ignore[arg-type]
    assert driver.state.weight_kg == 37000.0


# ---------------------------------------------------------------------------
# Кадр «=» (XK3190-D2+): разбор тела
# ---------------------------------------------------------------------------


def display_frame(shown: str) -> bytes:
    """Собрать кадр «=» по тексту табло: ``"70"`` → ``b"=07000000"``.

    Текст дополняется слева нулями до 8 символов и разворачивается —
    как делает индикатор (младший разряд идёт первым).
    """
    assert len(shown) <= 8
    return b"=" + shown.rjust(8, "0").encode("ascii")[::-1]


def display_weights(packets: list[VesarPacket]) -> list[float]:
    return [packet.raw_weight_kg for packet in packets]


class TestParseDisplayFrame:
    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            (b"00000000", 0.0),  # живой дамп: пустые весы
            (b"06000000", 60.0),  # живой дамп: человек встаёт
            (b"07000000", 70.0),  # живой дамп: человек 70 кг
            (b"06412000", 21460.0),  # гружёная машина
            (b"00008000", 80000.0),  # НПВ 80 т
            (b"99999999", 99999999.0),  # все разряды заняты
            (b"10000000", 1.0),  # младший разряд — ПЕРВЫЙ байт тела
            (b"00000001", 10000000.0),  # старший разряд — ПОСЛЕДНИЙ байт тела
        ],
    )
    def test_digits_are_reversed_display(self, body: bytes, expected: float) -> None:
        packet = parse_display_frame(body)
        assert packet is not None
        assert packet.raw_weight_kg == expected
        assert packet.display is True

    def test_helper_matches_live_frames(self) -> None:
        """Помощник тестов собирает ровно те кадры, что в живом дампе."""
        assert display_frame("70") == b"=07000000"
        assert display_frame("60") == b"=06000000"
        assert display_frame("21460") == b"=06412000"

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            (b"0700000-", -70.0),  # минус — старший символ (последний байт тела)
            (b"0200000-", -20.0),
            (b"07     -", -70.0),  # минус, затем гашёные разряды
            (b"07-     ", -70.0),  # минус вплотную к цифрам
        ],
    )
    def test_minus_is_senior_character(self, body: bytes, expected: float) -> None:
        packet = parse_display_frame(body)
        assert packet is not None and packet.raw_weight_kg == expected
        assert packet.display is True

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            (b"5.210000", 12.5),  # табло «00012.5»
            (b"05.41200", 214.5),  # табло «00214.50»
            (b"00.00000", 0.0),
            (b"5.21000-", -12.5),
            (b"5.21    ", 12.5),
            (b"54.12   ", 21.45),
        ],
    )
    def test_decimal_point(self, body: bytes, expected: float) -> None:
        packet = parse_display_frame(body)
        assert packet is not None
        assert packet.raw_weight_kg == pytest.approx(expected)

    @pytest.mark.parametrize(
        ("body", "expected"),
        [
            (b"07      ", 70.0),  # гашёные старшие разряды
            (b"0       ", 0.0),
            (b"06412   ", 21460.0),
        ],
    )
    def test_blank_fillers_are_dropped(self, body: bytes, expected: float) -> None:
        packet = parse_display_frame(body)
        assert packet is not None and packet.raw_weight_kg == expected

    @pytest.mark.parametrize(
        "body",
        [
            b"",  # пусто
            b"0700000",  # 7 байт
            b"070000000",  # 9 байт
            b"=07000000",  # кадр целиком вместо тела
            b"0700000A",  # буква
            b"0700000a",
            b"07e00000",  # «экспонента» — float() её понял бы
            b"0x700000",
            b"fni00000",  # «inf» задом наперёд
            b"nan00000",
            b"0700000+",  # плюс в формате не описан
            b"070000--",  # два минуса
            b"-700000-",
            b"0.7.0000",  # две точки
            b"..000000",
            b".7000000",  # после разворота «0000007.» — точка без дробной части
            b".0000000",
            b".       ",  # одна точка
            b"        ",  # одни пробелы
            b"-       ",  # один минус
            b"       -",
            b".-      ",
            b"0700\xff000",  # не-ASCII
            b"\xb2\xb2\xb2\xb2\xb2\xb2\xb2\xb2",  # «²» в latin-1: str.isdigit() сказал бы True
            b"\xd9\xa0\xd9\xa0\xd9\xa0\xd9\xa0",  # арабские цифры в UTF-8
            b"070=0000",  # «=» внутри тела — это начало следующего кадра
            b"0700000=",
            b"0700\x02000",  # STX внутри тела
            b"0700\x00000",  # NUL
            b"070000\t0",  # табуляция — не пробел-заполнитель
            b"07000\r\n0",
            b"0700_000",  # float() допускает «_» между цифрами
        ],
    )
    def test_garbage_is_rejected(self, body: bytes) -> None:
        assert parse_display_frame(body) is None

    @pytest.mark.parametrize(
        "body",
        [
            b"0700000.",  # после разворота «.0000070»
            b"07.     ",  # «     .70»
            b"07.    -",  # «-    .70»
        ],
    )
    def test_leading_point_without_whole_part_is_rejected(self, body: bytes) -> None:
        """Точка без целой части: табло всегда показывает «0.70», не «.70»."""
        assert parse_display_frame(body) is None

    def test_any_single_byte_never_raises_and_never_gives_odd_value(self) -> None:
        """Любой байт 0..255 в любой позиции тела: либо None, либо конечное число.

        Контрольной суммы у кадра нет, поэтому разбор — единственный рубеж:
        исключение уронило бы цикл чтения, а inf/nan ушли бы в состояние.
        """
        for position in range(8):
            for value in range(256):
                body = bytearray(b"06412000")
                body[position] = value
                packet = parse_display_frame(bytes(body))
                if packet is None:
                    continue
                assert chr(value) in "0123456789 .-", (position, value)
                assert math.isfinite(packet.raw_weight_kg)
                assert packet.display is True

    def test_minus_inside_blank_fillers_is_accepted(self) -> None:
        """ФИКСАЦИЯ фактического поведения: минус среди гашёных разрядов
        («  -   70») принимается. Где D2+ рисует минус при гашёных нулях,
        по живому дампу неизвестно — подтвердить на объекте."""
        packet = parse_display_frame(b"07   -  ")
        assert packet is not None and packet.raw_weight_kg == -70.0

    @pytest.mark.parametrize(
        "body",
        [
            b"0700-000",  # после разворота «000-0070»
            b"07-00000",  # «00000-70»
            b"-0700000",  # «0000070-» — минус в МЛАДШЕМ разряде
            b"06-12000",  # «00021-60» было −2160: битая цифра становилась минусом
        ],
    )
    def test_minus_between_digits_is_rejected(self, body: bytes) -> None:
        assert parse_display_frame(body) is None

    @pytest.mark.parametrize(
        "body",
        [
            b"0 7     ",  # «     7 0»
            b"06 12000",  # «00021 60»
            b" 6412000",  # «0002146 » — гашёный МЛАДШИЙ разряд
            b"5. 21000",  # «00012 .5»
        ],
    )
    def test_blank_between_digits_is_rejected(self, body: bytes) -> None:
        assert parse_display_frame(body) is None

    def test_negative_zero_is_plain_zero(self) -> None:
        packet = parse_display_frame(b"0000000-")
        assert packet is not None
        assert math.copysign(1.0, packet.raw_weight_kg) == 1.0


# ---------------------------------------------------------------------------
# Сборщик кадров обоих форматов
# ---------------------------------------------------------------------------


def feed_all(assembler: PacketAssembler, chunks: list[bytes]) -> list[VesarPacket]:
    packets: list[VesarPacket] = []
    for chunk in chunks:
        packets.extend(assembler.feed(chunk))
    return packets


class TestXk3190AssemblerDisplay:
    def test_clean_stream(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = display_frame("0") + display_frame("60") + display_frame("70")
        packets = assembler.feed(stream)
        assert display_weights(packets) == [0.0, 60.0, 70.0]
        assert all(packet.display for packet in packets)

    def test_divisor_is_not_applied_to_display_frame(self) -> None:
        """Делитель сборщика — только для кадра VESAR; «=» — уже показание табло."""
        for divisor in (1.0, 10.0, 100.0):
            assembler = Xk3190Assembler(divisor=divisor)
            assert display_weights(assembler.feed(display_frame("21460"))) == [21460.0]

    def test_byte_by_byte(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = display_frame("0") + display_frame("21460") + display_frame("70")
        emitted: list[tuple[int, float]] = []
        for index in range(len(stream)):
            for packet in assembler.feed(stream[index : index + 1]):
                emitted.append((index, packet.raw_weight_kg))
        # кадр выдаётся ровно на своём последнем байте, не раньше и не позже
        assert emitted == [(8, 0.0), (17, 21460.0), (26, 70.0)]

    @pytest.mark.parametrize("cut", range(1, 2 * DISPLAY_FRAME_LEN))
    def test_frame_split_at_every_position(self, cut: int) -> None:
        stream = display_frame("21460") + display_frame("70")
        assembler = Xk3190Assembler(divisor=10.0)
        packets = feed_all(assembler, [stream[:cut], stream[cut:]])
        assert display_weights(packets) == [21460.0, 70.0]

    def test_incomplete_frame_waits_for_the_rest(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        assert assembler.feed(b"=0700000") == []
        assert display_weights(assembler.feed(b"0")) == [70.0]

    @pytest.mark.parametrize("lost", range(DISPLAY_FRAME_LEN))
    def test_lost_byte_drops_only_damaged_frame(self, lost: int) -> None:
        """Потерян один байт кадра: пропадает ТОЛЬКО этот кадр.

        Следующий кадр не съеден, и из обломка с началом следующего кадра
        не складывается ложный вес («=» в теле недопустим).
        """
        damaged = display_frame("21460")
        damaged = damaged[:lost] + damaged[lost + 1 :]
        stream = display_frame("100") + damaged + display_frame("300") + display_frame("400")
        assembler = Xk3190Assembler(divisor=10.0)
        assert display_weights(assembler.feed(stream)) == [100.0, 300.0, 400.0]

    @pytest.mark.parametrize("lost", range(DISPLAY_FRAME_LEN))
    def test_lost_byte_byte_by_byte(self, lost: int) -> None:
        damaged = display_frame("21460")
        damaged = damaged[:lost] + damaged[lost + 1 :]
        stream = display_frame("100") + damaged + display_frame("300") + display_frame("400")
        assembler = Xk3190Assembler(divisor=10.0)
        packets = feed_all(assembler, [bytes([byte]) for byte in stream])
        assert display_weights(packets) == [100.0, 300.0, 400.0]

    @pytest.mark.parametrize(
        "garbage",
        [
            b"\xff\x00\xfe",
            b"\r\n",
            b"=",  # одинокий маркер прямо перед кадром
            b"==",
            b"=12",  # обрывок кадра
            b"=1234567",  # кадр без последнего байта
            b"12345678",  # тело без маркера
            b"\x03\x0312",
            b"wn\xe5\x9c=\x80",
        ],
    )
    def test_garbage_between_frames(self, garbage: bytes) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = garbage + display_frame("60") + garbage + display_frame("70") + garbage
        packets = assembler.feed(stream + display_frame("80"))
        assert display_weights(packets) == [60.0, 70.0, 80.0]

    def test_stray_stx_does_not_eat_display_frames(self) -> None:
        """Ложный STX перед кадрами «=»: кадры не теряются, но выдача первого
        ЗАДЕРЖИВАЕТСЯ, пока не наберётся 12 байт кандидата VESAR (ФИКСАЦИЯ:
        при 2 кадрах/с это одна задержка на шаг кадра)."""
        assembler = Xk3190Assembler(divisor=10.0)
        assert assembler.feed(b"\x02" + display_frame("60")) == []  # 10 байт < 12
        packets = assembler.feed(display_frame("70"))
        assert display_weights(packets) == [60.0, 70.0]

    def test_stx_inside_display_body_drops_only_that_frame(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = b"=0700\x02000" + display_frame("60") + display_frame("70")
        assert display_weights(assembler.feed(stream)) == [60.0, 70.0]

    def test_buffer_bounded_on_garbage_without_markers(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        for _ in range(200):
            assert assembler.feed(b"\xff0123456789abcdef\r\n" * 50) == []
            assert len(assembler._buffer) == 0

    def test_buffer_bounded_on_stream_of_marks(self) -> None:
        """Поток из одних «=» (или одних STX): в буфере — не больше неполного кадра."""
        assembler = Xk3190Assembler(divisor=10.0)
        for _ in range(200):
            assert assembler.feed(b"=" * 1000) == []
            assert len(assembler._buffer) < DISPLAY_FRAME_LEN
        for _ in range(200):
            assert assembler.feed(b"\x02" * 1000) == []
            assert len(assembler._buffer) < 12
        # после мусора сборщик жив; первый кадр ждёт, пока хвост из STX
        # не доберёт 12 байт кандидата VESAR, и выдаётся вместе со вторым
        assert assembler.feed(b"\xff" + display_frame("60")) == []
        assert display_weights(assembler.feed(display_frame("70"))) == [60.0, 70.0]

    def test_garbage_digits_after_mark_are_a_false_weight(self) -> None:
        """ФИКСАЦИЯ ограничения формата: контрольной суммы нет, поэтому «=» и
        любые 8 цифр — «вес». Мусор из цифр отличить нечем; защита — только
        маркер и состав тела."""
        assembler = Xk3190Assembler(divisor=10.0)
        assert display_weights(assembler.feed(b"=12345678")) == [87654321.0]


class TestXk3190AssemblerLiveDump:
    def test_dump_file_is_the_live_one(self) -> None:
        data = D2PLUS_DUMP.read_bytes()
        assert len(data) == 532
        assert data.count(b"=") == 60  # 59 полных кадров и оборванный хвост
        assert set(data) <= set(b"=0123456789")

    def test_whole_dump(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        weights = display_weights(assembler.feed(D2PLUS_DUMP.read_bytes()))
        assert len(weights) == 59
        assert set(weights) == {0.0, 60.0, 70.0}
        assert weights == [0.0] * 42 + [60.0] + [70.0] * 16
        # оборванный хвост дампа ждёт продолжения, а не выдаётся и не теряется
        assert assembler._buffer == b"="
        assert display_weights(assembler.feed(b"07000000")) == [70.0]

    def test_dump_in_random_chunks(self) -> None:
        data = D2PLUS_DUMP.read_bytes()
        rng = random.Random(20261117)
        for _ in range(50):
            assembler = Xk3190Assembler(divisor=10.0)
            chunks: list[bytes] = []
            position = 0
            while position < len(data):
                size = rng.randint(1, 20)
                chunks.append(data[position : position + size])
                position += size
            weights = display_weights(feed_all(assembler, chunks))
            assert weights == [0.0] * 42 + [60.0] + [70.0] * 16

    def test_dump_with_one_lost_byte_anywhere(self) -> None:
        """Потеря любого одного байта дампа: теряется не больше одного кадра,
        посторонних значений не появляется."""
        data = D2PLUS_DUMP.read_bytes()
        for lost in range(len(data) - 1):
            assembler = Xk3190Assembler(divisor=10.0)
            weights = display_weights(assembler.feed(data[:lost] + data[lost + 1 :]))
            assert len(weights) == 58, lost
            assert set(weights) <= {0.0, 60.0, 70.0}, lost


class TestXk3190AssemblerBothFormats:
    def test_mixed_stream(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = (
            frame(b"+", "0000800")
            + display_frame("70")
            + b"\xff"
            + frame(b"-", "0000200")
            + display_frame("21460")
            + KOKCHO_EMPTY
        )
        packets = assembler.feed(stream)
        assert display_weights(packets) == [80.0, 70.0, -20.0, 21460.0, 0.0]
        assert [packet.display for packet in packets] == [False, True, False, True, False]

    def test_mixed_stream_byte_by_byte(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        stream = frame(b"+", "0000800") + display_frame("70") + frame(b"+", "0370100")
        packets = feed_all(assembler, [bytes([byte]) for byte in stream])
        assert display_weights(packets) == [80.0, 70.0, 37010.0]

    def test_stray_mark_before_vesar_frame(self) -> None:
        """Одинокий «=» перед кадром VESAR (мусор на линии DS3/A9) кадр не съедает."""
        assembler = Xk3190Assembler(divisor=10.0)
        packets = assembler.feed(b"=" + frame(b"+", "0000800") + b"=1" + KOKCHO_EMPTY)
        assert display_weights(packets) == [80.0, 0.0]
        assert not any(packet.display for packet in packets)

    def test_vesar_checksum_failure_is_rejected(self) -> None:
        good = frame(b"+", "0370100")
        bad = good[:9] + b"00" + good[11:]
        assembler = Xk3190Assembler(divisor=10.0)
        assert display_weights(assembler.feed(bad + good)) == [37010.0]

    def test_vesar_frame_with_bad_checksum_does_not_yield_display_frame(self) -> None:
        """В кадре VESAR нет «=» — из битого кадра не выудить кадр табло."""
        bad = b"\x02+0370100ZZ\x03"
        assembler = Xk3190Assembler(divisor=10.0)
        assert assembler.feed(bad * 20) == []

    def test_old_format_matches_packet_assembler(self) -> None:
        """Старый формат через новый сборщик — то же, что PacketAssembler:
        те же пакеты в тех же чанках, включая отказы по КС и мусор.

        «=» в поток сознательно не подмешивается: PacketAssembler его не знает.
        """
        rng = random.Random(3190)
        good = [frame(b"+", f"{rng.randrange(10**7):07d}") for _ in range(30)]
        good += [frame(b"-", f"{rng.randrange(10**4):07d}") for _ in range(5)]
        for _ in range(40):
            pieces: list[bytes] = []
            for _ in range(40):
                kind = rng.random()
                packet = rng.choice(good)
                if kind < 0.6:
                    pieces.append(packet)
                elif kind < 0.7:  # битая КС
                    pieces.append(packet[:9] + b"ZZ" + packet[11:])
                elif kind < 0.8:  # потерян байт
                    lost = rng.randrange(len(packet))
                    pieces.append(packet[:lost] + packet[lost + 1 :])
                elif kind < 0.9:  # мусор без «=»
                    pieces.append(bytes(rng.choice(b"\x00\x02\x03+-019AF\xff") for _ in range(7)))
                else:  # ложный STX
                    pieces.append(b"\x02")
            stream = b"".join(pieces)
            old = PacketAssembler(divisor=10.0)
            new = Xk3190Assembler(divisor=10.0)
            position = 0
            while position < len(stream):
                size = rng.randint(1, 30)
                chunk = stream[position : position + size]
                position += size
                assert new.feed(chunk) == old.feed(chunk)
            assert new._buffer == old._buffer

    def test_old_format_respects_divisor(self) -> None:
        assembler = Xk3190Assembler(divisor=1.0)
        packets = assembler.feed(b"\xff" + frame(b"+", "0000080") + KOKCHO_EMPTY)
        assert display_weights(packets) == [80.0, 0.0]


# ---------------------------------------------------------------------------
# Стабильность на редком потоке: опорное показание (keep_anchor)
# ---------------------------------------------------------------------------


def run_tracker(
    times: list[float],
    weights: list[float] | None = None,
    *,
    keep_anchor: bool,
    amplitude_kg: float = 60.0,
) -> list[bool]:
    """Прогнать ряд показаний, вернуть флаг стабильности после каждого."""
    tracker = StabilityTracker(window_s=1.0, amplitude_kg=amplitude_kg, keep_anchor=keep_anchor)
    values = weights if weights is not None else [70.0] * len(times)
    return [tracker.update(value, now=moment) for moment, value in zip(times, values, strict=True)]


def jittered_times(count: int, *, seed: int) -> list[float]:
    """Шаг 0,5 с с дрожанием ±50 мс — как приходит кадр D2+ через read()."""
    rng = random.Random(seed)
    return [index * 0.5 + rng.uniform(-0.05, 0.05) for index in range(count)]


class TestStabilityTrackerKeepAnchor:
    def test_constant_weight_at_half_second_step_becomes_stable(self) -> None:
        results = run_tracker([index * 0.5 for index in range(8)], keep_anchor=True)
        assert results == [False, False, True, True, True, True, True, True]

    def test_without_anchor_exact_grid_is_stable_too(self) -> None:
        """ФИКСАЦИЯ: на ИДЕАЛЬНОЙ сетке 0,5 с окно набирается и без опорного
        показания (сэмпл ровно на границе окна не выбрасывается)."""
        results = run_tracker([index * 0.5 for index in range(8)], keep_anchor=False)
        assert results == [False, False, True, True, True, True, True, True]

    @pytest.mark.parametrize("seed", range(20))
    def test_jittered_step_is_stable_and_does_not_flicker(self, seed: int) -> None:
        results = run_tracker(jittered_times(40, seed=seed), keep_anchor=True)
        assert results[0] is False  # одно показание — не стабильность
        assert all(results[3:]), results
        first_stable = results.index(True)
        assert all(results[first_stable:]), results  # набралась — больше не мигает

    def test_without_anchor_jittered_step_flickers(self) -> None:
        """ФИКСАЦИЯ того, ради чего введён keep_anchor: без него на живом
        шаге (0,5 с ± 50 мс) стабильность постоянного веса мигает."""
        flickering = 0
        for seed in range(20):
            results = run_tracker(jittered_times(40, seed=seed), keep_anchor=False)
            if True in results and False in results[results.index(True) :]:
                flickering += 1
        assert flickering == 20

    def test_single_sample_is_not_stable(self) -> None:
        tracker = StabilityTracker(amplitude_kg=60.0, keep_anchor=True)
        assert tracker.update(70.0, now=100.0) is False

    def test_two_samples_half_second_apart_are_not_stable(self) -> None:
        assert run_tracker([0.0, 0.5], keep_anchor=True) == [False, False]

    def test_jump_beyond_amplitude_is_unstable_while_in_window_or_anchor(self) -> None:
        """Скачок 0 → 70 (амплитуда 60): нестабильно, пока показание «до скачка»
        остаётся в окне или служит опорным; затем стабильность возвращается."""
        times = [index * 0.5 for index in range(9)]
        weights = [0.0, 0.0, 0.0, 0.0, 70.0, 70.0, 70.0, 70.0, 70.0]
        results = run_tracker(times, weights, keep_anchor=True)
        assert results == [False, False, True, True, False, False, True, True, True]

    @pytest.mark.parametrize("seed", range(20))
    def test_jump_with_jitter_holds_instability_at_least_two_frames(self, seed: int) -> None:
        times = jittered_times(12, seed=seed)
        weights = [0.0] * 6 + [70.0] * 6
        results = run_tracker(times, weights, keep_anchor=True)
        assert results[6] is False and results[7] is False, results
        assert results[9] is True, results  # не позже 1,5 с + дрожание

    def test_jump_within_amplitude_stays_stable(self) -> None:
        """Живой дамп: 0 → 60 при дискрете 20 (допуск 60) — в пределах допуска."""
        times = [index * 0.5 for index in range(6)]
        results = run_tracker(times, [0.0, 0.0, 0.0, 60.0, 70.0, 70.0], keep_anchor=True)
        # 0 → 60 в допуске; 0 → 70 за допуском — нестабильно, пока ноль в окне
        assert results == [False, False, True, True, False, True]

    def test_single_spike_is_seen(self) -> None:
        """Одиночный выброс между ровными показаниями не проходит незамеченным."""
        times = [index * 0.5 for index in range(8)]
        weights = [70.0, 70.0, 70.0, 500.0, 70.0, 70.0, 70.0, 70.0]
        results = run_tracker(times, weights, keep_anchor=True)
        assert results == [False, False, True, False, False, False, True, True]

    def test_reset_forgets_history_and_anchor(self) -> None:
        tracker = StabilityTracker(amplitude_kg=60.0, keep_anchor=True)
        for index in range(6):
            tracker.update(70.0, now=index * 0.5)
        tracker.reset()
        assert tracker.update(70.0, now=3.0) is False
        assert tracker.update(70.0, now=3.5) is False
        assert tracker.update(70.0, now=4.0) is True

    def test_resume_after_pause_refills_window(self) -> None:
        """После паузы 2,5 с опора старше двух окон отбрасывается: окно
        набирается заново, один свежий кадр стабильность не подтверждает."""
        times = [0.0, 0.5, 1.0, 3.5, 4.0, 4.5]
        assert run_tracker(times, keep_anchor=True) == [False, False, True, False, False, True]
        # то же при весе, изменившемся в пределах допуска (70 → 30 при допуске 60)
        weights = [70.0, 70.0, 70.0, 30.0, 30.0, 30.0]
        assert run_tracker(times, weights, keep_anchor=True) == [
            False,
            False,
            True,
            False,
            False,
            True,
        ]

    def test_anchor_survives_one_missed_frame(self) -> None:
        """Пропуск одного кадра (разрыв 1,0 с — не больше окна) стабильность
        не сбрасывает."""
        times = [0.0, 0.5, 1.0, 2.0, 2.5]
        assert run_tracker(times, keep_anchor=True) == [False, False, True, True, True]

    def test_gap_longer_than_window_refills(self) -> None:
        """Разрыв между опорой и следующим кадром больше окна (1,5 с) — опора
        отбрасывается, окно набирается заново (замечание reviewer 17.11.2026:
        один свежий кадр после паузы не должен подтверждать стабильность)."""
        times = [0.0, 0.5, 1.0, 2.5, 3.0, 3.5]
        assert run_tracker(times, keep_anchor=True) == [False, False, True, False, False, True]
        # частый поток DS3/A9: пауза 1,5 с ведёт себя так же, как без опоры
        frequent = [index * 0.15 for index in range(10)] + [
            2.85 + index * 0.15 for index in range(10)
        ]
        with_anchor = run_tracker(frequent, keep_anchor=True)
        assert with_anchor[10] is False
        assert with_anchor.index(True, 10) >= run_tracker(frequent, keep_anchor=False).index(
            True, 10
        )

    def test_resume_after_pause_without_anchor_refills_window(self) -> None:
        times = [0.0, 0.5, 1.0, 3.5, 4.0, 4.5]
        assert run_tracker(times, keep_anchor=False) == [False, False, True, False, False, True]

    def test_resume_after_pause_with_other_weight_is_unstable(self) -> None:
        """Вес за паузу ушёл за допуск: старое опорное показание держит
        нестабильность, пока его не сменит свежее."""
        times = [0.0, 0.5, 1.0, 3.5, 4.0, 4.5, 5.0]
        weights = [0.0, 0.0, 0.0, 70.0, 70.0, 70.0, 70.0]
        results = run_tracker(times, weights, keep_anchor=True)
        assert results == [False, False, True, False, False, True, True]

    def test_very_sparse_stream_is_never_stable(self) -> None:
        """Шаг кадров больше двух окон (2,9 с): опора каждый раз устаревает,
        окно не набирается — вес не объявляется стабильным вслепую."""
        results = run_tracker([index * 2.9 for index in range(4)], keep_anchor=True)
        assert results == [False, False, False, False]

    def test_frequent_stream_differs_by_one_frame_at_most(self) -> None:
        """Частый поток DS3/A9 (~7 кадров/с): с опорным показанием стабильность
        после скачка возвращается не раньше, чем без него, и не позже чем на кадр."""
        step = 0.15
        times = [index * step for index in range(40)]
        weights = [0.0] * 15 + [800.0] * 25
        plain = run_tracker(times, weights, keep_anchor=False)
        anchored = run_tracker(times, weights, keep_anchor=True)
        assert plain[:15] == anchored[:15]
        back_plain = plain.index(True, 15)
        back_anchored = anchored.index(True, 15)
        assert 0 <= back_anchored - back_plain <= 1
        assert all(anchored[back_anchored:])

    def test_default_is_without_anchor(self) -> None:
        """Умолчание трекера не изменилось: vesar/cas-подобные потоки без опоры."""
        times = jittered_times(40, seed=1)
        default = StabilityTracker(window_s=1.0, amplitude_kg=60.0)
        results = [default.update(70.0, now=moment) for moment in times]
        assert results == run_tracker(times, keep_anchor=False)
        assert results != run_tracker(times, keep_anchor=True)


# ---------------------------------------------------------------------------
# Закрепление формата потока (замечание reviewer 17.11.2026)
# ---------------------------------------------------------------------------

# кадр VESAR «−20,0 кг», у которого знак «-» (0x2D) стал «=» (0x3D) — один бит;
# хвост «=0000200» + первый символ КС «1» выглядит кадром «=» на 10 020 000 кг
VESAR_MINUS_FLIPPED = bytes.fromhex("02 3D 30 30 30 30 32 30 30 31 46 03")


class TestFormatLock:
    def test_flipped_sign_is_what_we_think_it_is(self) -> None:
        good = frame(b"-", "0000200")
        assert good[:1] + b"=" + good[2:] == VESAR_MINUS_FLIPPED
        packet = parse_display_frame(VESAR_MINUS_FLIPPED[2:10])
        assert packet is not None and packet.raw_weight_kg == 10020000.0

    def test_locked_vesar_stream_rejects_flipped_sign_frame(self) -> None:
        """После трёх верных кадров VESAR «=» больше не ищется: искажённый
        кадр отбрасывается, как у PacketAssembler, ложного веса нет."""
        assembler = Xk3190Assembler(divisor=10.0)
        stream = KOKCHO_EMPTY * FORMAT_LOCK_FRAMES + VESAR_MINUS_FLIPPED + frame(b"+", "0000800")
        assert [p.raw_weight_kg for p in assembler.feed(stream)] == [0.0, 0.0, 0.0, 80.0]

    def test_locked_vesar_stream_equals_plain_assembler_byte_by_byte(self) -> None:
        stream = KOKCHO_EMPTY * 4 + VESAR_MINUS_FLIPPED * 3 + b"=07000000" + frame(b"-", "0000200")
        ours, plain = Xk3190Assembler(divisor=10.0), PacketAssembler(divisor=10.0)
        got = [p for byte in stream for p in ours.feed(bytes([byte]))]
        expected = [p for byte in stream for p in plain.feed(bytes([byte]))]
        assert got == expected and len(got) == 5

    def test_before_lock_flipped_frame_still_leaks(self) -> None:
        """ФИКСАЦИЯ остаточного риска: до закрепления (первые кадры после
        открытия порта) искажённый кадр VESAR даёт ложное показание. Стабильным
        оно стать не может — окно ещё не набрано, скачок далеко за допуском."""
        assembler = Xk3190Assembler(divisor=10.0)
        weights = [p.raw_weight_kg for p in assembler.feed(VESAR_MINUS_FLIPPED + KOKCHO_EMPTY * 4)]
        assert weights == [10020000.0, 0.0, 0.0, 0.0, 0.0]

    def test_streak_must_be_consecutive_same_format(self) -> None:
        """Два кадра VESAR, кадр «=», снова VESAR — серия начинается заново."""
        assembler = Xk3190Assembler(divisor=10.0)
        assembler.feed(KOKCHO_EMPTY * 2 + b"=07000000" + KOKCHO_EMPTY * 2)
        assert [p.display for p in assembler.feed(b"=07000000")] == [True]
        assembler.feed(KOKCHO_EMPTY * FORMAT_LOCK_FRAMES)
        assert assembler.feed(b"=07000000") == []

    def test_locked_display_stream_ignores_stx(self) -> None:
        """Поток «=» закреплён: STX в мусоре не задерживает и не съедает кадры."""
        assembler = Xk3190Assembler(divisor=10.0)
        assembler.feed(b"=00000000" * FORMAT_LOCK_FRAMES)
        packets = assembler.feed(b"\x02" + b"=07000000")
        assert [p.raw_weight_kg for p in packets] == [70.0]
        assert assembler.feed(KOKCHO_EMPTY) == []

    def test_live_dump_locks_to_display(self) -> None:
        assembler = Xk3190Assembler(divisor=10.0)
        dump = D2PLUS_DUMP.read_bytes()
        assert len(assembler.feed(dump)) == 59
        assert assembler.feed(KOKCHO_EMPTY) == []


# ---------------------------------------------------------------------------
# Сквозной проход кадров «=» через цикл чтения драйвера
# ---------------------------------------------------------------------------


class _Clock:
    """Управляемые часы вместо time.monotonic() в цикле чтения."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


class _TimedPort:
    """Порт по сценарию (шаг времени, байты): каждое чтение сдвигает часы.

    ``states[i]`` — состояние драйвера после обработки i-го чтения.
    Когда сценарий кончился — гасит цикл, как _FakePort.
    """

    def __init__(
        self,
        script: list[tuple[float, bytes]],
        clock: _Clock,
        driver: VesarDriver,
        stop_event: threading.Event,
    ) -> None:
        self._script = list(script)
        self._clock = clock
        self._driver = driver
        self._stop_event = stop_event
        self._reads = 0
        self.states: list[ScaleState] = []

    def read(self, _size: int) -> bytes:
        if self._reads:
            self.states.append(self._driver.state)
        self._reads += 1
        if not self._script:
            self._stop_event.set()
            return b""
        delay, payload = self._script.pop(0)
        self._clock.now += delay
        return payload


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """Подменить time в модуле драйвера управляемыми часами."""
    fake = _Clock()
    monkeypatch.setattr(vesar_module, "time", SimpleNamespace(monotonic=fake.monotonic))
    return fake


def run_script(
    driver: VesarDriver, clock: _Clock, script: list[tuple[float, bytes]]
) -> list[ScaleState]:
    """Прогнать сценарий через _read_loop, вернуть состояние после каждого чтения."""
    stop_event = threading.Event()
    port = _TimedPort(script, clock, driver, stop_event)
    driver._read_loop(port, stop_event)  # type: ignore[arg-type]
    assert len(port.states) == len(script)
    return port.states


class TestReadLoopDisplayFrames:
    def test_display_weight_is_neither_divided_nor_quantized(self) -> None:
        """«=07000000» — 70 кг на табло: не 7 (делитель 10) и не 60 (дискрета 20)."""
        driver = Xk3190Driver("loop://", baudrate=1200)
        assert driver.weight_divisor == 10.0 and driver.discrete_kg == 20.0
        stop_event = threading.Event()
        port = _FakePort([display_frame("0"), display_frame("70")], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.status is ScaleStatus.OK
        assert driver.state.weight_kg == 70.0
        assert driver.state.overload is False

    @pytest.mark.parametrize(
        ("shown", "expected"),
        [
            ("70", 70.0),
            ("10", 10.0),  # меньше дискреты — старый формат дал бы 0
            ("21470", 21470.0),  # не кратно 20
            ("00012.5", 12.5),
            ("-0000070", -70.0),
            ("0", 0.0),
        ],
    )
    @pytest.mark.parametrize("weight_divisor", [None, 1.0, 10.0])
    @pytest.mark.parametrize("discrete_kg", [None, 10.0, 20.0, 50.0])
    def test_display_weight_ignores_divisor_and_discrete(
        self, shown: str, expected: float, weight_divisor: float | None, discrete_kg: float | None
    ) -> None:
        driver = create_driver(
            "xk3190",
            "loop://",
            baudrate=1200,
            weight_divisor=weight_divisor,
            discrete_kg=discrete_kg,
        )
        assert isinstance(driver, Xk3190Driver)
        stop_event = threading.Event()
        port = _FakePort([display_frame(shown)], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.status is ScaleStatus.OK
        assert driver.state.weight_kg == expected

    def test_frame_split_across_reads(self) -> None:
        driver = Xk3190Driver("loop://", baudrate=1200)
        stop_event = threading.Event()
        port = _FakePort([b"=0641", b"", b"2000=0", b"7000000"], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.status is ScaleStatus.OK
        assert driver.state.weight_kg == 70.0

    def test_both_formats_in_one_chunk_last_frame_wins(self) -> None:
        """Оба формата в одном чанке: каждый кадр трактуется по-своему."""
        driver = Xk3190Driver("loop://", baudrate=1200)
        stop_event = threading.Event()
        port = _FakePort([display_frame("70") + frame(b"+", "0000850")], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.weight_kg == 80.0  # VESAR: 85,0 → к дискрете 20

        stop_event = threading.Event()
        port = _FakePort([frame(b"+", "0000850") + display_frame("70")], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.weight_kg == 70.0  # «=»: как на табло

    def test_live_dump_through_driver(self, clock: _Clock) -> None:
        """Живой дамп кадр за кадром с шагом 0,5 с: вес как на табло, статус OK,
        стабильность набирается и держится (0 → 60 → 70 при допуске 60)."""
        data = D2PLUS_DUMP.read_bytes()
        script = [
            (0.5, data[position : position + DISPLAY_FRAME_LEN])
            for position in range(0, len(data), DISPLAY_FRAME_LEN)
        ]
        driver = Xk3190Driver("loop://", baudrate=1200)
        states = run_script(driver, clock, script)
        frames = states[:59]  # 60-е чтение — оборванный хвост «=»
        assert all(state.status is ScaleStatus.OK for state in frames)
        assert [state.weight_kg for state in frames] == [0.0] * 42 + [60.0] + [70.0] * 16
        assert [state.stable for state in frames[:2]] == [False, False]
        assert all(state.stable for state in frames[2:42])
        assert frames[42].stable is True  # 0 → 60: ровно допуск (3 × 20), включительно
        # 0 → 70 за допуском: нестабильно один кадр, пока ноль ещё в окне;
        # дальше в окне 60 и 70 — человек, вставший на весы, даёт ОДИН
        # нестабильный кадр (0,5 с)
        assert [state.stable for state in frames[43:46]] == [False, True, True]
        assert all(state.stable for state in frames[44:])
        assert states[-1].weight_kg == 70.0 and states[-1].status is ScaleStatus.OK

    def test_live_dump_real_reads_with_jitter(self, clock: _Clock) -> None:
        """Тот же дамп, но как в жизни: read() отдаёт куски кадра, шаг дрожит."""
        data = D2PLUS_DUMP.read_bytes()
        rng = random.Random(1200)
        script: list[tuple[float, bytes]] = []
        for position in range(0, len(data) - 1, DISPLAY_FRAME_LEN):
            packet = data[position : position + DISPLAY_FRAME_LEN]
            cut = rng.randint(1, DISPLAY_FRAME_LEN - 1)
            pause = 0.42 + rng.uniform(-0.05, 0.05)
            script += [(pause, packet[:cut]), (0.075, packet[cut:])]
        driver = Xk3190Driver("loop://", baudrate=1200)
        states = run_script(driver, clock, script)
        assert all(state.status is ScaleStatus.OK for state in states[1:])
        weights = [state.weight_kg for state in states[1::2]]
        assert weights == [0.0] * 42 + [60.0] + [70.0] * 16
        assert all(state.stable for state in states[7:84])  # пустые весы: не мигает
        assert states[-1].stable is True and states[-1].weight_kg == 70.0

    def test_discrete_from_config_sets_only_stability_tolerance(self, clock: _Clock) -> None:
        """discrete_kg для кадра «=» — только допуск стабильности (3 дискреты):
        скачок 0 → 50 стабилен при дискрете 20 (допуск 60) и нестабилен при 10
        (допуск 30); сам вес в обоих случаях 50 (не 40)."""
        script = [(0.5, display_frame("0"))] * 4 + [(0.5, display_frame("50"))]
        coarse = create_driver("xk3190", "loop://", baudrate=1200, discrete_kg=20)
        fine = create_driver("xk3190", "loop://", baudrate=1200, discrete_kg=10)
        assert isinstance(coarse, VesarDriver) and isinstance(fine, VesarDriver)
        coarse_state = run_script(coarse, clock, list(script))[-1]
        fine_state = run_script(fine, clock, list(script))[-1]
        assert coarse_state.weight_kg == 50.0 and fine_state.weight_kg == 50.0
        assert coarse_state.stable is True
        assert fine_state.stable is False

    def test_silence_gives_no_data_and_resets_stability(self, clock: _Clock) -> None:
        """Тишина дольше 3 с — «нет данных»; после возобновления окно (и опорное
        показание) набираются заново."""
        tick = (0.5, display_frame("70"))
        script = [tick] * 4 + [(0.25, b"")] * 14 + [tick] * 3
        driver = Xk3190Driver("loop://", baudrate=1200)
        states = run_script(driver, clock, script)
        assert states[3].status is ScaleStatus.OK and states[3].stable is True
        assert states[15].status is ScaleStatus.OK  # ровно 3,0 с тишины — ещё не «дольше 3 с»
        assert states[15].weight_kg == 70.0  # до таймаута висит последний вес
        assert states[16].status is ScaleStatus.NO_DATA  # 3,25 с
        assert states[16].weight_kg is None and states[16].stable is False
        assert states[16].last_packet_at == states[3].last_packet_at
        assert [state.status for state in states[18:]] == [ScaleStatus.OK] * 3
        assert [state.stable for state in states[18:]] == [False, False, True]

    def test_pause_shorter_than_timeout_refills_window(self, clock: _Clock) -> None:
        """Пауза 2,5 с (короче таймаута тишины): статус остаётся OK, но первый
        кадр после паузы стабильность не подтверждает — опора устарела."""
        tick = (0.5, display_frame("70"))
        script = [tick] * 4 + [(0.25, b"")] * 8 + [tick] * 3
        driver = Xk3190Driver("loop://", baudrate=1200)
        states = run_script(driver, clock, script)
        assert all(state.status is ScaleStatus.OK for state in states)
        assert states[11].stable is True  # всю паузу висит последнее состояние
        assert states[12].stable is False  # один свежий кадр после 2,5 с тишины
        assert states[14].stable is True  # окно набрано заново

    def test_garbage_only_stream_is_no_data(self, clock: _Clock) -> None:
        """Мусор (сбита скорость порта) — не данные: «нет данных» по таймауту."""
        script = [(0.2, b"\xff\x00wn\xe5=\x9c\x80=\x02")] * 20
        driver = Xk3190Driver("loop://", baudrate=1200)
        states = run_script(driver, clock, script)
        assert all(state.weight_kg is None for state in states)
        assert all(state.status is ScaleStatus.NO_DATA for state in states)
        assert states[-1].last_packet_at is None

    def test_xk3190_tracker_keeps_anchor_vesar_does_not(self) -> None:
        """Xk3190Driver включает опорное показание, VesarDriver — нет; допуск —
        три дискреты из конфига у обоих."""
        times = jittered_times(40, seed=1)

        def flags(driver: VesarDriver) -> list[bool]:
            tracker = driver._make_stability()
            return [tracker.update(70.0, now=moment) for moment in times]

        xk3190 = Xk3190Driver("loop://", baudrate=1200)
        vesar = VesarDriver("loop://", baudrate=9600)
        assert flags(xk3190) == run_tracker(times, keep_anchor=True)
        assert flags(vesar) == run_tracker(times, keep_anchor=False)
        assert flags(xk3190) != flags(vesar)
        # допуск = 3 × discrete_kg: ровно 60 при дискрете 20 — стабильно, 61 — нет
        tracker = Xk3190Driver("loop://", baudrate=1200, discrete_kg=20)._make_stability()
        assert [tracker.update(w, now=t) for t, w in ((0.0, 0.0), (1.0, 60.0))] == [False, True]
        tracker = Xk3190Driver("loop://", baudrate=1200, discrete_kg=20)._make_stability()
        assert [tracker.update(w, now=t) for t, w in ((0.0, 0.0), (1.0, 61.0))] == [False, False]


class TestVesarUnchanged:
    def test_vesar_driver_does_not_understand_display_frame(self, clock: _Clock) -> None:
        """Драйвер vesar кадр «=» не понимает: веса нет, по таймауту «нет данных»."""
        driver = create_driver("vesar", "loop://", baudrate=9600)
        assert type(driver) is VesarDriver
        assert type(driver._make_assembler()) is PacketAssembler
        script = [(0.5, display_frame("70"))] * 8
        states = run_script(driver, clock, script)
        assert all(state.weight_kg is None for state in states)
        assert all(state.status is ScaleStatus.NO_DATA for state in states)

    def test_packet_assembler_ignores_display_frames(self) -> None:
        assembler = PacketAssembler()
        assert assembler.feed(D2PLUS_DUMP.read_bytes()) == []
        assert assembler._buffer == b""
        packets = assembler.feed(display_frame("70") + frame(b"+", "0000850"))
        assert packets == [VesarPacket(raw_weight_kg=85.0)]
        assert packets[0].display is False

    def test_vesar_packet_is_not_display_by_default(self) -> None:
        packet = parse_packet(frame(b"+", "0000850"))
        assert packet is not None and packet.display is False

    def test_vesar_driver_still_quantizes(self) -> None:
        driver = VesarDriver("loop://", baudrate=9600)
        stop_event = threading.Event()
        port = _FakePort([frame(b"+", "0000850")], stop_event)
        driver._read_loop(port, stop_event)  # type: ignore[arg-type]
        assert driver.state.weight_kg == 80.0
