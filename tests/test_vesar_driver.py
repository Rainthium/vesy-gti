"""Тесты драйвера «VESAR» (agent/drivers/vesar.py).

Пакеты и потоки генерируются эмулятором tools/vesar_emulator.py
(сверен с живыми дампами СВХ «Кара-Суу» 20.08.2026 — эталонный пакет
в тестах ниже байт в байт из дампа). Слои:

1. ``parse_packet`` — чистый разбор одного 12-байтового пакета
   (рамки STX/ETX, знак, 7 цифр, контрольная сумма XOR в ASCII-hex);
2. ``quantize`` — приведение к дискрете табло (усечение по модулю вниз);
3. ``PacketAssembler`` — сборка из потока с ресинхронизацией;
4. ``StabilityTracker`` — программная стабильность (окно/амплитуда);
5. ``VesarDriver`` — фоновое чтение через ``socket://`` с TCP-сервером
   эмулятора (интеграционно, без железа).
"""

import asyncio
import contextlib
import socket
import threading
import time
from collections.abc import Callable, Iterator

import pytest

from agent.drivers import DRIVERS, create_driver
from agent.drivers.vesar import (
    PACKET_LEN,
    PacketAssembler,
    StabilityTracker,
    VesarDriver,
    VesarPacket,
    parse_packet,
    quantize,
)
from shared.enums import ScaleStatus
from tools.vesar_emulator import (
    PacketBuilder,
    ScenarioFactory,
    Step,
    bad_checksum,
    garbage,
    negative_weight,
    serve,
    stabilizing,
    stable_weight,
    stream_break,
)

TARGET_KG = 12500.0

FAST_READ_TIMEOUT_S = 0.05
FAST_RX_ERROR_TIMEOUT_S = 0.5
FAST_REOPEN_DELAY_S = 0.15
RATE = 40.0  # пакетов в секунду в тестовых сценариях

# эталонные пакеты из живого дампа Кара-Суу 20.08.2026 (байт в байт)
DUMP_EMPTY = b"\x02+00000001B\x03"  # весы пусты, 0 кг
DUMP_PERSON = b"\x02+000085016\x03"  # оператор на платформе, 85,0 кг


@pytest.fixture
def builder() -> PacketBuilder:
    return PacketBuilder()


def parse_ok(pkt: bytes) -> VesarPacket:
    packet = parse_packet(pkt)
    assert packet is not None, f"валидный пакет отвергнут: {pkt!r}"
    return packet


def stream_of(steps: Iterator[Step]) -> bytes:
    return b"".join(step.payload for step in steps)


def make_packet(sign: bytes, digits: bytes) -> bytes:
    """Собрать пакет вручную по спецификации docs/protocols/vesar.md.

    Независим от PacketBuilder эмулятора (тот округляет к внутреннему
    шагу 5 кг и не умеет произвольные массы) — нужен для граничных
    значений: максимум 9999999, минус-ноль, точность десятых.
    """
    assert len(sign) == 1 and len(digits) == 7
    checksum = 0
    for byte in sign + digits:
        checksum ^= byte
    pkt = b"\x02" + sign + digits + f"{checksum:02X}".encode("ascii") + b"\x03"
    assert len(pkt) == PACKET_LEN
    return pkt


# сетка масс протокола для property-тестов: границы и разреженный перебор
# всех 7-значных значений (шаг — простое число, чтобы цифры перемешивались)
PROTOCOL_MASS_GRID = [0, 1, 5, 850, 9_999_999, *range(0, 10_000_000, 9973)]


# ---------------------------------------------------------------------------
# parse_packet
# ---------------------------------------------------------------------------


class TestParsePacket:
    def test_live_dump_empty(self) -> None:
        """Эталон живого дампа: пустые весы, 0 кг, КС «1B» сходится."""
        assert parse_ok(DUMP_EMPTY).raw_weight_kg == 0.0

    def test_live_dump_person(self) -> None:
        """Эталон живого дампа: оператор, «сырые» 85,0 кг, КС «16» сходится."""
        assert parse_ok(DUMP_PERSON).raw_weight_kg == 85.0

    def test_builder_round_trip(self, builder: PacketBuilder) -> None:
        """Пакет эмулятора разбирается в тот же вес (внутренний шаг 5 кг)."""
        assert parse_ok(builder.build(43390.0)).raw_weight_kg == 43390.0

    def test_negative_weight(self, builder: PacketBuilder) -> None:
        assert parse_ok(builder.build(-30.0)).raw_weight_kg == -30.0

    def test_emulator_matches_live_dump(self, builder: PacketBuilder) -> None:
        """Эмулятор собирает байт в байт то, что шлёт настоящий индикатор."""
        assert builder.build(0.0) == DUMP_EMPTY
        assert builder.build(85.0) == DUMP_PERSON

    def test_bad_checksum_rejected(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(85.0, bad_checksum=True)) is None

    def test_wrong_length(self, builder: PacketBuilder) -> None:
        pkt = builder.build(85.0)
        assert parse_packet(pkt[:-1]) is None
        assert parse_packet(pkt + b"\x00") is None
        assert parse_packet(b"") is None

    def test_wrong_frame_bytes(self, builder: PacketBuilder) -> None:
        pkt = builder.build(85.0)
        assert parse_packet(b"\x00" + pkt[1:]) is None  # не STX
        assert parse_packet(pkt[:-1] + b"\x00") is None  # не ETX

    def test_wrong_sign_byte(self, builder: PacketBuilder) -> None:
        pkt = builder.build(85.0)
        assert parse_packet(pkt[:1] + b" " + pkt[2:]) is None

    def test_non_digit_mass_rejected(self, builder: PacketBuilder) -> None:
        """Не-цифра в поле массы — мусор, даже если КС пересчитать."""
        pkt = bytearray(builder.build(85.0))
        pkt[4] = ord("X")
        checksum = 0
        for byte in pkt[1:9]:
            checksum ^= byte
        pkt[9:11] = f"{checksum:02X}".encode()
        assert parse_packet(bytes(pkt)) is None

    def test_lowercase_checksum_accepted(self, builder: PacketBuilder) -> None:
        """Реальный индикатор шлёт hex в верхнем регистре; нижний не встречался,
        но int(x,16) его принял бы — фиксируем текущее поведение разбора."""
        pkt = builder.build(85.0)
        lowered = pkt[:9] + pkt[9:11].lower() + pkt[11:]
        # нижний регистр даёт ту же численную КС — пакет валиден
        assert parse_packet(lowered) is not None

    def test_max_mass_9999999(self) -> None:
        """Максимум поля массы: 9999999 → 999999,9 кг, КС «12» сходится."""
        assert parse_ok(b"\x02+999999912\x03").raw_weight_kg == 999999.9
        assert make_packet(b"+", b"9999999") == b"\x02+999999912\x03"

    def test_max_mass_negative(self) -> None:
        """Минусовой максимум: знак меняет КС (12 → 14), вес отрицательный."""
        pkt = make_packet(b"-", b"9999999")
        assert pkt[9:11] == b"14"
        assert parse_ok(pkt).raw_weight_kg == -999999.9

    def test_minus_zero_packet(self) -> None:
        """Нулевая масса со знаком «-» (КС «1D») — валидна, вес равен нулю."""
        pkt = make_packet(b"-", b"0000000")
        assert pkt == b"\x02-00000001D\x03"
        assert parse_ok(pkt).raw_weight_kg == 0.0

    def test_tenth_kg_precision(self) -> None:
        """Младший разряд — десятые кг: 0000015 → 1,5 кг (не 15)."""
        assert parse_ok(make_packet(b"+", b"0000015")).raw_weight_kg == 1.5

    def test_checksum_of_valid_packet_always_in_10_1f(self) -> None:
        """Свойство протокола: XOR знака (2B/2D) и семи цифр (30..39)
        всегда лежит в 0x10..0x1F — первая hex-цифра КС всегда «1».
        Следствие: КС «00»/«FF» в валидном пакете невозможна, а ложный
        пакет из мусора обязан угадать одно из 16 значений."""
        for mass in PROTOCOL_MASS_GRID:
            digits = str(mass).rjust(7, "0").encode("ascii")
            for sign in (b"+", b"-"):
                pkt = make_packet(sign, digits)
                assert parse_packet(pkt) is not None
                declared = int(pkt[9:11].decode("ascii"), 16)
                assert 0x10 <= declared <= 0x1F, pkt

    @pytest.mark.parametrize("declared", [b"00", b"FF"])
    def test_checksum_00_ff_rejected(self, declared: bytes) -> None:
        """Граничные КС 00/FF: фактический XOR никогда им не равен —
        пакет с такой заявленной КС отбрасывается."""
        pkt = make_packet(b"+", b"0000850")
        forged = pkt[:9] + declared + pkt[11:]
        assert parse_packet(forged) is None

    @pytest.mark.parametrize("field", [b"\xff\xff", b"1\xff", b"\x80\x80"])
    def test_non_ascii_checksum_field(self, field: bytes) -> None:
        """Не-ASCII в поле КС — decode падает, пакет отбрасывается тихо."""
        pkt = make_packet(b"+", b"0000850")
        assert parse_packet(pkt[:9] + field + pkt[11:]) is None

    @pytest.mark.parametrize("field", [b"+5", b" 5", b"1 ", b"  ", b"0x", b"-1"])
    def test_lenient_hex_forms_do_not_slip_through(self, field: bytes) -> None:
        """int(x, 16) лоялен к пробелам и знакам («+5» → 5), но такие формы
        дают значение вне диапазона возможных КС (0x10..0x1F) либо ValueError —
        сквозь проверку они не проходят."""
        pkt = make_packet(b"+", b"0000850")
        assert parse_packet(pkt[:9] + field + pkt[11:]) is None

    def test_space_in_mass_rejected(self) -> None:
        """Масса всегда с лидирующими нулями; пробел в цифрах — мусор,
        даже с пересчитанной под него КС (в отличие от cas22, где пробелы
        в поле массы законны)."""
        digits = b"   0850"[:7]
        checksum = 0
        for byte in b"+" + digits:
            checksum ^= byte
        pkt = b"\x02+" + digits + f"{checksum:02X}".encode("ascii") + b"\x03"
        assert parse_packet(pkt) is None

    def test_etx_at_wrong_position(self) -> None:
        """ETX в середине при верной длине — не рамка, пакет отбрасывается."""
        pkt = make_packet(b"+", b"0000850")
        assert parse_packet(pkt[:5] + b"\x03" + pkt[6:]) is None


class TestQuantize:
    def test_dump_case(self) -> None:
        """85,0 в порту → «80» на табло (зафиксировано 20.08.2026)."""
        assert quantize(85.0) == 80.0

    def test_exact_discrete_kept(self) -> None:
        assert quantize(43390.0) == 43390.0

    def test_truncates_toward_zero_for_negative(self) -> None:
        """Знак сохраняется, усечение по модулю: −85,0 → −80."""
        assert quantize(-85.0) == -80.0

    def test_below_discrete_is_zero(self) -> None:
        assert quantize(5.0) == 0.0
        assert quantize(-5.0) == 0.0

    def test_edges_around_discrete(self) -> None:
        """Границы дискреты: чуть ниже — усекается, ровно — сохраняется."""
        assert quantize(9.9) == 0.0
        assert quantize(9.999) == 0.0
        assert quantize(10.0) == 10.0
        assert quantize(10.1) == 10.0
        assert quantize(19.99) == 10.0
        assert quantize(20.0) == 20.0

    def test_negative_near_zero(self) -> None:
        """Отрицательные около нуля: −9,9 → 0, −10 → −10, −19,9 → −10."""
        assert quantize(-9.9) == 0.0
        assert quantize(-10.0) == -10.0
        assert quantize(-19.9) == -10.0
        assert quantize(-0.1) == 0.0

    def test_max_protocol_mass_no_float_loss(self) -> None:
        """Максимум протокола: float не съедает разряд у больших значений."""
        assert quantize(999999.9) == 999990.0
        assert quantize(999990.0) == 999990.0
        assert quantize(-999999.9) == -999990.0

    def test_matches_integer_reference_on_protocol_grid(self) -> None:
        """Перебор сетки протокола (масса в десятых кг): quantize совпадает
        с целочисленным эталоном (raw_tenths // 100) * 10 — деление float
        не смещает результат ни на одной дискрете."""
        for raw_tenths in [*range(0, 3000), *PROTOCOL_MASS_GRID]:
            kg = raw_tenths / 10.0
            expected = float((raw_tenths // 100) * 10)
            assert quantize(kg) == expected, raw_tenths
            assert quantize(-kg) == -expected, raw_tenths


# ---------------------------------------------------------------------------
# PacketAssembler
# ---------------------------------------------------------------------------


class TestPacketAssembler:
    def test_clean_stream(self, builder: PacketBuilder) -> None:
        stream = builder.build(0.0) + builder.build(85.0) + builder.build(43390.0)
        packets = PacketAssembler().feed(stream)
        assert [p.raw_weight_kg for p in packets] == [0.0, 85.0, 43390.0]

    def test_byte_by_byte(self, builder: PacketBuilder) -> None:
        """Поток по одному байту (TCP так умеет) — пакеты собираются."""
        assembler = PacketAssembler()
        collected = []
        for byte in builder.build(85.0) + builder.build(0.0):
            collected += assembler.feed(bytes([byte]))
        assert [p.raw_weight_kg for p in collected] == [85.0, 0.0]

    def test_resync_after_garbage(self, builder: PacketBuilder) -> None:
        """Мусор с ложным STX между пакетами отбрасывается."""
        stream = (
            stream_of(garbage(seed=1, rate=RATE))
            + builder.build(85.0)
            + stream_of(garbage(seed=2, rate=RATE))
            + builder.build(0.0)
        )
        packets = PacketAssembler().feed(stream)
        assert [p.raw_weight_kg for p in packets] == [85.0, 0.0]

    def test_bad_checksum_stream_yields_nothing(self, builder: PacketBuilder) -> None:
        stream = stream_of(bad_checksum(builder, 85.0, duration_s=1.0, rate=RATE))
        assert PacketAssembler().feed(stream) == []

    def test_split_packet_across_feeds(self, builder: PacketBuilder) -> None:
        pkt = builder.build(85.0)
        assembler = PacketAssembler()
        assert assembler.feed(pkt[:7]) == []
        packets = assembler.feed(pkt[7:])
        assert [p.raw_weight_kg for p in packets] == [85.0]

    def test_buffer_bounded_without_stx(self) -> None:
        """Сплошной мусор без STX не копится в буфере."""
        assembler = PacketAssembler()
        assembler.feed(b"\xff" * 10_000)
        assert assembler._buffer == b""

    def test_stx_flood_keeps_buffer_bounded(self) -> None:
        """Бесконечный поток одних STX (наводка/short на линии): буфер
        не растёт — после feed остаётся хвост короче пакета, и следующий
        валидный пакет разбирается."""
        assembler = PacketAssembler()
        assert assembler.feed(b"\x02" * 10_000) == []
        assert len(assembler._buffer) < PACKET_LEN
        packets = assembler.feed(PacketBuilder().build(85.0))
        assert [p.raw_weight_kg for p in packets] == [85.0]

    def test_double_stx_before_valid_packet(self, builder: PacketBuilder) -> None:
        """Ложный STX вплотную перед настоящим: сдвиг на байт не съедает
        настоящий пакет."""
        packets = PacketAssembler().feed(b"\x02" + builder.build(85.0))
        assert [p.raw_weight_kg for p in packets] == [85.0]

    def test_bad_checksum_then_valid_not_eaten(self, builder: PacketBuilder) -> None:
        """Пакет с битой КС и валидный сразу за ним: ресинхронизация
        сдвигом на 1 байт находит следующий пакет без потерь."""
        stream = builder.build(85.0, bad_checksum=True) + builder.build(430.0)
        packets = PacketAssembler().feed(stream)
        assert [p.raw_weight_kg for p in packets] == [430.0]

    def test_bad_checksum_between_two_valid(self, builder: PacketBuilder) -> None:
        """Битый пакет в середине потока: соседние не теряются."""
        stream = builder.build(85.0) + builder.build(430.0, bad_checksum=True) + builder.build(85.0)
        packets = PacketAssembler().feed(stream)
        assert [p.raw_weight_kg for p in packets] == [85.0, 85.0]

    @pytest.mark.parametrize("cut", range(1, PACKET_LEN))
    def test_split_at_every_position(self, builder: PacketBuilder, cut: int) -> None:
        """Пакет, разрезанный по любой границе чтения (в том числе внутри
        поля КС и перед ETX), собирается из двух feed'ов."""
        pkt = builder.build(85.0)
        assembler = PacketAssembler()
        assert assembler.feed(pkt[:cut]) == []
        packets = assembler.feed(pkt[cut:])
        assert [p.raw_weight_kg for p in packets] == [85.0]

    def test_partial_tail_survives_between_feeds(self, builder: PacketBuilder) -> None:
        """Хвост неполного пакета живёт в буфере между feed'ами и
        доклеивается следующими кусками (порт отдаёт по 5 байт)."""
        stream = builder.build(0.0) + builder.build(85.0) + builder.build(430.0)
        assembler = PacketAssembler()
        collected = []
        for i in range(0, len(stream), 5):
            collected += assembler.feed(stream[i : i + 5])
        assert [p.raw_weight_kg for p in collected] == [0.0, 85.0, 430.0]

    def test_etx_only_garbage_between_packets(self, builder: PacketBuilder) -> None:
        """Мусор из одних ETX между пакетами: ETX без STX не рамка."""
        stream = b"\x03" * 7 + builder.build(85.0) + b"\x03" * 7 + builder.build(0.0)
        packets = PacketAssembler().feed(stream)
        assert [p.raw_weight_kg for p in packets] == [85.0, 0.0]

    def test_no_stx_etx_inside_valid_body(self) -> None:
        """Свойство протокола, на котором держится ресинхронизация: внутри
        тела валидного пакета (байты 1..10) не бывает ни STX, ни ETX —
        все значащие байты ≥ 0x2B. Значит скан по STX не может зацепиться
        за середину настоящего пакета."""
        for mass in PROTOCOL_MASS_GRID:
            digits = str(mass).rjust(7, "0").encode("ascii")
            for sign in (b"+", b"-"):
                body = make_packet(sign, digits)[1:11]
                assert 0x02 not in body and 0x03 not in body


# ---------------------------------------------------------------------------
# StabilityTracker (время подаётся руками — без сна)
# ---------------------------------------------------------------------------


class TestStabilityTracker:
    def test_unstable_until_window_filled(self) -> None:
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        assert tracker.update(80.0, now=0.0) is False
        assert tracker.update(80.0, now=0.5) is False  # окно ещё не набрано

    def test_stable_after_full_window(self) -> None:
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            stable = tracker.update(80.0, now=i * 0.1)
        assert stable is True

    def test_jump_beyond_amplitude_is_unstable(self) -> None:
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            tracker.update(12500.0, now=i * 0.1)
        assert tracker.update(12540.0, now=1.6) is False  # разброс 40 > 30

    def test_amplitude_within_three_discretes_is_stable(self) -> None:
        """Дрожь в пределах трёх дискрет (30 кг) — стабильно, как в UniServer."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        weights = [12500.0, 12510.0, 12520.0, 12500.0]
        stable = False
        for i in range(20):
            stable = tracker.update(weights[i % len(weights)], now=i * 0.1)
        assert stable is True

    def test_reset_forgets_history(self) -> None:
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            tracker.update(80.0, now=i * 0.1)
        tracker.reset()
        assert tracker.update(80.0, now=2.0) is False

    def test_boundary_amplitude_exactly_30_is_stable(self) -> None:
        """Ровно пороговая амплитуда (30,0 кг = 3 дискреты) — стабильно:
        порог включительный (≤), как «Автостабилизация 3 дискрет»."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        stable = False
        for i in range(15):
            stable = tracker.update(12500.0 if i % 2 == 0 else 12530.0, now=i * 0.1)
        assert stable is True

    def test_amplitude_just_above_30_unstable(self) -> None:
        """Амплитуда 30,1 кг — уже за порогом, стабильности нет."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        stable = True
        for i in range(15):
            stable = tracker.update(12500.0 if i % 2 == 0 else 12530.1, now=i * 0.1)
        assert stable is False

    def test_two_samples_at_window_edges_enough(self) -> None:
        """Щедрость критерия «окно набрано»: двух сэмплов на краях окна
        достаточно (старейший попал в первые 20% окна) — фиксируем,
        чтобы смена критерия не прошла незамеченной."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        assert tracker.update(500.0, now=0.0) is False
        assert tracker.update(500.0, now=1.0) is True

    def test_oldest_sample_too_deep_not_filled(self) -> None:
        """Старейший сэмпл глубже 20% окна (на его середине) — окно
        не считается набранным, стабильности нет даже при амплитуде 0."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        tracker.update(500.0, now=0.5)
        assert tracker.update(500.0, now=1.0) is False

    def test_single_sample_in_window_never_stable(self) -> None:
        """Пакеты реже окна (раз в 2 с): в окне всегда один сэмпл —
        стабильность не объявляется никогда."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(10):
            assert tracker.update(500.0, now=i * 2.0) is False

    def test_time_jump_backwards_is_fail_safe(self) -> None:
        """Немонотонное время (часы прыгнули назад): трекер не падает и
        не рапортует ложную стабильность, пока окно рассинхронизировано;
        штатный выход — reset (драйвер делает его при переоткрытии порта)."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            tracker.update(500.0, now=i * 0.1)
        assert tracker.update(500.0, now=1.5) is True
        # скачок назад на ~10 с: старые метки времени «из будущего»
        assert tracker.update(500.0, now=-8.6) is False
        for i in range(15):
            assert tracker.update(500.0, now=-8.6 + (i + 1) * 0.1) is False
        tracker.reset()
        stable = False
        for i in range(15):
            stable = tracker.update(500.0, now=100.0 + i * 0.1)
        assert stable is True

    def test_stable_again_after_reset_and_refill(self) -> None:
        """После reset окно копится заново и стабильность возвращается."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            tracker.update(80.0, now=i * 0.1)
        tracker.reset()
        stable = True
        for i in range(8):
            stable = tracker.update(80.0, now=2.0 + i * 0.1)
        assert stable is False  # 0,7 c из 1,0 c — окно ещё не набрано
        for i in range(8, 15):
            stable = tracker.update(80.0, now=2.0 + i * 0.1)
        assert stable is True  # 1,4 c — окно набрано заново

    def test_spike_leaves_window_after_time_passes(self) -> None:
        """Одиночный выброс портит стабильность, пока он в окне, и
        перестаёт влиять, когда окно его перерастает."""
        tracker = StabilityTracker(window_s=1.0, amplitude_kg=30.0)
        for i in range(15):
            tracker.update(12500.0, now=i * 0.1)
        assert tracker.update(12600.0, now=1.5) is False  # выброс +100 кг
        stable = False
        for i in range(1, 16):
            stable = tracker.update(12500.0, now=1.5 + i * 0.1)
        assert stable is True  # выброс покинул окно


# ---------------------------------------------------------------------------
# Интеграция: VesarDriver против TCP-эмулятора
# ---------------------------------------------------------------------------


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def wait_until(predicate: Callable[[], bool], timeout_s: float, poll_s: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll_s)
    return predicate()


class EmulatorServer:
    """asyncio-сервер ``vesar_emulator.serve()`` в отдельном потоке
    (устройство — как EmulatorServer в test_cas22_driver.py)."""

    def __init__(self, factory: ScenarioFactory, *, loop_forever: bool = True) -> None:
        self.port: int = free_port()
        self._factory = factory
        self._loop_forever = loop_forever
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[None] | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
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
) -> Iterator[VesarDriver]:
    driver = VesarDriver(
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


class TestDriverIntegration:
    def test_steady_stream_ok_stable_and_quantized(self, builder: PacketBuilder) -> None:
        """Ровный вес: статус OK, вес приведён к дискрете табло, стабильность
        появляется после набора окна успокоения (1 с)."""

        def factory() -> Iterator[Step]:
            return stable_weight(builder, 12505.0, duration_s=30.0, rate=RATE)

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                # 12505 → внутренний шаг 5 кг оставит 12505 → табло 12500
                assert wait_until(lambda: driver.state.weight_kg == 12500.0, 2.0)
                assert driver.state.stable is False  # окно ещё копится
                assert wait_until(lambda: driver.state.stable, 3.0)
        finally:
            server.stop()

    def test_unstable_stream_not_stable(self, builder: PacketBuilder) -> None:
        """Качание веса с амплитудой больше трёх дискрет — стабильности нет."""

        def factory() -> Iterator[Step]:
            return stabilizing(
                builder, TARGET_KG, duration_s=30.0, rate=RATE, amplitude_kg=200.0, seed=7
            )

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                time.sleep(1.5)  # больше окна успокоения
                assert driver.state.stable is False
        finally:
            server.stop()

    def test_dropout_leads_to_no_data(self, builder: PacketBuilder) -> None:
        def factory() -> Iterator[Step]:
            return iter(
                (
                    *stable_weight(builder, TARGET_KG, duration_s=0.5, rate=RATE),
                    *stream_break(duration_s=10.0),
                )
            )

        server = EmulatorServer(factory, loop_forever=False)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                assert wait_until(lambda: driver.state.status is ScaleStatus.NO_DATA, 3.0)
        finally:
            server.stop()

    def test_bad_checksum_stream_never_ok(self, builder: PacketBuilder) -> None:
        """Поток из пакетов с битой КС целиком отбраковывается — «нет данных»."""

        def factory() -> Iterator[Step]:
            return bad_checksum(builder, TARGET_KG, duration_s=30.0, rate=RATE)

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.NO_DATA, 3.0)
                assert driver.state.weight_kg is None
        finally:
            server.stop()

    def test_negative_weight_stream(self, builder: PacketBuilder) -> None:
        """Весы «ушли в минус» после съезда: драйвер честно отдаёт −30."""

        def factory() -> Iterator[Step]:
            return negative_weight(builder, -30.0, duration_s=30.0, rate=RATE)

        server = EmulatorServer(factory)
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(
                    lambda: (
                        driver.state.status is ScaleStatus.OK and driver.state.weight_kg == -30.0
                    ),
                    3.0,
                )
                assert driver.state.overload is False
        finally:
            server.stop()


# ---------------------------------------------------------------------------
# _read_loop с портом-заглушкой: тайминги тишины и подача пакетов в окно
# стабильности (детерминированнее TCP: чанки нарезаем сами)
# ---------------------------------------------------------------------------


class ScriptedPort:
    """Порт-заглушка: отдаёт заранее нарезанные чанки по расписанию.

    Элемент сценария — (payload, delay_s): read() спит delay_s (имитация
    таймаута чтения порта) и возвращает payload целиком. После исчерпания
    сценария — тишина (как замолчавший индикатор)."""

    def __init__(self, script: list[tuple[bytes, float]]) -> None:
        self._script = list(script)
        self._lock = threading.Lock()

    def read(self, size: int) -> bytes:
        with self._lock:
            if not self._script:
                time.sleep(0.05)
                return b""
            payload, delay = self._script.pop(0)
        time.sleep(delay)
        return payload


@contextlib.contextmanager
def scripted_driver(
    script: list[tuple[bytes, float]], *, rx_error_timeout_s: float = FAST_RX_ERROR_TIMEOUT_S
) -> Iterator[VesarDriver]:
    """Драйвер, читающий ScriptedPort напрямую через _read_loop."""
    driver = VesarDriver("scripted://", rx_error_timeout_s=rx_error_timeout_s)
    stop_event = threading.Event()
    thread = threading.Thread(
        target=driver._read_loop,
        args=(ScriptedPort(script), stop_event),
        name="vesar-scripted",
        daemon=True,
    )
    thread.start()
    try:
        yield driver
    finally:
        stop_event.set()
        thread.join(2.0)


class TestDriverReadLoop:
    """Поведение цикла чтения на управляемых чанках."""

    def test_stability_window_resets_after_no_data(self, builder: PacketBuilder) -> None:
        """Обрыв потока сбрасывает окно стабильности: после возобновления
        пакетов статус сразу OK, но стабильность появляется только после
        набора окна заново (старые показания не в счёт)."""
        pkt = builder.build(500.0)
        script = (
            [(pkt, 0.05)] * 24  # ~1,2 с ровного веса → стабильность набрана
            + [(b"", 0.05)] * 12  # ~0,6 с тишины (больше таймаута 0,3 с)
            + [(pkt, 0.05)] * 60  # снова ровный вес
        )
        with scripted_driver(script, rx_error_timeout_s=0.3) as driver:
            assert wait_until(lambda: driver.state.stable, 3.0)
            assert wait_until(lambda: driver.state.status is ScaleStatus.NO_DATA, 2.0)
            # отметка последнего пакета при тишине сохранена (диагностика)
            assert driver.state.last_packet_at is not None
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 2.0)
            # окно после сброса копится заново — стабильности ещё нет
            assert driver.state.stable is False
            assert wait_until(lambda: driver.state.stable, 3.0)

    def test_oscillation_within_chunk_must_not_look_stable(self, builder: PacketBuilder) -> None:
        """БАГ: _read_loop подаёт в StabilityTracker только ПОСЛЕДНИЙ пакет
        каждого чанка (packets[-1]) — колебания между чтениями невидимы.

        Реалистичный режим: индикатор шлёт ~12,5 пак/с, read(64) с таймаутом
        0,2 с отдаёт по 2–3 пакета за чанк. Если вес качается 500↔1500 кг
        (амплитуда 1000 кг при пороге 30 кг), но последним в каждом чанке
        приходит 1500, трекер видит амплитуду 0 и объявляет стабильность.

        Ожидание: stable=False на всём качании; фактически (баг) — True.
        Чинится подачей ВСЕХ пакетов чанка в stability.update()."""
        chunk = builder.build(500.0) + builder.build(1500.0)
        script = [(chunk, 0.05)] * 50  # 2,5 с качания с амплитудой 1000 кг
        with scripted_driver(script) as driver:
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 2.0)
            became_stable = wait_until(lambda: driver.state.stable, 1.8)
            assert not became_stable, (
                "качание 500↔1500 кг (амплитуда 1000 кг) не может быть стабильным"
            )


# ---------------------------------------------------------------------------
# Жизненный цикл драйвера и смена порта (настройки из центра)
# ---------------------------------------------------------------------------


def driver_thread_names(port: int) -> list[str]:
    """Имена живых фоновых потоков драйвера для данного порта."""
    name = f"vesar:socket://127.0.0.1:{port}"
    return [t.name for t in threading.enumerate() if t.name == name]


def steady_factory(weight_kg: float) -> ScenarioFactory:
    """Фабрика бесконечного ровного веса для серверов эмулятора."""
    factory_builder = PacketBuilder()

    def factory() -> Iterator[Step]:
        return stable_weight(factory_builder, weight_kg, duration_s=30.0, rate=RATE)

    return factory


class TestDriverLifecycle:
    """start/stop/повторный start: потоки не плодятся и не теряются."""

    def test_start_twice_does_not_spawn_threads(self) -> None:
        server = EmulatorServer(steady_factory(500.0))
        server.start()
        try:
            with running_driver(server.port) as driver:
                assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
                driver.start()  # повторный вызов при живом потоке — безвреден
                assert len(driver_thread_names(server.port)) == 1
                assert driver.state.status is ScaleStatus.OK
        finally:
            server.stop()

    def test_stop_then_restart(self) -> None:
        server = EmulatorServer(steady_factory(500.0))
        server.start()
        driver = VesarDriver(
            f"socket://127.0.0.1:{server.port}",
            read_timeout_s=FAST_READ_TIMEOUT_S,
            rx_error_timeout_s=FAST_RX_ERROR_TIMEOUT_S,
            reopen_delay_s=FAST_REOPEN_DELAY_S,
        )
        try:
            driver.start()
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
            driver.stop()
            assert driver_thread_names(server.port) == []
            driver.start()
            assert wait_until(lambda: driver.state.status is ScaleStatus.OK, 3.0)
        finally:
            driver.stop()
            server.stop()

    def test_zero_is_not_supported(self) -> None:
        """Команд к весам в протоколе нет: zero() честно возвращает False."""
        assert VesarDriver("socket://127.0.0.1:1").zero() is False


class TestDriverSetPort:
    """Смена порта на лету (set_port, настройки из центра) — как в cas22."""

    WEIGHT_A = 1000.0
    WEIGHT_B = 2000.0

    def _server(self, weight: float) -> EmulatorServer:
        server = EmulatorServer(steady_factory(weight))
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
        сброшено (NO_DATA/PORT_ERROR), OK не появляется; откат обратно
        оживляет чтение (как это делает SettingsManager)."""
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
                assert driver.state.status is not ScaleStatus.OK
                assert wait_until(
                    lambda: driver.state.status in (ScaleStatus.NO_DATA, ScaleStatus.PORT_ERROR),
                    2.0,
                )
                assert not wait_until(lambda: driver.state.status is ScaleStatus.OK, 0.6)
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


# ---------------------------------------------------------------------------
# Реестр драйверов
# ---------------------------------------------------------------------------


class TestDriverRegistry:
    def test_vesar_registered(self) -> None:
        assert "vesar" in DRIVERS and "cas22" in DRIVERS

    def test_create_driver_vesar(self) -> None:
        driver = create_driver("vesar", "socket://127.0.0.1:1", baudrate=9600)
        assert isinstance(driver, VesarDriver)
        assert driver.port_url == "socket://127.0.0.1:1"
        assert driver.baudrate == 9600

    def test_create_driver_unknown_name(self) -> None:
        with pytest.raises(ValueError, match="неизвестный драйвер"):
            create_driver("nonexistent", "COM1", baudrate=9600)

    def test_packet_len_matches_protocol(self) -> None:
        assert PACKET_LEN == 12
