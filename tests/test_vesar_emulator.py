"""Тесты эмулятора индикатора «VESAR» (tools/vesar_emulator.py).

Ключевая идея — как у test_cas22_emulator.py: каждый пакет эмулятора
сверяется с ЭТАЛОННЫМ парсером, написанным здесь независимо от драйвера
прямо по спецификации docs/protocols/vesar.md (регулярное выражение по
таблице пакета + XOR). Эталон закреплён на живых дампах СВХ «Кара-Суу»
20.08.2026 (пакеты байт в байт в тестах ниже). Если эталон разбирает
поток эмулятора — драйвер, написанный по той же спецификации, тоже
разберёт; расхождение эмулятора и дампов ловится немедленно.
"""

import re
from collections.abc import Iterable

import pytest

from tools.vesar_emulator import (
    DEFAULT_RATE,
    INTERNAL_STEP_KG,
    PACKET_LEN,
    PacketBuilder,
    Step,
    bad_checksum,
    demo,
    drive_off,
    drive_on,
    empty_scale,
    full_cycle,
    garbage,
    make_scenario,
    negative_weight,
    stabilizing,
    stable_weight,
    stream_break,
)

# эталонные пакеты из живого дампа Кара-Суу 20.08.2026 (байт в байт)
DUMP_EMPTY = b"\x02+00000001B\x03"  # весы пусты, 0 кг
DUMP_PERSON = b"\x02+000085016\x03"  # оператор на платформе, 85,0 кг

# спецификация пакета: STX, знак, 7 цифр, две hex-цифры КС (ВЕРХНИЙ регистр), ETX
SPEC_RE = re.compile(rb"\A\x02([+-])(\d{7})([0-9A-F]{2})\x03\Z")


def reference_parse(pkt: bytes) -> float | None:
    """ЭТАЛОН: разбор пакета строго по docs/protocols/vesar.md.

    Написан независимо от драйвера (регулярное выражение по таблице
    формата) и строже него: hex КС — только верхний регистр, как шлёт
    настоящий индикатор. Возвращает вес в кг или None.
    """
    if len(pkt) != PACKET_LEN:
        return None
    match = SPEC_RE.match(pkt)
    if match is None:
        return None
    sign, digits, declared = match.group(1), match.group(2), match.group(3)
    checksum = 0
    for byte in sign + digits:
        checksum ^= byte
    if checksum != int(declared.decode("ascii"), 16):
        return None
    value = int(digits) / 10.0  # младший разряд — десятые кг
    return -value if sign == b"-" else value


def reference_chunked(chunks: Iterable[bytes]) -> list[float]:
    """ЭТАЛОН синхронизации: скан по STX с проверкой пакета целиком.

    Мусор до STX отбрасывается, невалидный кандидат — сдвиг на байт
    (та же дисциплина, что в спецификации: «ресинхронизация по STX
    с проверкой ETX на своём месте»). Кусочная подача воспроизводит
    чтение порта произвольными порциями.
    """
    buffer = b""
    results: list[float] = []
    for chunk in chunks:
        buffer += chunk
        while True:
            idx = buffer.find(b"\x02")
            if idx < 0:
                buffer = b""
                break
            buffer = buffer[idx:]
            if len(buffer) < PACKET_LEN:
                break
            value = reference_parse(buffer[:PACKET_LEN])
            if value is not None:
                results.append(value)
                buffer = buffer[PACKET_LEN:]
            else:
                buffer = buffer[1:]
    return results


def stream_of(steps: Iterable[Step]) -> bytes:
    """Склеить полезную нагрузку шагов сценария в один байтовый поток."""
    return b"".join(step.payload for step in steps)


def parse_steps(steps: Iterable[Step]) -> list[float]:
    """Прогнать шаги сценария через эталонную синхронизацию и разбор."""
    return reference_chunked(step.payload for step in steps)


@pytest.fixture
def builder() -> PacketBuilder:
    """Сборщик пакетов с внутренним шагом индикатора по умолчанию (5 кг)."""
    return PacketBuilder()


class TestPacketStructure:
    """Структура 12-байтового пакета — по docs/protocols/vesar.md."""

    def test_length_and_frame_bytes(self, builder: PacketBuilder) -> None:
        pkt = builder.build(12500.0)
        assert len(pkt) == PACKET_LEN == 12
        assert pkt[0] == 0x02  # STX
        assert pkt[11] == 0x03  # ETX

    def test_sign_and_seven_digits(self, builder: PacketBuilder) -> None:
        pkt = builder.build(12500.0)
        assert pkt[1:2] == b"+"
        assert pkt[2:9] == b"0125000"  # десятые кг с лидирующими нулями
        assert pkt[2:9].isdigit()

    def test_checksum_is_uppercase_hex_of_xor(self, builder: PacketBuilder) -> None:
        """КС — XOR байтов [1..8] двумя ASCII-hex цифрами ВЕРХНЕГО регистра."""
        for weight in (0.0, 85.0, 430.0, 12500.0, -30.0):
            pkt = builder.build(weight)
            checksum = 0
            for byte in pkt[1:9]:
                checksum ^= byte
            assert pkt[9:11] == f"{checksum:02X}".encode("ascii")
            assert re.fullmatch(rb"[0-9A-F]{2}", pkt[9:11])

    def test_matches_live_dump_byte_for_byte(self, builder: PacketBuilder) -> None:
        """Эмулятор собирает байт в байт то, что шлёт настоящий индикатор."""
        assert builder.build(0.0) == DUMP_EMPTY
        assert builder.build(85.0) == DUMP_PERSON

    def test_reference_parses_dumps(self) -> None:
        assert reference_parse(DUMP_EMPTY) == 0.0
        assert reference_parse(DUMP_PERSON) == 85.0

    def test_reference_round_trip(self, builder: PacketBuilder) -> None:
        """Любой пакет builder-а разбирается эталоном в round_weight(вес)."""
        for weight in (0.0, 2.5, 85.0, 430.0, 12500.0, 43390.0, -30.0, -12500.0):
            assert reference_parse(builder.build(weight)) == builder.round_weight(weight)

    def test_no_stx_etx_inside_body(self, builder: PacketBuilder) -> None:
        """Внутри тела пакета (байты 1..10) нет ни STX, ни ETX — свойство,
        на котором держится ресинхронизация драйвера."""
        for weight in (0.0, 85.0, 999995.0, -999995.0, 12345.0):
            body = builder.build(weight)[1:11]
            assert 0x02 not in body and 0x03 not in body


class TestRoundWeight:
    """Внутренний шаг показаний индикатора (5 кг по живому дампу)."""

    def test_default_step_constant(self) -> None:
        assert INTERNAL_STEP_KG == 5.0

    def test_rounds_to_step(self, builder: PacketBuilder) -> None:
        assert builder.round_weight(0.0) == 0.0
        assert builder.round_weight(12502.0) == 12500.0
        assert builder.round_weight(12503.0) == 12505.0
        assert builder.round_weight(85.0) == 85.0

    def test_negative_rounding(self, builder: PacketBuilder) -> None:
        assert builder.round_weight(-27.0) == -25.0
        assert builder.round_weight(-28.0) == -30.0

    def test_result_is_multiple_of_step(self, builder: PacketBuilder) -> None:
        for weight in (0.3, 7.7, 84.9, 12503.4, -17.2):
            rounded = builder.round_weight(weight)
            assert rounded == round(rounded / builder.step_kg) * builder.step_kg

    def test_custom_step(self) -> None:
        """Другая дискрета (появится второй vesar-объект): шаг настраиваем."""
        coarse = PacketBuilder(step_kg=10.0)
        assert coarse.round_weight(87.0) == 90.0
        fine = PacketBuilder(step_kg=0.1)
        assert fine.round_weight(85.07) == pytest.approx(85.1)

    def test_near_zero_negative_becomes_plus_zero(self, builder: PacketBuilder) -> None:
        """−2 кг округляется к нулю — знак пакета «+» (минус-ноль не шлём)."""
        pkt = builder.build(-2.0)
        assert pkt[1:2] == b"+"
        assert reference_parse(pkt) == 0.0


class TestBuildLimits:
    """Границы поля массы: 7 цифр десятых кг (максимум 999999,9 кг)."""

    def test_max_mass_with_default_step(self, builder: PacketBuilder) -> None:
        pkt = builder.build(999995.0)
        assert reference_parse(pkt) == 999995.0

    def test_default_step_overflow_raises(self, builder: PacketBuilder) -> None:
        """999999,9 при шаге 5 округляется к 1 000 000 — не помещается."""
        with pytest.raises(ValueError, match="не помещается"):
            builder.build(999999.9)

    def test_max_protocol_mass_with_fine_step(self) -> None:
        """Шаг 0,1: максимум протокола 999999,9 собирается и равен дампу
        по структуре (7 девяток, КС «12»)."""
        fine = PacketBuilder(step_kg=0.1)
        pkt = fine.build(999999.9)
        assert pkt == b"\x02+999999912\x03"
        assert reference_parse(pkt) == 999999.9

    def test_overflow_raises(self) -> None:
        fine = PacketBuilder(step_kg=0.1)
        with pytest.raises(ValueError, match="не помещается"):
            fine.build(1_000_000.0)

    def test_negative_overflow_raises(self) -> None:
        fine = PacketBuilder(step_kg=0.1)
        with pytest.raises(ValueError, match="не помещается"):
            fine.build(-1_000_000.0)


class TestBadChecksum:
    """Пакет с испорченной КС: структурно цел, но эталон его отвергает."""

    def test_frame_intact_but_checksum_wrong(self, builder: PacketBuilder) -> None:
        good = builder.build(85.0)
        bad = builder.build(85.0, bad_checksum=True)
        # рамки, знак и цифры не тронуты — бьётся именно КС
        assert bad[0] == 0x02 and bad[11] == 0x03
        assert bad[1:9] == good[1:9]
        assert bad[9:11] != good[9:11]
        assert reference_parse(bad) is None

    def test_corruption_is_xor_ff(self, builder: PacketBuilder) -> None:
        """Порча детерминирована: заявленная КС — инверсия настоящей."""
        good = builder.build(85.0)
        bad = builder.build(85.0, bad_checksum=True)
        assert int(bad[9:11], 16) == int(good[9:11], 16) ^ 0xFF

    def test_badsum_scenario_all_rejected(self, builder: PacketBuilder) -> None:
        steps = list(bad_checksum(builder, 12500.0, 2.0, 8.0))
        assert len(steps) == 16
        assert parse_steps(steps) == []


class TestNegativeWeight:
    """Отрицательный вес: весы «ушли в минус» после съезда без обнуления."""

    def test_sign_byte_is_minus(self, builder: PacketBuilder) -> None:
        pkt = builder.build(-30.0)
        assert pkt[1:2] == b"-"
        assert pkt[2:9] == b"0000300"

    def test_reference_returns_negative(self, builder: PacketBuilder) -> None:
        assert reference_parse(builder.build(-30.0)) == -30.0

    def test_negative_scenario(self, builder: PacketBuilder) -> None:
        results = parse_steps(negative_weight(builder, -30.0, 2.0, 8.0))
        assert len(results) == 16
        assert all(value == -30.0 for value in results)


class TestScenarios:
    """Базовые сценарии: каждый пакет разбирается эталоном, значения верны."""

    TARGET = 12500.0

    def test_empty_scale(self, builder: PacketBuilder) -> None:
        results = parse_steps(empty_scale(builder, 3.0, 8.0))
        assert len(results) == 24  # duration_s * rate
        assert all(value == 0.0 for value in results)

    def test_drive_on_monotonic_growth(self, builder: PacketBuilder) -> None:
        results = parse_steps(drive_on(builder, self.TARGET, 3.0, 8.0))
        assert len(results) == 24
        assert results[0] == 0.0
        assert results[-1] == builder.round_weight(self.TARGET)
        assert results == sorted(results)  # монотонный рост

    def test_stable_weight_constant(self, builder: PacketBuilder) -> None:
        results = parse_steps(stable_weight(builder, self.TARGET, 3.0, 8.0))
        assert len(results) == 24
        assert all(value == self.TARGET for value in results)

    def test_drive_off_falls_to_zero(self, builder: PacketBuilder) -> None:
        results = parse_steps(drive_off(builder, self.TARGET, 2.0, 8.0))
        assert len(results) == 16
        assert results[0] == self.TARGET
        assert results[-1] == 0.0
        assert results == sorted(results, reverse=True)  # монотонное падение

    def test_stabilizing_within_amplitude(self, builder: PacketBuilder) -> None:
        amplitude = 60.0
        results = parse_steps(stabilizing(builder, self.TARGET, 2.0, 8.0, amplitude, seed=1))
        assert len(results) == 16
        # округление к внутреннему шагу может добавить до step/2 к амплитуде
        margin = amplitude + builder.step_kg / 2
        for value in results:
            assert self.TARGET - margin <= value <= self.TARGET + margin

    def test_stabilizing_starts_beyond_driver_threshold(self, builder: PacketBuilder) -> None:
        """Амплитуда по умолчанию (60 кг) заведомо больше порога успокоения
        драйвера (30 кг) — начало фазы гарантированно нестабильно."""
        results = parse_steps(stabilizing(builder, self.TARGET, 2.0, 8.0, seed=0))
        first_second = results[:8]
        assert max(first_second) - min(first_second) > 30.0

    def test_stabilizing_damping(self, builder: PacketBuilder) -> None:
        results = parse_steps(stabilizing(builder, self.TARGET, 10.0, 8.0, seed=0))
        half = len(results) // 2
        spread_first = max(abs(v - self.TARGET) for v in results[:half])
        spread_second = max(abs(v - self.TARGET) for v in results[half:])
        assert spread_second <= spread_first

    def test_full_cycle_phases(self, builder: PacketBuilder) -> None:
        steps = list(full_cycle(builder, self.TARGET, 8.0, seed=0))
        results = parse_steps(steps)
        # каждый пакет цикла разобран эталоном без потерь
        assert len(results) == len(steps) == 112  # (2+3+2+3+2+2) с * 8 пак/с
        assert results[0] == 0.0  # начинается с пустых весов
        assert results[-1] == 0.0  # и заканчивается ими
        assert self.TARGET in results  # целевой вес достигнут
        # фаза качания может «переваливать» цель не дальше своей амплитуды
        assert max(results) <= self.TARGET + 60.0 + builder.step_kg / 2

    def test_full_cycle_has_stable_phase_longer_than_window(self, builder: PacketBuilder) -> None:
        """В цикле есть непрерывная фаза ровного целевого веса длиннее окна
        успокоения драйвера (1 с) — иначе стабильность не наступит."""
        rate = 8.0
        results = parse_steps(full_cycle(builder, self.TARGET, rate, seed=0))
        longest = run = 0
        for value in results:
            run = run + 1 if value == self.TARGET else 0
            longest = max(longest, run)
        assert longest >= int(3.0 * rate)  # фаза stable_weight целиком

    def test_demo_all_packets_accounted_for(self, builder: PacketBuilder) -> None:
        """Демо: всё, что построено builder-ом, разобрано; мусор и битые КС
        отброшены; ни ложных весов, ни потерянных пакетов."""
        steps = list(demo(builder, self.TARGET, 8.0, seed=0))
        expected = [
            value
            for step in steps
            if len(step.payload) == PACKET_LEN
            and (value := reference_parse(step.payload)) is not None
        ]
        parsed = parse_steps(steps)
        assert parsed == expected
        assert any(value < 0 for value in parsed)  # отрицательный вес показан
        assert parsed[-1] == 0.0  # финал — пустые весы


class TestGarbage:
    """Мусор в потоке: эталонная синхронизация не выдаёт ни одного веса."""

    @pytest.mark.parametrize("seed", range(30))
    def test_garbage_rejected_for_many_seeds(self, seed: int) -> None:
        assert parse_steps(garbage(seed=seed)) == []

    def test_garbage_contains_false_stx(self) -> None:
        """Мусор обязан содержать ложный STX с обрывком пакета — иначе тест
        ресинхронизации сводится к простому поиску STX."""
        stream = stream_of(garbage(seed=0))
        assert b"\x02+00" in stream
        assert len(stream) == 40 + 4  # n_bytes + вставленные STX и '+00'

    def test_resync_after_garbage_scenario(self, builder: PacketBuilder) -> None:
        steps = list(make_scenario("garbage", builder, 12500.0, 8.0)())
        results = parse_steps(steps)
        assert len(results) == 16  # все пакеты stable_weight (2 с * 8/с)
        assert all(value == 12500.0 for value in results)

    def test_packets_split_across_reads(self, builder: PacketBuilder) -> None:
        stream = stream_of(stable_weight(builder, 12500.0, 2.0, 8.0))
        chunks = [stream[i : i + 5] for i in range(0, len(stream), 5)]
        assert reference_chunked(chunks) == [12500.0] * 16


class TestStreamBreak:
    """Обрыв потока: тишина дольше таймаута драйвера (3 с)."""

    def test_single_silent_step(self) -> None:
        steps = list(stream_break())
        assert len(steps) == 1
        assert steps[0].payload == b""
        assert steps[0].delay_s >= 4.0 > 3.0  # дольше RxErrorTimeOut драйвера

    def test_dropout_scenario_ends_with_silence(self, builder: PacketBuilder) -> None:
        steps = list(make_scenario("dropout", builder, 12500.0, 8.0)())
        assert steps[-1].payload == b""
        assert steps[-1].delay_s >= 4.0


class TestTimings:
    """Тайминги шагов: delay == 1/rate, число пакетов == duration * rate."""

    def test_default_rate_matches_live_indicator(self) -> None:
        assert DEFAULT_RATE == 12.5  # ~12,5 пакетов/с по дампу 20.08.2026

    @pytest.mark.parametrize("rate", [8.0, 12.5, 40.0])
    def test_step_delay_matches_rate(self, builder: PacketBuilder, rate: float) -> None:
        steps = list(empty_scale(builder, 2.0, rate))
        assert all(step.delay_s == pytest.approx(1.0 / rate) for step in steps)

    @pytest.mark.parametrize(
        ("duration_s", "rate", "expected"),
        [(3.0, 8.0, 24), (2.0, 12.5, 25), (1.0, 2.0, 2)],
    )
    def test_packet_count(
        self, builder: PacketBuilder, duration_s: float, rate: float, expected: int
    ) -> None:
        assert len(list(stable_weight(builder, 12500.0, duration_s, rate))) == expected


class TestDeterminism:
    """Один seed — байт-в-байт одинаковый поток (важно для отладки)."""

    def test_stabilizing_same_seed(self, builder: PacketBuilder) -> None:
        a = stream_of(stabilizing(builder, 12500.0, 2.0, 8.0, seed=7))
        b = stream_of(stabilizing(builder, 12500.0, 2.0, 8.0, seed=7))
        assert a == b

    def test_stabilizing_different_seed(self, builder: PacketBuilder) -> None:
        a = stream_of(stabilizing(builder, 12500.0, 2.0, 8.0, seed=7))
        b = stream_of(stabilizing(builder, 12500.0, 2.0, 8.0, seed=8))
        assert a != b

    @pytest.mark.parametrize(
        "name",
        ["empty", "cycle", "unstable", "dropout", "garbage", "badsum", "negative", "demo"],
    )
    def test_every_named_scenario_is_deterministic(self, name: str) -> None:
        factory_a = make_scenario(name, PacketBuilder(), 12500.0, 8.0, seed=0)
        factory_b = make_scenario(name, PacketBuilder(), 12500.0, 8.0, seed=0)
        assert list(factory_a()) == list(factory_b())


class TestMakeScenario:
    """Фабрика сценариев по имени."""

    def test_unknown_name_raises(self, builder: PacketBuilder) -> None:
        with pytest.raises(ValueError, match="неизвестный сценарий"):
            make_scenario("no_such", builder, 12500.0, 8.0)

    @pytest.mark.parametrize(
        "name",
        ["empty", "cycle", "unstable", "dropout", "garbage", "badsum", "negative", "demo"],
    )
    def test_all_scenarios_reference_safe(self, name: str) -> None:
        """Любой сценарий: эталон разбирает все валидные пакеты и не выдаёт
        ничего лишнего на мусоре, битых КС и тишине."""
        steps = list(make_scenario(name, PacketBuilder(), 12500.0, 8.0, seed=0)())
        expected = [
            value
            for step in steps
            if len(step.payload) == PACKET_LEN
            and (value := reference_parse(step.payload)) is not None
        ]
        assert parse_steps(steps) == expected
