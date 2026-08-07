"""Тесты эмулятора индикатора CAS 22 byte (tools/cas22_emulator.py).

Ключевая идея: каждый пакет эмулятора сверяется с ЭТАЛОННЫМ парсером —
копией логики ``parse_packet`` из прототипа ``docs/справка/cas22_reader.py``
(вне репозитория), проверенного на реальном индикаторе (СВХ «Кызыл-Кыя»,
06.08.2026). Если эталон разбирает поток эмулятора — драйвер, написанный
по той же логике, тоже разберёт.
"""

from collections.abc import Iterable, Iterator

import pytest

from tools.cas22_emulator import (
    DEFAULT_RATE,
    PACKET_LEN,
    PacketBuilder,
    Step,
    demo,
    drive_off,
    drive_on,
    empty_scale,
    full_cycle,
    garbage,
    make_scenario,
    negative_weight,
    overload,
    stabilizing,
    stable_weight,
    stream_break,
)

# Результат эталонного разбора: (вес_кг | None при перегрузе, стабильно, режим)
ParseResult = tuple[float | None, bool, str]

OVERLOAD_RESULT: ParseResult = (None, False, "ПЕРЕГРУЗ")


def parse_packet(pkt: bytes) -> ParseResult | None:
    """ЭТАЛОН: копия parse_packet из прототипа cas22_reader.py.

    Прототип проверен на реальном индикаторе CAS CI-серии — логика
    скопирована один в один (переформатирован только выбор mode_name).
    Возвращает (вес_кг, стабильно, режим) или None, если пакет не распознан.
    """
    if len(pkt) != PACKET_LEN:
        return None
    # проверка байтов синхронизации: запятые и CR LF на своих местах
    if pkt[2:3] != b"," or pkt[5:6] != b"," or pkt[8:9] != b"," or pkt[20:22] != b"\r\n":
        return None
    flag = pkt[0:2]  # ST / US / OL
    mode = pkt[3:5]  # GS / NT
    sign = pkt[6:8]  # поле знака/статуса
    massa_raw = pkt[9:17]  # 8 символов массы

    if flag == b"OL":
        return (None, False, "ПЕРЕГРУЗ")

    try:
        # убираем ВСЕ пробелы (они могут быть и внутри числа) и плюс
        text = massa_raw.decode("ascii").replace(" ", "").replace("+", "")
        value = float(text) if text not in ("", "-") else 0.0
    except (UnicodeDecodeError, ValueError):
        return None
    if b"-" in sign or b"-" in massa_raw:
        value = -abs(value)

    stable = flag == b"ST"
    if mode == b"GS":
        mode_name = "БРУТТО"
    elif mode == b"NT":
        mode_name = "НЕТТО"
    else:
        mode_name = mode.decode("ascii", "replace")
    return (value, stable, mode_name)


def parse_chunked(chunks: Iterable[bytes]) -> list[ParseResult]:
    """ЭТАЛОН: логика синхронизации из главного цикла cas22_reader.py.

    Ищем CR LF, берём 22 байта до него включительно, проверяем пакет;
    мусор без CR LF копится в буфере и подрезается. Кусочная подача
    воспроизводит чтение порта произвольными порциями.
    """
    buffer = b""
    results: list[ParseResult] = []
    for chunk in chunks:
        buffer += chunk
        while True:
            idx = buffer.find(b"\r\n")
            if idx < 0:
                if len(buffer) > 4 * PACKET_LEN:
                    buffer = buffer[-PACKET_LEN:]
                break
            start = idx + 2 - PACKET_LEN
            candidate = buffer[start : idx + 2] if start >= 0 else b""
            buffer = buffer[idx + 2 :]
            result = parse_packet(candidate)
            if result is not None:
                results.append(result)
    return results


def stream_of(steps: Iterable[Step]) -> bytes:
    """Склеить полезную нагрузку шагов сценария в один байтовый поток."""
    return b"".join(step.payload for step in steps)


def parse_steps(steps: Iterable[Step]) -> list[ParseResult]:
    """Прогнать шаги сценария через эталонную синхронизацию и разбор."""
    return parse_chunked(step.payload for step in steps)


def weights_of(results: Iterable[ParseResult]) -> list[float]:
    """Вытащить веса из результатов разбора (перегрузы недопустимы)."""
    weights: list[float] = []
    for value, _stable, _mode in results:
        assert value is not None, "неожиданный перегруз в сценарии"
        weights.append(value)
    return weights


@pytest.fixture
def builder() -> PacketBuilder:
    """Сборщик пакетов с дискретностью по умолчанию (10 кг)."""
    return PacketBuilder()


class TestPacketStructure:
    """Структура 22-байтового пакета — по docs/protocols/cas22.md."""

    def test_length_and_sync_bytes(self, builder: PacketBuilder) -> None:
        # Длина 22, запятые на позициях 2/5/8, CR LF в конце
        pkt = builder.build(12500)
        assert len(pkt) == PACKET_LEN == 22
        assert pkt[2:3] == b","
        assert pkt[5:6] == b","
        assert pkt[8:9] == b","
        assert pkt[20:22] == b"\r\n"

    def test_unit_field(self, builder: PacketBuilder) -> None:
        # Байты 17-19 — единица измерения «kg » (с пробелом)
        pkt = builder.build(12500)
        assert pkt[17:20] == b"kg "

    def test_mass_field_is_8_ascii_right_justified(self, builder: PacketBuilder) -> None:
        # Поле массы — ровно 8 ASCII-символов, число прижато вправо
        pkt = builder.build(12500)
        mass = pkt[9:17]
        assert len(mass) == 8
        assert mass == b"   12500"
        mass.decode("ascii")  # не должно упасть

    def test_flag_stable(self, builder: PacketBuilder) -> None:
        assert builder.build(100, stable=True)[0:2] == b"ST"

    def test_flag_unstable(self, builder: PacketBuilder) -> None:
        assert builder.build(100, stable=False)[0:2] == b"US"

    def test_flag_overload_wins_over_stable(self, builder: PacketBuilder) -> None:
        # OL имеет приоритет над признаком стабильности
        assert builder.build(100, stable=True, overload=True)[0:2] == b"OL"
        assert builder.build(100, stable=False, overload=True)[0:2] == b"OL"

    def test_mode_gross_and_net(self, builder: PacketBuilder) -> None:
        assert builder.build(100)[3:5] == b"GS"
        assert builder.build(100, mode=b"NT")[3:5] == b"NT"

    def test_reference_parses_gross_stable(self, builder: PacketBuilder) -> None:
        # Эталон возвращает вес, признак стабильности и режим
        assert parse_packet(builder.build(12500, stable=True)) == (12500.0, True, "БРУТТО")

    def test_reference_parses_net_unstable(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(340, stable=False, mode=b"NT")) == (340.0, False, "НЕТТО")

    def test_reference_parses_zero(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(0)) == (0.0, True, "БРУТТО")


class TestRoundWeight:
    """Приведение веса к дискретности — как это делает сам индикатор."""

    def test_discret_10(self, builder: PacketBuilder) -> None:
        assert builder.round_weight(0) == 0
        assert builder.round_weight(12504) == 12500
        assert builder.round_weight(12507) == 12510
        assert builder.round_weight(12510) == 12510

    def test_discret_20(self) -> None:
        b20 = PacketBuilder(discret=20)
        assert b20.round_weight(12509) == 12500
        assert b20.round_weight(12511) == 12520
        # и вес в пакете кратен дискретности
        pkt = b20.build(12511)
        result = parse_packet(pkt)
        assert result is not None
        assert result[0] == 12520.0

    def test_negative_rounding(self, builder: PacketBuilder) -> None:
        assert builder.round_weight(-27) == -30


class TestNegativeWeight:
    """Отрицательный вес: весы «ушли в минус» после съезда без обнуления."""

    def test_sign_in_bytes_6_7(self, builder: PacketBuilder) -> None:
        # Знак — в поле знака/статуса (байты 6-7), не в поле массы
        pkt = builder.build(-30)
        assert pkt[6:8] == b" -"
        assert b"-" not in pkt[9:17]

    def test_positive_sign_field_is_spaces(self, builder: PacketBuilder) -> None:
        assert builder.build(30)[6:8] == b"  "

    def test_reference_returns_negative(self, builder: PacketBuilder) -> None:
        assert parse_packet(builder.build(-30)) == (-30.0, True, "БРУТТО")

    def test_negative_scenario(self, builder: PacketBuilder) -> None:
        results = parse_steps(negative_weight(builder, -30.0, 2.0, 8.0))
        assert len(results) == 16
        assert all(r == (-30.0, True, "БРУТТО") for r in results)


class TestSpaceInMass:
    """Пробел внутри числа — причуда реального железа, эталон её переживает."""

    def test_space_inserted_as_thousands_separator(self, builder: PacketBuilder) -> None:
        pkt = builder.build(1460, space_in_mass=True)
        assert pkt[9:17] == b"   1 460"

    def test_reference_still_parses(self, builder: PacketBuilder) -> None:
        result = parse_packet(builder.build(1460, space_in_mass=True))
        assert result == (1460.0, True, "БРУТТО")

    def test_large_value_with_space(self, builder: PacketBuilder) -> None:
        result = parse_packet(builder.build(12500, space_in_mass=True))
        assert result == (12500.0, True, "БРУТТО")

    def test_small_value_without_space(self, builder: PacketBuilder) -> None:
        # До 3 цифр разделитель тысяч не ставится
        assert builder.build(500, space_in_mass=True)[9:17] == b"     500"

    def test_negative_with_space(self, builder: PacketBuilder) -> None:
        result = parse_packet(builder.build(-1460, space_in_mass=True))
        assert result == (-1460.0, True, "БРУТТО")


class TestOverload:
    """Перегруз OL: эталон возвращает признак перегруза, вес не используется."""

    def test_reference_returns_overload_marker(self, builder: PacketBuilder) -> None:
        result = parse_packet(builder.build(99990, overload=True))
        assert result == OVERLOAD_RESULT
        assert result[0] is None  # веса нет — использовать нечего

    def test_overload_scenario(self, builder: PacketBuilder) -> None:
        results = parse_steps(overload(builder, 2.0, 8.0))
        assert len(results) == 16
        assert all(r == OVERLOAD_RESULT for r in results)


class TestMassOverflow:
    """Вес, не влезающий в 8 символов, — ошибка сборки пакета."""

    def test_overflow_raises_value_error(self, builder: PacketBuilder) -> None:
        with pytest.raises(ValueError, match="не помещается"):
            builder.build(1_000_000_000)

    def test_eight_digits_fit(self, builder: PacketBuilder) -> None:
        # Ровно 8 цифр — предел поля массы
        result = parse_packet(builder.build(99_999_990))
        assert result == (99_999_990.0, True, "БРУТТО")

    def test_negative_overflow_raises(self, builder: PacketBuilder) -> None:
        with pytest.raises(ValueError, match="не помещается"):
            builder.build(-1_000_000_000)


class TestScenarios:
    """Базовые сценарии: каждый пакет разбирается эталоном, значения верны."""

    def test_empty_scale(self, builder: PacketBuilder) -> None:
        results = parse_steps(empty_scale(builder, 3.0, 8.0))
        assert len(results) == 24  # duration_s * rate
        assert all(r == (0.0, True, "БРУТТО") for r in results)

    def test_drive_on_monotonic_growth_unstable(self, builder: PacketBuilder) -> None:
        target = 12500.0
        results = parse_steps(drive_on(builder, target, 3.0, 8.0))
        assert len(results) == 24
        assert all(not stable for _, stable, _ in results)
        weights = weights_of(results)
        assert weights[0] == 0.0
        assert weights[-1] == builder.round_weight(target)
        assert weights == sorted(weights)  # монотонный рост

    def test_stable_weight_constant(self, builder: PacketBuilder) -> None:
        results = parse_steps(stable_weight(builder, 12500.0, 3.0, 8.0))
        assert len(results) == 24
        assert all(r == (12500.0, True, "БРУТТО") for r in results)

    def test_drive_off_falls_to_zero(self, builder: PacketBuilder) -> None:
        results = parse_steps(drive_off(builder, 12500.0, 2.0, 8.0))
        assert len(results) == 16
        assert all(not stable for _, stable, _ in results)
        weights = weights_of(results)
        assert weights[0] == 12500.0
        assert weights[-1] == 0.0
        assert weights == sorted(weights, reverse=True)  # монотонное падение

    def test_stabilizing_oscillates_around_target(self, builder: PacketBuilder) -> None:
        around, amplitude = 12500.0, 30.0
        results = parse_steps(stabilizing(builder, around, 2.0, 8.0, amplitude, seed=1))
        assert len(results) == 16
        assert all(not stable for _, stable, _ in results)
        # округление к дискретности может добавить до discret/2 к амплитуде
        margin = amplitude + builder.discret / 2
        for w in weights_of(results):
            assert around - margin <= w <= around + margin

    def test_stabilizing_damping(self, builder: PacketBuilder) -> None:
        # Амплитуда затухает: разброс второй половины не больше первой
        around = 12500.0
        weights = weights_of(parse_steps(stabilizing(builder, around, 10.0, 8.0, seed=0)))
        half = len(weights) // 2
        spread_first = max(abs(w - around) for w in weights[:half])
        spread_second = max(abs(w - around) for w in weights[half:])
        assert spread_second <= spread_first

    def test_full_cycle_phases_and_final_zero(self, builder: PacketBuilder) -> None:
        target = 12500.0
        steps = list(full_cycle(builder, target, 8.0, seed=0))
        results = parse_steps(steps)
        # каждый пакет цикла разобран эталоном без потерь
        assert len(results) == len(steps) == 112  # (2+3+2+3+2+2) с * 8 пак/с
        weights = weights_of(results)
        assert weights[0] == 0.0  # начинается с пустых весов
        assert weights[-1] == 0.0  # последний вес — ноль (весы освободились)
        # есть фаза стабильного целевого веса — то, что фиксирует драйвер
        assert (target, True, "БРУТТО") in results

    def test_demo_all_packets_accounted_for(self, builder: PacketBuilder) -> None:
        """Демо: всё, что построено builder-ом, разобрано; мусор отброшен."""
        steps = list(demo(builder, 12500.0, 8.0, seed=0))
        # эталонный список: валидные 22-байтовые пакеты в порядке следования
        expected = [
            result
            for step in steps
            if len(step.payload) == PACKET_LEN
            and (result := parse_packet(step.payload)) is not None
        ]
        parsed = parse_steps(steps)
        assert parsed == expected  # ни ложных весов из мусора, ни потерянных пакетов
        # демо действительно показывает все режимы
        weights = [w for w, _, _ in parsed if w is not None]
        assert any(w < 0 for w in weights)  # отрицательный вес
        assert OVERLOAD_RESULT in parsed  # перегруз
        assert parsed[-1] == (0.0, True, "БРУТТО")  # финал — пустые весы


class TestStreamBreak:
    """Обрыв потока: тишина дольше таймаута драйвера (3 с)."""

    def test_single_silent_step(self) -> None:
        steps = list(stream_break())
        assert len(steps) == 1
        assert steps[0].payload == b""  # тишина, ни одного байта
        assert steps[0].delay_s >= 4.0 > 3.0  # дольше RX_ERROR_TIMEOUT драйвера

    def test_dropout_scenario_ends_with_silence(self, builder: PacketBuilder) -> None:
        steps = list(make_scenario("dropout", builder, 12500.0, 8.0)())
        assert steps[-1].payload == b""
        assert steps[-1].delay_s >= 4.0


class TestGarbage:
    """Мусор в потоке: эталонная синхронизация не выдаёт ни одного веса."""

    @pytest.mark.parametrize("seed", range(30))
    def test_garbage_rejected_for_many_seeds(self, seed: int) -> None:
        # Ни один кусок мусора не должен разобраться как вес или перегруз
        assert parse_steps(garbage(seed=seed)) == []

    def test_garbage_contains_false_crlf(self) -> None:
        # Мусор обязан содержать ложный конец пакета — иначе тест
        # ресинхронизации сводится к простому поиску CR LF
        stream = stream_of(garbage(seed=0))
        assert b"\r\n" in stream
        assert len(stream) == 40 + 2  # n_bytes + вставленный CR LF

    def test_resync_after_garbage(self, builder: PacketBuilder) -> None:
        # После мусора настоящие пакеты снова разбираются — без потерь
        steps = list(make_scenario("garbage", builder, 12500.0, 8.0)())
        results = parse_steps(steps)
        assert len(results) == 16  # все 16 пакетов stable_weight (2 с * 8/с)
        assert all(r == (12500.0, True, "БРУТТО") for r in results)

    def test_packet_split_across_reads(self, builder: PacketBuilder) -> None:
        # Пакеты, разрезанные по границе чтения (порт отдаёт куски по 5 байт)
        stream = stream_of(stable_weight(builder, 12500.0, 2.0, 8.0))
        chunks = [stream[i : i + 5] for i in range(0, len(stream), 5)]
        results = parse_chunked(chunks)
        assert len(results) == 16
        assert all(r == (12500.0, True, "БРУТТО") for r in results)


class TestTimings:
    """Тайминги шагов: delay == 1/rate, число пакетов == duration * rate."""

    @pytest.mark.parametrize("rate", [8.0, 10.0, 2.0])
    def test_step_delay_matches_rate(self, builder: PacketBuilder, rate: float) -> None:
        steps = list(empty_scale(builder, 2.0, rate))
        assert all(step.delay_s == pytest.approx(1.0 / rate) for step in steps)

    @pytest.mark.parametrize(
        ("duration_s", "rate", "expected"),
        [(3.0, 8.0, 24), (2.0, 10.0, 20), (1.0, 2.0, 2)],
    )
    def test_packet_count(
        self, builder: PacketBuilder, duration_s: float, rate: float, expected: int
    ) -> None:
        assert len(list(stable_weight(builder, 12500.0, duration_s, rate))) == expected

    def test_garbage_steps_use_rate_delay(self) -> None:
        steps = list(garbage(seed=0, rate=8.0))
        assert all(step.delay_s == pytest.approx(1.0 / 8.0) for step in steps)


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

    def test_demo_same_seed_bytes_and_delays(self, builder: PacketBuilder) -> None:
        first = [(s.payload, s.delay_s) for s in demo(builder, 12500.0, 8.0, seed=0)]
        second = [(s.payload, s.delay_s) for s in demo(builder, 12500.0, 8.0, seed=0)]
        assert first == second

    @pytest.mark.parametrize(
        "name",
        ["empty", "cycle", "unstable", "dropout", "garbage", "overload", "negative", "demo"],
    )
    def test_every_named_scenario_is_deterministic(self, name: str) -> None:
        # Фабрика с одними параметрами даёт воспроизводимый поток
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
        ["empty", "cycle", "unstable", "dropout", "garbage", "overload", "negative", "demo"],
    )
    def test_all_scenarios_reference_safe(self, name: str) -> None:
        # Любой сценарий: эталон разбирает все валидные пакеты и не выдаёт
        # ничего лишнего на мусоре/тишине
        steps = list(make_scenario(name, PacketBuilder(), 12500.0, 8.0, seed=0)())
        expected = [
            result
            for step in steps
            if len(step.payload) == PACKET_LEN
            and (result := parse_packet(step.payload)) is not None
        ]
        assert parse_steps(steps) == expected


class TestRateNonDefault:
    """Сценарии уважают нестандартную частоту (реальный индикатор ~10/с)."""

    def test_default_rate_constant(self) -> None:
        assert DEFAULT_RATE == 8.0

    def test_full_cycle_count_scales_with_rate(self, builder: PacketBuilder) -> None:
        steps_8 = list(full_cycle(builder, 12500.0, 8.0))
        steps_16 = list(full_cycle(builder, 12500.0, 16.0))
        assert len(steps_16) == 2 * len(steps_8)


def _iter_type_check() -> Iterator[Step]:
    """Служебная проверка типов: сценарии — итераторы шагов."""
    return empty_scale(PacketBuilder(), 1.0, 8.0)
