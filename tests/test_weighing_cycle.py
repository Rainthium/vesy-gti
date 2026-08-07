"""Тесты конечного автомата цикла взвешивания (agent/weighing/cycle.py).

Автомат детерминирован: время инъецируется через ``clock`` (замыкание над
переменной), состояние индикатора конструируется напрямую как ``ScaleState``.
Никаких ожиданий и sleep — все тесты мгновенные.

Спецификация: architecture §3.3 (IDLE → WAIT_ZERO → WAIT_VEHICLE →
WAIT_STABLE → CAPTURE → DONE), коды ошибок — §4.1 (shared.enums.ErrorCode).
"""

from collections.abc import Callable

import pytest

from agent.drivers.base import ScaleState
from agent.weighing.cycle import CycleConfig, CycleResult, CycleState, WeighingCycle
from shared.enums import ErrorCode, ScaleStatus

# --- вспомогательные фабрики ---

# Часы: (получить время, сдвинуть время на N секунд)
Clock = tuple[Callable[[], float], Callable[[float], None]]


def make_clock(start: float = 0.0) -> Clock:
    """Управляемые часы: пара замыканий (clock, advance) над одной переменной."""
    now = start

    def clock() -> float:
        return now

    def advance(seconds: float) -> None:
        nonlocal now
        now += seconds

    return clock, advance


def ok(weight: float | None, *, stable: bool = True, overload: bool = False) -> ScaleState:
    """Снимок индикатора с идущим потоком данных (status OK)."""
    return ScaleState(status=ScaleStatus.OK, weight_kg=weight, stable=stable, overload=overload)


# Снимки «данных нет»: порт молчит и порт не открылся
NO_DATA = ScaleState(status=ScaleStatus.NO_DATA)
PORT_ERROR = ScaleState(status=ScaleStatus.PORT_ERROR, error="ошибка 31")

CFG = CycleConfig()  # значения по умолчанию: пороги 50/500 кг, таймауты 10/60/30 с, 2 с, 5 с

VEHICLE_KG = 12000.0  # типовой вес гружёного АТС в тестах


def started_cycle(config: CycleConfig = CFG, start: float = 100.0) -> tuple[WeighingCycle, Clock]:
    """Цикл, запущенный в WAIT_ZERO, с управляемыми часами.

    Старт часов не с нуля — чтобы ловить ошибки с неинициализированными
    метками времени (например, ``_candidate_since = 0.0``).
    """
    clock = make_clock(start)
    cycle = WeighingCycle(config, clock=clock[0])
    cycle.start()
    assert cycle.state is CycleState.WAIT_ZERO  # start() переводит IDLE → WAIT_ZERO
    return cycle, clock


def drive_to_wait_vehicle(cycle: WeighingCycle, advance: Callable[[float], None]) -> None:
    """Довести цикл до WAIT_VEHICLE: пустые стабильные весы."""
    advance(0.1)
    assert cycle.tick(ok(0.0)) is CycleState.WAIT_VEHICLE


def drive_to_wait_stable(
    cycle: WeighingCycle, advance: Callable[[float], None], weight: float = VEHICLE_KG
) -> None:
    """Довести цикл до WAIT_STABLE: заезд АТС (вес ещё нестабилен)."""
    drive_to_wait_vehicle(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(weight, stable=False)) is CycleState.WAIT_STABLE


def drive_to_capture(
    cycle: WeighingCycle, advance: Callable[[float], None], weight: float = VEHICLE_KG
) -> None:
    """Довести цикл до CAPTURE: стабильный неизменный вес в течение stable_duration_s."""
    drive_to_wait_stable(cycle, advance, weight)
    advance(0.1)
    assert cycle.tick(ok(weight)) is CycleState.WAIT_STABLE  # первый кандидат
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(weight)) is CycleState.CAPTURE


# --- счастливый путь ---


def test_full_cycle_capture_ok() -> None:
    """Полный цикл до CAPTURE и complete_capture(camera_ok=True) → OK с весом."""
    cycle, (_, advance) = started_cycle()
    drive_to_capture(cycle, advance)
    assert cycle.result is None  # в CAPTURE итог ещё не заполнен

    result = cycle.complete_capture(camera_ok=True)
    assert cycle.state is CycleState.DONE
    assert result == CycleResult(code=ErrorCode.OK, weight_kg=VEHICLE_KG)
    assert cycle.result is result


def test_capture_camera_fail_returns_weight() -> None:
    """ERR_CAMERA: вес зафиксирован и возвращается, ошибка камеры — предупреждение."""
    cycle, (_, advance) = started_cycle()
    drive_to_capture(cycle, advance)

    result = cycle.complete_capture(camera_ok=False, message="камера front недоступна")
    assert result.code is ErrorCode.ERR_CAMERA
    assert result.weight_kg == VEHICLE_KG  # вес не теряется
    assert result.message == "камера front недоступна"


def test_custom_config_thresholds_followed() -> None:
    """Пороги берутся из CycleConfig, а не захардкожены.

    С порогами 5/100 кг: 10 кг — уже не пусто (при дефолтных 50 было бы пусто),
    150 кг — уже заезд (при дефолтных 500 не было бы), фиксация через 1 с.
    """
    config = CycleConfig(
        zero_threshold_kg=5.0,
        vehicle_threshold_kg=100.0,
        stable_duration_s=1.0,
    )
    cycle, (_, advance) = started_cycle(config)

    advance(0.1)
    assert cycle.tick(ok(10.0)) is CycleState.WAIT_ZERO  # 10 >= 5 — не пусто
    advance(0.1)
    assert cycle.tick(ok(2.0)) is CycleState.WAIT_VEHICLE  # 2 < 5 — пусто
    advance(0.1)
    assert cycle.tick(ok(150.0, stable=False)) is CycleState.WAIT_STABLE  # 150 >= 100 — заезд
    advance(0.1)
    assert cycle.tick(ok(150.0)) is CycleState.WAIT_STABLE  # кандидат
    advance(1.0)
    assert cycle.tick(ok(150.0)) is CycleState.CAPTURE  # выдержка ровно 1 с из конфига


def test_custom_config_timeouts_followed() -> None:
    """Таймауты берутся из CycleConfig: zero_timeout_s=1 срабатывает через ~1 с."""
    config = CycleConfig(zero_timeout_s=1.0)
    cycle, (_, advance) = started_cycle(config)

    advance(0.9)
    assert cycle.tick(ok(3000.0)) is CycleState.WAIT_ZERO  # ещё в пределах таймаута
    advance(0.2)
    assert cycle.tick(ok(3000.0)) is CycleState.DONE  # 1.1 c > 1 c из конфига
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_NOT_ZERO


# --- таймауты фаз ---


def test_wait_zero_timeout_scale_busy() -> None:
    """WAIT_ZERO: весы заняты дольше zero_timeout_s → ERR_NOT_ZERO."""
    cycle, (_, advance) = started_cycle()
    advance(CFG.zero_timeout_s)
    assert cycle.tick(ok(3000.0)) is CycleState.WAIT_ZERO  # ровно на границе — ещё ждём
    advance(0.1)
    assert cycle.tick(ok(3000.0)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_NOT_ZERO
    assert cycle.result.weight_kg is None


def test_wait_zero_timeout_zero_but_unstable() -> None:
    """WAIT_ZERO: вес нулевой, но нестабилен → пустыми весы не считаются, ERR_NOT_ZERO."""
    cycle, (_, advance) = started_cycle()
    for _ in range(3):
        advance(CFG.zero_timeout_s / 3)
        cycle.tick(ok(0.0, stable=False))  # ноль, но флага стабильности нет
    advance(0.1)
    assert cycle.tick(ok(0.0, stable=False)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_NOT_ZERO


def test_wait_vehicle_timeout() -> None:
    """WAIT_VEHICLE: никто не заехал дольше vehicle_timeout_s → ERR_VEHICLE_TIMEOUT."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_vehicle(cycle, advance)
    advance(CFG.vehicle_timeout_s + 0.1)
    assert cycle.tick(ok(0.0)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_VEHICLE_TIMEOUT


def test_wait_stable_timeout_jumping_weight() -> None:
    """WAIT_STABLE: вес прыгает (stable=False) дольше stable_timeout_s → ERR_UNSTABLE."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    for step in range(5):
        advance(CFG.stable_timeout_s / 5)
        cycle.tick(ok(VEHICLE_KG + step * 40, stable=False))
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG, stable=False)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_UNSTABLE


def test_wait_stable_timeout_stable_but_drifting() -> None:
    """WAIT_STABLE: показания стабильны, но значение дрейфует каждый тик → ERR_UNSTABLE."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    weight = VEHICLE_KG
    for _ in range(20):
        advance(CFG.stable_timeout_s / 20)
        weight += 10.0  # каждое показание «стабильно», но значение меняется
        cycle.tick(ok(weight))
    advance(0.1)
    assert cycle.tick(ok(weight + 10.0)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_UNSTABLE


@pytest.mark.parametrize("bad", [NO_DATA, PORT_ERROR], ids=["no_data", "port_error"])
@pytest.mark.parametrize(
    "phase", [CycleState.WAIT_ZERO, CycleState.WAIT_VEHICLE, CycleState.WAIT_STABLE]
)
def test_no_data_timeout_in_any_active_phase(bad: ScaleState, phase: CycleState) -> None:
    """Нет данных (NO_DATA/PORT_ERROR) дольше no_data_timeout_s → ERR_SCALE_OFFLINE."""
    cycle, (_, advance) = started_cycle()
    if phase is not CycleState.WAIT_ZERO:
        drive_to_wait_vehicle(cycle, advance)
    if phase is CycleState.WAIT_STABLE:
        advance(0.1)
        assert cycle.tick(ok(VEHICLE_KG, stable=False)) is CycleState.WAIT_STABLE
    assert cycle.state is phase

    advance(0.1)
    assert cycle.tick(bad) is phase  # первый провал данных — цикл ещё ждёт
    advance(CFG.no_data_timeout_s + 0.1)
    assert cycle.tick(bad) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_SCALE_OFFLINE


def test_short_no_data_gap_cycle_recovers() -> None:
    """Провал данных короче no_data_timeout_s — цикл продолжается и завершается успешно."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE  # кандидат зафиксирован

    # провал данных на 4 из 5 допустимых секунд
    advance(0.1)
    assert cycle.tick(NO_DATA) is CycleState.WAIT_STABLE
    advance(CFG.no_data_timeout_s - 1.0)
    assert cycle.tick(NO_DATA) is CycleState.WAIT_STABLE

    # данные вернулись: провал сбросил кандидата — выдержка неизменности
    # начинается заново (вес во время провала не наблюдался), затем фиксация
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE
    result = cycle.complete_capture(camera_ok=True)
    assert result.code is ErrorCode.OK
    assert result.weight_kg == VEHICLE_KG


def test_no_data_gap_exactly_at_timeout_boundary() -> None:
    """Провал данных ровно no_data_timeout_s: ошибки нет («дольше» — строго),
    после возобновления потока цикл завершается успешно."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_vehicle(cycle, advance)

    advance(0.1)
    assert cycle.tick(NO_DATA) is CycleState.WAIT_VEHICLE  # отсчёт провала пошёл
    advance(CFG.no_data_timeout_s)  # ровно граница таймаута
    assert cycle.tick(NO_DATA) is CycleState.WAIT_VEHICLE  # ещё не «дольше» — не ошибка

    # поток вернулся — цикл продолжается как ни в чём не бывало
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG, stable=False)) is CycleState.WAIT_STABLE
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE
    assert cycle.complete_capture(camera_ok=True).code is ErrorCode.OK


def test_repeated_gaps_do_not_accumulate() -> None:
    """Счётчик провала данных сбрасывается при каждом возобновлении потока:
    несколько коротких провалов подряд не складываются в ERR_SCALE_OFFLINE."""
    cycle, (_, advance) = started_cycle(CycleConfig(vehicle_timeout_s=1000.0))
    drive_to_wait_vehicle(cycle, advance)
    for _ in range(4):  # суммарно 4 * 3 c = 12 c > 5 c, но по отдельности — меньше
        advance(0.1)
        assert cycle.tick(NO_DATA) is CycleState.WAIT_VEHICLE
        advance(3.0)
        assert cycle.tick(NO_DATA) is CycleState.WAIT_VEHICLE
        advance(0.1)
        assert cycle.tick(ok(0.0)) is CycleState.WAIT_VEHICLE
    assert cycle.result is None


# --- краевые случаи WAIT_STABLE ---


def test_stable_but_changing_value_no_premature_capture() -> None:
    """Стабильные показания разных значений (12500 → 12510) не фиксируются,
    пока новое значение не продержится stable_duration_s."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance, weight=12500.0)

    advance(0.1)
    assert cycle.tick(ok(12500.0)) is CycleState.WAIT_STABLE  # кандидат 12500
    advance(CFG.stable_duration_s - 0.5)
    assert cycle.tick(ok(12510.0)) is CycleState.WAIT_STABLE  # значение сменилось — отсчёт заново
    advance(CFG.stable_duration_s - 0.5)
    assert cycle.tick(ok(12510.0)) is CycleState.WAIT_STABLE  # 12510 держится лишь 1.5 c
    advance(0.5)
    assert cycle.tick(ok(12510.0)) is CycleState.CAPTURE  # 12510 продержалось 2 c
    assert cycle.complete_capture(camera_ok=True).weight_kg == 12510.0


def test_vehicle_left_returns_to_wait_vehicle_then_new_entry_captures() -> None:
    """Съезд в WAIT_STABLE (вес ниже vehicle_threshold) → возврат в WAIT_VEHICLE,
    новый заезд доводит цикл до CAPTURE."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE  # кандидат появился

    advance(0.1)
    assert cycle.tick(ok(120.0)) is CycleState.WAIT_VEHICLE  # АТС съехало

    advance(0.1)
    assert cycle.tick(ok(15000.0, stable=False)) is CycleState.WAIT_STABLE  # новый заезд
    advance(0.1)
    assert cycle.tick(ok(15000.0)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(15000.0)) is CycleState.CAPTURE
    assert cycle.complete_capture(camera_ok=True).weight_kg == 15000.0


def test_overload_is_not_treated_as_vehicle_exit() -> None:
    """Перегруз (overload=True, weight=None) — не съезд: цикл остаётся в WAIT_STABLE,
    кандидат сбрасывается, после снятия перегруза выдержка отсчитывается заново."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE  # кандидат почти дозрел
    advance(CFG.stable_duration_s - 0.1)

    assert cycle.tick(ok(None, overload=True)) is CycleState.WAIT_STABLE  # перегруз — не съезд

    # вес вернулся: старый кандидат сброшен, отсчёт stable_duration_s начинается заново
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s - 0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE  # выдержка ещё не набрана
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE


def test_prolonged_overload_ends_with_err_unstable() -> None:
    """Затянувшийся перегруз в WAIT_STABLE → ERR_UNSTABLE по stable_timeout_s."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    for _ in range(5):
        advance(CFG.stable_timeout_s / 5)
        cycle.tick(ok(None, overload=True))
    advance(0.1)
    assert cycle.tick(ok(None, overload=True)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_UNSTABLE


# --- граничные значения ---


def test_negative_weight_within_threshold_is_empty() -> None:
    """Отрицательный вес в пределах порога (по модулю) — весы считаются пустыми."""
    cycle, (_, advance) = started_cycle()
    advance(0.1)
    assert cycle.tick(ok(-(CFG.zero_threshold_kg - 10.0))) is CycleState.WAIT_VEHICLE


def test_weight_equal_vehicle_threshold_is_entry() -> None:
    """Вес ровно vehicle_threshold_kg — заезд состоялся (нестрогое >=)."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_vehicle(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(CFG.vehicle_threshold_kg, stable=False)) is CycleState.WAIT_STABLE


def test_weight_equal_zero_threshold_is_not_empty() -> None:
    """Вес ровно zero_threshold_kg — НЕ пусто (строгое <): остаёмся в WAIT_ZERO."""
    cycle, (_, advance) = started_cycle()
    advance(0.1)
    assert cycle.tick(ok(CFG.zero_threshold_kg)) is CycleState.WAIT_ZERO
    advance(CFG.zero_timeout_s + 0.1)
    assert cycle.tick(ok(CFG.zero_threshold_kg)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_NOT_ZERO


def test_stable_duration_exact_boundary_captures() -> None:
    """Неизменность ровно stable_duration_s — фиксация есть (нестрогое >=)."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE  # кандидат, отсчёт с этого тика
    advance(CFG.stable_duration_s)  # ровно граница выдержки
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE


def test_stable_duration_just_below_boundary_no_capture() -> None:
    """Чуть меньше stable_duration_s — фиксации ещё нет."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s - 0.01)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE


# --- жизненный цикл экземпляра ---


def test_tick_is_noop_in_idle() -> None:
    """tick() в IDLE ничего не меняет (цикл не запущен)."""
    clock, _ = make_clock()
    cycle = WeighingCycle(CFG, clock=clock)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.IDLE
    assert cycle.result is None


def test_tick_is_noop_in_capture() -> None:
    """tick() в CAPTURE не движет автомат: даже съезд АТС не отменяет фиксацию."""
    cycle, (_, advance) = started_cycle()
    drive_to_capture(cycle, advance)
    advance(60.0)
    assert cycle.tick(ok(0.0)) is CycleState.CAPTURE  # АТС уехало — фиксация уже сделана
    assert cycle.tick(NO_DATA) is CycleState.CAPTURE
    assert cycle.complete_capture(camera_ok=True).weight_kg == VEHICLE_KG


def test_tick_is_noop_in_done() -> None:
    """tick() в DONE не меняет ни состояние, ни итог."""
    cycle, (_, advance) = started_cycle()
    result = cycle.abort(ErrorCode.ERR_BUSY)
    advance(1000.0)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.DONE
    assert cycle.result is result


def test_start_twice_raises() -> None:
    """Повторный start() → RuntimeError (экземпляр одноразовый)."""
    cycle, _ = started_cycle()
    with pytest.raises(RuntimeError):
        cycle.start()


def test_start_after_done_raises() -> None:
    """start() после завершения цикла тоже запрещён."""
    cycle, _ = started_cycle()
    cycle.abort(ErrorCode.ERR_BUSY)
    with pytest.raises(RuntimeError):
        cycle.start()


@pytest.mark.parametrize("phase", [CycleState.IDLE, CycleState.WAIT_ZERO, CycleState.DONE])
def test_complete_capture_outside_capture_raises(phase: CycleState) -> None:
    """complete_capture() вне CAPTURE → RuntimeError."""
    clock = make_clock(100.0)
    cycle = WeighingCycle(CFG, clock=clock[0])
    if phase is not CycleState.IDLE:
        cycle.start()
    if phase is CycleState.DONE:
        cycle.abort(ErrorCode.ERR_BUSY)
    assert cycle.state is phase
    with pytest.raises(RuntimeError):
        cycle.complete_capture(camera_ok=True)


@pytest.mark.parametrize(
    "phase",
    [CycleState.WAIT_ZERO, CycleState.WAIT_VEHICLE, CycleState.WAIT_STABLE, CycleState.CAPTURE],
)
def test_abort_from_any_phase(phase: CycleState) -> None:
    """abort(ERR_BUSY) из любой фазы → DONE с этим кодом."""
    cycle, (_, advance) = started_cycle()
    if phase is CycleState.WAIT_VEHICLE:
        drive_to_wait_vehicle(cycle, advance)
    elif phase is CycleState.WAIT_STABLE:
        drive_to_wait_stable(cycle, advance)
    elif phase is CycleState.CAPTURE:
        drive_to_capture(cycle, advance)
    assert cycle.state is phase

    result = cycle.abort(ErrorCode.ERR_BUSY, message="весы заняты")
    assert cycle.state is CycleState.DONE
    assert result.code is ErrorCode.ERR_BUSY
    assert result.message == "весы заняты"
    assert cycle.result is result


def test_result_none_until_done_then_filled() -> None:
    """result is None во всех промежуточных состояниях и заполнен после завершения."""
    cycle, (_, advance) = started_cycle()
    assert cycle.result is None  # WAIT_ZERO
    drive_to_wait_vehicle(cycle, advance)
    assert cycle.result is None
    advance(0.1)
    cycle.tick(ok(VEHICLE_KG, stable=False))
    assert cycle.result is None  # WAIT_STABLE
    advance(0.1)
    cycle.tick(ok(VEHICLE_KG))
    advance(CFG.stable_duration_s)
    cycle.tick(ok(VEHICLE_KG))
    assert cycle.state is CycleState.CAPTURE
    assert cycle.result is None  # CAPTURE: вес есть, но итога ещё нет

    result = cycle.complete_capture(camera_ok=True)
    assert cycle.result is result
    assert result.code is ErrorCode.OK
    assert result.weight_kg == VEHICLE_KG


# --- защита от недобросовестного драйвера и повторного abort ---


def test_overload_with_numeric_weight_never_captured() -> None:
    """Перегруз с числовым весом (нарушение контракта ScaleState драйвером)
    не должен дойти до фиксации: перегрузное показание — не кандидат."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    for _ in range(10):
        advance(CFG.stable_duration_s)
        state = cycle.tick(ok(VEHICLE_KG, overload=True))
        assert state is not CycleState.CAPTURE
    # после прекращения перегруза фиксация идёт с нуля по честным показаниям
    advance(0.1)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.WAIT_STABLE
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE


def test_overload_in_wait_vehicle_means_vehicle_present() -> None:
    """Перегруз в фазе заезда означает, что АТС на весах: переход к стабилизации,
    затянувшийся перегруз завершается честным ERR_UNSTABLE, а не VEHICLE_TIMEOUT."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_vehicle(cycle, advance)
    advance(0.1)
    assert cycle.tick(ok(None, overload=True)) is CycleState.WAIT_STABLE
    advance(CFG.stable_timeout_s + 0.1)
    assert cycle.tick(ok(None, overload=True)) is CycleState.DONE
    assert cycle.result is not None
    assert cycle.result.code is ErrorCode.ERR_UNSTABLE


def test_abort_after_done_keeps_original_result() -> None:
    """abort() после завершения цикла не перезаписывает готовый результат."""
    cycle, (_, advance) = started_cycle()
    drive_to_wait_stable(cycle, advance)
    advance(0.1)
    cycle.tick(ok(VEHICLE_KG))
    advance(CFG.stable_duration_s)
    assert cycle.tick(ok(VEHICLE_KG)) is CycleState.CAPTURE
    original = cycle.complete_capture(camera_ok=True)
    aborted = cycle.abort(ErrorCode.ERR_BUSY)
    assert aborted is original
    assert cycle.result is original
    assert cycle.result.code is ErrorCode.OK
