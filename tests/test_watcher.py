"""Тесты пассивного наблюдателя платформы (agent/weighing/watcher.py).

ScaleWatcher — вечный автомат WAIT_EMPTY → WAIT_VEHICLE → STABILIZING →
READY (схема UniServer, решение Игоря 10.08.2026). Покрытие:

- полная цепочка до READY: фиксация появляется только после свидетельства
  «пустые стабильные весы → заезд → неизменный вес stable_duration_s»;
- fixation недоступна во всех фазах, кроме READY;
- старт при уже стоящей машине НЕ даёт READY: заезд не засвидетельствован,
  автомат ждёт съезда и полного цикла;
- съезд из STABILIZING/READY → WAIT_EMPTY (свидетельство истрачено);
- изменение веса/нестабильность/перегруз в READY → STABILIZING и новая
  фиксация только после повторной выдержки;
- нестабильность и дрожание веса сбрасывают накопление выдержки;
- перегруз (включая реальный cas22: пакет OL БЕЗ веса): АТС на платформе —
  WAIT_VEHICLE → STABILIZING, накопления нет, длинный перегруз НЕ обрыв
  и НЕ роняет в WAIT_EMPTY; в WAIT_EMPTY перегруз не «пустые весы»;
- потеря данных: короткая сбрасывает накопление, фаза сохраняется;
  длинная (> no_data_timeout_s) → WAIT_EMPTY; status OK с weight None
  БЕЗ перегруза — тоже потеря данных (драйвер не разобрал вес);
- отрицательный вес около нуля считается пустыми весами (abs);
- границы порогов zero_threshold_kg (строго <) и vehicle_threshold_kg (>=).

Часы фейковые (clock инъецируется) — реальных ожиданий нет.
"""

from agent.drivers.base import ScaleState
from agent.weighing.cycle import CycleConfig
from agent.weighing.watcher import ScaleWatcher, WatcherPhase
from shared.enums import ScaleStatus

CFG = CycleConfig()  # пороги 50/500 кг, выдержка 2 с, обрыв 5 с

WEIGHT_KG = 43310.0

NO_DATA = ScaleState(status=ScaleStatus.NO_DATA)
# перегруз реального cas22: пакет OL идёт БЕЗ веса (weight_kg=None)
REAL_OVERLOAD = ScaleState(status=ScaleStatus.OK, weight_kg=None, stable=False, overload=True)
# вырожденный случай: поток есть, но вес не разобран и перегруза нет
OK_NO_WEIGHT = ScaleState(status=ScaleStatus.OK, weight_kg=None, stable=False, overload=False)


def ok(weight: float, *, stable: bool = True, overload: bool = False) -> ScaleState:
    """Снимок индикатора с идущим потоком данных (status OK)."""
    return ScaleState(status=ScaleStatus.OK, weight_kg=weight, stable=stable, overload=overload)


class FakeClock:
    """Управляемые монотонные часы."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_watcher() -> tuple[ScaleWatcher, FakeClock]:
    clock = FakeClock()
    return ScaleWatcher(CFG, clock=clock), clock


def drive_to_ready(watcher: ScaleWatcher, clock: FakeClock, weight: float = WEIGHT_KG) -> None:
    """Полный засвидетельствованный проезд: пусто → заезд → выдержка → READY."""
    watcher.tick(ok(0.0))  # пустые стабильные весы → WAIT_VEHICLE
    watcher.tick(ok(weight, stable=False))  # заезд → STABILIZING
    watcher.tick(ok(weight))  # первый кандидат неизменности
    clock.advance(CFG.stable_duration_s)
    watcher.tick(ok(weight))  # выдержка набрана → READY
    assert watcher.phase is WatcherPhase.READY


# --- полная цепочка и доступность фиксации ---


def test_initial_phase_is_wait_empty_without_fixation() -> None:
    """Свежий наблюдатель: WAIT_EMPTY, фиксации нет."""
    watcher, _ = make_watcher()
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None


def test_full_chain_to_ready_fixation_only_in_ready() -> None:
    """Цепочка до READY по шагам; fixation становится не-None только в READY
    и содержит зафиксированный вес и момент фиксации по монотонным часам."""
    watcher, clock = make_watcher()

    assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE
    assert watcher.fixation is None

    assert watcher.tick(ok(WEIGHT_KG, stable=False)) is WatcherPhase.STABILIZING
    assert watcher.fixation is None

    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING  # кандидат
    assert watcher.fixation is None

    clock.advance(CFG.stable_duration_s - 0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING  # выдержка не набрана
    assert watcher.fixation is None

    clock.advance(0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    assert fixation.weight_kg == WEIGHT_KG
    assert fixation.fixed_at_monotonic == clock.now


def test_ready_holds_while_vehicle_stands_still() -> None:
    """АТС стоит, вес не меняется — READY и фиксация держатся сколь угодно."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    fixation = watcher.fixation
    for _ in range(10):
        clock.advance(60.0)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    assert watcher.fixation == fixation  # та же фиксация, не пересоздаётся


# --- заезд без свидетельства пустых весов ---


def test_vehicle_already_on_scale_at_start_never_ready() -> None:
    """Старт при стоящей машине: стабильный вес хоть час — READY не наступает,
    автомат остаётся в WAIT_EMPTY до съезда (заезд не засвидетельствован)."""
    watcher, clock = make_watcher()
    for _ in range(100):
        clock.advance(36.0)  # суммарно час стояния
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY
        assert watcher.fixation is None

    # съезд: платформа опустела — только теперь начинается честный цикл
    watcher.tick(ok(0.0))
    assert watcher.phase is WatcherPhase.WAIT_VEHICLE
    drive_to_ready_after_empty(watcher, clock)


def drive_to_ready_after_empty(watcher: ScaleWatcher, clock: FakeClock) -> None:
    """Дожать цикл до READY, когда WAIT_VEHICLE уже достигнут."""
    watcher.tick(ok(WEIGHT_KG))  # заезд
    watcher.tick(ok(WEIGHT_KG))  # кандидат
    clock.advance(CFG.stable_duration_s)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def test_unstable_or_overloaded_empty_scale_is_not_witnessed() -> None:
    """Пустые, но нестабильные (или перегруженные) весы не считаются
    свидетельством: WAIT_EMPTY не покидается."""
    watcher, _ = make_watcher()
    assert watcher.tick(ok(0.0, stable=False)) is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(0.0, overload=True)) is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE  # стабильный ноль — да


# --- съезд ---


def test_exit_from_ready_drops_to_wait_empty_and_loses_fixation() -> None:
    """Съезд из READY: фаза WAIT_EMPTY, фиксация потеряна; повторный заезд
    без нового свидетельства пустых весов READY не даёт."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)

    assert watcher.tick(ok(120.0)) is WatcherPhase.WAIT_EMPTY  # ниже порога заезда
    assert watcher.fixation is None

    # машина заехала обратно, но пустых весов автомат не видел — WAIT_EMPTY
    clock.advance(1.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY


def test_exit_from_stabilizing_drops_to_wait_empty() -> None:
    """Съезд во время стабилизации тоже тратит свидетельство заезда."""
    watcher, _ = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    assert watcher.phase is WatcherPhase.STABILIZING
    assert watcher.tick(ok(0.0, stable=False)) is WatcherPhase.WAIT_EMPTY


# --- возмущения в READY ---


def test_weight_change_in_ready_requires_new_hold() -> None:
    """Вес в READY изменился (догрузка): STABILIZING без фиксации, новая
    фиксация нового веса — только после полной повторной выдержки."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    new_weight = WEIGHT_KG + 500.0

    assert watcher.tick(ok(new_weight)) is WatcherPhase.STABILIZING
    assert watcher.fixation is None

    watcher.tick(ok(new_weight))  # кандидат нового веса
    clock.advance(CFG.stable_duration_s - 0.1)
    assert watcher.tick(ok(new_weight)) is WatcherPhase.STABILIZING  # выдержка не полна
    clock.advance(0.1)
    assert watcher.tick(ok(new_weight)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    assert fixation.weight_kg == new_weight


def test_instability_in_ready_restabilizes() -> None:
    """Нестабильность в READY (машина качнулась) → STABILIZING, фиксации нет."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    assert watcher.tick(ok(WEIGHT_KG, stable=False)) is WatcherPhase.STABILIZING
    assert watcher.fixation is None


def test_overload_in_ready_restabilizes() -> None:
    """Перегруз в READY → STABILIZING, фиксация недействительна."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    assert watcher.tick(ok(WEIGHT_KG, overload=True)) is WatcherPhase.STABILIZING
    assert watcher.fixation is None


# --- накопление выдержки в STABILIZING ---


def test_instability_resets_accumulated_hold() -> None:
    """Нестабильный тик посреди выдержки обнуляет накопление: после него
    выдержка отсчитывается заново с нуля."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))  # кандидат, начало выдержки
    clock.advance(1.5)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING

    watcher.tick(ok(WEIGHT_KG, stable=False))  # сброс накопления

    watcher.tick(ok(WEIGHT_KG))  # новый отсчёт
    clock.advance(CFG.stable_duration_s - 0.1)
    # старые 1.5 с не в счёт: без полной новой выдержки READY нет
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
    clock.advance(0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def test_weight_jitter_restarts_hold_from_new_value() -> None:
    """Дрожание веса: смена значения перезапускает отсчёт от нового веса."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))
    clock.advance(1.5)
    watcher.tick(ok(WEIGHT_KG + 10.0))  # вес дрогнул — отсчёт заново

    clock.advance(CFG.stable_duration_s - 0.1)
    assert watcher.tick(ok(WEIGHT_KG + 10.0)) is WatcherPhase.STABILIZING
    clock.advance(0.1)
    assert watcher.tick(ok(WEIGHT_KG + 10.0)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    assert fixation.weight_kg == WEIGHT_KG + 10.0


def test_overload_pauses_accumulation() -> None:
    """Перегруз (синтетический, с весом) удерживает STABILIZING и не копит
    выдержку; после снятия перегруза выдержка начинается заново."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    # перегруз означает, что АТС на весах: WAIT_VEHICLE → STABILIZING
    assert watcher.tick(ok(400.0, overload=True)) is WatcherPhase.STABILIZING
    clock.advance(10.0)
    assert watcher.tick(ok(90000.0, overload=True)) is WatcherPhase.STABILIZING
    assert watcher.fixation is None

    watcher.tick(ok(WEIGHT_KG))  # перегруз снят — кандидат
    clock.advance(CFG.stable_duration_s)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


# --- потеря данных ---


def test_short_data_loss_keeps_phase_but_resets_hold() -> None:
    """Короткий обрыв (< no_data_timeout_s) в STABILIZING: фаза сохраняется,
    накопленная выдержка сбрасывается и отсчитывается заново."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))
    clock.advance(1.5)  # накоплено 1.5 с выдержки

    clock.advance(CFG.no_data_timeout_s - 1.0)
    assert watcher.tick(NO_DATA) is WatcherPhase.STABILIZING  # обрыв короткий

    watcher.tick(ok(WEIGHT_KG))  # поток вернулся — новый кандидат
    clock.advance(CFG.stable_duration_s - 0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING  # старое не в счёт
    clock.advance(0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def test_short_data_loss_in_ready_keeps_fixation() -> None:
    """Короткий обрыв в READY терпим: фаза и фиксация сохраняются."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    fixation = watcher.fixation

    clock.advance(CFG.no_data_timeout_s - 1.0)
    assert watcher.tick(NO_DATA) is WatcherPhase.READY
    assert watcher.fixation == fixation


def test_long_data_loss_resets_to_wait_empty_from_ready() -> None:
    """Обрыв дольше no_data_timeout_s: не знаем, что было на платформе, —
    сброс в WAIT_EMPTY, фиксация потеряна, возобновившийся тяжёлый вес
    READY не возвращает (нужен полный цикл)."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)

    watcher.tick(NO_DATA)  # начало обрыва
    clock.advance(CFG.no_data_timeout_s + 0.1)
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None

    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY  # свидетельства нет


def test_long_data_loss_resets_wait_vehicle_witness() -> None:
    """Длинный обрыв в WAIT_VEHICLE тратит свидетельство пустых весов."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    assert watcher.phase is WatcherPhase.WAIT_VEHICLE

    watcher.tick(NO_DATA)
    clock.advance(CFG.no_data_timeout_s + 0.1)
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_EMPTY


def test_weight_none_without_overload_counts_as_data_loss() -> None:
    """status OK, weight_kg is None и БЕЗ признака перегруза — драйвер не
    разобрал вес: это потеря данных. Короткий эпизод сбрасывает выдержку,
    затянувшийся (> no_data_timeout_s) роняет в WAIT_EMPTY. Перегруз OL
    без веса потерей данных НЕ считается — см. тесты ветки перегруза."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))
    clock.advance(1.0)
    assert watcher.tick(OK_NO_WEIGHT) is WatcherPhase.STABILIZING  # короткий эпизод

    clock.advance(CFG.no_data_timeout_s + 0.1)
    # затянулось: не знаем, что на платформе, — свидетельство потеряно
    assert watcher.tick(OK_NO_WEIGHT) is WatcherPhase.WAIT_EMPTY


# --- реальный перегруз cas22 (пакет OL без веса) ---


def test_real_overload_in_wait_vehicle_opens_stabilizing() -> None:
    """Перегруз после засвидетельствованных пустых весов: АТС точно на
    платформе — WAIT_VEHICLE → STABILIZING, фиксации, конечно, нет."""
    watcher, _ = make_watcher()
    watcher.tick(ok(0.0))
    assert watcher.phase is WatcherPhase.WAIT_VEHICLE
    assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.STABILIZING
    assert watcher.fixation is None


def test_long_real_overload_is_not_data_loss() -> None:
    """Затянувшийся перегруз (много дольше no_data_timeout_s) — НЕ обрыв:
    машина стоит на весах, STABILIZING держится, свидетельство заезда не
    теряется; накопленная до перегруза выдержка сбрасывается, и после
    снятия перегруза фиксация достигается полной повторной выдержкой."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))  # кандидат, начало выдержки
    clock.advance(1.5)  # накоплено 1.5 с — сгорит при перегрузе

    for _ in range(5):
        clock.advance(CFG.no_data_timeout_s + 1.0)
        assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.STABILIZING  # не WAIT_EMPTY
        assert watcher.fixation is None

    watcher.tick(ok(WEIGHT_KG))  # перегруз снят — новый кандидат
    clock.advance(CFG.stable_duration_s - 0.1)
    # старые 1.5 с не в счёт: READY только после полной новой выдержки
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
    clock.advance(0.1)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    assert fixation.weight_kg == WEIGHT_KG


def test_real_overload_in_ready_restabilizes() -> None:
    """Перегруз OL без веса в READY (догрузили сверх НПВ) → STABILIZING,
    фиксация недействительна."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.STABILIZING
    assert watcher.fixation is None


def test_real_overload_in_wait_empty_is_not_empty_scale() -> None:
    """Перегруз в WAIT_EMPTY — платформа занята, а не пуста: свидетельства
    пустых весов нет, сколь угодно долгий перегруз фазы не меняет, и
    последующий тяжёлый вес заездом не признаётся."""
    watcher, clock = make_watcher()
    for _ in range(3):
        clock.advance(CFG.no_data_timeout_s + 1.0)
        assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY  # заезд не засвидетельствован
    assert watcher.fixation is None


# --- отрицательные веса и границы порогов ---


def test_negative_weight_near_zero_is_empty() -> None:
    """Отрицательный вес около нуля (дрейф нуля) — пустые весы (abs)."""
    watcher, _ = make_watcher()
    assert watcher.tick(ok(-20.0)) is WatcherPhase.WAIT_VEHICLE


def test_negative_weight_beyond_threshold_is_not_empty() -> None:
    """Большой отрицательный вес пустыми весами не считается."""
    watcher, _ = make_watcher()
    assert watcher.tick(ok(-CFG.zero_threshold_kg - 10.0)) is WatcherPhase.WAIT_EMPTY


def test_threshold_boundaries() -> None:
    """Границы: ровно zero_threshold — НЕ пусто (строго <); вес между
    порогами не заезд; ровно vehicle_threshold — заезд (>=)."""
    watcher, _ = make_watcher()
    assert watcher.tick(ok(CFG.zero_threshold_kg)) is WatcherPhase.WAIT_EMPTY
    watcher.tick(ok(0.0))
    assert watcher.phase is WatcherPhase.WAIT_VEHICLE
    # вес между порогами (человек, мусор) — ещё не заезд и не потеря фазы
    assert watcher.tick(ok(CFG.vehicle_threshold_kg - 0.1)) is WatcherPhase.WAIT_VEHICLE
    assert watcher.tick(ok(CFG.vehicle_threshold_kg)) is WatcherPhase.STABILIZING
