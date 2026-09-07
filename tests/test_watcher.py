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
- границы порогов zero_threshold_kg (строго <) и vehicle_threshold_kg (>=);
- reconfigure (07.09.2026): при тех же параметрах наблюдения фаза, фиксация
  и накопленная выдержка сохраняются (панель шлёт цикл при каждом
  «Сохранить»); смена любого из OBSERVATION_FIELDS → WAIT_EMPTY без фиксации,
  стоящая машина требует пересъезда;
- краевые случаи reconfigure: каждая фаза × те же параметры (цепочка
  продолжается с того же места) и каждая фаза × смена ровно одного параметра
  в обе стороны (после сброса накопление с нуля по новой выдержке);
  сокращение выдержки не засчитывает старое накопление; «туда-обратно» не
  воскрешает фиксацию; равенство числовое, а не по типу; reconfigure посреди
  обрыва: те же параметры — фаза и момент начала обрыва сохраняются (короткий
  обрыв терпим, длинный истекает по исходному моменту, граница строго >),
  смена параметра — чистый рестарт с новым допуском; перегруз и reconfigure;
  серия одинаковых reconfigure — ничего не меняет.

Часы фейковые (clock инъецируется) — реальных ожиданий нет.
"""

from dataclasses import replace

import pytest

from agent.drivers.base import ScaleState
from agent.weighing.cycle import CycleConfig
from agent.weighing.watcher import OBSERVATION_FIELDS, ScaleWatcher, WatcherPhase
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


# --- reconfigure: сброс только при смене параметров наблюдения (07.09.2026) ---


def test_reconfigure_same_observation_params_keeps_ready() -> None:
    """Панель центра шлёт цикл при каждом «Сохранить»: если пороги наблюдения
    те же (менялись лимит тары, таймауты операции), READY и фиксация живут —
    стоящей машине пересъезд не нужен (Кара-Суу 07.09.2026)."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    fixation = watcher.fixation
    watcher.reconfigure(
        replace(CFG, max_tare_kg=30_000.0, vehicle_timeout_s=90.0, stable_timeout_s=45.0)
    )
    assert watcher.phase is WatcherPhase.READY
    assert watcher.fixation == fixation
    clock.advance(1.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    assert watcher.fixation == fixation


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("zero_threshold_kg", 200.0),
        ("vehicle_threshold_kg", 800.0),
        ("stable_duration_s", 5.0),
        ("no_data_timeout_s", 10.0),
    ],
)
def test_reconfigure_changed_observation_param_restarts_from_wait_empty(
    field: str, value: float
) -> None:
    """Смена порога/выдержки/допуска обрыва: старая фиксация могла быть снята по
    прежним правилам — наблюдение заново, стоящая машина без пересъезда READY
    не получает (та же семантика, что после рестарта агента)."""
    assert field in OBSERVATION_FIELDS
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    watcher.reconfigure(replace(CFG, **{field: value}))
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None
    clock.advance(600.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None


def test_reconfigure_same_params_keeps_stabilizing_hold() -> None:
    """В STABILIZING накопленная выдержка при тех же параметрах не теряется:
    фиксация наступает в тот же момент, что и без reconfigure."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG, stable=False))
    watcher.tick(ok(WEIGHT_KG))  # кандидат неизменности
    clock.advance(CFG.stable_duration_s - 0.5)
    watcher.reconfigure(replace(CFG, max_tare_kg=1.0))
    assert watcher.phase is WatcherPhase.STABILIZING
    clock.advance(0.5)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def test_reconfigure_changed_vehicle_threshold_applies_new_value() -> None:
    """Сменившийся порог заезда действует сразу после сброса: прежний вес
    машины ниже нового порога — заездом не считается."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    watcher.reconfigure(replace(CFG, vehicle_threshold_kg=WEIGHT_KG + 1.0))
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    watcher.tick(ok(0.0))
    # прежний вес теперь ниже порога заезда — заезда нет
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_VEHICLE


# --- reconfigure: краевые случаи по фазам, обрыв данных, повторные вызовы ---

# конфиг с теми же параметрами наблюдения, но другими параметрами операции:
# так выглядит «Сохранить» в панели, когда правили лимит тары или таймауты
SAME_OBSERVATION = replace(
    CFG, max_tare_kg=30_000.0, zero_timeout_s=3.0, vehicle_timeout_s=90.0, stable_timeout_s=45.0
)

ALL_PHASES = (
    WatcherPhase.WAIT_EMPTY,
    WatcherPhase.WAIT_VEHICLE,
    WatcherPhase.STABILIZING,
    WatcherPhase.READY,
)
WITNESSED_PHASES = ALL_PHASES[1:]  # фазы, в которых есть что терять

# смена ровно одного параметра наблюдения — в обе стороны (ужесточение и ослабление)
OBSERVATION_CHANGES = [
    ("zero_threshold_kg", 200.0),
    ("zero_threshold_kg", 10.0),
    ("vehicle_threshold_kg", 800.0),
    ("vehicle_threshold_kg", 300.0),
    ("stable_duration_s", 5.0),
    ("stable_duration_s", 1.0),
    ("no_data_timeout_s", 10.0),
    ("no_data_timeout_s", 1.0),
]

STABILIZING_HOLD_S = 1.5  # накопленная (неполная) выдержка в STABILIZING у drive_to
# шаги часов ниже — двоично точные доли секунды (0.25, 0.125, 1.875): сумма шагов
# должна попадать РОВНО в границу выдержки, а 0.1 накапливает ошибку float


def drive_to(watcher: ScaleWatcher, clock: FakeClock, phase: WatcherPhase) -> None:
    """Довести наблюдение до фазы; в STABILIZING — с накопленной, но не полной
    выдержкой (STABILIZING_HOLD_S из CFG.stable_duration_s)."""
    if phase is WatcherPhase.WAIT_EMPTY:
        assert watcher.phase is WatcherPhase.WAIT_EMPTY
        return
    assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE
    if phase is WatcherPhase.WAIT_VEHICLE:
        return
    assert watcher.tick(ok(WEIGHT_KG, stable=False)) is WatcherPhase.STABILIZING
    watcher.tick(ok(WEIGHT_KG))  # кандидат неизменности, начало выдержки
    if phase is WatcherPhase.STABILIZING:
        clock.advance(STABILIZING_HOLD_S)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
        return
    clock.advance(CFG.stable_duration_s)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def drive_to_ready_under(watcher: ScaleWatcher, clock: FakeClock, config: CycleConfig) -> None:
    """Честный проезд по правилам config из WAIT_EMPTY: READY не раньше
    полной выдержки config.stable_duration_s."""
    assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE
    assert watcher.tick(ok(WEIGHT_KG, stable=False)) is WatcherPhase.STABILIZING
    watcher.tick(ok(WEIGHT_KG))  # кандидат
    clock.advance(config.stable_duration_s - 0.25)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING  # выдержка не полна
    clock.advance(0.25)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    assert fixation.weight_kg == WEIGHT_KG


@pytest.mark.parametrize("phase", ALL_PHASES)
def test_reconfigure_same_observation_keeps_every_phase(phase: WatcherPhase) -> None:
    """Те же параметры наблюдения — в КАЖДОЙ фазе: фаза и фиксация сохраняются,
    а следующий tick продолжает цепочку с того же места, как без reconfigure
    (в STABILIZING — READY ровно по остатку выдержки)."""
    watcher, clock = make_watcher()
    drive_to(watcher, clock, phase)
    fixation_before = watcher.fixation

    watcher.reconfigure(SAME_OBSERVATION)
    assert watcher.phase is phase
    assert watcher.fixation == fixation_before

    if phase is WatcherPhase.WAIT_EMPTY:
        assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE
    elif phase is WatcherPhase.WAIT_VEHICLE:
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
    elif phase is WatcherPhase.STABILIZING:
        clock.advance(CFG.stable_duration_s - STABILIZING_HOLD_S - 0.25)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING  # остаток не набран
        clock.advance(0.25)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY  # накопленное зачтено
    else:
        clock.advance(600.0)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
        assert watcher.fixation == fixation_before


@pytest.mark.parametrize("phase", ALL_PHASES)
@pytest.mark.parametrize(("field", "value"), OBSERVATION_CHANGES)
def test_reconfigure_single_observation_change_resets_from_every_phase(
    phase: WatcherPhase, field: str, value: float
) -> None:
    """Смена ровно одного параметра наблюдения из КАЖДОЙ фазы → WAIT_EMPTY без
    фиксации; стоящая машина READY не получает; после сброса накопление идёт
    с нуля по новому конфигу — фиксация не раньше полной новой выдержки."""
    assert field in OBSERVATION_FIELDS
    watcher, clock = make_watcher()
    drive_to(watcher, clock, phase)
    new_config = replace(CFG, **{field: value})

    watcher.reconfigure(new_config)
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None

    # машина стоит на весах сколь угодно долго — заезд не засвидетельствован
    clock.advance(new_config.stable_duration_s + 60.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None

    drive_to_ready_under(watcher, clock, new_config)


def test_reconfigure_shorter_hold_does_not_credit_old_accumulation() -> None:
    """В STABILIZING накоплено 1.875 с; выдержку сократили до 1 с. Если бы
    накопление пережило reconfigure, следующий же tick дал бы READY (1.875 ≥ 1) —
    но смена параметра наблюдения сбрасывает всё: WAIT_EMPTY, пересъезд, и
    фиксация только после полной новой выдержки."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))  # кандидат
    clock.advance(1.875)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING

    shorter = replace(CFG, stable_duration_s=1.0)
    watcher.reconfigure(shorter)
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY  # не READY «по старым 1.875 с»
    clock.advance(10.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None

    drive_to_ready_under(watcher, clock, shorter)


def test_reconfigure_shorter_hold_in_ready_drops_fixation() -> None:
    """READY при выдержке 2 с; выдержку сократили до 1 с — фиксация всё равно
    аннулируется (правило одно для любой смены параметра наблюдения)."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    watcher.reconfigure(replace(CFG, stable_duration_s=1.0))
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None


def test_reconfigure_back_and_forth_does_not_restore_fixation() -> None:
    """Сменили порог и тут же вернули прежний: второй reconfigure — тоже смена
    (относительно уже применённого конфига), фиксация не воскресает."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    watcher.reconfigure(replace(CFG, vehicle_threshold_kg=800.0))
    watcher.reconfigure(CFG)
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY


def test_reconfigure_lowered_zero_threshold_applies_to_new_witness() -> None:
    """После сброса свидетельство пустых весов ищется по НОВОМУ порогу:
    показание, бывшее «пусто» по старому порогу, по новому — не пусто."""
    watcher, _ = make_watcher()
    assert watcher.tick(ok(30.0)) is WatcherPhase.WAIT_VEHICLE  # 30 < 50 — пусто
    watcher.reconfigure(replace(CFG, zero_threshold_kg=20.0))
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(30.0)) is WatcherPhase.WAIT_EMPTY  # 30 ≥ 20 — уже не пусто
    assert watcher.tick(ok(10.0)) is WatcherPhase.WAIT_VEHICLE


def test_reconfigure_numeric_equality_ignores_type() -> None:
    """Те же значения другого числового типа (5 вместо 5.0) — не смена
    параметра: сравниваются значения, а не представление."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    fixation = watcher.fixation
    watcher.reconfigure(
        replace(
            CFG,
            zero_threshold_kg=50,
            vehicle_threshold_kg=500,
            stable_duration_s=2,
            no_data_timeout_s=5,
        )
    )
    assert watcher.phase is WatcherPhase.READY
    assert watcher.fixation == fixation


@pytest.mark.parametrize("phase", WITNESSED_PHASES)
def test_reconfigure_same_params_during_short_data_loss_keeps_phase(phase: WatcherPhase) -> None:
    """reconfigure с теми же параметрами посреди КОРОТКОГО обрыва: фаза и
    фиксация переживают и обрыв, и reconfigure; поток вернулся — цепочка
    продолжается."""
    watcher, clock = make_watcher()
    drive_to(watcher, clock, phase)
    fixation = watcher.fixation

    assert watcher.tick(NO_DATA) is phase  # начало обрыва
    clock.advance(2.0)
    watcher.reconfigure(SAME_OBSERVATION)
    assert watcher.phase is phase
    assert watcher.fixation == fixation

    clock.advance(CFG.no_data_timeout_s - 2.0 - 0.1)  # всего 4.9 с — допуск не исчерпан
    assert watcher.tick(NO_DATA) is phase
    assert watcher.fixation == fixation

    resumed = ok(0.0) if phase is WatcherPhase.WAIT_VEHICLE else ok(WEIGHT_KG)
    assert watcher.tick(resumed) is phase
    assert watcher.fixation == fixation


@pytest.mark.parametrize("phase", WITNESSED_PHASES)
def test_reconfigure_same_params_during_data_loss_keeps_loss_clock(phase: WatcherPhase) -> None:
    """При неизменных параметрах момент начала обрыва (no_data_since) НЕ
    сбрасывается: обрыв, начавшийся до reconfigure, истекает по исходному
    моменту — «Сохранить» в панели не должно маскировать длинный обрыв.
    Граница допуска — строго больше no_data_timeout_s."""
    watcher, clock = make_watcher()
    drive_to(watcher, clock, phase)

    watcher.tick(NO_DATA)  # t0 — начало обрыва
    clock.advance(CFG.no_data_timeout_s - 0.5)
    watcher.reconfigure(SAME_OBSERVATION)  # после reconfigure пройдёт лишь 0.5–0.6 с
    assert watcher.phase is phase

    clock.advance(0.5)  # ровно no_data_timeout_s — ещё терпимо
    assert watcher.tick(NO_DATA) is phase
    clock.advance(0.1)  # 5.1 с > допуска — свидетельство потеряно
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY


def test_reconfigure_changed_param_during_data_loss_restarts_cleanly() -> None:
    """Смена параметра наблюдения посреди обрыва: сразу WAIT_EMPTY; дальнейший
    обрыв любой длины ничего не ломает; после возврата потока — обычный
    новый цикл, и новый допуск обрыва действует."""
    watcher, clock = make_watcher()
    drive_to_ready(watcher, clock)
    watcher.tick(NO_DATA)
    clock.advance(1.0)

    watcher.reconfigure(replace(CFG, no_data_timeout_s=10.0))
    assert watcher.phase is WatcherPhase.WAIT_EMPTY
    assert watcher.fixation is None

    clock.advance(20.0)
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_EMPTY
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.WAIT_EMPTY  # заезд не засвидетельствован
    assert watcher.tick(ok(0.0)) is WatcherPhase.WAIT_VEHICLE  # пустые весы — новый цикл

    # новый допуск обрыва (10 с): 7 с обрыва (> старых 5) свидетельство не тратят
    watcher.tick(NO_DATA)
    clock.advance(7.0)
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_VEHICLE
    clock.advance(3.1)
    assert watcher.tick(NO_DATA) is WatcherPhase.WAIT_EMPTY  # 10.1 с > 10


def test_reconfigure_same_params_during_overload_keeps_stabilizing() -> None:
    """reconfigure с теми же параметрами во время перегруза OL без веса:
    STABILIZING сохраняется, перегруз и после reconfigure не считается
    обрывом, фиксация — после снятия перегруза и полной выдержки."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.STABILIZING
    watcher.reconfigure(SAME_OBSERVATION)
    assert watcher.phase is WatcherPhase.STABILIZING
    clock.advance(CFG.no_data_timeout_s + 1.0)
    assert watcher.tick(REAL_OVERLOAD) is WatcherPhase.STABILIZING
    watcher.tick(ok(WEIGHT_KG))  # перегруз снят — кандидат
    clock.advance(CFG.stable_duration_s)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY


def test_repeated_reconfigure_same_params_is_noop() -> None:
    """Серия reconfigure подряд с одинаковыми (по наблюдению) конфигами — в том
    числе новыми экземплярами с теми же значениями — ничего не меняет: выдержка
    копится сквозь них, READY наступает в тот же момент, потом держится."""
    watcher, clock = make_watcher()
    watcher.tick(ok(0.0))
    watcher.tick(ok(WEIGHT_KG))
    watcher.tick(ok(WEIGHT_KG))  # кандидат, t = 0 выдержки
    clock.advance(1.0)
    equivalents = (
        CFG,
        CycleConfig(),
        SAME_OBSERVATION,
        replace(SAME_OBSERVATION, max_tare_kg=0.0),
        CFG,
    )
    for config in equivalents:
        watcher.reconfigure(config)
        clock.advance(0.125)
        assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
    # накоплено 1.625 с; ровно через остаток выдержки — READY (не раньше)
    clock.advance(CFG.stable_duration_s - 1.625 - 0.125)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.STABILIZING
    clock.advance(0.125)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    fixation = watcher.fixation
    assert fixation is not None
    for config in equivalents:
        watcher.reconfigure(config)
        assert watcher.phase is WatcherPhase.READY
        assert watcher.fixation == fixation
    clock.advance(60.0)
    assert watcher.tick(ok(WEIGHT_KG)) is WatcherPhase.READY
    assert watcher.fixation == fixation
