"""Непрерывное наблюдение за весами: фиксация готова ДО команды.

Схема UniServer (решение Игоря 10.08.2026): программа сама следит
за весами всё время — видит пустую платформу, заезд, стабилизацию — и
держит готовую фиксацию, пока АТС стоит. Команда взвешивания срабатывает
мгновенно по этой фиксации; заезда команда НЕ ждёт — если платформа
пуста, auto.py немедленно отказывает (сначала загоняют машину).

Доказательность заезда сохраняется: READY достижимо только по цепочке
«пустые стабильные весы → вес выше порога заезда → неизменный вес»,
то есть агент засвидетельствовал полный заезд — просто заранее.

Автомат пассивный и вечный (в отличие от одноразового WeighingCycle):
ошибок и таймаутов фаз у него нет, он просто отражает происходящее
на платформе. Пороги — те же, из CycleConfig объекта. Часы инъецируются
(тестируемость без ожиданий), время — монотонное.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from agent.drivers.base import ScaleState
from agent.weighing.cycle import CycleConfig
from shared.enums import ScaleStatus


class WatcherPhase(StrEnum):
    """Фазы наблюдения."""

    WAIT_EMPTY = "wait_empty"  # заезд не засвидетельствован, ждём пустые весы
    WAIT_VEHICLE = "wait_vehicle"  # весы видели пустыми, ждём заезда
    STABILIZING = "stabilizing"  # АТС на весах, ждём неизменного веса
    READY = "ready"  # фиксация готова, АТС стоит на весах


@dataclass(frozen=True)
class Fixation:
    """Готовая фиксация: вес, выдержавший stable_duration_s без изменений."""

    weight_kg: float
    fixed_at_monotonic: float


class ScaleWatcher:
    """Вечный автомат наблюдения; tick() дёргается опросом драйвера (5–10 раз/с)."""

    def __init__(
        self,
        config: CycleConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._clock = clock
        self._phase = WatcherPhase.WAIT_EMPTY
        self._fixation: Fixation | None = None
        self._no_data_since: float | None = None
        # отслеживание неизменности веса в STABILIZING (как в WeighingCycle)
        self._candidate_weight: float | None = None
        self._candidate_since = 0.0

    @property
    def phase(self) -> WatcherPhase:
        return self._phase

    def reconfigure(self, config: CycleConfig) -> None:
        """Применить новые пороги/таймауты (настройки из центра).

        Наблюдение начинается заново с WAIT_EMPTY: старая фиксация могла
        быть снята по прежним порогам — стоящая машина потребует пересъезда
        (та же семантика, что после рестарта агента)."""
        self._config = config
        self._phase = WatcherPhase.WAIT_EMPTY
        self._fixation = None
        self._no_data_since = None
        self._candidate_weight = None
        self._candidate_since = 0.0

    @property
    def fixation(self) -> Fixation | None:
        """Готовая фиксация, пока АТС стоит на весах; иначе None."""
        return self._fixation if self._phase is WatcherPhase.READY else None

    def tick(self, scale: ScaleState) -> WatcherPhase:
        """Продвинуть наблюдение по свежему состоянию индикатора."""
        now = self._clock()

        if scale.status is not ScaleStatus.OK or (scale.weight_kg is None and not scale.overload):
            # поток пропал: короткий обрыв лишь сбрасывает накопленную
            # стабильность, длинный — и свидетельство заезда (не знаем,
            # что происходило на платформе, пока данных не было)
            if self._no_data_since is None:
                self._no_data_since = now
            self._candidate_weight = None
            if (
                now - self._no_data_since > self._config.no_data_timeout_s
                and self._phase is not WatcherPhase.WAIT_EMPTY
            ):
                self._enter(WatcherPhase.WAIT_EMPTY)
            return self._phase
        self._no_data_since = None

        if scale.overload:
            # перегруз (реальный cas22 шлёт OL БЕЗ веса): АТС точно на
            # платформе — присутствие видим, но фиксация невозможна
            if self._phase is WatcherPhase.WAIT_VEHICLE:
                self._enter(WatcherPhase.STABILIZING)
            elif self._phase is WatcherPhase.STABILIZING:
                self._candidate_weight = None
            elif self._phase is WatcherPhase.READY:
                self._enter(WatcherPhase.STABILIZING)
            return self._phase

        weight = scale.weight_kg
        assert weight is not None  # None без overload отсеян выше как обрыв
        empty_and_stable = scale.stable and abs(weight) < self._config.zero_threshold_kg
        on_scale = weight >= self._config.vehicle_threshold_kg

        if self._phase is WatcherPhase.WAIT_EMPTY:
            if empty_and_stable:
                self._enter(WatcherPhase.WAIT_VEHICLE)
        elif self._phase is WatcherPhase.WAIT_VEHICLE:
            if on_scale:
                self._enter(WatcherPhase.STABILIZING)
        elif self._phase is WatcherPhase.STABILIZING:
            self._tick_stabilizing(scale, weight, on_scale, now)
        elif self._phase is WatcherPhase.READY:
            self._tick_ready(scale, weight, on_scale, now)
        return self._phase

    # --- внутреннее ---

    def _enter(self, phase: WatcherPhase) -> None:
        self._phase = phase
        self._fixation = None
        self._candidate_weight = None

    def _tick_stabilizing(
        self, scale: ScaleState, weight: float, on_scale: bool, now: float
    ) -> None:
        if not on_scale:
            # съезд: свидетельство заезда истрачено, ждём пустые весы заново
            self._enter(WatcherPhase.WAIT_EMPTY)
            return
        if not scale.stable:
            self._candidate_weight = None
            return
        if weight == self._candidate_weight:
            if now - self._candidate_since >= self._config.stable_duration_s:
                self._fixation = Fixation(weight_kg=weight, fixed_at_monotonic=now)
                self._phase = WatcherPhase.READY
        else:
            self._candidate_weight = weight
            self._candidate_since = now

    def _tick_ready(self, scale: ScaleState, weight: float, on_scale: bool, now: float) -> None:
        if not on_scale:
            self._enter(WatcherPhase.WAIT_EMPTY)
            return
        assert self._fixation is not None
        if not scale.stable or weight != self._fixation.weight_kg:
            # машина двинулась/доехала — фиксация недействительна,
            # новая появится после повторной стабилизации
            self._enter(WatcherPhase.STABILIZING)
