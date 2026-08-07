"""Конечный автомат цикла взвешивания (architecture §3.3).

IDLE → WAIT_ZERO → WAIT_VEHICLE → WAIT_STABLE → CAPTURE → DONE

Автомат не знает ни о протоколе индикатора (получает готовый ``ScaleState``),
ни о камерах и сети: он доводит цикл до фиксации веса (CAPTURE) и выдаёт
результат. Снимки камер и отправка результата — забота вызывающего кода:
увидев CAPTURE, он делает снимки и завершает цикл ``complete_capture()``.

Все пороги и таймауты — из конфига объекта (``CycleConfig``), ничего
не зашито. Ошибки — коды ERR_* из shared.enums (контракт АИС §4.1).

Управление временем — через инъецируемые часы ``clock`` (по умолчанию
``time.monotonic``), поэтому автомат полностью тестируем без ожиданий.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from agent.drivers.base import ScaleState
from shared.enums import ErrorCode, ScaleStatus


class CycleState(StrEnum):
    """Состояния автомата."""

    IDLE = "idle"
    WAIT_ZERO = "wait_zero"  # ждём пустые стабильные весы
    WAIT_VEHICLE = "wait_vehicle"  # ждём заезда (вес выше порога)
    WAIT_STABLE = "wait_stable"  # ждём стабилизации веса
    CAPTURE = "capture"  # вес зафиксирован, идёт съёмка камер
    DONE = "done"  # завершено (см. result.code)


@dataclass(frozen=True)
class CycleConfig:
    """Параметры цикла для конкретного объекта (конфиг агента).

    Значения по умолчанию — стартовые для Кызыл-Кыи (дискретность 10 кг),
    подбираются на пилоте.
    """

    zero_threshold_kg: float = 50.0  # ниже — весы считаются пустыми
    vehicle_threshold_kg: float = 500.0  # выше — на весах есть АТС
    zero_timeout_s: float = 10.0  # не опустели за это время → ERR_NOT_ZERO
    vehicle_timeout_s: float = 60.0  # АТС не заехало → ERR_VEHICLE_TIMEOUT
    stable_duration_s: float = 2.0  # вес неизменен столько секунд → фиксация
    stable_timeout_s: float = 30.0  # не стабилизировался → ERR_UNSTABLE
    no_data_timeout_s: float = 5.0  # нет данных с индикатора → ERR_SCALE_OFFLINE


@dataclass(frozen=True)
class CycleResult:
    """Итог цикла: код по контракту АИС и зафиксированный вес (если есть)."""

    code: ErrorCode
    weight_kg: float | None = None
    message: str | None = None


class WeighingCycle:
    """Один цикл взвешивания. Экземпляр одноразовый: start() → tick()… → result.

    Вызывающий код опрашивает драйвер и передаёт свежий ``ScaleState``
    в ``tick()`` с любой периодичностью (рекомендуется 5–10 раз в секунду).
    """

    def __init__(
        self,
        config: CycleConfig,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._clock = clock
        self._state = CycleState.IDLE
        self._result: CycleResult | None = None
        self._phase_started_at = 0.0  # момент входа в текущее состояние
        self._no_data_since: float | None = None
        # отслеживание неизменности веса в WAIT_STABLE
        self._candidate_weight: float | None = None
        self._candidate_since = 0.0

    # --- публичный интерфейс ---

    @property
    def state(self) -> CycleState:
        return self._state

    @property
    def result(self) -> CycleResult | None:
        """Итог цикла; заполнен, когда state == DONE."""
        return self._result

    def start(self) -> None:
        """Начать цикл (из IDLE)."""
        if self._state is not CycleState.IDLE:
            raise RuntimeError(f"цикл уже запущен (состояние {self._state})")
        self._enter(CycleState.WAIT_ZERO)

    def tick(self, scale: ScaleState) -> CycleState:
        """Продвинуть автомат по свежему состоянию индикатора."""
        if self._state in (CycleState.IDLE, CycleState.CAPTURE, CycleState.DONE):
            return self._state  # в этих состояниях время не движет автомат

        now = self._clock()

        if self._check_no_data(scale, now):
            return self._state

        # активные фазы: данные есть
        if self._state is CycleState.WAIT_ZERO:
            self._tick_wait_zero(scale, now)
        elif self._state is CycleState.WAIT_VEHICLE:
            self._tick_wait_vehicle(scale, now)
        elif self._state is CycleState.WAIT_STABLE:
            self._tick_wait_stable(scale, now)
        return self._state

    def complete_capture(self, *, camera_ok: bool, message: str | None = None) -> CycleResult:
        """Завершить цикл после съёмки камер (вызывается из CAPTURE).

        ``camera_ok=False`` → код ERR_CAMERA, но вес всё равно возвращается
        (правило §4.1: вес зафиксирован, ошибка камеры — предупреждение).
        """
        if self._state is not CycleState.CAPTURE:
            raise RuntimeError(f"complete_capture вне CAPTURE (состояние {self._state})")
        assert self._candidate_weight is not None
        code = ErrorCode.OK if camera_ok else ErrorCode.ERR_CAMERA
        return self._finish(code, weight_kg=self._candidate_weight, message=message)

    def abort(self, code: ErrorCode, message: str | None = None) -> CycleResult:
        """Прервать цикл извне (например, ERR_BUSY или остановка агента).

        В состоянии DONE ничего не меняет: готовый результат неизменен,
        возвращается он же.
        """
        if self._state is CycleState.DONE and self._result is not None:
            return self._result
        return self._finish(code, message=message)

    # --- внутреннее ---

    def _enter(self, state: CycleState) -> None:
        self._state = state
        self._phase_started_at = self._clock()
        self._candidate_weight = None

    def _finish(
        self, code: ErrorCode, *, weight_kg: float | None = None, message: str | None = None
    ) -> CycleResult:
        self._result = CycleResult(code=code, weight_kg=weight_kg, message=message)
        self._state = CycleState.DONE
        return self._result

    def _check_no_data(self, scale: ScaleState, now: float) -> bool:
        """Слежение за потоком данных; True — цикл завершён ошибкой."""
        if scale.status is ScaleStatus.OK:
            self._no_data_since = None
            return False
        if self._no_data_since is None:
            self._no_data_since = now
            # вес мог измениться, пока данных не было, — отсчёт неизменности
            # начинается заново после восстановления потока
            self._candidate_weight = None
        if now - self._no_data_since > self._config.no_data_timeout_s:
            self._finish(
                ErrorCode.ERR_SCALE_OFFLINE,
                message=f"нет данных с индикатора ({scale.status}): {scale.error or ''}".strip(),
            )
            return True
        return True  # данных нет, но таймаут ещё не вышел — ждём, не продвигаясь

    def _tick_wait_zero(self, scale: ScaleState, now: float) -> None:
        weight = scale.weight_kg
        empty_and_stable = (
            weight is not None
            and not scale.overload
            and scale.stable
            and abs(weight) < self._config.zero_threshold_kg
        )
        if empty_and_stable:
            self._enter(CycleState.WAIT_VEHICLE)
            return
        if now - self._phase_started_at > self._config.zero_timeout_s:
            self._finish(
                ErrorCode.ERR_NOT_ZERO,
                message="весы не пусты перед началом операции",
            )

    def _tick_wait_vehicle(self, scale: ScaleState, now: float) -> None:
        weight = scale.weight_kg
        # перегруз означает, что АТС точно на весах — переходим к стабилизации
        # (затянувшийся перегруз завершится там честным ERR_UNSTABLE)
        if scale.overload or (weight is not None and weight >= self._config.vehicle_threshold_kg):
            self._enter(CycleState.WAIT_STABLE)
            return
        if now - self._phase_started_at > self._config.vehicle_timeout_s:
            self._finish(
                ErrorCode.ERR_VEHICLE_TIMEOUT,
                message="АТС не заехало на весы за отведённое время",
            )

    def _tick_wait_stable(self, scale: ScaleState, now: float) -> None:
        weight = scale.weight_kg

        # съезд/пропажа веса ниже порога заезда возвращает к ожиданию заезда
        if weight is None or weight < self._config.vehicle_threshold_kg:
            if scale.overload:
                # перегруз: вес не использовать, ждём стабилизации дальше
                self._candidate_weight = None
            else:
                self._enter(CycleState.WAIT_VEHICLE)
                return
        elif scale.stable and not scale.overload and weight == self._candidate_weight:
            # вес стабилен и не меняется — копим время неизменности
            if now - self._candidate_since >= self._config.stable_duration_s:
                self._state = CycleState.CAPTURE
                return
        elif scale.stable and not scale.overload:
            self._candidate_weight = weight
            self._candidate_since = now
        else:
            self._candidate_weight = None

        if now - self._phase_started_at > self._config.stable_timeout_s:
            self._finish(
                ErrorCode.ERR_UNSTABLE,
                message="вес не стабилизировался за отведённое время",
            )
