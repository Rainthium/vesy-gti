"""Автоматическая операция по команде центра (architecture §3.3).

Оркестратор связывает кирпичи агента в ответ на ``weigh_request``:
конечный автомат цикла (cycle.py) ← состояние индикатора (драйвер),
на фазе CAPTURE — снимки камер (cameras/capture.py) и их сохранение
(shots.py), затем расчёт тары/нетто по локальной реплике реестра
(правило №4) и запись в журнал.

Обработчик отдаётся в ``CenterClient(on_weigh_request=runner.handle)``.

Доставка результата двухканальная и самовосстанавливающаяся:
- запись с зафиксированным весом сохраняется локально (synced=0) ДО
  отправки weigh_result — обрыв связи её не теряет, досылка offline_sync
  доведёт до центра (приём идемпотентен по uuid), ack пометит synced,
  после чего PhotoUploader дольёт файлы снимков;
- операции без результата — ошибки цикла (ERR_NOT_ZERO и т.п.) и отказ
  из-за камер (ERR_CAMERA: без снимков обеих камер операция не проводится,
  решение 09.08.2026) — локально не сохраняются: живой weigh_result донесёт
  код до АИС, а при обрыве связи запрос АИС всё равно уже завершился
  тайм-аутом центра.

Одновременно выполняется не больше одной операции: параллельная команда
сразу получает ERR_BUSY (весы физически одни).
"""

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from agent.cameras.capture import CameraConfig, capture_all
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage, StoredPhoto, photo_meta
from agent.weighing.cycle import CycleConfig, CycleState, WeighingCycle
from agent.weighing.shots import store_shots
from shared.enums import ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord, WeighRequest, WeighResult

logger = logging.getLogger(__name__)

# честный код при общем тайм-ауте: чего именно не дождались
_PHASE_TIMEOUT_CODES = {
    CycleState.WAIT_ZERO: ErrorCode.ERR_NOT_ZERO,
    CycleState.WAIT_VEHICLE: ErrorCode.ERR_VEHICLE_TIMEOUT,
    CycleState.WAIT_STABLE: ErrorCode.ERR_UNSTABLE,
}


@dataclass(frozen=True)
class AutoConfig:
    """Параметры автоматического режима (конфиг объекта)."""

    cycle: CycleConfig = field(default_factory=CycleConfig)
    tick_interval_s: float = 0.1  # период опроса драйвера (5–10 раз/с)
    # общий потолок операции, если центр не прислал timeout_s;
    # None — ограничивают только фазовые таймауты цикла.
    # Потолок действует до фиксации веса: начатая съёмка камер доводится
    # до конца (вес уже зафиксирован — бросать операцию хуже, чем довести;
    # страховкой остаются таймауты HTTP камер и тайм-аут запроса в центре)
    default_timeout_s: float | None = None


class AutoOperationRunner:
    """Выполняет команды взвешивания/тарирования от центра.

    ``scale_state`` — снимок состояния индикатора (обычно driver.state);
    ``photos_dir`` — корень локального хранения снимков (ГГГГ/ММ/ДД).
    """

    def __init__(
        self,
        *,
        scale_state: Callable[[], ScaleState],
        storage: AgentStorage,
        cameras: list[CameraConfig],
        photos_dir: str | Path,
        config: AutoConfig | None = None,
        ffmpeg_path: str = "ffmpeg",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._scale_state = scale_state
        self._storage = storage
        self._cameras = cameras
        self._photos_dir = Path(photos_dir)
        self._config = config or AutoConfig()
        self._ffmpeg_path = ffmpeg_path
        self._clock = clock
        self._lock = asyncio.Lock()

    async def handle(self, request: WeighRequest) -> WeighResult:
        """Обработчик для CenterClient: команда → результат операции."""
        if self._lock.locked():
            return self._error_result(
                request, ErrorCode.ERR_BUSY, "на весах уже выполняется операция"
            )
        async with self._lock:
            logger.info(
                "команда центра %s: %s %s",
                request.request_id,
                request.operation.value,
                request.vehicle_number or "без номера",
            )
            record = await self._run_operation(request)
        logger.info("операция %s завершена: %s", request.request_id, record.code.value)
        return WeighResult(request_id=request.request_id, record=record)

    # --- внутреннее ---

    async def _run_operation(self, request: WeighRequest) -> WeighingRecord:
        cycle = WeighingCycle(self._config.cycle, clock=self._clock)
        cycle.start()
        timeout_s = (
            request.timeout_s if request.timeout_s is not None else self._config.default_timeout_s
        )
        deadline = self._clock() + timeout_s if timeout_s else None

        while True:
            state = cycle.tick(self._scale_state())
            if state in (CycleState.CAPTURE, CycleState.DONE):
                break
            if deadline is not None and self._clock() > deadline:
                cycle.abort(
                    _PHASE_TIMEOUT_CODES.get(state, ErrorCode.ERR_INTERNAL),
                    message="превышен общий тайм-аут операции",
                )
                break
            await asyncio.sleep(self._config.tick_interval_s)

        weighed_at: datetime | None = None
        record_uuid = uuid4()
        photos: list[StoredPhoto] = []
        if cycle.state is CycleState.CAPTURE:
            # вес зафиксирован — снимаем камеры (блокирующий HTTP/ffmpeg — в поток)
            weighed_at = datetime.now(UTC)
            shots = await asyncio.to_thread(
                capture_all, self._cameras, ffmpeg_path=self._ffmpeg_path
            )
            camera_errors = [shot.error or "камера недоступна" for shot in shots if not shot.ok]
            if camera_errors:
                # решение Игоря 09.08.2026: без снимков ОБЕИХ камер операция
                # не проводится — вес не возвращается, запись не создаётся
                cycle.complete_capture(camera_ok=False, message="; ".join(camera_errors))
                return WeighingRecord(
                    uuid=record_uuid,
                    operation=request.operation,
                    code=ErrorCode.ERR_CAMERA,
                    source=WeighingSource.AIS,
                    message="операция не проведена, камера недоступна: " + "; ".join(camera_errors),
                )
            # прожиг оверлея — тоже CPU-работа (PIL, кадры 2560×1440):
            # в поток, чтобы heartbeat и живой вес не замирали
            photos, _ = await asyncio.to_thread(
                store_shots,
                self._photos_dir,
                record_uuid,
                weighed_at,
                shots,
                weight_kg=cycle.captured_weight_kg,
            )
            result = cycle.complete_capture(camera_ok=True)
        else:
            done_result = cycle.result
            assert done_result is not None  # state == DONE
            result = done_result

        vehicle = (request.vehicle_number or "").strip().upper() or None
        trailer = (request.trailer_number or "").strip().upper() or None

        # правило №4 (ред. 09.08.2026): нетто только из тарирований системы,
        # тара ≤ 3 месяцев и только совпавшей СЦЕПКИ голова+прицеп
        tare = None
        netto = None
        if (
            result.weight_kg is not None
            and request.operation is Operation.WEIGHING
            and vehicle is not None
            and weighed_at is not None
        ):
            tare = self._storage.find_active_tare(vehicle, weighed_at, trailer)
            if tare is not None:
                netto = result.weight_kg - tare.tare_value

        record = WeighingRecord(
            uuid=record_uuid,
            operation=request.operation,
            code=result.code,
            massa=result.weight_kg,
            stable=result.weight_kg is not None,
            weighed_at=weighed_at,
            vehicle_number=vehicle,
            trailer_number=trailer,
            tare_value=tare.tare_value if tare else None,
            tare_weighing_uuid=tare.weighing_uuid if tare else None,
            netto=netto,
            source=WeighingSource.AIS,
            message=result.message,
            photos=[photo_meta(p) for p in photos],
        )
        if result.weight_kg is not None:
            # локальная запись до отправки результата: обрыв связи не теряет
            # операцию, доставка гарантируется досылкой (см. докстринг модуля)
            self._storage.save_weighing(record, photos)
        return record

    @staticmethod
    def _error_result(request: WeighRequest, code: ErrorCode, message: str) -> WeighResult:
        return WeighResult(
            request_id=request.request_id,
            record=WeighingRecord(
                uuid=uuid4(),
                operation=request.operation,
                code=code,
                source=WeighingSource.AIS,
                message=message,
            ),
        )
