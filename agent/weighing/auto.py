"""Автоматическая операция по команде центра (architecture §3.3).

Порядок работы — как в UniServer (решение Игоря 10.08.2026): агент
непрерывно следит за платформой (ScaleWatcher, тикается из main.py) и
держит готовую фиксацию, пока АТС стоит на весах. Команда центра
срабатывает МГНОВЕННО по этой фиксации: вес берётся из неё, обе камеры
снимаются в момент команды, запись уходит в журнал.

Команда ЗАЕЗДА НЕ ЖДЁТ. Если платформа пуста (или заезд не
засвидетельствован — агент обязан увидеть цепочку «пустые весы → заезд →
стабилизация») — немедленный отказ ERR_VEHICLE_TIMEOUT: сначала загоняют
машину, потом шлют команду. Единственное короткое ожидание — стабилизация
веса только что заехавшей машины (фаза STABILIZING, потолок
stable_timeout_s); не дождались — ERR_UNSTABLE. Молчащий индикатор —
немедленный ERR_SCALE_OFFLINE.

Обработчик отдаётся в ``CenterClient(on_weigh_request=runner.handle)``.

Доставка результата двухканальная и самовосстанавливающаяся:
- запись с зафиксированным весом сохраняется локально (synced=0) ДО
  отправки weigh_result — обрыв связи её не теряет, досылка offline_sync
  доведёт до центра (приём идемпотентен по uuid), ack пометит synced,
  после чего PhotoUploader дольёт файлы снимков;
- отказы без веса (пустая платформа, нестабильность, молчащий индикатор,
  ERR_CAMERA: без снимков обеих камер операция не проводится, решение
  09.08.2026) — локально не сохраняются: живой weigh_result донесёт код
  до АИС, а при обрыве связи запрос АИС всё равно уже завершился
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
from uuid import UUID, uuid4

from agent.cameras.capture import CameraConfig
from agent.cameras.stream import CameraStreams, shots_or_capture_all
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage, photo_meta
from agent.weighing.cycle import CycleConfig
from agent.weighing.shots import store_shots
from agent.weighing.watcher import ScaleWatcher, WatcherPhase
from shared.enums import ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import WeighingRecord, WeighRequest, WeighResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AutoConfig:
    """Параметры автоматического режима (конфиг объекта)."""

    cycle: CycleConfig = field(default_factory=CycleConfig)
    tick_interval_s: float = 0.1  # период опроса фиксации при стабилизации


class AutoOperationRunner:
    """Выполняет команды взвешивания/тарирования от центра.

    ``scale_state`` — снимок состояния индикатора (обычно driver.state);
    ``watcher`` — наблюдатель платформы (его tick() крутит main.py);
    ``photos_dir`` — корень локального хранения снимков (ГГГГ/ММ/ДД).
    """

    def __init__(
        self,
        *,
        scale_state: Callable[[], ScaleState],
        watcher: ScaleWatcher,
        storage: AgentStorage,
        cameras: list[CameraConfig],
        photos_dir: str | Path,
        config: AutoConfig | None = None,
        ffmpeg_path: str = "ffmpeg",
        streams: CameraStreams | None = None,
        clock: Callable[[], float] = time.monotonic,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        self._scale_state = scale_state
        self._watcher = watcher
        self._storage = storage
        self._cameras = cameras
        self._photos_dir = Path(photos_dir)
        self._config = config or AutoConfig()
        self._ffmpeg_path = ffmpeg_path
        # буфер потоковых камер: кадр мгновенно, без RTSP-подключения
        self._streams = streams
        self._clock = clock
        # время записи: часы центра (agent/clock.py), по умолчанию локальные
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._lock = asyncio.Lock()

    def busy(self) -> bool:
        """Идёт ли операция прямо сейчас (автообновление ждёт её конца)."""
        return self._lock.locked()

    def set_cycle(self, cycle: CycleConfig) -> None:
        """Новые параметры цикла (настройки из центра)."""
        self._config = AutoConfig(cycle=cycle, tick_interval_s=self._config.tick_interval_s)

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Новый список камер (настройки из центра); текущую операцию
        не трогает — применится со следующей."""
        self._cameras = cameras

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
        record_uuid = uuid4()

        if self._scale_state().status is not ScaleStatus.OK:
            return self._refusal(
                request,
                record_uuid,
                ErrorCode.ERR_SCALE_OFFLINE,
                "нет данных с весового индикатора",
            )

        fixation = self._watcher.fixation
        if fixation is None and self._watcher.phase is WatcherPhase.STABILIZING:
            # машина только что заехала — даём весу устояться, это секунды;
            # timeout_s центра (если прислан и короче) уважаем
            wait_s = self._config.cycle.stable_timeout_s
            if request.timeout_s is not None:
                wait_s = min(wait_s, request.timeout_s)
            deadline = self._clock() + wait_s
            while (
                self._watcher.fixation is None
                and self._watcher.phase is WatcherPhase.STABILIZING
                and self._clock() < deadline
            ):
                await asyncio.sleep(self._config.tick_interval_s)
            fixation = self._watcher.fixation
            if fixation is None and self._watcher.phase is WatcherPhase.STABILIZING:
                return self._refusal(
                    request,
                    record_uuid,
                    ErrorCode.ERR_UNSTABLE,
                    "вес не стабилизировался за отведённое время",
                )

        if fixation is None:
            if self._scale_state().status is not ScaleStatus.OK:
                # индикатор умолк, пока ждали стабилизацию, — честный код:
                # машина может стоять на платформе, дело не в ней
                return self._refusal(
                    request,
                    record_uuid,
                    ErrorCode.ERR_SCALE_OFFLINE,
                    "нет данных с весового индикатора",
                )
            # платформа пуста либо заезд не засвидетельствован — не ждём:
            # сначала загоняют машину, потом шлют команду (порядок UniServer)
            return self._refusal(
                request,
                record_uuid,
                ErrorCode.ERR_VEHICLE_TIMEOUT,
                "на весах нет АТС с зафиксированным весом: "
                "загоните машину на платформу и повторите команду",
            )

        logger.info(
            "операция %s по готовой фиксации: %.0f кг", request.request_id, fixation.weight_kg
        )
        return await self._capture_and_record(request, record_uuid, weight_kg=fixation.weight_kg)

    async def _capture_and_record(
        self,
        request: WeighRequest,
        record_uuid: UUID,
        *,
        weight_kg: float,
    ) -> WeighingRecord:
        """Финал операции: снимки обеих камер → запись журнала."""
        weighed_at = self._now_utc()
        # блокирующий HTTP/ffmpeg — в поток, чтобы heartbeat не замирал
        shots = await asyncio.to_thread(
            shots_or_capture_all, self._cameras, self._streams, ffmpeg_path=self._ffmpeg_path
        )
        camera_errors = [shot.error or "камера недоступна" for shot in shots if not shot.ok]
        if camera_errors:
            # решение Игоря 09.08.2026: без снимков ОБЕИХ камер операция
            # не проводится — вес не возвращается, запись не создаётся
            return self._refusal(
                request,
                record_uuid,
                ErrorCode.ERR_CAMERA,
                "операция не проведена, камера недоступна: " + "; ".join(camera_errors),
            )
        # прожиг оверлея — тоже CPU-работа (PIL, кадры 2560×1440): в поток
        photos, _ = await asyncio.to_thread(
            store_shots,
            self._photos_dir,
            record_uuid,
            weighed_at,
            shots,
            weight_kg=weight_kg,
        )

        vehicle, trailer = self._normalized_numbers(request)

        # правило №4 (ред. 09.08.2026): нетто только из тарирований системы,
        # тара ≤ 3 месяцев и только совпавшей СЦЕПКИ голова+прицеп
        tare = None
        netto = None
        if request.operation is Operation.WEIGHING and vehicle is not None:
            tare = self._storage.find_active_tare(vehicle, weighed_at, trailer)
            if tare is not None:
                netto = weight_kg - tare.tare_value

        record = WeighingRecord(
            uuid=record_uuid,
            operation=request.operation,
            code=ErrorCode.OK,
            massa=weight_kg,
            stable=True,
            weighed_at=weighed_at,
            vehicle_number=vehicle,
            trailer_number=trailer,
            tare_value=tare.tare_value if tare else None,
            tare_weighing_uuid=tare.weighing_uuid if tare else None,
            netto=netto,
            source=WeighingSource.AIS,
            # ФИО оператора весового контроля из запроса АИС (контракт v1) —
            # печатается на весовой карточке
            operator=(request.operator or "").strip() or None,
            message=None,
            photos=[photo_meta(p) for p in photos],
        )
        # локальная запись до отправки результата: обрыв связи не теряет
        # операцию, доставка гарантируется досылкой (см. докстринг модуля)
        self._storage.save_weighing(record, photos)
        return record

    def _refusal(
        self,
        request: WeighRequest,
        record_uuid: UUID,
        code: ErrorCode,
        message: str,
    ) -> WeighingRecord:
        """Отказ без веса: запись не сохраняется, код уходит в АИС."""
        vehicle, trailer = self._normalized_numbers(request)
        return WeighingRecord(
            uuid=record_uuid,
            operation=request.operation,
            code=code,
            vehicle_number=vehicle,
            trailer_number=trailer,
            source=WeighingSource.AIS,
            message=message,
        )

    @staticmethod
    def _normalized_numbers(request: WeighRequest) -> tuple[str | None, str | None]:
        vehicle = (request.vehicle_number or "").strip().upper() or None
        trailer = (request.trailer_number or "").strip().upper() or None
        return vehicle, trailer

    @classmethod
    def _error_result(cls, request: WeighRequest, code: ErrorCode, message: str) -> WeighResult:
        vehicle, trailer = cls._normalized_numbers(request)
        return WeighResult(
            request_id=request.request_id,
            record=WeighingRecord(
                uuid=uuid4(),
                operation=request.operation,
                code=code,
                vehicle_number=vehicle,
                trailer_number=trailer,
                source=WeighingSource.AIS,
                message=message,
            ),
        )
