"""Применение настроек весов, присланных центром (scale_config).

Решение Игоря 10.08.2026: объекты настраиваются из панели центра без
AnyDesk — параметры цикла, камеры (URL с паролями) и COM-порт индикатора.
Локальным на весовом ПК остаётся только токен агента (ключ канала).

Порядок применения:
- цикл: новый CycleConfig подставляется наблюдателю (наблюдение начинается
  заново с WAIT_EMPTY — стоящая на весах машина потребует пересъезда,
  как после рестарта агента), авторежиму и порогу ручного режима;
- камеры: списки камер операций, ручного режима и фоновой проверки;
- COM-порт: драйвер перезапускается на новом порту; если за
  PORT_CHECK_TIMEOUT_S индикатор не ожил — откат на прежний порт
  и отчёт rolled_back (защита от дистанционной опечатки).

Применённый снимок сохраняется в SQLite и накатывается на конфиг при
старте агента (merge_center_settings) — настройки переживают рестарт
и офлайн. При откате порта снимок сохраняется без порта.
"""

import asyncio
import logging
from typing import Protocol

from agent.cameras.capture import DEFAULT_TIMEOUT_S, CameraConfig
from agent.config import AgentConfig, CameraSection, CycleSection, ScaleSection
from agent.drivers.base import SerialScaleDriver
from agent.sync.storage import AgentStorage
from agent.weighing.auto import AutoOperationRunner
from agent.weighing.manual import ManualOperationFlow
from agent.weighing.watcher import ScaleWatcher
from shared.enums import CameraRole, ScaleStatus
from shared.messages import ConfigStatus, ScaleConfigUpdate, ScaleSettingsPayload

logger = logging.getLogger(__name__)


class CameraHealthLike(Protocol):
    """Минимум от фоновой проверки камер (реализация — main.CameraHealth)."""

    def set_cameras(self, cameras: list[CameraConfig]) -> None: ...


class CameraStreamsLike(Protocol):
    """Минимум от потоков камер (реализация — cameras.stream.CameraStreams)."""

    def set_cameras(self, cameras: list[CameraConfig]) -> None: ...


class CameraPreviewLike(Protocol):
    """Минимум от превью веб-интерфейса (реализация — main.AgentRuntime).

    Боевой урок Кызыл-Кыи 14.08.2026: свап ролей камер из центра доехал
    до съёмки операций, но превью оператора продолжало снимать по
    локальному config.toml до рестарта службы — оператор видел старые
    камеры и считал, что настройка не сработала.
    """

    def set_cameras(self, cameras: list[CameraConfig]) -> None: ...


class AgentInfoLike(Protocol):
    """Минимум от шапки веб-интерфейса (реализация — main.AgentRuntime):
    подпись индикатора/весов из центра применяется без рестарта (20.08.2026)."""

    def set_indicator_model(self, model: str) -> None: ...

    def set_manual_allowed(self, allowed: bool) -> None:
        """Разрешение ручного режима при связи с центром (0.4.28)."""
        ...


class RetentionLike(Protocol):
    """Минимум от уборки локальных фото (реализация — sync.retention.PhotoRetention):
    срок хранения из центра применяется на лету (решение Игоря 02.09.2026)."""

    def set_retention_days(self, days: int) -> None: ...


# ожидание живого индикатора после смены порта: живой cas22 шлёт поток
# непрерывно, статус OK появляется за ~1-2 с после открытия порта; запас
# покрывает пару циклов переоткрытия драйвера (rx_error_timeout_s=3 +
# reopen_delay_s=2). От cycle.no_data_timeout_s наблюдателя НЕ зависит
PORT_CHECK_TIMEOUT_S = 12.0
PORT_CHECK_INTERVAL_S = 0.2


def merge_center_settings(config: AgentConfig, payload: ScaleSettingsPayload) -> AgentConfig:
    """Наложить сохранённый снимок центра на конфиг при старте агента.

    Снимок главнее config.toml для всего, чем управляет центр; None в поле
    снимка — параметром продолжает управлять локальный конфиг.
    """
    updates: dict[str, object] = {}
    if payload.cycle is not None:
        updates["cycle"] = CycleSection(**payload.cycle.model_dump())
    # пустой список камер игнорируем (центр такого не шлёт; агент без
    # камер не смог бы провести ни одну операцию — ERR_CAMERA)
    if payload.cameras:
        # таймаут съёмки — свойство площадки (скорость весового ПК и камер),
        # центр им не управляет: наследуется от локальной камеры той же роли
        # (урок Джалал-Абада 12.08.2026 — центр затирал поднятый таймаут)
        local_timeouts = {camera.role: camera.timeout_s for camera in config.cameras}
        updates["cameras"] = [
            CameraSection(
                role=camera.role,
                snapshot_url=camera.snapshot_url,
                rtsp_url=camera.rtsp_url,
                preview_url=camera.preview_url,
                timeout_s=local_timeouts.get(camera.role, DEFAULT_TIMEOUT_S),
            )
            for camera in payload.cameras
        ]
    if payload.scale_port:
        updates["scale"] = ScaleSection(
            driver=config.scale.driver,
            port=payload.scale_port,
            baudrate=payload.baudrate or config.scale.baudrate,
        )
    if payload.indicator_model:
        updates["indicator_model"] = payload.indicator_model
    if payload.photo_retention_days is not None:
        # срок хранения локальных фото (0.4.25): 0 из центра — «не убирать»,
        # это тоже управление, а не «не задано»
        updates["storage"] = config.storage.model_copy(
            update={"photo_retention_days": payload.photo_retention_days}
        )
    if not updates:
        return config
    return config.model_copy(update=updates)


class SettingsManager:
    """Живое применение scale_config к работающим кирпичам агента."""

    def __init__(
        self,
        *,
        driver: SerialScaleDriver,
        watcher: ScaleWatcher,
        runner: AutoOperationRunner,
        manual: ManualOperationFlow,
        camera_health: "CameraHealthLike",
        storage: AgentStorage,
        camera_streams: "CameraStreamsLike | None" = None,
        local_camera_timeouts: dict[CameraRole, float] | None = None,
    ) -> None:
        self._driver = driver
        self._watcher = watcher
        self._runner = runner
        self._manual = manual
        self._camera_health = camera_health
        # потоки RTSP-камер: смена URL из центра пересоздаёт их на лету
        self._camera_streams = camera_streams
        # превью и шапка веб-интерфейса подписываются отдельным шагом сборки:
        # AgentRuntime создаётся ПОЗЖЕ менеджера (см. build_runtime)
        self._preview: CameraPreviewLike | None = None
        self._info_sink: AgentInfoLike | None = None
        self._retention: RetentionLike | None = None
        self._storage = storage
        # таймауты съёмки локального конфига по ролям: камеры из центра
        # наследуют их (см. merge_center_settings — та же логика при старте)
        self._local_camera_timeouts = local_camera_timeouts or {}
        self._lock = asyncio.Lock()  # настройки применяются по одной

    def set_preview(self, preview: CameraPreviewLike) -> None:
        """Подписать превью веб-интерфейса на смену камер из центра."""
        self._preview = preview

    def set_info_sink(self, sink: AgentInfoLike) -> None:
        """Подписать шапку веб-интерфейса на подпись индикатора из центра."""
        self._info_sink = sink

    def set_retention(self, retention: RetentionLike) -> None:
        """Подписать уборку локальных фото на срок хранения из центра."""
        self._retention = retention

    async def handle(self, update: ScaleConfigUpdate) -> ConfigStatus:
        """Обработчик для CenterClient: применить и сохранить снимок."""
        async with self._lock:
            try:
                return await self._apply(update.settings)
            except Exception as exc:  # отчёт вместо падения клиента
                logger.exception("применение настроек центра упало")
                return ConfigStatus(ok=False, error=str(exc))

    async def _apply(self, settings: ScaleSettingsPayload) -> ConfigStatus:
        if settings.cycle is not None:
            cycle = CycleSection(**settings.cycle.model_dump()).to_cycle_config()
            self._watcher.reconfigure(cycle)
            self._runner.set_cycle(cycle)
            self._manual.set_vehicle_threshold(cycle.vehicle_threshold_kg)
            self._manual.set_max_tare(cycle.max_tare_kg)
            logger.info("настройки центра: параметры цикла применены")

        if settings.cameras:  # пустой список — как «не задано» (см. merge)
            cameras = [
                CameraConfig(
                    role=camera.role,
                    snapshot_url=camera.snapshot_url,
                    rtsp_url=camera.rtsp_url,
                    preview_url=camera.preview_url,
                    timeout_s=self._local_camera_timeouts.get(camera.role, DEFAULT_TIMEOUT_S),
                )
                for camera in settings.cameras
            ]
            self._runner.set_cameras(cameras)
            self._manual.set_cameras(cameras)
            self._camera_health.set_cameras(cameras)
            if self._camera_streams is not None:
                self._camera_streams.set_cameras(cameras)
            if self._preview is not None:
                self._preview.set_cameras(cameras)
            logger.info("настройки центра: камеры применены (%d)", len(cameras))

        if settings.indicator_model and self._info_sink is not None:
            self._info_sink.set_indicator_model(settings.indicator_model)
            logger.info("настройки центра: подпись индикатора применена")

        if settings.manual_allowed is not None and self._info_sink is not None:
            # исключение из правила №3 по решению центра (объект без АИС);
            # False из центра — тоже управление: снимает разрешение
            self._info_sink.set_manual_allowed(settings.manual_allowed)
            logger.info(
                "настройки центра: ручной режим при связи с центром — %s",
                "разрешён" if settings.manual_allowed else "запрещён",
            )

        if settings.photo_retention_days is not None and self._retention is not None:
            self._retention.set_retention_days(settings.photo_retention_days)
            logger.info(
                "настройки центра: срок хранения локальных фото — %d дн.",
                settings.photo_retention_days,
            )

        status = ConfigStatus(ok=True)
        if settings.scale_port:
            status = await self._apply_port(settings.scale_port, settings.baudrate)

        persisted = settings
        if status.rolled_back:
            # неудачный порт не сохраняем: после рестарта агент не должен
            # снова пытаться слушать мёртвый порт
            persisted = settings.model_copy(update={"scale_port": None, "baudrate": None})
        self._storage.save_center_settings(persisted.model_dump_json())
        return status

    async def _apply_port(self, port: str, baudrate: int | None) -> ConfigStatus:
        old_port = self._driver.port_url
        old_baudrate = self._driver.baudrate
        if port == old_port and (baudrate is None or baudrate == old_baudrate):
            return ConfigStatus(ok=True)
        logger.info("настройки центра: смена порта %s → %s", old_port, port)
        await asyncio.to_thread(self._driver.set_port, port, baudrate)
        if await self._wait_indicator_alive():
            logger.info("настройки центра: индикатор отвечает на %s", port)
            return ConfigStatus(ok=True)
        logger.error("настройки центра: индикатор молчит на %s — откат на %s", port, old_port)
        await asyncio.to_thread(self._driver.set_port, old_port, old_baudrate)
        return ConfigStatus(
            ok=False,
            rolled_back=True,
            error=f"индикатор молчит на порту {port} — возвращён {old_port}",
        )

    async def _wait_indicator_alive(self) -> bool:
        deadline = asyncio.get_running_loop().time() + PORT_CHECK_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            if self._driver.state.status is ScaleStatus.OK:
                return True
            await asyncio.sleep(PORT_CHECK_INTERVAL_S)
        return False
