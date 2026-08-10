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

from agent.cameras.capture import CameraConfig
from agent.config import AgentConfig, CameraSection, CycleSection, ScaleSection
from agent.drivers.cas22 import Cas22Driver
from agent.sync.storage import AgentStorage
from agent.weighing.auto import AutoOperationRunner
from agent.weighing.manual import ManualOperationFlow
from agent.weighing.watcher import ScaleWatcher
from shared.enums import ScaleStatus
from shared.messages import ConfigStatus, ScaleConfigUpdate, ScaleSettingsPayload

logger = logging.getLogger(__name__)


class CameraHealthLike(Protocol):
    """Минимум от фоновой проверки камер (реализация — main.CameraHealth)."""

    def set_cameras(self, cameras: list[CameraConfig]) -> None: ...


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
        updates["cameras"] = [
            CameraSection(
                role=camera.role,
                snapshot_url=camera.snapshot_url,
                rtsp_url=camera.rtsp_url,
            )
            for camera in payload.cameras
        ]
    if payload.scale_port:
        updates["scale"] = ScaleSection(
            driver=config.scale.driver,
            port=payload.scale_port,
            baudrate=payload.baudrate or config.scale.baudrate,
        )
    if not updates:
        return config
    return config.model_copy(update=updates)


class SettingsManager:
    """Живое применение scale_config к работающим кирпичам агента."""

    def __init__(
        self,
        *,
        driver: Cas22Driver,
        watcher: ScaleWatcher,
        runner: AutoOperationRunner,
        manual: ManualOperationFlow,
        camera_health: "CameraHealthLike",
        storage: AgentStorage,
    ) -> None:
        self._driver = driver
        self._watcher = watcher
        self._runner = runner
        self._manual = manual
        self._camera_health = camera_health
        self._storage = storage
        self._lock = asyncio.Lock()  # настройки применяются по одной

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
            logger.info("настройки центра: параметры цикла применены")

        if settings.cameras:  # пустой список — как «не задано» (см. merge)
            cameras = [
                CameraConfig(
                    role=camera.role,
                    snapshot_url=camera.snapshot_url,
                    rtsp_url=camera.rtsp_url,
                )
                for camera in settings.cameras
            ]
            self._runner.set_cameras(cameras)
            self._manual.set_cameras(cameras)
            self._camera_health.set_cameras(cameras)
            logger.info("настройки центра: камеры применены (%d)", len(cameras))

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
