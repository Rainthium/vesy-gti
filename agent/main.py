"""Оркестратор агента: сборка всех кирпичей по конфигу и запуск службы.

    python -m agent.main --config C:/vesy-agent/config.toml
    python -m agent.main --config ... add-operator --login a.osmonov --full-name 'А. Осмонов'

Что собирается (architecture §3.1):
- драйвер индикатора (COM-порт, автопереоткрытие — правило №6);
- AutoOperationRunner — операции по командам центра;
- ManualOperationFlow — ручной офлайн-режим; правило №3 воплощено ЗДЕСЬ:
  ``manual_allowed = нет связи с центром`` (клиент отдаёт connected);
- CenterClient (WebSocket, досылка офлайн-записей) + PhotoUploader (HTTP);
- локальный веб-интерфейс оператора (uvicorn на 127.0.0.1).

При старте, до запуска остальных частей, убираются снимки-сироты:
файлы в photos_dir без записи в журнале (погибшие превью ручного
режима после краха). Записи журнала и их снимки не трогаются никогда
(правило №2).
"""

import argparse
import asyncio
import contextlib
import getpass
import logging
import sys
import urllib.parse
from datetime import UTC, datetime
from pathlib import Path

import uvicorn

import agent
from agent.cameras.capture import CameraConfig, CameraShot, capture
from agent.config import AgentConfig, load_config
from agent.drivers.base import ScaleState
from agent.drivers.cas22 import Cas22Driver
from agent.sync.photo_uploader import PhotoUploader
from agent.sync.storage import AgentStorage
from agent.sync.ws_client import CenterClient, ClientConfig, run_forever
from agent.web.app import create_app
from agent.web.services import AgentInfo
from agent.weighing.auto import AutoConfig, AutoOperationRunner
from agent.weighing.manual import ManualOperationFlow, ManualPreview
from shared.enums import CameraRole, Operation
from shared.messages import CameraStatus, EquipmentStatus, TareRecord, WeighingRecord

logger = logging.getLogger(__name__)


def http_base_url(center_ws_url: str) -> str:
    """ws(s)://host[:port]/agents/ws → http(s)://host[:port] (для загрузки фото)."""
    parts = urllib.parse.urlsplit(center_ws_url)
    scheme = "https" if parts.scheme == "wss" else "http"
    return f"{scheme}://{parts.netloc}"


def cleanup_orphan_photos(storage: AgentStorage, photos_dir: Path) -> int:
    """Удалить снимки-сироты (файлы без записи в журнале); вернуть число.

    Вызывается при старте, ДО запуска веб-интерфейса и клиента центра:
    в этот момент незавершённых превью быть не может, значит любой
    неизвестный журналу файл — мусор от краха.
    """
    if not photos_dir.is_dir():
        return 0
    known = {str(Path(path)) for path in storage.photo_paths()}
    removed = 0
    for file in photos_dir.rglob("*.jpeg"):
        if str(file) not in known:
            with contextlib.suppress(OSError):
                file.unlink()
                removed += 1
    if removed:
        logger.info("уборка снимков-сирот: удалено %d файлов", removed)
    return removed


class CameraHealth:
    """Фоновая проверка камер: статусы для heartbeat и дашборда центра.

    Раз в ``interval_s`` пробует снимок каждой камеры (недоступная камера —
    это видно диспетчеру на экране объектов, запрос Игоря 09.08.2026).
    Снимок-проба и снимок операции не конфликтуют: камеры отдают JPEG
    любому числу клиентов.
    """

    def __init__(self, cameras: list[CameraConfig], *, interval_s: float, ffmpeg_path: str) -> None:
        self._cameras = cameras
        self._interval_s = interval_s
        self._ffmpeg_path = ffmpeg_path
        self._statuses: dict[CameraRole, CameraStatus] = {}

    @property
    def statuses(self) -> list[CameraStatus]:
        return list(self._statuses.values())

    async def check_once(self) -> None:
        for camera in self._cameras:
            shot = await asyncio.to_thread(capture, camera, ffmpeg_path=self._ffmpeg_path)
            previous = self._statuses.get(camera.role)
            self._statuses[camera.role] = CameraStatus(
                role=camera.role,
                available=shot.ok,
                last_snapshot_at=(
                    shot.captured_at
                    if shot.ok
                    else (previous.last_snapshot_at if previous else None)
                ),
            )
            if not shot.ok:
                logger.warning("проверка камеры: %s", shot.error)

    async def run(self) -> None:
        while True:
            try:
                await self.check_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("сбой цикла проверки камер")
            await asyncio.sleep(self._interval_s)


class AgentRuntime:
    """Реализация AgentServices: связывает веб-интерфейс с кирпичами агента."""

    def __init__(
        self,
        config: AgentConfig,
        *,
        driver: Cas22Driver,
        storage: AgentStorage,
        client: CenterClient,
        manual: ManualOperationFlow,
    ) -> None:
        self._config = config
        self._driver = driver
        self._storage = storage
        self._client = client
        self._manual = manual
        self._info = AgentInfo(
            site_name=config.site_name,
            scale_name=config.scale_name,
            indicator_model=config.indicator_model,
            driver_name=config.scale.driver,
            port_label=f"{config.scale.port} · {config.scale.baudrate} · 8-N-1",
            agent_version=agent.__version__,
            center_url=config.center.url,
        )

    @property
    def info(self) -> AgentInfo:
        return self._info

    def scale_state(self) -> ScaleState:
        return self._driver.state

    def center_connected(self) -> bool:
        return self._client.connected

    def pending_count(self) -> int:
        return self._storage.pending_count()

    def tare_registry_size(self) -> int:
        return self._storage.tare_registry_size()

    def recent_weighings(self, limit: int = 50) -> list[tuple[WeighingRecord, bool]]:
        return self._storage.recent_weighings_synced(limit)

    def camera_roles(self) -> list[CameraRole]:
        return [camera.role for camera in self._config.cameras]

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        for camera in self._config.cameras:
            if camera.role is role:
                return capture(camera.to_camera_config(), ffmpeg_path=self._config.ffmpeg_path)
        raise ValueError(f"камера {role} не настроена")

    def verify_operator(self, login: str, password: str) -> str | None:
        return self._storage.verify_operator(login, password)

    def reopen_port(self) -> None:
        # принудительный перезапуск потока чтения (автопереоткрытие и так есть)
        self._driver.stop()
        self._driver.start()

    def manual_ready(self) -> bool:
        return self._manual.ready()

    def manual_capture(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        return self._manual.capture_and_save(
            operation,
            vehicle_number=vehicle_number,
            trailer_number=trailer_number,
            operator=operator,
        )

    def find_active_tare(
        self, vehicle_number: str, trailer_number: str | None = None
    ) -> TareRecord | None:
        return self._storage.find_active_tare(
            vehicle_number.strip().upper(),
            datetime.now(UTC),
            (trailer_number or "").strip().upper() or None,
        )


def build_runtime(
    config: AgentConfig,
) -> tuple[AgentRuntime, Cas22Driver, AgentStorage, CenterClient, PhotoUploader, CameraHealth]:
    """Собрать все кирпичи агента (без запуска фоновых задач)."""
    driver = Cas22Driver(config.scale.port, baudrate=config.scale.baudrate)
    storage = AgentStorage(config.storage.db_path)
    config.storage.photos_dir.mkdir(parents=True, exist_ok=True)
    camera_health = CameraHealth(
        config.camera_configs(),
        interval_s=config.camera_check_interval_s,
        ffmpeg_path=config.ffmpeg_path,
    )

    runner = AutoOperationRunner(
        scale_state=lambda: driver.state,
        storage=storage,
        cameras=config.camera_configs(),
        photos_dir=config.storage.photos_dir,
        config=AutoConfig(cycle=config.cycle.to_cycle_config()),
        ffmpeg_path=config.ffmpeg_path,
    )

    def equipment_status() -> EquipmentStatus:
        state = driver.state
        return EquipmentStatus(
            scale_status=state.status,
            current_weight=state.weight_kg,
            stable=state.stable,
            cameras=camera_health.statuses,
            pending_sync_count=storage.pending_count(),
        )

    client = CenterClient(
        ClientConfig(
            url=config.center.url,
            token=config.center.token,
            agent_id=config.agent_id,
            version=agent.__version__,
            driver=config.scale.driver,
            heartbeat_interval_s=config.center.heartbeat_interval_s,
        ),
        storage,
        equipment_status=equipment_status,
        on_weigh_request=runner.handle,
    )
    uploader = PhotoUploader(
        storage,
        base_url=http_base_url(config.center.url),
        token=config.center.token,
    )
    manual = ManualOperationFlow(
        scale_state=lambda: driver.state,
        # правило №3: ручной режим только без связи с центром
        manual_allowed=lambda: not client.connected,
        storage=storage,
        cameras=config.camera_configs(),
        photos_dir=config.storage.photos_dir,
        vehicle_threshold_kg=config.cycle.vehicle_threshold_kg,
        ffmpeg_path=config.ffmpeg_path,
    )
    runtime = AgentRuntime(config, driver=driver, storage=storage, client=client, manual=manual)
    return runtime, driver, storage, client, uploader, camera_health


async def run_agent(config: AgentConfig) -> None:
    """Запустить агента целиком; остановка — отменой (Ctrl-C / stop службы)."""
    runtime, driver, storage, client, uploader, camera_health = build_runtime(config)
    driver.start()
    cleanup_orphan_photos(storage, config.storage.photos_dir)

    web_app = create_app(runtime, session_secret=config.web.session_secret)
    server = uvicorn.Server(
        uvicorn.Config(web_app, host=config.web.host, port=config.web.port, log_level="info")
    )
    logger.info(
        "агент %s запущен: индикатор %s, центр %s, интерфейс оператора http://%s:%d",
        config.agent_id,
        config.scale.port,
        config.center.url,
        config.web.host,
        config.web.port,
    )
    tasks = [
        asyncio.create_task(run_forever(client), name="center-client"),
        asyncio.create_task(uploader.run(), name="photo-uploader"),
        asyncio.create_task(camera_health.run(), name="camera-health"),
        asyncio.create_task(server.serve(), name="operator-web"),
    ]
    try:
        # веб-сервер завершается только по сигналу — ждём любую из задач
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()  # поднять исключение упавшей задачи
    finally:
        server.should_exit = True
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        driver.stop()
        storage.close()
        logger.info("агент остановлен")


def _add_operator(config: AgentConfig, login: str, full_name: str) -> None:
    """Создать/обновить локального оператора (пароль — интерактивно)."""
    password = getpass.getpass("Пароль оператора: ")
    if (login, password) == ("admin", "admin"):
        sys.exit("admin/admin запрещён (правило проекта №7).")
    if len(password) < 8:
        sys.exit("Пароль короче 8 символов — откажемся.")
    storage = AgentStorage(config.storage.db_path)
    try:
        storage.upsert_operator(login, password, full_name)
    finally:
        storage.close()
    print(f"Оператор {login} сохранён.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Агент весового ПК")
    parser.add_argument("--config", required=True, help="путь к config.toml")
    sub = parser.add_subparsers(dest="command")
    p_operator = sub.add_parser("add-operator", help="создать/обновить локального оператора")
    p_operator.add_argument("--login", required=True)
    p_operator.add_argument("--full-name", default="")
    args = parser.parse_args()

    config = load_config(args.config)
    if args.command == "add-operator":
        _add_operator(config, args.login, args.full_name)
        return
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_agent(config))


if __name__ == "__main__":
    main()
