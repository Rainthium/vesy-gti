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
import shutil
import sys
import threading
import time
import urllib.parse
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import uvicorn

import agent
from agent.cameras.capture import CameraConfig, CameraShot, capture
from agent.cameras.stream import CameraStreams, shot_or_capture
from agent.clock import CenterClock
from agent.config import AgentConfig, load_config
from agent.diagnostics import default_log_path, read_log_tail
from agent.drivers import create_driver
from agent.drivers.base import ScaleState, SerialScaleDriver
from agent.photos import THUMB_SUFFIX, PhotoLibrary
from agent.selfcheck import UpdateSelfCheck
from agent.settings import SettingsManager, merge_center_settings
from agent.sync.photo_uploader import PhotoUploader
from agent.sync.retention import CleanupResult, PhotoRetention
from agent.sync.storage import AgentStorage
from agent.sync.ws_client import CenterClient, ClientConfig, run_forever
from agent.updater import AgentUpdater, install_base
from agent.web.app import create_app
from agent.web.services import AgentInfo
from agent.weighing.auto import AutoConfig, AutoOperationRunner
from agent.weighing.manual import ManualOperationFlow, ManualPreview
from agent.weighing.watcher import ScaleWatcher
from shared.enums import CameraRole, Operation, ScaleStatus
from shared.messages import (
    CameraStatus,
    ConfigStatus,
    EquipmentStatus,
    ScaleConfigUpdate,
    ScaleSettingsPayload,
    TareRecord,
    VerificationInfo,
    WeighingRecord,
)

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

    Миниатюры журнала (``..._thumb.jpeg``, agent/photos.py) в журнале не
    числятся, но принадлежат своим кадрам: их судьба — судьба оригинала,
    иначе кэш стирался бы при каждом старте (находка ревью 11.08.2026).
    """
    if not photos_dir.is_dir():
        return 0
    known = {str(Path(path)) for path in storage.photo_paths()}
    removed = 0
    for file in photos_dir.rglob("*.jpeg"):
        owner = file
        if file.stem.endswith(THUMB_SUFFIX):
            owner = file.with_name(file.stem[: -len(THUMB_SUFFIX)] + file.suffix)
        if str(owner) not in known:
            with contextlib.suppress(OSError):
                file.unlink()
                removed += 1
    if removed:
        logger.info("уборка снимков-сирот: удалено %d файлов", removed)
    return removed


# срок жизни кадра превью: браузер оператора просит раз в 2 с; снапшот-
# камеры (Кызыл-Кыя) успевают каждый раз, RTSP-камеры обновляются
# с темпом собственной съёмки (2-5 с)
PREVIEW_TTL_S = 1.5
# камера с preview_url (лёгкий кадр суб-потока, запрос Игоря 20.08.2026 для
# Аламедина: полный кадр 6 МП камера отдаёт медленно) — превью раз в секунду
PREVIEW_FAST_TTL_S = 0.75
PREVIEW_INTERVAL_MS = 2000
PREVIEW_FAST_INTERVAL_MS = 1000


class CameraHealth:
    """Фоновая проверка камер: статусы для heartbeat и дашборда центра.

    Раз в ``interval_s`` пробует снимок каждой камеры (недоступная камера —
    это видно диспетчеру на экране объектов, запрос Игоря 09.08.2026).
    Снимок-проба и снимок операции не конфликтуют: камеры отдают JPEG
    любому числу клиентов.
    """

    def __init__(
        self,
        cameras: list[CameraConfig],
        *,
        interval_s: float,
        ffmpeg_path: str,
        streams: CameraStreams | None = None,
    ) -> None:
        self._cameras = cameras
        self._interval_s = interval_s
        self._ffmpeg_path = ffmpeg_path
        # живой буфер потока считается пробой камеры: не дёргаем её лишний раз
        self._streams = streams
        self._statuses: dict[CameraRole, CameraStatus] = {}

    @property
    def statuses(self) -> list[CameraStatus]:
        return list(self._statuses.values())

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Новый список камер (настройки из центра); статусы обнуляются
        и заполнятся ближайшей проверкой."""
        self._cameras = cameras
        self._statuses = {}

    async def check_once(self) -> None:
        for camera in self._cameras:
            shot = await asyncio.to_thread(
                shot_or_capture, camera, self._streams, ffmpeg_path=self._ffmpeg_path
            )
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
        driver: SerialScaleDriver,
        storage: AgentStorage,
        client: CenterClient,
        manual: ManualOperationFlow,
        photos: PhotoLibrary,
        clock: CenterClock,
        log_path: Path | None,
        streams: CameraStreams | None = None,
    ) -> None:
        self._config = config
        self._driver = driver
        self._storage = storage
        self._client = client
        self._manual = manual
        self._photos = photos
        self._clock = clock
        self._log_path = log_path
        self._streams = streams
        # превью камер: последний готовый кадр по роли + замок «съёмка идёт».
        # Браузер оператора просит кадр каждые 2 с; RTSP-камера отдаёт его
        # 2–5 с (Джалал-Абад) — без кэша запросы наслаиваются каскадом
        # ffmpeg-процессов и лишних RTSP-сессий к камере
        self._preview_cache: dict[CameraRole, tuple[CameraShot, float]] = {}
        self._preview_locks: dict[CameraRole, threading.Lock] = {
            camera.role: threading.Lock() for camera in config.cameras
        }
        # камеры превью изменяемы: применение scale_config на лету заменяет
        # их через set_cameras (боевой урок Кызыл-Кыи 14.08.2026 — свап ролей
        # из центра доезжал до съёмки операций, но не до превью оператора)
        self._preview_cameras: dict[CameraRole, CameraConfig] = {
            camera.role: camera for camera in config.camera_configs()
        }
        # самопроверка после автообновления (0.4.19): собирается в build_runtime,
        # run_agent подставляет web_ready и запускает задачу
        self.selfcheck: UpdateSelfCheck | None = None
        # уборка локальных фото (0.4.25): собирается в build_runtime, цикл
        # запускает run_agent; срок меняется из центра на лету
        self.retention: PhotoRetention | None = None
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

    def set_indicator_model(self, model: str) -> None:
        """Подпись индикатора из центра — в шапку и «Оборудование» на лету
        (страница оператора покажет при следующей загрузке)."""
        self._info = replace(self._info, indicator_model=model)

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
        return list(self._preview_cameras)

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Заменить камеры превью на лету (scale_config из центра).

        Кэш сбрасывается: последний кадр прежней камеры не должен
        отдаваться как «свежий» после смены URL или ролей.
        """
        self._preview_cameras = {camera.role: camera for camera in cameras}
        self._preview_cache.clear()

    def preview_interval_ms(self) -> int:
        """Период опроса превью браузером оператора.

        Хоть у одной камеры задан preview_url → раз в секунду (лёгкий кадр
        камера отдаёт быстро), иначе прежние 2 с. Значение вшивается в
        страницу при рендере: смена настройки из центра подхватится при
        следующей загрузке страницы оператора.
        """
        fast = any(camera.preview_url for camera in self._preview_cameras.values())
        return PREVIEW_FAST_INTERVAL_MS if fast else PREVIEW_INTERVAL_MS

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        """Кадр для превью оператора: из кэша, съёмка — не чаще одной за раз.

        Свежий кадр (моложе TTL) отдаётся из памяти; если съёмка уже идёт
        (RTSP-кадр занимает секунды) — отдаётся последний готовый, даже
        подустаревший: превью живёт с темпом, который тянет камера,
        а каскад параллельных ffmpeg не возникает.

        Камера с preview_url снимается по нему (лёгкий кадр суб-потока,
        минуя и RTSP-буфер) и с коротким TTL — превью частое, а фото
        операций по-прежнему идут с основного URL в полном качестве.
        """
        camera = self._preview_cameras.get(role)
        if camera is None:
            raise ValueError(f"камера {role} не настроена")
        ttl = PREVIEW_TTL_S
        if camera.preview_url:
            camera = replace(camera, snapshot_url=camera.preview_url, rtsp_url=None)
            ttl = PREVIEW_FAST_TTL_S
        elif self._streams is not None:
            # потоковая камера: буфер обновляется раз в секунду — превью
            # живое, ffmpeg на каждый запрос браузера не запускается
            streamed = self._streams.shot(role)
            if streamed is not None:
                return streamed
        cached = self._preview_cache.get(role)
        now = time.monotonic()
        if cached is not None and now - cached[1] < ttl:
            return cached[0]
        lock = self._preview_locks.setdefault(role, threading.Lock())
        if not lock.acquire(blocking=False):
            # съёмка уже идёт в соседнем запросе — не плодим вторую
            if cached is not None:
                return cached[0]
            return CameraShot(
                role=role, jpeg=None, captured_at=datetime.now(UTC), error="съёмка уже идёт"
            )
        try:
            shot = capture(camera, ffmpeg_path=self._config.ffmpeg_path)
            # ошибку тоже кэшируем: мёртвая камера не должна заставлять
            # каждый запрос превью висеть полный таймаут съёмки
            self._preview_cache[role] = (shot, time.monotonic())
            return shot
        finally:
            lock.release()

    def photo_roles(self, weighing_uuid: UUID) -> list[CameraRole]:
        return self._photos.roles_of(weighing_uuid)

    def photo_bytes(
        self, weighing_uuid: UUID, role: CameraRole, *, thumb: bool = False
    ) -> bytes | None:
        return self._photos.photo_bytes(weighing_uuid, role, thumb=thumb)

    def record_by_uuid(self, weighing_uuid: UUID) -> WeighingRecord | None:
        return self._storage.get_weighing(weighing_uuid)

    def tare_by_weighing_uuid(self, weighing_uuid: UUID) -> TareRecord | None:
        return self._storage.tare_by_weighing_uuid(weighing_uuid)

    def verification(self) -> VerificationInfo | None:
        """Поверка — из сохранённого снимка настроек центра (SQLite).

        Читается при каждой печати: снимок обновляется scale_config'ом
        на лету, кэшировать нечего — чтение дешёвое.
        """
        raw = self._storage.load_center_settings()
        if raw is None:
            return None
        try:
            return ScaleSettingsPayload.model_validate_json(raw).verification
        except ValueError:
            return None

    def photo_available(self, weighing_uuid: UUID, role: CameraRole) -> bool:
        return self._photos.photo_available(weighing_uuid, role)

    def photo_queue(self) -> tuple[int, int]:
        return self._storage.photo_queue_stats()

    def clock_offset_s(self) -> float | None:
        return self._clock.offset_s if self._clock.synced else None

    def log_tail(self, lines: int = 300) -> list[str]:
        return read_log_tail(self._log_path, lines=lines)

    def log_location(self) -> str:
        return str(self._log_path) if self._log_path else "вывод в консоль (dev-запуск)"

    def verify_operator(self, login: str, password: str) -> str | None:
        return self._storage.verify_operator(login, password)

    def operator_stamp(self, login: str) -> str | None:
        return self._storage.operator_stamp(login)

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

    def latest_tare(
        self, vehicle_number: str, trailer_number: str | None = None
    ) -> TareRecord | None:
        return self._storage.latest_tare(
            vehicle_number.strip().upper(),
            (trailer_number or "").strip().upper() or None,
        )


def build_runtime(
    config: AgentConfig,
    *,
    local_camera_timeouts: dict[CameraRole, float] | None = None,
) -> tuple[
    AgentRuntime,
    SerialScaleDriver,
    AgentStorage,
    CenterClient,
    PhotoUploader,
    CameraHealth,
    ScaleWatcher,
    AutoConfig,
    CameraStreams,
]:
    """Собрать все кирпичи агента (без запуска фоновых задач)."""
    driver = create_driver(config.scale.driver, config.scale.port, baudrate=config.scale.baudrate)
    storage = AgentStorage(config.storage.db_path)
    # время записей — по часам центра (heartbeat_ack), офлайн — по
    # последнему известному смещению из SQLite (вопрос Игоря 10.08.2026)
    center_clock = CenterClock(storage)
    config.storage.photos_dir.mkdir(parents=True, exist_ok=True)
    # постоянные потоки RTSP-камер (агент 0.4.7): фоновый ffmpeg держит
    # соединение и кладёт свежий кадр в память — превью и снимок операции
    # берут его мгновенно; камерам со снапшотом поток не заводится
    streams = CameraStreams(config.camera_configs(), ffmpeg_path=config.ffmpeg_path)
    camera_health = CameraHealth(
        config.camera_configs(),
        interval_s=config.camera_check_interval_s,
        ffmpeg_path=config.ffmpeg_path,
        streams=streams,
    )

    # непрерывное наблюдение за платформой (схема UniServer): команда
    # срабатывает мгновенно по готовой фиксации стоящей машины, заезда
    # не ждёт (решение Игоря 10.08.2026)
    auto_config = AutoConfig(cycle=config.cycle.to_cycle_config())
    watcher = ScaleWatcher(auto_config.cycle)
    runner = AutoOperationRunner(
        scale_state=lambda: driver.state,
        watcher=watcher,
        storage=storage,
        cameras=config.camera_configs(),
        photos_dir=config.storage.photos_dir,
        config=auto_config,
        ffmpeg_path=config.ffmpeg_path,
        streams=streams,
        now_utc=center_clock.now,
    )

    def equipment_status() -> EquipmentStatus:
        state = driver.state
        # свободное место на диске с фото (0.4.13): фото — самое прожорливое,
        # что пишет агент; недоступность диска не должна ронять heartbeat
        try:
            disk_free_mb = shutil.disk_usage(config.storage.photos_dir).free // (1024 * 1024)
        except OSError:
            disk_free_mb = None
        return EquipmentStatus(
            scale_status=state.status,
            current_weight=state.weight_kg,
            stable=state.stable,
            cameras=camera_health.statuses,
            pending_sync_count=storage.pending_count(),
            pending_photos_count=storage.pending_photos_count(),
            disk_free_mb=disk_free_mb,
        )

    updater = AgentUpdater(
        agent_id=config.agent_id,
        base_url=http_base_url(config.center.url),
        token=config.center.token,
        busy=runner.busy,
        # шёл ли поток индикатора перед обновлением — новая версия обязана
        # его сохранить (самопроверка 0.4.19, architecture §7а)
        indicator_ok=lambda: driver.state.status is ScaleStatus.OK,
    )
    # сторожок обновления докладывает центру через клиента (он создаётся ниже)

    # SettingsManager собирается ниже (ему нужен manual, а manual — клиенту);
    # колбэк связывает их через late-binding
    manager_ref: list[SettingsManager] = []

    async def on_scale_config(update: ScaleConfigUpdate) -> ConfigStatus:
        return await manager_ref[0].handle(update)

    # уборка локальных фото: срок из config.toml (поверх него — снимок центра,
    # на лету), принудительная уборка — по команде центра «Освободить место»
    retention = PhotoRetention(storage, retention_days=config.storage.photo_retention_days)

    async def on_photo_cleanup() -> CleanupResult:
        return await asyncio.to_thread(retention.cleanup_now)

    # путь к журналу службы нужен и клиенту (ответ центру), и веб-интерфейсу
    log_path = default_log_path()
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
        on_update_command=updater.handle,
        on_scale_config=on_scale_config,
        on_server_time=center_clock.set_server_time,
        on_log_tail=lambda lines: (
            read_log_tail(log_path, lines=lines),
            str(log_path) if log_path else "агент запущен не службой (вывод в консоль)",
        ),
        on_photo_cleanup=on_photo_cleanup,
    )
    updater.notify = client.post_message
    # самопроверка после автообновления и доклад об откате (0.4.19): собирается
    # здесь, чтобы обновление знало о ней (второе обновление не стартует, пока
    # идёт проверка первого); web_ready подставит run_agent после старта uvicorn
    selfcheck = UpdateSelfCheck(
        install_base(),
        agent_id=config.agent_id,
        web_ready=lambda: False,
        center_connected=lambda: client.connected,
        indicator_ok=lambda: driver.state.status is ScaleStatus.OK,
        notify=client.post_message,
    )
    updater.selfcheck_hold = selfcheck.hold_reason
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
        streams=streams,
        now_utc=center_clock.now,
    )
    manager_ref.append(
        SettingsManager(
            driver=driver,
            watcher=watcher,
            runner=runner,
            manual=manual,
            camera_health=camera_health,
            camera_streams=streams,
            storage=storage,
            # словарь снимается с СЫРОГО config.toml (main передаёт его до
            # merge): роль, выпавшая из старого снимка центра, не должна
            # терять локальный таймаут (замечание ревью 12.08.2026). Фолбэк
            # на post-merge конфиг — для тестов, зовущих build_runtime напрямую
            local_camera_timeouts=(
                local_camera_timeouts
                if local_camera_timeouts is not None
                else {c.role: c.timeout_s for c in config.cameras}
            ),
        )
    )
    runtime = AgentRuntime(
        config,
        driver=driver,
        storage=storage,
        client=client,
        manual=manual,
        photos=PhotoLibrary(
            storage,
            base_url=http_base_url(config.center.url),
            token=config.center.token,
            online=lambda: client.connected,
        ),
        clock=center_clock,
        streams=streams,
        log_path=log_path,
    )
    # превью подписывается на смену камер из центра ПОСЛЕ создания runtime
    # (менеджер собирается раньше); без подписки превью снимало бы по
    # локальному конфигу до рестарта службы (боевой урок К-К 14.08.2026)
    manager_ref[-1].set_preview(runtime)
    manager_ref[-1].set_info_sink(runtime)
    manager_ref[-1].set_retention(retention)
    runtime.selfcheck = selfcheck
    runtime.retention = retention
    return runtime, driver, storage, client, uploader, camera_health, watcher, auto_config, streams


def apply_stored_settings(config: AgentConfig) -> AgentConfig:
    """Накатить последний применённый снимок настроек центра на config.toml.

    Снимок сохраняется SettingsManager'ом при каждом scale_config —
    настройки центра переживают рестарт агента и офлайн. Битый снимок
    просто пропускается (агент стартует по локальному конфигу).
    """
    if not Path(config.storage.db_path).exists():
        return config  # первая установка: БД ещё нет
    settings_storage = AgentStorage(config.storage.db_path)
    try:
        raw = settings_storage.load_center_settings()
    finally:
        settings_storage.close()
    if raw is None:
        return config
    try:
        payload = ScaleSettingsPayload.model_validate_json(raw)
        merged = merge_center_settings(config, payload)
    except ValueError:
        # битый или несовместимый снимок не должен мешать старту агента
        logger.warning("сохранённые настройки центра не разбираются — пропущены")
        return config
    logger.info("применён сохранённый снимок настроек центра")
    return merged


async def watch_scale(watcher: ScaleWatcher, driver: SerialScaleDriver, interval_s: float) -> None:
    """Фоновый опрос драйвера для наблюдателя платформы (5–10 раз/с)."""
    while True:
        watcher.tick(driver.state)
        await asyncio.sleep(interval_s)


async def run_agent(
    config: AgentConfig,
    *,
    local_camera_timeouts: dict[CameraRole, float] | None = None,
) -> None:
    """Запустить агента целиком; остановка — отменой (Ctrl-C / stop службы)."""
    runtime, driver, storage, client, uploader, camera_health, watcher, auto_config, streams = (
        build_runtime(config, local_camera_timeouts=local_camera_timeouts)
    )
    driver.start()
    cleanup_orphan_photos(storage, config.storage.photos_dir)

    web_app = create_app(
        runtime,
        session_secret=config.web.session_secret,
        # порт различает агентов одного ПК: cookie к порту не привязан
        cookie_name=f"ves_session_{config.web.port}",
    )
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
    retention = runtime.retention
    assert retention is not None  # собран в build_runtime
    tasks = [
        asyncio.create_task(run_forever(client), name="center-client"),
        asyncio.create_task(uploader.run(), name="photo-uploader"),
        asyncio.create_task(camera_health.run(), name="camera-health"),
        asyncio.create_task(
            watch_scale(watcher, driver, auto_config.tick_interval_s), name="scale-watcher"
        ),
        asyncio.create_task(server.serve(), name="operator-web"),
    ]
    # цикл уборки живёт всегда (0.4.25): срок меняется из центра на лету, а
    # выключенная уборка внутри цикла просто спит — выход ЛЮБОЙ задачи
    # останавливает агента (находка qa-tester 11.08.2026)
    tasks.append(asyncio.create_task(retention.run(), name="photo-retention"))
    if not retention.enabled:
        logger.info("ретеншн локальных фото выключен (photo_retention_days = 0)")
    # самопроверка после автообновления и доклад об откате (0.4.19): задача
    # ЗАКАНЧИВАЕТСЯ за минуты, поэтому живёт вне списка выше — иначе её
    # штатный выход остановил бы агента; в dev-запуске (не frozen) молчит
    selfcheck = runtime.selfcheck
    assert selfcheck is not None  # собран в build_runtime
    selfcheck.web_ready = lambda: server.started
    selfcheck_task = asyncio.create_task(selfcheck.run(), name="update-selfcheck")
    try:
        # веб-сервер завершается только по сигналу — ждём любую из задач
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()  # поднять исключение упавшей задачи
    finally:
        server.should_exit = True
        selfcheck_task.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(selfcheck_task, *tasks, return_exceptions=True)
        driver.stop()
        streams.stop_all()
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
    # таймауты съёмки — с сырого config.toml, ДО наложения снимка центра:
    # merge мог выбросить роль, которой нет в старом снимке, а живой
    # scale_config позже может её вернуть — локальный таймаут должен выжить
    local_camera_timeouts = {camera.role: camera.timeout_s for camera in config.cameras}
    config = apply_stored_settings(config)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_agent(config, local_camera_timeouts=local_camera_timeouts))


if __name__ == "__main__":
    main()
