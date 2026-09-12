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
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import uvicorn

import agent
from agent.cameras.capture import CameraConfig, CameraShot, capture
from agent.cameras.overlay import OverlayInfo, burn_overlay
from agent.cameras.stream import CameraStreams, shot_or_capture
from agent.clock import CenterClock
from agent.config import AgentConfig, load_config
from agent.diagnostics import default_log_path, read_log_tail
from agent.drivers import create_driver
from agent.drivers.base import ScaleState, SerialScaleDriver
from agent.ffmpeg_tool import FfmpegProvisioner
from agent.photos import THUMB_SUFFIX, PhotoLibrary, shrink_preview
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


# срок жизни кадра превью для разовой съёмки: браузер оператора просит раз
# в 2 с; снапшот-камеры (Кызыл-Кыя) успевают каждый раз, разовая RTSP-съёмка
# (поток оборвался) обновляется с темпом камеры (2-5 с)
PREVIEW_TTL_S = 1.5
# камера с preview_url (лёгкий кадр суб-потока, запрос Игоря 20.08.2026 для
# Аламедина: полный кадр 6 МП камера отдаёт медленно) — превью раз в секунду
PREVIEW_FAST_TTL_S = 0.75
PREVIEW_INTERVAL_MS = 2000
# раз в секунду — и для потоковых камер (только RTSP, 0.4.34, запрос Игоря по
# таре Канта): кадр берётся из буфера потока (fps=1), камера не трогается
PREVIEW_FAST_INTERVAL_MS = 1000
# 0.4.31 (урок Канта 11.09.2026): при срыве съёмки превью отдаётся последний
# удачный кадр не старше PREVIEW_STALE_MAX_S — единичная осечка камеры не
# превращается в «Нет сигнала» на экране оператора; срывы пишутся в лог не
# чаще раза в PREVIEW_WARN_EVERY_S на камеру (мёртвая камера не забивает лог)
PREVIEW_STALE_MAX_S = 10.0
PREVIEW_WARN_EVERY_S = 30.0
# 0.4.32: съёмка превью дольше PREVIEW_SLOW_S попадает в лог (с длительностью,
# не чаще раза в PREVIEW_WARN_EVERY_S на камеру) — на Канте кадры доходили
# до экрана редко, а по журналу всё было «200 OK»: без длительности съёмки
# не отличить медленную камеру от залипшего кэша
PREVIEW_SLOW_S = 1.5


class CameraHealth:
    """Фоновая проверка камер: статусы для heartbeat и дашборда центра.

    Раз в ``interval_s`` пробует снимок каждой камеры (недоступная камера —
    это видно диспетчеру на экране объектов, запрос Игоря 09.08.2026).
    Проба идёт под замком съёмки превью той же камеры (``set_capture_lock``,
    0.4.31): одновременных HTTP-запросов к одной камере не делаем — на
    Канте задняя Hikvision при наложении отвечала срывом. Снимок операции
    замок не берёт (по замыслу: операцию не задерживаем).
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
        # замок съёмки по роли от превью (0.4.31): проба и превью не ходят к
        # одной камере одновременно — на Канте задняя Hikvision при наложении
        # двух HTTP-запросов отвечала срывом, и оператор видел «Нет сигнала»
        self._capture_lock: Callable[[CameraRole], threading.Lock] | None = None

    @property
    def statuses(self) -> list[CameraStatus]:
        return list(self._statuses.values())

    def set_capture_lock(self, provider: Callable[[CameraRole], threading.Lock]) -> None:
        """Брать замок съёмки камеры (тот же, что у превью) на время пробы."""
        self._capture_lock = provider

    def _probe(self, camera: CameraConfig) -> CameraShot:
        if self._capture_lock is None:
            return shot_or_capture(camera, self._streams, ffmpeg_path=self._ffmpeg_path)
        with self._capture_lock(camera.role):
            return shot_or_capture(camera, self._streams, ffmpeg_path=self._ffmpeg_path)

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Новый список камер (настройки из центра); статусы обнуляются
        и заполнятся ближайшей проверкой."""
        self._cameras = cameras
        self._statuses = {}

    async def check_once(self) -> None:
        for camera in self._cameras:
            shot = await asyncio.to_thread(self._probe, camera)
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


class ManualPermit:
    """Разрешение ручного режима при живой связи с центром (0.4.28).

    Живёт в памяти процесса: центр включает/выключает его снимком настроек,
    при старте восстанавливается из сохранённого снимка (build_runtime).
    Общий объект для потока ручных операций и веб-интерфейса.
    """

    def __init__(self) -> None:
        self.by_center = False


def port_label(port: str, baudrate: int) -> str:
    """Строка порта для экранов оператора: «COM4 · 9600 · 8-N-1»."""
    return f"{port} · {baudrate} · 8-N-1"


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
        manual_permit: ManualPermit | None = None,
    ) -> None:
        self._config = config
        self._driver = driver
        self._storage = storage
        self._client = client
        self._manual = manual
        self._manual_permit = manual_permit or ManualPermit()
        self._photos = photos
        self._clock = clock
        self._log_path = log_path
        self._streams = streams
        # превью камер: последний готовый кадр по роли + замок «съёмка идёт».
        # Браузер оператора просит кадр каждые 1–2 с; разовая RTSP-съёмка
        # отдаёт его 2–5 с (Джалал-Абад) — без кэша запросы наслаивались бы
        # каскадом ffmpeg-процессов и лишних RTSP-сессий к камере
        self._preview_cache: dict[CameraRole, tuple[CameraShot, float]] = {}
        self._preview_locks: dict[CameraRole, threading.Lock] = {
            camera.role: threading.Lock() for camera in config.cameras
        }
        # 0.4.31: последний удачный кадр по роли (подмена при срыве съёмки),
        # время последнего предупреждения о срыве и памятка ужатого кадра
        # (кадр потоковой камеры между запросами браузера один и тот же —
        # не пережимаем его каждый раз)
        self._preview_last_ok: dict[CameraRole, tuple[CameraShot, float]] = {}
        self._preview_warned_at: dict[CameraRole, float] = {}
        self._preview_slow_warned_at: dict[CameraRole, float] = {}
        self._preview_small: dict[CameraRole, tuple[datetime, bytes]] = {}
        # поколение набора камер: съёмка, начатая до set_cameras, не кладёт
        # кадр прежней камеры в памятки (иначе он отдавался бы до 10 с)
        self._preview_generation = 0
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
        # доставка ffmpeg с центра по надобности (0.4.33): собирается в
        # build_runtime, цикл запускает run_agent
        self.ffmpeg_provisioner: FfmpegProvisioner | None = None
        self._info = AgentInfo(
            site_name=config.site_name,
            scale_name=config.scale_name,
            indicator_model=config.indicator_model,
            driver_name=config.scale.driver,
            port_label=port_label(config.scale.port, config.scale.baudrate),
            agent_version=agent.__version__,
            center_url=config.center.url,
        )

    @property
    def info(self) -> AgentInfo:
        # порт — живой, из драйвера: центр меняет его на лету
        # (SettingsManager._apply_port), а строка, собранная при старте,
        # устаревала — Кара-Суу 07.09.2026: после перехода на COM4 экран до
        # перезапуска показывал socket://127.0.0.1:4001
        return replace(
            self._info, port_label=port_label(self._driver.port_url, self._driver.baudrate)
        )

    def set_indicator_model(self, model: str) -> None:
        """Подпись индикатора из центра — в шапку и «Оборудование» на лету
        (страница оператора покажет при следующей загрузке)."""
        self._info = replace(self._info, indicator_model=model)

    def scale_state(self) -> ScaleState:
        return self._driver.state

    def center_connected(self) -> bool:
        return self._client.connected

    def manual_allowed(self) -> bool:
        """Правило №3 (нет связи) либо разрешение центра при связи (0.4.28)."""
        return not self._client.connected or self._manual_permit.by_center

    def manual_allowed_by_center(self) -> bool:
        return self._manual_permit.by_center

    def set_manual_allowed(self, allowed: bool) -> None:
        """Снимок настроек центра включает/выключает ручной режим при связи."""
        self._manual_permit.by_center = allowed

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
        self._preview_generation += 1
        self._preview_cache.clear()
        self._preview_last_ok.clear()
        self._preview_small.clear()
        self._preview_warned_at.clear()

    def preview_lock(self, role: CameraRole) -> threading.Lock:
        """Замок съёмки камеры: под ним же идёт проба CameraHealth (0.4.31)."""
        return self._preview_locks.setdefault(role, threading.Lock())

    def preview_interval_ms(self) -> int:
        """Период опроса превью браузером оператора.

        Хоть у одной камеры задан preview_url (лёгкий кадр камера отдаёт
        быстро) или камера потоковая (только RTSP: кадр раз в секунду лежит
        в буфере потока, камера не трогается — 0.4.34) → раз в секунду,
        иначе прежние 2 с: разовая HTTP-съёмка полного кадра чаще не тянет.
        Ограничение 12.08.2026 «не чаще 2 с для RTSP» относилось к разовым
        подключениям ffmpeg, постоянный поток его снял. Значение вшивается
        в страницу при рендере: смена настройки из центра подхватится при
        следующей загрузке страницы оператора.
        """
        fast = any(
            camera.preview_url or camera.rtsp_only for camera in self._preview_cameras.values()
        )
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
                shot = self._shrunk(streamed)
                # кадр потока — тоже «последний удачный»: при переподключении
                # потока буфер протухает на 1–3 с, разовая съёмка занимает
                # секунды — оператору в это окно отдаётся этот кадр, а не
                # «Нет сигнала» (замечание ревью 12.09.2026, опрос раз в секунду)
                self._preview_last_ok[role] = (shot, time.monotonic())
                return shot
        cached = self._preview_cache.get(role)
        now = time.monotonic()
        if cached is not None and now - cached[1] < ttl:
            return self._preview_result(role, cached[0], now)
        lock = self.preview_lock(role)
        if not lock.acquire(blocking=False):
            # съёмка уже идёт в соседнем запросе — не плодим вторую
            if cached is not None:
                return self._preview_result(role, cached[0], now)
            busy = CameraShot(
                role=role, jpeg=None, captured_at=datetime.now(UTC), error="съёмка уже идёт"
            )
            return self._preview_result(role, busy, now)
        try:
            generation = self._preview_generation
            started = time.monotonic()
            shot = capture(camera, ffmpeg_path=self._config.ffmpeg_path)
            taken = time.monotonic()
            if taken - started >= PREVIEW_SLOW_S:
                self._note_slow_preview(role, taken - started, taken)
            if generation != self._preview_generation:
                # камеры сменились, пока шла съёмка: кадр прежней камеры
                # отдаём один раз, в памятки не кладём
                return self._shrunk(shot) if shot.ok else shot
            if shot.ok:
                shot = self._shrunk(shot)
                self._preview_last_ok[role] = (shot, taken)
            else:
                self._warn_preview_failure(role, shot.error, taken)
            # ошибку тоже кэшируем: мёртвая камера не должна заставлять
            # каждый запрос превью висеть полный таймаут съёмки
            self._preview_cache[role] = (shot, taken)
            return self._preview_result(role, shot, taken)
        finally:
            lock.release()

    def _preview_result(self, role: CameraRole, shot: CameraShot, now: float) -> CameraShot:
        """Кадр как есть; при срыве — последний удачный, пока он не старше
        PREVIEW_STALE_MAX_S (единичная осечка камеры оператору не видна)."""
        if shot.ok:
            return shot
        last = self._preview_last_ok.get(role)
        if last is not None and now - last[1] <= PREVIEW_STALE_MAX_S:
            return last[0]
        return shot

    def _shrunk(self, shot: CameraShot) -> CameraShot:
        """Ужатая копия кадра для превью с плашкой «камера · дата время»
        (снимки операций не задеты).

        Плашка со временем съёмки (0.4.32) — чтобы свежесть кадра была видна
        глазами: на Канте картинка «не обновлялась», а по журналу всё было
        200 OK. Памятка по времени съёмки: буфер потоковой камеры отдаёт один
        и тот же кадр между запросами браузера — пережимать его каждый раз
        незачем.
        """
        if shot.jpeg is None:
            return shot
        memo = self._preview_small.get(shot.role)
        if memo is not None and memo[0] == shot.captured_at:
            return replace(shot, jpeg=memo[1])
        small = burn_overlay(
            shrink_preview(shot.jpeg),
            OverlayInfo(role=shot.role, moment=shot.captured_at, weight_kg=None),
        )
        self._preview_small[shot.role] = (shot.captured_at, small)
        return replace(shot, jpeg=small)

    def _note_slow_preview(self, role: CameraRole, seconds: float, now: float) -> None:
        last = self._preview_slow_warned_at.get(role)
        if last is not None and now - last < PREVIEW_WARN_EVERY_S:
            return
        self._preview_slow_warned_at[role] = now
        logger.warning("превью камеры %s: кадр снят за %.1f с", role.value, seconds)

    def _warn_preview_failure(self, role: CameraRole, error: str | None, now: float) -> None:
        last = self._preview_warned_at.get(role)
        if last is not None and now - last < PREVIEW_WARN_EVERY_S:
            return
        self._preview_warned_at[role] = now
        # текст ошибки capture уже начинается с роли («rear: … (url)»)
        logger.warning("превью камеры: %s", error)

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
    driver = create_driver(
        config.scale.driver,
        config.scale.port,
        baudrate=config.scale.baudrate,
        weight_divisor=config.scale.weight_divisor,
        discrete_kg=config.scale.discrete_kg,
    )
    storage = AgentStorage(config.storage.db_path)
    # разрешение ручного режима при связи (0.4.28): восстанавливается из
    # последнего снимка настроек центра, чтобы после рестарта службы объект
    # без АИС не остался без кнопки до следующего hello
    permit = ManualPermit()
    stored_settings = storage.load_center_settings()
    if stored_settings is not None:
        with contextlib.suppress(ValueError):
            permit.by_center = bool(
                ScaleSettingsPayload.model_validate_json(stored_settings).manual_allowed
            )
    # время записей — по часам центра (heartbeat_ack), офлайн — по
    # последнему известному смещению из SQLite (вопрос Игоря 10.08.2026)
    center_clock = CenterClock(storage)
    config.storage.photos_dir.mkdir(parents=True, exist_ok=True)
    # постоянные потоки RTSP-камер (агент 0.4.7): фоновый ffmpeg держит
    # соединение и кладёт свежий кадр в память — превью и снимок операции
    # берут его мгновенно; камерам со снапшотом поток не заводится
    streams = CameraStreams(config.camera_configs(), ffmpeg_path=config.ffmpeg_path)
    # ffmpeg по надобности (0.4.33, урок Канта): камера только с RTSP есть, а
    # ffmpeg на ПК нет — агент скачает его с центра сам, в корень установки
    ffmpeg_provisioner = FfmpegProvisioner(
        base_url=http_base_url(config.center.url),
        token=config.center.token,
        configured_path=config.ffmpeg_path,
        install_dir=install_base(),
        cameras=config.camera_configs(),
    )
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
        # правило №3: ручной режим без связи с центром — либо при связи по
        # разрешению центра (0.4.28, объект без АИС)
        manual_allowed=lambda: not client.connected or permit.by_center,
        # при живой связи команда АИС и ручная фиксация не должны пересечься
        busy=runner.busy,
        storage=storage,
        cameras=config.camera_configs(),
        photos_dir=config.storage.photos_dir,
        vehicle_threshold_kg=config.cycle.vehicle_threshold_kg,
        max_tare_kg=config.cycle.max_tare_kg,
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
        manual_permit=permit,
    )
    # превью подписывается на смену камер из центра ПОСЛЕ создания runtime
    # (менеджер собирается раньше); без подписки превью снимало бы по
    # локальному конфигу до рестарта службы (боевой урок К-К 14.08.2026)
    manager_ref[-1].set_preview(runtime)
    manager_ref[-1].set_info_sink(runtime)
    manager_ref[-1].set_retention(retention)
    manager_ref[-1].set_ffmpeg_provisioner(ffmpeg_provisioner)
    # проба камеры и съёмка превью — под одним замком на роль (0.4.31)
    camera_health.set_capture_lock(runtime.preview_lock)
    runtime.selfcheck = selfcheck
    runtime.retention = retention
    runtime.ffmpeg_provisioner = ffmpeg_provisioner
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
    # доставка ffmpeg с центра по надобности (0.4.33): цикл спит, пока ffmpeg
    # не нужен или уже есть; RTSP-камера из панели его будит
    ffmpeg_provisioner = runtime.ffmpeg_provisioner
    assert ffmpeg_provisioner is not None  # собран в build_runtime
    tasks.append(asyncio.create_task(ffmpeg_provisioner.run(), name="ffmpeg-provisioner"))
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
