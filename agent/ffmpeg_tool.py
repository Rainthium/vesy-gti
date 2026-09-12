"""ffmpeg на весовом ПК: наличие и доставка с центра по надобности (агент 0.4.33).

Урок Канта 12.09.2026: три камеры объекта не отдают весовому ПК HTTP-снимок
(сеть объекта; с ВМ центра те же камеры отвечают за 0,15 с), а RTSP-видео
с них идёт — UniServer на том же ПК показывает картинку. Перевести камеры
на RTSP-поток из панели можно за минуту, но потоки держит ffmpeg, а его
в мастер-папке Канта не было (камеры отдавали снимки), и зайти на ПК
нельзя. Класть ffmpeg (100 МБ) в каждый релиз — раздавать его всем
объектам, где он не нужен. Поэтому агент берёт ffmpeg сам и только
по надобности:

- **где искать** — ``agent.cameras.capture.resolve_ffmpeg_path``: голое имя
  из конфига (``ffmpeg``) заменяется на ``ffmpeg.exe`` из корня установки
  (родитель ``app/``, там же config.toml), если он там лежит; обновления
  подменяют только ``app/``, корень не трогают. Явный путь из конфига
  (Джалал-Абад: ``D:/vesy-agent/ffmpeg.exe``) — как есть. Путь разрешается
  при КАЖДОМ запуске ffmpeg: появившийся файл подхватывается на следующем
  переподключении потока, без рестарта службы;
- **откуда взять** — ``FfmpegProvisioner``: есть камера только с RTSP,
  а ffmpeg не найден — скачать ``ffmpeg.exe`` из каталога инструментов
  центра (``/agents/tools/``, рядом с релизами, тот же Bearer-токен агента,
  center/tools_router.py), сверить sha256 из заголовка ответа и размер,
  положить в корень установки атомарно (``.part`` → rename). Не получилось —
  повтор с нарастающей паузой, пока не получится. В dev-запуске (не frozen,
  корня установки нет) ничего не качается: ffmpeg ставится руками в PATH.
"""

import asyncio
import contextlib
import hashlib
import logging
import os
import re
import shutil
import urllib.request
from collections.abc import Callable, Iterable
from pathlib import Path

from agent.cameras.capture import (
    FFMPEG_TOOL_FILENAME,
    CameraConfig,
    is_bare_name,
    resolve_ffmpeg_path,
)

logger = logging.getLogger(__name__)

TOOL_URL_PATH = f"/agents/tools/{FFMPEG_TOOL_FILENAME}"
SHA256_HEADER = "X-Sha256"
# 100 МБ через туннель объекта: верхняя граница, а не ожидаемое время
DOWNLOAD_TIMEOUT_S = 600.0
# паузы между попытками: растут вдвое до потолка
RETRY_MIN_S = 30.0
RETRY_MAX_S = 600.0
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# загрузчик: (url, token, куда положить) → (размер, sha256); подменяется в тестах
Downloader = Callable[[str, str, Path], tuple[int, str]]


class ToolError(Exception):
    """Инструмент с центра не получен или не прошёл проверку (текст — в лог)."""


def needs_ffmpeg(cameras: Iterable[CameraConfig]) -> bool:
    """Нужен ли ffmpeg этому набору камер: хотя бы одна берёт кадр только из RTSP."""
    return any(camera.rtsp_only for camera in cameras)


def ffmpeg_available(configured: str, root: Path | None) -> bool:
    """Найдётся ли ffmpeg при запуске: файл по разрешённому пути или в PATH."""
    resolved = resolve_ffmpeg_path(configured, root)
    if Path(resolved).is_file():
        return True
    return shutil.which(resolved) is not None


def download_tool(url: str, token: str, target: Path) -> tuple[int, str]:
    """Скачать файл инструмента с центра в ``target`` атомарно, сверив sha256 и размер.

    Контрольную сумму центр присылает заголовком ``X-Sha256``; без неё файл
    отвергается — подменённый или битый ffmpeg не должен попасть на ПК.
    Хвост ``.part`` после любого исхода не остаётся.
    """
    part = target.with_name(target.name + ".part")
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    digest = hashlib.sha256()
    size = 0
    try:
        with (
            urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_S) as response,
            part.open("wb") as out,
        ):
            expected = (response.headers.get(SHA256_HEADER) or "").strip().lower()
            if _SHA256_RE.match(expected) is None:
                raise ToolError("центр не прислал контрольную сумму файла")
            declared = response.headers.get("Content-Length")
            while chunk := response.read(1 << 20):
                out.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if declared is not None and int(declared) != size:
            raise ToolError(f"размер файла {size} != заявленных {declared}")
        if digest.hexdigest() != expected:
            raise ToolError("sha256 файла не совпал — файл отвергнут")
        os.replace(part, target)
    finally:
        part.unlink(missing_ok=True)
    return size, expected


class FfmpegProvisioner:
    """Фоновая доставка ffmpeg с центра: качает, когда он нужен и его нет."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        configured_path: str,
        install_dir: Path | None,
        cameras: Iterable[CameraConfig] = (),
        download: Downloader = download_tool,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._configured = configured_path
        # корень установки (родитель app/); None — dev-запуск, качать некуда
        self._install_dir = install_dir
        self._download = download
        self._needed = needs_ffmpeg(cameras)
        self._wake = asyncio.Event()
        self.attempts = 0  # число попыток скачивания — для диагностики и тестов
        self._warned_explicit = False  # про явный ffmpeg_path без файла говорим один раз

    @property
    def needed(self) -> bool:
        """Есть ли сейчас камера, которой нужен ffmpeg."""
        return self._needed

    def set_cameras(self, cameras: Iterable[CameraConfig]) -> None:
        """Новый список камер (настройки центра): пересчитать надобность, разбудить цикл."""
        self._needed = needs_ffmpeg(cameras)
        self._wake.set()

    def missing(self) -> bool:
        """ffmpeg нужен, а найти его при запуске не удастся."""
        return self._needed and not ffmpeg_available(self._configured, self._install_dir)

    async def run(self) -> None:
        """Цикл службы: спит, пока ffmpeg не нужен или уже есть; иначе качает с повторами."""
        pause_s = RETRY_MIN_S
        while True:
            self._wake.clear()
            if self._install_dir is None or not self.missing():
                pause_s = RETRY_MIN_S
                await self._wake.wait()
                continue
            if not is_bare_name(self._configured):
                # явный путь в конфиге (Джалал-Абад) файлом из корня не удовлетворить:
                # качать бессмысленно — сказать один раз и ждать смены камер
                if not self._warned_explicit:
                    self._warned_explicit = True
                    logger.warning(
                        "ffmpeg_path задан явно (%s), файла нет — доставка с центра не поможет, "
                        "поправьте config.toml",
                        self._configured,
                    )
                await self._wake.wait()
                continue
            target = self._install_dir / FFMPEG_TOOL_FILENAME
            self.attempts += 1
            try:
                size, sha256 = await asyncio.to_thread(
                    self._download, self._base_url + TOOL_URL_PATH, self._token, target
                )
            except Exception as exc:
                logger.warning("ffmpeg с центра не получен: %s — повтор через %.0f с", exc, pause_s)
                await self._pause(pause_s)
                pause_s = min(pause_s * 2, RETRY_MAX_S)
                continue
            if self.missing():
                # файл записан, но при запуске не найдётся (убран антивирусом?) —
                # не штормить центр загрузками по 100 МБ (замечание ревью 12.09.2026)
                logger.warning(
                    "ffmpeg получен (%s), но при запуске не найдётся — возможно, убран "
                    "антивирусом; повтор через %.0f с",
                    target,
                    pause_s,
                )
                await self._pause(pause_s)
                pause_s = min(pause_s * 2, RETRY_MAX_S)
                continue
            logger.info(
                "ffmpeg получен с центра: %d МБ, sha256 %s… → %s; "
                "потоки RTSP-камер поднимутся сами",
                size >> 20,
                sha256[:12],
                target,
            )
            pause_s = RETRY_MIN_S

    async def _pause(self, seconds: float) -> None:
        """Подождать паузу; новые камеры из центра будят раньше (надобность могла отпасть)."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
