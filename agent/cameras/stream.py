"""Постоянный поток RTSP-камеры с буфером последнего кадра (агент 0.4.7).

Зачем (запрос Игоря 12.08.2026, ПЗТК «Джалал-Абад»): камеры без
HTTP-снапшота отдают кадр только через подключение к видеопотоку —
2–5 с на каждое. Разовые подключения дают дёрганое превью у оператора
и секунды ожидания снимка при взвешивании. UniServer решает это
постоянным видеосоединением — делаем так же: фоновый ffmpeg держит
поток камеры и раз в секунду кладёт свежий JPEG в память. Превью и
съёмка операции берут кадр из буфера мгновенно.

Устройство одного потока (CameraStream):
- ffmpeg: ``-rtsp_transport tcp`` (UDP на площадках теряет пакеты),
  ``-timeout`` (сокетный таймаут RTSP-демьюксера; generic ``-rw_timeout``
  до вложенного TCP-сокета rtsp НЕ доезжает — проверено ревью 12.08.2026
  на живом ffmpeg) — молча умершее соединение ffmpeg завершает САМ,
  поэтому сторожевой поток не нужен: читатель получает EOF
  и переподключается с нарастающей паузой;
- кадры режутся из stdout по JPEG-маркерам (ffmpeg кодирует mjpeg без
  EXIF-миниатюр, вложенных маркеров конца в таких кадрах нет);
- кадр кодируется ffmpeg ОДИН раз (то же качество, что разовый снимок,
  правило №2 — дальше байты не пересжимаются);
- буфер хранит последний кадр + монотонное время получения; свежесть
  проверяет читатель через ``latest(max_age_s)``.

Менеджер (CameraStreams) заводит потоки только для камер, у которых
есть ТОЛЬКО rtsp_url: камерам с HTTP-снапшотом (Hikvision Кызыл-Кыи)
поток не нужен — их разовый снимок и так мгновенный. Протухший буфер
(поток оборвался) — не отказ: вызывающий код падает обратно на разовую
съёмку (shot_or_capture / shots_or_capture_all).
"""

import contextlib
import logging
import subprocess
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import IO, Protocol

from agent.cameras.capture import (
    FFMPEG_JPEG_QSCALE,
    CameraConfig,
    CameraShot,
    capture,
    capture_all,
    sanitize_url,
)
from shared.enums import CameraRole

logger = logging.getLogger(__name__)

# кадров в секунду в буфер: 1 — достаточно для превью и снимка операции,
# декодировать больше незачем
STREAM_FPS = 1.0
# сокетный таймаут RTSP (мкс, опция -timeout демьюксера): молчащее
# соединение завершает сам ffmpeg, читатель видит EOF и переподключается —
# отдельный сторож не нужен. НЕ -rw_timeout: он для rtsp не действует
RTSP_TIMEOUT_US = 10_000_000
# паузы между переподключениями: растут вдвое до потолка
RECONNECT_MIN_S = 2.0
RECONNECT_MAX_S = 30.0
# защита буфера разбора от распухания на битом потоке (кадры камер — мегабайты)
MAX_FRAME_BYTES = 20 * 1024 * 1024
# свежесть кадра для снимка операции: кадры идут раз в секунду, допуск
# покрывает паузу декодера; старше — честная разовая съёмка
SHOT_MAX_AGE_S = 3.0

_SOI = b"\xff\xd8"  # начало JPEG
_EOI = b"\xff\xd9"  # конец JPEG


def iter_jpeg_frames(
    stream: IO[bytes], *, max_frame_bytes: int = MAX_FRAME_BYTES
) -> Iterator[bytes]:
    """Резать поток ``image2pipe`` на отдельные JPEG по маркерам.

    Мусор до начала кадра отбрасывается; недочитанный кадр ждёт следующих
    порций; кадр длиннее ``max_frame_bytes`` считается битым потоком и
    отбрасывается целиком (защита памяти).
    """
    buffer = bytearray()
    while True:
        chunk = stream.read(65536)
        if not chunk:
            return
        buffer.extend(chunk)
        while True:
            start = buffer.find(_SOI)
            if start < 0:
                # маркер мог быть разрезан по границе порции — хранить хвост
                del buffer[:-1]
                break
            end = buffer.find(_EOI, start + 2)
            if end < 0:
                if start > 0:
                    del buffer[:start]
                if len(buffer) > max_frame_bytes:
                    buffer.clear()
                break
            yield bytes(buffer[start : end + 2])
            del buffer[: end + 2]


class _ProcessLike(Protocol):
    """Минимум от subprocess.Popen (для подмены в тестах)."""

    stdout: IO[bytes] | None

    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


@dataclass(frozen=True)
class _Frame:
    jpeg: bytes
    captured_at: datetime  # время получения кадра (UTC)
    received_monotonic: float


class CameraStream:
    """Один фоновый поток камеры: процесс ffmpeg + читатель кадров."""

    def __init__(
        self,
        role: CameraRole,
        rtsp_url: str,
        *,
        ffmpeg_path: str = "ffmpeg",
        spawn: Callable[[list[str]], _ProcessLike] | None = None,
    ) -> None:
        self._role = role
        self._url = rtsp_url
        self._ffmpeg_path = ffmpeg_path
        self._spawn = spawn or self._default_spawn
        self._latest: _Frame | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._process: _ProcessLike | None = None
        self._thread = threading.Thread(
            target=self._run, name=f"cam-stream-{role.value}", daemon=True
        )

    @property
    def role(self) -> CameraRole:
        return self._role

    @property
    def url(self) -> str:
        return self._url

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        """Остановить поток; блокируется до выхода читателя (недолго)."""
        self._stop.set()
        self._terminate_process()
        if self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def latest(self, max_age_s: float) -> CameraShot | None:
        """Последний кадр не старше ``max_age_s``; None — буфер пуст/протух."""
        with self._lock:
            frame = self._latest
        if frame is None:
            return None
        if time.monotonic() - frame.received_monotonic > max_age_s:
            return None
        return CameraShot(role=self._role, jpeg=frame.jpeg, captured_at=frame.captured_at)

    # --- внутренности ---

    def _command(self) -> list[str]:
        return [
            self._ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-rtsp_transport",
            "tcp",
            "-timeout",
            str(RTSP_TIMEOUT_US),
            "-i",
            self._url,
            "-vf",
            f"fps={STREAM_FPS}",
            "-q:v",
            FFMPEG_JPEG_QSCALE,
            "-f",
            "image2pipe",
            "pipe:1",
        ]

    @staticmethod
    def _default_spawn(command: list[str]) -> _ProcessLike:
        # команда собрана из конфига агента (ffmpeg_path + rtsp_url)
        return subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )

    def _terminate_process(self) -> None:
        process = self._process
        if process is None:
            return
        with contextlib.suppress(Exception):
            process.terminate()
            try:
                process.wait(timeout=3.0)
            except Exception:
                process.kill()
                with contextlib.suppress(Exception):
                    process.wait(timeout=3.0)

    def _run(self) -> None:
        pause_s = RECONNECT_MIN_S
        while not self._stop.is_set():
            got_frames = False
            try:
                self._process = self._spawn(self._command())
                stdout = self._process.stdout
                if stdout is None:
                    raise RuntimeError("у процесса ffmpeg нет stdout")
                for jpeg in iter_jpeg_frames(stdout):
                    with self._lock:
                        self._latest = _Frame(
                            jpeg=jpeg,
                            captured_at=datetime.now(UTC),
                            received_monotonic=time.monotonic(),
                        )
                    got_frames = True
                    pause_s = RECONNECT_MIN_S  # поток жив — пауза сброшена
                    if self._stop.is_set():
                        break
            except Exception as exc:
                if not self._stop.is_set():
                    logger.warning(
                        "поток камеры %s: %s (%s)",
                        self._role.value,
                        exc,
                        sanitize_url(self._url),
                    )
            finally:
                self._terminate_process()
                self._process = None
            if self._stop.is_set():
                return
            # EOF без кадров — камера молчит/недоступна: пауза растёт;
            # обрыв живого потока — переподключение почти сразу
            if not got_frames:
                logger.warning(
                    "поток камеры %s оборвался без кадров, пауза %.0f с (%s)",
                    self._role.value,
                    pause_s,
                    sanitize_url(self._url),
                )
            if self._stop.wait(timeout=pause_s if not got_frames else RECONNECT_MIN_S):
                return
            pause_s = min(pause_s * 2, RECONNECT_MAX_S)


class CameraStreams:
    """Потоки всех RTSP-камер агента; живёт от старта до остановки службы."""

    def __init__(
        self,
        cameras: list[CameraConfig],
        *,
        ffmpeg_path: str = "ffmpeg",
        stream_factory: Callable[..., CameraStream] = CameraStream,
    ) -> None:
        self._ffmpeg_path = ffmpeg_path
        self._factory = stream_factory
        self._streams: dict[CameraRole, CameraStream] = {}
        self._lock = threading.Lock()
        self.set_cameras(cameras)

    @staticmethod
    def is_streamable(camera: CameraConfig) -> bool:
        """Поток нужен камерам, у которых кадр берётся ТОЛЬКО из RTSP."""
        return bool(camera.rtsp_url) and not camera.snapshot_url

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Синхронизировать потоки со списком камер (настройки центра).

        Неизменившиеся потоки не трогаются; исчезнувшие/изменившие URL —
        останавливаются; новые — запускаются.
        """
        wanted = {
            camera.role: camera.rtsp_url
            for camera in cameras
            if self.is_streamable(camera) and camera.rtsp_url
        }
        # stop() джойнит читателя (секунды) — вне лока, чтобы не морозить
        # shot() превью и съёмки операций (замечание ревью 12.08.2026)
        stale: list[CameraStream] = []
        with self._lock:
            for role in list(self._streams):
                if wanted.get(role) != self._streams[role].url:
                    stale.append(self._streams.pop(role))
            for role, url in wanted.items():
                if role not in self._streams:
                    stream = self._factory(role, url, ffmpeg_path=self._ffmpeg_path)
                    stream.start()
                    self._streams[role] = stream
        for stream in stale:
            stream.stop()

    def shot(self, role: CameraRole, *, max_age_s: float = SHOT_MAX_AGE_S) -> CameraShot | None:
        """Свежий кадр из буфера потока; None — камера не потоковая/буфер протух."""
        with self._lock:
            stream = self._streams.get(role)
        if stream is None:
            return None
        return stream.latest(max_age_s)

    def stop_all(self) -> None:
        with self._lock:
            streams = list(self._streams.values())
            self._streams.clear()
        for stream in streams:
            stream.stop()


def shot_or_capture(
    camera: CameraConfig,
    streams: "CameraStreams | None",
    *,
    ffmpeg_path: str,
    max_age_s: float = SHOT_MAX_AGE_S,
) -> CameraShot:
    """Кадр из буфера потока, а при его отсутствии — разовая съёмка."""
    if streams is not None:
        shot = streams.shot(camera.role, max_age_s=max_age_s)
        if shot is not None:
            return shot
    return capture(camera, ffmpeg_path=ffmpeg_path)


def shots_or_capture_all(
    cameras: list[CameraConfig],
    streams: "CameraStreams | None",
    *,
    ffmpeg_path: str,
    max_age_s: float = SHOT_MAX_AGE_S,
) -> list[CameraShot]:
    """Кадры всех камер: потоковые — из буфера, остальные — параллельной съёмкой.

    Порядок результатов повторяет ``cameras`` (контракт capture_all).
    """
    buffered: dict[CameraRole, CameraShot] = {}
    to_capture: list[CameraConfig] = []
    if streams is not None:
        for camera in cameras:
            shot = streams.shot(camera.role, max_age_s=max_age_s)
            if shot is not None:
                buffered[camera.role] = shot
            else:
                to_capture.append(camera)
    else:
        to_capture = list(cameras)
    captured = {shot.role: shot for shot in capture_all(to_capture, ffmpeg_path=ffmpeg_path)}
    return [buffered.get(camera.role) or captured[camera.role] for camera in cameras]
