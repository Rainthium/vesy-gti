"""Снимки с IP-камер в момент фиксации веса (architecture §3.1, §3.4).

Два способа получить кадр:
- **HTTP-snapshot** — если камера отдаёт готовый JPEG по HTTP (у Hikvision
  это `/ISAPI/Streaming/channels/101/picture`). Предпочтительный путь:
  JPEG приходит «как есть» с камеры, без перекодирования.
- **Кадр из RTSP** — через ffmpeg: один кадр основного потока в родном
  разрешении. JPEG кодируется один раз в момент снимка и далее неизменен
  (правило проекта №2: после сохранения фото не пересжимаются никогда).

Ошибка камеры не срывает взвешивание: модуль возвращает результат по
каждой камере отдельно, вызывающий код отклоняет операцию (ERR_CAMERA),
но вес отдаёт (architecture §4.1).

Учётные данные камер приходят в URL из конфига агента (вне git,
правило №7); в логи и сообщения об ошибках URL пишется без пароля.
"""

import base64
import concurrent.futures
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime

from shared.enums import CameraRole

JPEG_MAGIC = b"\xff\xd8"
# Урок Джалал-Абада (12.08.2026): прежних 5 с не хватало — запуск ffmpeg.exe
# (~100 МБ) с HDD под антивирусом плюс ожидание ключевого кадра RTSP-потока
# занимают больше. Для живой камеры это верхняя граница, а не задержка.
DEFAULT_TIMEOUT_S = 15.0
# качество однократного кодирования кадра из RTSP (шкала ffmpeg/mjpeg 2..31,
# меньше — лучше; 4 ≈ JPEG q85 из architecture «Хранение фото»).
# Снятый JPEG далее неизменен (правило №2).
FFMPEG_JPEG_QSCALE = "4"


@dataclass(frozen=True)
class CameraConfig:
    """Настройки одной камеры (конфиг агента).

    Если задан ``snapshot_url`` — используется HTTP-snapshot,
    иначе ``rtsp_url`` через ffmpeg. Хотя бы один должен быть задан.

    ``preview_url`` — необязательный лёгкий кадр ТОЛЬКО для превью
    оператора (обычно HTTP-снапшот суб-потока камеры: у Hikvision
    ``channels/102/picture``). Фото операций всегда снимаются по
    основному URL в полном качестве (правило №2 не задето).
    """

    role: CameraRole
    snapshot_url: str | None = None
    rtsp_url: str | None = None
    preview_url: str | None = None
    timeout_s: float = DEFAULT_TIMEOUT_S

    def __post_init__(self) -> None:
        if not self.snapshot_url and not self.rtsp_url:
            raise ValueError(f"камера {self.role}: не задан ни snapshot_url, ни rtsp_url")


@dataclass(frozen=True)
class CameraShot:
    """Результат снимка одной камеры: JPEG либо текст ошибки."""

    role: CameraRole
    jpeg: bytes | None
    captured_at: datetime
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.jpeg is not None


def sanitize_url(url: str) -> str:
    """Убрать пароль из URL для логов и сообщений об ошибках.

    Никогда не выбрасывает исключение: вызывается из обработчиков ошибок,
    и даже битый URL из конфига (нечисловой порт и т.п.) не должен
    сорвать взвешивание.
    """
    try:
        parts = urllib.parse.urlsplit(url)
        if parts.password is None and parts.username is None:
            return url
        host = parts.hostname or ""
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        netloc = f"{parts.username or ''}:***@{host}"
        return urllib.parse.urlunsplit(parts._replace(netloc=netloc))
    except ValueError:
        # URL не разбирается — маскируем всё, что похоже на учётные данные
        return re.sub(r"//[^@/]*@", "//***@", url)


def _http_snapshot(url: str, timeout_s: float) -> bytes:
    """Получить JPEG по HTTP; учётные данные из URL — Basic сразу, Digest по challenge.

    Basic отправляется превентивно первым же запросом (камеры без challenge,
    лишнего круга обмена нет). Hikvision ISAPI по заводской настройке
    принимает ТОЛЬКО Digest (проверено на Кызыл-Кые 10.08.2026): на 401
    с Digest-challenge запрос повторяет HTTPDigestAuthHandler — его
    Authorization кладётся в unredirected_hdrs и вытесняет Basic.
    """
    parts = urllib.parse.urlsplit(url)
    headers = {}
    handlers: list[urllib.request.BaseHandler] = []
    if parts.username is not None:
        # urllib не использует учётные данные из URL сам — переносим в заголовок;
        # percent-encoding раскрываем: пароль с @/: задаётся в конфиге закодированным
        username = urllib.parse.unquote(parts.username)
        password = urllib.parse.unquote(parts.password or "")
        credentials = f"{username}:{password}"
        headers["Authorization"] = "Basic " + base64.b64encode(credentials.encode()).decode()
        host = parts.hostname or ""
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        url = urllib.parse.urlunsplit(parts._replace(netloc=host))
        manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        manager.add_password(None, url, username, password)
        handlers.append(urllib.request.HTTPDigestAuthHandler(manager))

    opener = urllib.request.build_opener(*handlers)
    request = urllib.request.Request(url, headers=headers)
    with opener.open(request, timeout=timeout_s) as response:
        data: bytes = response.read()
    if not data.startswith(JPEG_MAGIC):
        raise ValueError(f"ответ не является JPEG ({len(data)} байт)")
    return data


def _rtsp_frame(url: str, timeout_s: float, ffmpeg_path: str) -> bytes:
    """Взять один кадр из RTSP-потока через ffmpeg (родное разрешение)."""
    command = [
        ffmpeg_path,
        "-hide_banner",
        "-loglevel",
        "error",
        "-rtsp_transport",
        "tcp",  # UDP на площадках теряет пакеты — только TCP
        "-i",
        url,
        "-frames:v",
        "1",
        "-q:v",
        FFMPEG_JPEG_QSCALE,
        "-f",
        "image2",
        "pipe:1",
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"ffmpeg не уложился в {timeout_s} с") from exc
    except FileNotFoundError as exc:
        raise RuntimeError(f"ffmpeg не найден: {ffmpeg_path}") from exc

    if completed.returncode != 0 or not completed.stdout:
        stderr = completed.stderr.decode("utf-8", "replace").strip()
        # ffmpeg печатает входной URL целиком, вместе с логином и паролем
        # камеры; текст ошибки уходит в лог, а лог теперь виден оператору
        # на экране «Диагностика» (замечание ревью 11.08.2026)
        safe = stderr.replace(url, sanitize_url(url))
        raise RuntimeError(f"ffmpeg завершился с кодом {completed.returncode}: {safe[-300:]}")
    if not completed.stdout.startswith(JPEG_MAGIC):
        raise ValueError("вывод ffmpeg не является JPEG")
    return completed.stdout


def capture(config: CameraConfig, *, ffmpeg_path: str = "ffmpeg") -> CameraShot:
    """Снять кадр с одной камеры; ошибки не выбрасываются, а возвращаются."""
    captured_at = datetime.now(UTC)
    url = config.snapshot_url or config.rtsp_url
    assert url is not None  # гарантировано валидацией CameraConfig
    try:
        if config.snapshot_url:
            jpeg = _http_snapshot(config.snapshot_url, config.timeout_s)
        else:
            jpeg = _rtsp_frame(url, config.timeout_s, ffmpeg_path)
    except Exception as exc:
        return CameraShot(
            role=config.role,
            jpeg=None,
            captured_at=captured_at,
            error=f"{config.role}: {exc} ({sanitize_url(url)})",
        )
    return CameraShot(role=config.role, jpeg=jpeg, captured_at=captured_at)


def capture_all(configs: list[CameraConfig], *, ffmpeg_path: str = "ffmpeg") -> list[CameraShot]:
    """Снять кадры со всех камер параллельно (момент фиксации веса один).

    Возвращает результаты в порядке ``configs``; ошибка одной камеры
    не мешает остальным.
    """
    if not configs:
        return []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(configs)) as pool:
        return list(pool.map(lambda cfg: capture(cfg, ffmpeg_path=ffmpeg_path), configs))
