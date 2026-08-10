"""Тесты модуля снимков с IP-камер (agent/cameras/capture.py).

Реальные камеры и сеть наружу не используются:
- HTTP-snapshot проверяется через локальный ``http.server`` в фоновом
  потоке (порт выделяет ОС);
- путь RTSP/ffmpeg — через фейковый исполняемый sh-скрипт вместо ffmpeg
  (печатает нужные байты в stdout, пишет в stderr, спит, падает).

Ключевые инварианты: ``capture`` никогда не выбрасывает исключение,
JPEG отдаётся байт-в-байт без перекодирования (правило проекта №2),
пароль из URL не попадает в тексты ошибок (правило №7).
"""

import base64
import hashlib
import http.server
import itertools
import re
import socket
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from agent.cameras.capture import CameraConfig, CameraShot, capture, capture_all, sanitize_url
from shared.enums import CameraRole

# Валидный с точки зрения модуля JPEG: магия FF D8 + произвольное тело
JPEG_BODY = b"\xff\xd8\xff\xe0" + b"fake-jpeg-payload" + b"\xff\xd9"

# Записанный сервером запрос: (путь, заголовки)
RecordedRequest = tuple[str, dict[str, str]]


# --- фикстуры ---


def _make_handler(requests: list[RecordedRequest]) -> type[http.server.BaseHTTPRequestHandler]:
    """Обработчик тестового HTTP-сервера: маршруты-сценарии, запись запросов."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.path, dict(self.headers.items())))
            try:
                if self.path == "/ok.jpg":
                    self._send(200, JPEG_BODY)
                elif self.path == "/nonjpeg":
                    self._send(200, b"<html>not a jpeg</html>")
                elif self.path == "/status/401":
                    self._send(401, b"Unauthorized")
                elif self.path == "/digest.jpg":
                    # имитация Hikvision ISAPI: Basic отвергается, принимается
                    # только корректный Digest (MD5, qop=auth) с верным хешем
                    auth = self.headers.get("Authorization", "")
                    if auth.startswith("Digest ") and self._digest_valid(auth):
                        self._send(200, JPEG_BODY)
                    else:
                        self.send_response(401)
                        self.send_header(
                            "WWW-Authenticate",
                            'Digest realm="cam", nonce="abc123", qop="auth"',
                        )
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                elif self.path == "/status/500":
                    self._send(500, b"Internal Server Error")
                elif self.path == "/slow":
                    # дольше клиентского таймаута в тесте (0.3 с)
                    time.sleep(1.5)
                    self._send(200, JPEG_BODY)
                else:
                    self._send(404, b"Not Found")
            except BrokenPipeError:
                # клиент отвалился по таймауту — для сервера это не ошибка
                pass

        def _digest_valid(self, auth: str) -> bool:
            """Проверить Digest-ответ клиента по RFC 2617 (admin/secret)."""
            fields = dict(
                (m.group(1), m.group(2) or m.group(3))
                for m in re.finditer(r'(\w+)=(?:"([^"]*)"|([^",\s]+))', auth)
            )
            ha1 = hashlib.md5(b"admin:cam:secret").hexdigest()
            ha2 = hashlib.md5(b"GET:/digest.jpg").hexdigest()
            expected = hashlib.md5(
                (
                    f"{ha1}:abc123:{fields.get('nc', '')}:{fields.get('cnonce', '')}:auth:{ha2}"
                ).encode()
            ).hexdigest()
            return fields.get("response") == expected

        def _send(self, code: int, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            # не засорять вывод pytest логами сервера
            pass

    return Handler


@pytest.fixture
def http_camera() -> Iterator[tuple[str, list[RecordedRequest]]]:
    """Локальный HTTP-сервер, имитирующий snapshot-эндпоинт камеры.

    Возвращает базовый URL и список записанных запросов (путь, заголовки).
    """
    requests: list[RecordedRequest] = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(requests))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requests
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def fake_ffmpeg(tmp_path: Path) -> Callable[[str], str]:
    """Фабрика фейкового ffmpeg: sh-скрипт с заданным телом вместо бинарника."""
    counter = itertools.count()

    def factory(body: str) -> str:
        path = tmp_path / f"ffmpeg_{next(counter)}"
        path.write_text(f"#!/bin/sh\n{body}\n")
        path.chmod(0o755)
        return str(path)

    return factory


def free_port() -> int:
    """Порт, который заведомо никто не слушает (выделили и сразу закрыли)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# --- CameraConfig ---


class TestCameraConfig:
    def test_no_urls_raises(self) -> None:
        """Без обоих URL конфиг невалиден: снимать неоткуда."""
        with pytest.raises(ValueError, match="snapshot_url"):
            CameraConfig(role=CameraRole.FRONT)

    def test_snapshot_url_only_is_valid(self) -> None:
        """Достаточно одного snapshot_url."""
        config = CameraConfig(role=CameraRole.FRONT, snapshot_url="http://cam/snap.jpg")
        assert config.rtsp_url is None
        assert config.timeout_s == 5.0

    def test_rtsp_url_only_is_valid(self) -> None:
        """Достаточно одного rtsp_url."""
        config = CameraConfig(role=CameraRole.REAR, rtsp_url="rtsp://cam/stream1")
        assert config.snapshot_url is None


# --- sanitize_url ---


class TestSanitizeUrl:
    def test_user_and_password_masked(self) -> None:
        """Пароль заменяется на ***, логин остаётся."""
        url = "rtsp://admin:secret@10.0.0.5/stream1"
        assert sanitize_url(url) == "rtsp://admin:***@10.0.0.5/stream1"
        assert "secret" not in sanitize_url(url)

    def test_username_only_masked(self) -> None:
        """URL только с логином тоже маскируется (лишним не будет)."""
        assert sanitize_url("http://admin@cam.local/snap") == "http://admin:***@cam.local/snap"

    def test_no_credentials_returned_as_is(self) -> None:
        """URL без учётных данных возвращается без изменений."""
        url = "http://cam.local:8080/snap.jpg?channel=1"
        assert sanitize_url(url) == url

    def test_port_and_path_preserved(self) -> None:
        """Порт, путь и query сохраняются при маскировании."""
        url = "rtsp://user:pw@10.0.0.5:554/streaming/channels/101?tcp=1"
        assert sanitize_url(url) == "rtsp://user:***@10.0.0.5:554/streaming/channels/101?tcp=1"


# --- HTTP-snapshot через публичный capture ---


class TestHttpSnapshot:
    def test_success_returns_body_byte_for_byte(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Успешный снимок: JPEG равен телу ответа байт-в-байт (без перекодирования)."""
        base, _ = http_camera
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{base}/ok.jpg"))
        assert shot.ok
        assert shot.error is None
        assert shot.jpeg == JPEG_BODY
        assert shot.role is CameraRole.FRONT

    def test_captured_at_is_utc(self, http_camera: tuple[str, list[RecordedRequest]]) -> None:
        """Момент снимка заполнен и лежит в UTC."""
        base, _ = http_camera
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{base}/ok.jpg"))
        assert shot.captured_at.tzinfo == UTC
        assert abs((datetime.now(UTC) - shot.captured_at).total_seconds()) < 30

    def test_non_jpeg_body_is_error(self, http_camera: tuple[str, list[RecordedRequest]]) -> None:
        """Тело без магии FF D8 — ошибка, а не «как бы фото»."""
        base, _ = http_camera
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{base}/nonjpeg"))
        assert not shot.ok
        assert shot.jpeg is None
        assert shot.error is not None and "JPEG" in shot.error

    @pytest.mark.parametrize("status", [401, 500])
    def test_http_error_status_is_error(
        self, http_camera: tuple[str, list[RecordedRequest]], status: int
    ) -> None:
        """Ошибки HTTP (401 — неверные креды, 500 — сбой камеры) не выбрасываются."""
        base, _ = http_camera
        shot = capture(CameraConfig(role=CameraRole.REAR, snapshot_url=f"{base}/status/{status}"))
        assert not shot.ok
        assert shot.error is not None and str(status) in shot.error

    def test_unreachable_port_is_error(self) -> None:
        """Камера недоступна (порт никто не слушает) — ошибка, не исключение."""
        url = f"http://127.0.0.1:{free_port()}/snap.jpg"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url, timeout_s=1.0))
        assert not shot.ok
        assert shot.error

    def test_slow_response_hits_timeout(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Сервер отвечает дольше timeout_s — снимок завершается ошибкой таймаута."""
        base, _ = http_camera
        config = CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{base}/slow", timeout_s=0.3)
        started = time.monotonic()
        shot = capture(config)
        elapsed = time.monotonic() - started
        assert not shot.ok
        assert shot.error
        assert elapsed < 1.4  # не дождались полных 1.5 с сна сервера

    def test_url_credentials_become_basic_header(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Креды из URL уходят в заголовок Authorization, а не в строку запроса."""
        base, requests = http_camera
        url_with_creds = base.replace("http://", "http://admin:secret@") + "/ok.jpg"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url_with_creds))
        assert shot.ok
        assert shot.jpeg == JPEG_BODY
        path, headers = requests[-1]
        # в пути запроса кредов нет
        assert path == "/ok.jpg"
        assert "secret" not in path
        expected = "Basic " + base64.b64encode(b"admin:secret").decode()
        assert headers.get("Authorization") == expected

    def test_percent_encoded_credentials_are_decoded(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Пароль со спецсимволами (@/:) задаётся в URL закодированным
        и раскрывается перед сборкой Basic-заголовка."""
        base, requests = http_camera
        # пароль p@ss:w кодируется в конфиге как p%40ss%3Aw
        url = base.replace("http://", "http://admin:p%40ss%3Aw@") + "/ok.jpg"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url))
        assert shot.ok
        _, headers = requests[-1]
        expected = "Basic " + base64.b64encode(b"admin:p@ss:w").decode()
        assert headers.get("Authorization") == expected

    def test_digest_only_camera_works(self, http_camera: tuple[str, list[RecordedRequest]]) -> None:
        """Камера, принимающая только Digest (Hikvision ISAPI): первый запрос
        уходит с превентивным Basic, на 401-challenge повтор идёт с Digest."""
        base, requests = http_camera
        url = base.replace("http://", "http://admin:secret@") + "/digest.jpg"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url))
        assert shot.ok
        assert shot.jpeg == JPEG_BODY
        first_auth = requests[-2][1].get("Authorization", "")
        retry_auth = requests[-1][1].get("Authorization", "")
        assert first_auth.startswith("Basic ")
        assert retry_auth.startswith("Digest ")
        # Basic не должен уехать вместе с Digest в повторном запросе
        assert "Basic" not in retry_auth

    def test_digest_wrong_password_is_error(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Неверный пароль на digest-камере — ошибка 401 без пароля в тексте."""
        base, _ = http_camera
        url = base.replace("http://", "http://admin:wrongpass@") + "/digest.jpg"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url))
        assert not shot.ok
        assert shot.error is not None and "401" in shot.error
        assert "wrongpass" not in shot.error

    def test_error_text_has_no_password(
        self, http_camera: tuple[str, list[RecordedRequest]]
    ) -> None:
        """Пароль из snapshot_url не попадает в текст ошибки (правило №7)."""
        base, _ = http_camera
        url = base.replace("http://", "http://admin:secret@") + "/status/500"
        shot = capture(CameraConfig(role=CameraRole.FRONT, snapshot_url=url))
        assert not shot.ok
        assert shot.error is not None
        assert "secret" not in shot.error
        assert "***" in shot.error


# --- RTSP через фейковый ffmpeg ---


class TestRtspFrame:
    def test_success_and_command_arguments(
        self, fake_ffmpeg: Callable[[str], str], tmp_path: Path
    ) -> None:
        """Успех: JPEG из stdout; ffmpeg вызван с TCP-транспортом и одним кадром."""
        args_file = tmp_path / "argv.txt"
        # \377\330 — октальная запись магии JPEG FF D8 для printf
        ffmpeg = fake_ffmpeg(
            f"printf '%s\\n' \"$@\" > \"{args_file}\"\nprintf '\\377\\330\\377\\340fake-frame'"
        )
        url = "rtsp://10.0.0.5:554/stream1"
        shot = capture(CameraConfig(role=CameraRole.REAR, rtsp_url=url), ffmpeg_path=ffmpeg)
        assert shot.ok
        assert shot.jpeg == b"\xff\xd8\xff\xe0fake-frame"

        args = args_file.read_text().splitlines()
        # только TCP: UDP на площадках теряет пакеты
        i = args.index("-rtsp_transport")
        assert args[i + 1] == "tcp"
        # ровно один кадр
        j = args.index("-frames:v")
        assert args[j + 1] == "1"
        # URL передан как есть
        assert url in args

    def test_nonzero_exit_reports_stderr(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """Ненулевой код выхода — ошибка с текстом stderr от ffmpeg."""
        ffmpeg = fake_ffmpeg("echo 'connection refused' >&2\nexit 1")
        shot = capture(
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            ffmpeg_path=ffmpeg,
        )
        assert not shot.ok
        assert shot.error is not None
        assert "connection refused" in shot.error

    def test_empty_stdout_is_error(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """Код 0, но кадра нет (пустой stdout) — это ошибка, а не пустое фото."""
        ffmpeg = fake_ffmpeg("exit 0")
        shot = capture(
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            ffmpeg_path=ffmpeg,
        )
        assert not shot.ok
        assert shot.jpeg is None
        assert shot.error

    def test_non_jpeg_stdout_is_error(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """stdout без магии JPEG (например, текст ошибки) — ошибка."""
        ffmpeg = fake_ffmpeg("printf 'not a jpeg at all'")
        shot = capture(
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            ffmpeg_path=ffmpeg,
        )
        assert not shot.ok
        assert shot.error is not None and "JPEG" in shot.error

    def test_hanging_ffmpeg_hits_timeout(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """Зависший ffmpeg убивается по timeout_s, снимок — ошибка таймаута."""
        ffmpeg = fake_ffmpeg("sleep 60")
        config = CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1", timeout_s=0.5)
        started = time.monotonic()
        shot = capture(config, ffmpeg_path=ffmpeg)
        elapsed = time.monotonic() - started
        assert not shot.ok
        assert shot.error is not None and "0.5" in shot.error
        assert elapsed < 5  # не ждали 60 с

    def test_missing_ffmpeg_binary_is_error(self, tmp_path: Path) -> None:
        """Несуществующий путь к ffmpeg — понятная ошибка «не найден»."""
        shot = capture(
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            ffmpeg_path=str(tmp_path / "no-such-ffmpeg"),
        )
        assert not shot.ok
        assert shot.error is not None and "не найден" in shot.error

    def test_error_text_has_no_password(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """Пароль из rtsp_url не попадает в текст ошибки (правило №7)."""
        ffmpeg = fake_ffmpeg("exit 1")
        shot = capture(
            CameraConfig(role=CameraRole.REAR, rtsp_url="rtsp://admin:secret@10.0.0.5/s1"),
            ffmpeg_path=ffmpeg,
        )
        assert not shot.ok
        assert shot.error is not None
        assert "secret" not in shot.error
        assert "***" in shot.error

    def test_captured_at_is_utc(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """captured_at заполняется в UTC и для ошибочного снимка тоже."""
        ffmpeg = fake_ffmpeg("exit 1")
        shot = capture(
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            ffmpeg_path=ffmpeg,
        )
        assert shot.captured_at.tzinfo == UTC


# --- capture_all ---


class TestCaptureAll:
    def test_empty_configs_returns_empty_list(self) -> None:
        """Пустой список камер — пустой результат, без исключений."""
        assert capture_all([]) == []

    def test_order_matches_configs_and_error_is_isolated(
        self,
        http_camera: tuple[str, list[RecordedRequest]],
        fake_ffmpeg: Callable[[str], str],
    ) -> None:
        """Порядок результатов совпадает с configs; сбой одной камеры не мешает другой."""
        base, _ = http_camera
        failing_ffmpeg = fake_ffmpeg("echo 'no route to host' >&2\nexit 1")
        configs = [
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            CameraConfig(role=CameraRole.REAR, snapshot_url=f"{base}/ok.jpg"),
        ]
        shots = capture_all(configs, ffmpeg_path=failing_ffmpeg)
        assert [shot.role for shot in shots] == [CameraRole.FRONT, CameraRole.REAR]
        front, rear = shots
        assert not front.ok
        assert front.error is not None and "no route to host" in front.error
        assert rear.ok
        assert rear.jpeg == JPEG_BODY

    def test_cameras_run_in_parallel(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """Две медленные камеры (~1 с) снимаются суммарно ~1 с, а не ~2 с."""
        slow_ffmpeg = fake_ffmpeg("sleep 1\nprintf '\\377\\330frame'")
        configs = [
            CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1"),
            CameraConfig(role=CameraRole.REAR, rtsp_url="rtsp://10.0.0.6/s1"),
        ]
        started = time.monotonic()
        shots = capture_all(configs, ffmpeg_path=slow_ffmpeg)
        elapsed = time.monotonic() - started
        assert all(shot.ok for shot in shots)
        assert 0.9 <= elapsed < 1.8, f"снимали {elapsed:.2f} с: похоже, камеры шли по очереди"

    def test_all_results_are_camera_shots(self, fake_ffmpeg: Callable[[str], str]) -> None:
        """capture_all не выбрасывает исключения — только CameraShot с ok=False."""
        ffmpeg = fake_ffmpeg("exit 7")
        shots = capture_all(
            [CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://10.0.0.5/s1")],
            ffmpeg_path=ffmpeg,
        )
        assert len(shots) == 1
        assert isinstance(shots[0], CameraShot)
        assert not shots[0].ok
        assert shots[0].error


class TestSanitizeUrlNeverRaises:
    """sanitize_url вызывается из обработчиков ошибок и не имеет права падать."""

    def test_broken_port_is_masked_not_raised(self) -> None:
        """Нечисловой порт в URL (опечатка в конфиге) — маскировка, не исключение."""
        url = "http://admin:secret@10.0.0.5:notaport/snap"
        masked = sanitize_url(url)
        assert "secret" not in masked
        assert "***" in masked

    def test_capture_with_broken_url_returns_error_shot(self) -> None:
        """Битый URL в конфиге → CameraShot с ошибкой, взвешивание не срывается."""
        config = CameraConfig(
            role=CameraRole.FRONT,
            snapshot_url="http://admin:secret@10.0.0.5:notaport/snap",
            timeout_s=0.5,
        )
        shot = capture(config)
        assert not shot.ok
        assert shot.error
        assert "secret" not in shot.error
