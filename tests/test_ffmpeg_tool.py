"""Тесты доставки ffmpeg с центра по надобности (агент 0.4.33, урок Канта 12.09.2026).

Покрытие:
- capture.resolve_ffmpeg_path: голое имя → ffmpeg.exe из корня установки,
  явный путь и отсутствие файла — как есть, dev-запуск без корня — как есть;
- ffmpeg_tool.needs_ffmpeg / ffmpeg_available;
- download_tool через локальный http.server: Bearer-заголовок, атомарная
  запись, отказ при неверной или отсутствующей контрольной сумме и при 401 —
  без хвоста .part;
- FfmpegProvisioner.run: спит без надобности, качает при появлении RTSP-камеры
  из настроек центра, повторяет после ошибки, молчит в dev-запуске.
"""

import asyncio
import contextlib
import hashlib
import http.server
import logging
import threading
import time
import urllib.error
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

import agent.cameras.capture as capture_module
import agent.ffmpeg_tool as ffmpeg_tool
from agent.cameras.capture import CameraConfig, resolve_ffmpeg_path
from agent.ffmpeg_tool import (
    FfmpegProvisioner,
    ToolError,
    download_tool,
    ffmpeg_available,
    needs_ffmpeg,
)
from shared.enums import CameraRole

BODY = b"fake-ffmpeg-binary\x00\x01\x02" * 1000
BODY_SHA256 = hashlib.sha256(BODY).hexdigest()
TOKEN = "agent-token-for-tools"

SNAPSHOT_CAMERA = CameraConfig(role=CameraRole.FRONT, snapshot_url="http://u:p@cam/f.jpg")
RTSP_CAMERA = CameraConfig(role=CameraRole.REAR, rtsp_url="rtsp://u:p@cam:554/1")
BOTH_CAMERA = CameraConfig(
    role=CameraRole.REAR, snapshot_url="http://u:p@cam/r.jpg", rtsp_url="rtsp://u:p@cam:554/2"
)


def _no_which() -> SimpleNamespace:
    """Подмена shutil в модуле: ffmpeg в PATH машины разработчика не считается."""
    return SimpleNamespace(which=lambda *_args: None)


# ---------------------------------------------------------------------------
# разрешение пути
# ---------------------------------------------------------------------------


class TestResolveFfmpegPath:
    def test_bare_name_takes_root_file(self, tmp_path: Path) -> None:
        (tmp_path / "ffmpeg.exe").write_bytes(b"x")
        assert resolve_ffmpeg_path("ffmpeg", tmp_path) == str(tmp_path / "ffmpeg.exe")
        assert resolve_ffmpeg_path("ffmpeg.exe", tmp_path) == str(tmp_path / "ffmpeg.exe")

    def test_bare_name_without_root_file_unchanged(self, tmp_path: Path) -> None:
        assert resolve_ffmpeg_path("ffmpeg", tmp_path) == "ffmpeg"

    @pytest.mark.parametrize(
        "configured",
        ["D:/vesy-agent/ffmpeg.exe", "C:\\tools\\ffmpeg.exe", "/usr/local/bin/ffmpeg", "./ffmpeg"],
    )
    def test_explicit_path_unchanged(self, tmp_path: Path, configured: str) -> None:
        """Явный путь из конфига (Джалал-Абад) не подменяется, даже если файл в корне есть."""
        (tmp_path / "ffmpeg.exe").write_bytes(b"x")
        assert resolve_ffmpeg_path(configured, tmp_path) == configured

    def test_dev_run_without_install_root_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "ffmpeg.exe").write_bytes(b"x")
        monkeypatch.setattr(capture_module, "install_base", lambda: None)
        assert resolve_ffmpeg_path("ffmpeg") == "ffmpeg"

    def test_default_root_is_install_base(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "ffmpeg.exe").write_bytes(b"x")
        monkeypatch.setattr(capture_module, "install_base", lambda: tmp_path)
        assert resolve_ffmpeg_path("ffmpeg") == str(tmp_path / "ffmpeg.exe")

    def test_empty_name_unchanged(self, tmp_path: Path) -> None:
        assert resolve_ffmpeg_path("", tmp_path) == ""


class TestNeedsAndAvailable:
    def test_needs_only_for_rtsp_only_cameras(self) -> None:
        assert needs_ffmpeg([]) is False
        assert needs_ffmpeg([SNAPSHOT_CAMERA]) is False
        assert needs_ffmpeg([BOTH_CAMERA]) is False  # снимок главнее, поток не заводится
        assert needs_ffmpeg([SNAPSHOT_CAMERA, RTSP_CAMERA]) is True
        assert RTSP_CAMERA.rtsp_only is True
        assert BOTH_CAMERA.rtsp_only is False

    def test_available_by_root_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ffmpeg_tool, "shutil", _no_which())
        assert ffmpeg_available("ffmpeg", tmp_path) is False
        (tmp_path / "ffmpeg.exe").write_bytes(b"x")
        assert ffmpeg_available("ffmpeg", tmp_path) is True

    def test_available_by_explicit_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ffmpeg_tool, "shutil", _no_which())
        explicit = tmp_path / "bin" / "ffmpeg.exe"
        assert ffmpeg_available(str(explicit), None) is False
        explicit.parent.mkdir()
        explicit.write_bytes(b"x")
        assert ffmpeg_available(str(explicit), None) is True

    def test_available_by_path_lookup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            ffmpeg_tool, "shutil", SimpleNamespace(which=lambda *_args: "/usr/bin/ffmpeg")
        )
        assert ffmpeg_available("ffmpeg", None) is True


# ---------------------------------------------------------------------------
# скачивание с центра
# ---------------------------------------------------------------------------


RecordedRequest = tuple[str, str]  # (путь, заголовок Authorization)


def _make_handler(
    records: list[RecordedRequest], *, sha: str | None, status: int
) -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            records.append((self.path, self.headers.get("Authorization", "")))
            if status != 200:
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(BODY)))
            if sha is not None:
                self.send_header("X-Sha256", sha)
            self.end_headers()
            self.wfile.write(BODY)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return Handler


@contextlib.contextmanager
def _tools_server(
    records: list[RecordedRequest], *, sha: str | None = BODY_SHA256, status: int = 200
) -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", 0), _make_handler(records, sha=sha, status=status)
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class TestDownloadTool:
    def test_success_writes_file_atomically(self, tmp_path: Path) -> None:
        records: list[RecordedRequest] = []
        target = tmp_path / "ffmpeg.exe"
        with _tools_server(records) as base:
            size, sha = download_tool(f"{base}/agents/tools/ffmpeg.exe", TOKEN, target)
        assert (size, sha) == (len(BODY), BODY_SHA256)
        assert target.read_bytes() == BODY
        assert not (tmp_path / "ffmpeg.exe.part").exists()
        assert records == [("/agents/tools/ffmpeg.exe", f"Bearer {TOKEN}")]

    def test_wrong_sha256_rejected(self, tmp_path: Path) -> None:
        records: list[RecordedRequest] = []
        target = tmp_path / "ffmpeg.exe"
        with (
            _tools_server(records, sha="0" * 64) as base,
            pytest.raises(ToolError, match="sha256"),
        ):
            download_tool(f"{base}/agents/tools/ffmpeg.exe", TOKEN, target)
        assert not target.exists()
        assert not (tmp_path / "ffmpeg.exe.part").exists()

    def test_missing_sha256_header_rejected(self, tmp_path: Path) -> None:
        records: list[RecordedRequest] = []
        target = tmp_path / "ffmpeg.exe"
        with (
            _tools_server(records, sha=None) as base,
            pytest.raises(ToolError, match="контрольную сумму"),
        ):
            download_tool(f"{base}/agents/tools/ffmpeg.exe", TOKEN, target)
        assert not target.exists()
        assert not (tmp_path / "ffmpeg.exe.part").exists()

    def test_unauthorized_leaves_no_trace(self, tmp_path: Path) -> None:
        records: list[RecordedRequest] = []
        target = tmp_path / "ffmpeg.exe"
        with (
            _tools_server(records, status=401) as base,
            pytest.raises(urllib.error.HTTPError),
        ):
            download_tool(f"{base}/agents/tools/ffmpeg.exe", TOKEN, target)
        assert not target.exists()
        assert not (tmp_path / "ffmpeg.exe.part").exists()


# ---------------------------------------------------------------------------
# цикл доставки
# ---------------------------------------------------------------------------


async def _wait_until(predicate: Callable[[], bool], timeout_s: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("ожидаемое состояние не наступило")
        await asyncio.sleep(0.005)


class TestProvisionerLoop:
    @pytest.fixture(autouse=True)
    def _fast_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ffmpeg_tool, "RETRY_MIN_S", 0.01)
        monkeypatch.setattr(ffmpeg_tool, "RETRY_MAX_S", 0.05)
        monkeypatch.setattr(ffmpeg_tool, "shutil", _no_which())

    @staticmethod
    def _fake_download(
        calls: list[tuple[str, str]], *, fail_first: bool = False
    ) -> ffmpeg_tool.Downloader:
        def download(url: str, token: str, target: Path) -> tuple[int, str]:
            calls.append((url, token))
            if fail_first and len(calls) == 1:
                raise ToolError("сеть моргнула")
            target.write_bytes(b"ffmpeg")
            return len(b"ffmpeg"), "f" * 64

        return download

    def test_idle_until_rtsp_camera_appears(self, tmp_path: Path) -> None:
        """Камеры со снимками — загрузки нет; RTSP-камера из центра — одна загрузка."""
        calls: list[tuple[str, str]] = []
        provisioner = FfmpegProvisioner(
            base_url="http://center/",
            token=TOKEN,
            configured_path="ffmpeg",
            install_dir=tmp_path,
            cameras=[SNAPSHOT_CAMERA],
            download=self._fake_download(calls),
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await asyncio.sleep(0.05)
            assert calls == []
            assert provisioner.needed is False
            provisioner.set_cameras([SNAPSHOT_CAMERA, RTSP_CAMERA])
            await _wait_until(lambda: len(calls) == 1)
            assert calls == [("http://center/agents/tools/ffmpeg.exe", TOKEN)]
            assert (tmp_path / "ffmpeg.exe").read_bytes() == b"ffmpeg"
            await asyncio.sleep(0.05)
            assert len(calls) == 1  # файл есть — цикл спит
            assert provisioner.missing() is False
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())

    def test_retries_after_failure(self, tmp_path: Path) -> None:
        calls: list[tuple[str, str]] = []
        provisioner = FfmpegProvisioner(
            base_url="http://center",
            token=TOKEN,
            configured_path="ffmpeg",
            install_dir=tmp_path,
            cameras=[RTSP_CAMERA],
            download=self._fake_download(calls, fail_first=True),
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await _wait_until(lambda: len(calls) == 2)
            assert (tmp_path / "ffmpeg.exe").exists()
            assert provisioner.attempts == 2
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())

    def test_present_file_is_not_downloaded_again(self, tmp_path: Path) -> None:
        (tmp_path / "ffmpeg.exe").write_bytes(b"already")
        calls: list[tuple[str, str]] = []
        provisioner = FfmpegProvisioner(
            base_url="http://center",
            token=TOKEN,
            configured_path="ffmpeg",
            install_dir=tmp_path,
            cameras=[RTSP_CAMERA],
            download=self._fake_download(calls),
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await asyncio.sleep(0.05)
            assert calls == []
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())

    def test_dev_run_never_downloads(self) -> None:
        """Без корня установки (не frozen) качать некуда — ffmpeg ставится в PATH руками."""
        calls: list[tuple[str, str]] = []
        provisioner = FfmpegProvisioner(
            base_url="http://center",
            token=TOKEN,
            configured_path="ffmpeg",
            install_dir=None,
            cameras=[RTSP_CAMERA],
            download=self._fake_download(calls),
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await asyncio.sleep(0.05)
            provisioner.set_cameras([RTSP_CAMERA])
            await asyncio.sleep(0.05)
            assert calls == []
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())


class TestProvisionerNoStorm:
    """Замечания ревью 12.09.2026: «успешная» загрузка без файла и явный путь не должны
    превращаться в шторм загрузок по 100 МБ через туннель объекта."""

    @pytest.fixture(autouse=True)
    def _fast_retries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(ffmpeg_tool, "RETRY_MIN_S", 0.01)
        monkeypatch.setattr(ffmpeg_tool, "RETRY_MAX_S", 0.05)
        monkeypatch.setattr(ffmpeg_tool, "shutil", _no_which())

    def test_download_without_file_backs_off(self, tmp_path: Path) -> None:
        """Загрузчик отчитался успехом, а файла нет (антивирус убрал) — повтор с паузой."""
        calls: list[str] = []

        def vanished(url: str, token: str, target: Path) -> tuple[int, str]:
            calls.append(url)
            return 1, "e" * 64  # файл не записан

        provisioner = FfmpegProvisioner(
            base_url="http://center",
            token=TOKEN,
            configured_path="ffmpeg",
            install_dir=tmp_path,
            cameras=[RTSP_CAMERA],
            download=vanished,
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await asyncio.sleep(0.2)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())
        # паузы 0.01 → 0.02 → 0.04 → 0.05…: за 0,2 с — единицы попыток, не тысячи
        assert 1 <= provisioner.attempts <= 12
        assert not (tmp_path / "ffmpeg.exe").exists()

    def test_explicit_path_never_downloads(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Явный ffmpeg_path без файла: загрузка в корень ему не поможет — одно предупреждение."""
        calls: list[str] = []

        def download(url: str, token: str, target: Path) -> tuple[int, str]:
            calls.append(url)
            target.write_bytes(b"x")
            return 1, "e" * 64

        provisioner = FfmpegProvisioner(
            base_url="http://center",
            token=TOKEN,
            configured_path="D:/vesy-agent/ffmpeg.exe",
            install_dir=tmp_path,
            cameras=[RTSP_CAMERA],
            download=download,
        )

        async def scenario() -> None:
            task = asyncio.create_task(provisioner.run())
            await asyncio.sleep(0.05)
            provisioner.set_cameras(
                [RTSP_CAMERA]
            )  # повторное пробуждение — без второго предупреждения
            await asyncio.sleep(0.05)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        with caplog.at_level(logging.WARNING, logger="agent.ffmpeg_tool"):
            asyncio.run(scenario())
        assert calls == []
        assert provisioner.attempts == 0
        assert caplog.text.count("задан явно") == 1
