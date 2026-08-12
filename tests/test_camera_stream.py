"""Тесты постоянных потоков камер (agent/cameras/stream.py, агент 0.4.7).

Реальный ffmpeg не запускается: процесс подменяется фейком с заранее
подготовленным stdout; разбор кадров тестируется на чистой функции.
"""

import io
import threading
import time
from typing import IO

import pytest

from agent.cameras.capture import CameraConfig, CameraShot
from agent.cameras.stream import (
    CameraStream,
    CameraStreams,
    iter_jpeg_frames,
    shot_or_capture,
    shots_or_capture_all,
)
from shared.enums import CameraRole

JPEG_A = b"\xff\xd8AAAA\xff\xd9"
JPEG_B = b"\xff\xd8BBBBBB\xff\xd9"


class ChunkedStream:
    """file-like, отдающий данные порциями заданного размера."""

    def __init__(self, data: bytes, chunk: int) -> None:
        self._data = data
        self._chunk = chunk
        self._offset = 0

    def read(self, _size: int) -> bytes:
        piece = self._data[self._offset : self._offset + self._chunk]
        self._offset += len(piece)
        return piece


class TestIterJpegFrames:
    def test_two_frames_with_garbage_between(self) -> None:
        """Мусор до, между и после кадров отбрасывается."""
        raw = b"garbage" + JPEG_A + b"noise" + JPEG_B + b"tail"
        frames = list(iter_jpeg_frames(io.BytesIO(raw)))
        assert frames == [JPEG_A, JPEG_B]

    @pytest.mark.parametrize("chunk", [1, 2, 3, 5])
    def test_frames_split_across_chunks(self, chunk: int) -> None:
        """Кадры собираются целиком при любой нарезке потока на порции."""
        raw = JPEG_A + JPEG_B
        frames = list(iter_jpeg_frames(ChunkedStream(raw, chunk)))  # type: ignore[arg-type]
        assert frames == [JPEG_A, JPEG_B]

    def test_oversized_partial_frame_dropped(self) -> None:
        """Начатый кадр без конца, переросший лимит, отбрасывается
        (битый поток не должен раздувать буфер), а следующий целый
        кадр после него находится."""
        giant_start = b"\xff\xd8" + b"x" * 60  # SOI без EOI
        raw = giant_start + JPEG_A
        frames = list(
            iter_jpeg_frames(
                ChunkedStream(raw, len(giant_start)),  # type: ignore[arg-type]
                max_frame_bytes=50,
            )
        )
        assert frames == [JPEG_A]


class FakeProcess:
    """Подмена Popen: stdout отдаёт кадры, затем EOF (гибель процесса)."""

    def __init__(self, payload: bytes) -> None:
        # тип совпадает с протоколом _ProcessLike (инвариантность атрибута)
        self.stdout: IO[bytes] | None = io.BytesIO(payload)
        self.terminated = False

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.terminated = True

    def wait(self, timeout: float | None = None) -> int:
        return 0


class TestCameraStream:
    def test_frame_reaches_buffer_and_survives_stop(self) -> None:
        """Кадр из процесса попадает в буфер; stop() завершает поток чисто."""
        started = threading.Event()

        def spawn(_command: list[str]) -> FakeProcess:
            started.set()
            return FakeProcess(JPEG_A)

        stream = CameraStream(CameraRole.FRONT, "rtsp://cam/1", spawn=spawn)
        stream.start()
        assert started.wait(timeout=2.0)
        deadline = time.monotonic() + 2.0
        shot = None
        while shot is None and time.monotonic() < deadline:
            shot = stream.latest(max_age_s=60.0)
            time.sleep(0.01)
        stream.stop()
        assert shot is not None
        assert shot.jpeg == JPEG_A
        assert shot.role is CameraRole.FRONT

    def test_reconnects_after_eof(self) -> None:
        """EOF процесса (обрыв потока) ведёт к повторному запуску."""
        spawns: list[FakeProcess] = []
        respawned = threading.Event()

        def spawn(_command: list[str]) -> FakeProcess:
            process = FakeProcess(JPEG_A if not spawns else JPEG_B)
            spawns.append(process)
            if len(spawns) >= 2:
                respawned.set()
            return process

        stream = CameraStream(CameraRole.REAR, "rtsp://cam/2", spawn=spawn)
        stream.start()
        # пауза переподключения после живого потока — RECONNECT_MIN_S (2 с)
        assert respawned.wait(timeout=10.0)
        stream.stop()
        assert len(spawns) >= 2

    def test_latest_respects_max_age(self) -> None:
        """Протухший кадр не отдаётся."""
        stream = CameraStream(
            CameraRole.FRONT, "rtsp://cam/1", spawn=lambda _c: FakeProcess(JPEG_A)
        )
        stream.start()
        deadline = time.monotonic() + 2.0
        while stream.latest(max_age_s=60.0) is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert stream.latest(max_age_s=60.0) is not None
        assert stream.latest(max_age_s=0.0) is None
        stream.stop()


def rtsp_camera(role: CameraRole = CameraRole.FRONT) -> CameraConfig:
    return CameraConfig(role=role, rtsp_url="rtsp://cam/stream")


def snapshot_camera(role: CameraRole = CameraRole.FRONT) -> CameraConfig:
    return CameraConfig(role=role, snapshot_url="http://cam/snap.jpg", rtsp_url="rtsp://cam/backup")


class RecordingFactory:
    """Фабрика стримов, не запускающая настоящие процессы."""

    def __init__(self) -> None:
        self.created: list[CameraStream] = []
        self.stopped: list[CameraStream] = []

    def __call__(self, role: CameraRole, url: str, *, ffmpeg_path: str = "ffmpeg") -> CameraStream:
        factory = self

        class _Stream(CameraStream):
            def start(self) -> None:  # процессы в тестах не нужны
                pass

            def stop(self) -> None:
                factory.stopped.append(self)

        stream = _Stream(role, url, ffmpeg_path=ffmpeg_path)
        self.created.append(stream)
        return stream


class TestCameraStreams:
    def test_streams_only_for_rtsp_only_cameras(self) -> None:
        """Поток заводится камере без снапшота; Hikvision живёт без потока."""
        factory = RecordingFactory()
        streams = CameraStreams(
            [rtsp_camera(CameraRole.FRONT), snapshot_camera(CameraRole.REAR)],
            stream_factory=factory,
        )
        assert [s.role for s in factory.created] == [CameraRole.FRONT]
        assert streams.shot(CameraRole.REAR) is None  # не потоковая

    def test_set_cameras_recreates_on_url_change(self) -> None:
        """Смена URL из центра пересоздаёт поток; прочие не трогаются."""
        factory = RecordingFactory()
        streams = CameraStreams([rtsp_camera()], stream_factory=factory)
        first = factory.created[0]
        streams.set_cameras([rtsp_camera()])  # тот же URL — поток живёт
        assert factory.stopped == []
        changed = CameraConfig(role=CameraRole.FRONT, rtsp_url="rtsp://cam/other")
        streams.set_cameras([changed])
        assert factory.stopped == [first]
        assert factory.created[-1].url == "rtsp://cam/other"

    def test_stop_all_stops_everything(self) -> None:
        factory = RecordingFactory()
        streams = CameraStreams(
            [rtsp_camera(CameraRole.FRONT), rtsp_camera(CameraRole.REAR)],
            stream_factory=factory,
        )
        streams.stop_all()
        assert set(factory.stopped) == set(factory.created)
        assert streams.shot(CameraRole.FRONT) is None


class BufferedStreams:
    """Подмена CameraStreams: отдаёт заранее заданные кадры."""

    def __init__(self, shots: dict[CameraRole, CameraShot]) -> None:
        self._shots = shots

    def shot(self, role: CameraRole, *, max_age_s: float = 3.0) -> CameraShot | None:
        return self._shots.get(role)


class TestShotOrCapture:
    def test_buffered_frame_wins_no_capture(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Свежий буфер — capture не вызывается вовсе."""
        calls: list[str] = []
        monkeypatch.setattr(
            "agent.cameras.stream.capture",
            lambda cfg, *, ffmpeg_path: calls.append("capture"),
        )
        from datetime import UTC, datetime

        shot = CameraShot(role=CameraRole.FRONT, jpeg=JPEG_A, captured_at=datetime.now(UTC))
        streams = BufferedStreams({CameraRole.FRONT: shot})
        result = shot_or_capture(rtsp_camera(), streams, ffmpeg_path="ffmpeg")  # type: ignore[arg-type]
        assert result is shot
        assert calls == []

    def test_stale_buffer_falls_back_to_capture(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Пустой/протухший буфер — честная разовая съёмка."""
        from datetime import UTC, datetime

        fallback = CameraShot(role=CameraRole.FRONT, jpeg=JPEG_B, captured_at=datetime.now(UTC))
        monkeypatch.setattr("agent.cameras.stream.capture", lambda cfg, *, ffmpeg_path: fallback)
        streams = BufferedStreams({})
        result = shot_or_capture(rtsp_camera(), streams, ffmpeg_path="ffmpeg")  # type: ignore[arg-type]
        assert result is fallback

    def test_shots_or_capture_all_mixes_sources_in_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Потоковая камера — из буфера, снапшотная — параллельной съёмкой;
        порядок результатов повторяет порядок камер."""
        from datetime import UTC, datetime

        now = datetime.now(UTC)
        buffered = CameraShot(role=CameraRole.FRONT, jpeg=JPEG_A, captured_at=now)
        captured = CameraShot(role=CameraRole.REAR, jpeg=JPEG_B, captured_at=now)
        monkeypatch.setattr(
            "agent.cameras.stream.capture_all",
            lambda cfgs, *, ffmpeg_path: [captured for _ in cfgs],
        )
        streams = BufferedStreams({CameraRole.FRONT: buffered})
        shots = shots_or_capture_all(
            [rtsp_camera(CameraRole.FRONT), snapshot_camera(CameraRole.REAR)],
            streams,  # type: ignore[arg-type]
            ffmpeg_path="ffmpeg",
        )
        assert shots == [buffered, captured]

    def test_none_streams_captures_everything(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Без потоков (агент без RTSP-камер) поведение прежнее."""
        from datetime import UTC, datetime

        captured = CameraShot(role=CameraRole.FRONT, jpeg=JPEG_A, captured_at=datetime.now(UTC))
        monkeypatch.setattr(
            "agent.cameras.stream.capture_all",
            lambda cfgs, *, ffmpeg_path: [captured for _ in cfgs],
        )
        shots = shots_or_capture_all([rtsp_camera()], None, ffmpeg_path="ffmpeg")
        assert shots == [captured]
