"""Снимки журнала оператора: локальный файл, миниатюра, фолбэк на центр.

Проверяется agent/photos.py (PhotoLibrary):
- пока файл лежит на диске агента, к центру не обращаемся вовсе;
- миниатюра строится один раз и кладётся рядом с кадром; оригинал при
  этом не меняется (правило №2: sha256 связан с записью);
- после ретеншна (файла нет) снимок берётся из центра по токену агента,
  причём для миниатюры запрашивается ?thumb=1;
- офлайн и отказ центра — не ошибка: журнал просто покажет прочерк.

Центр заменён локальным http.server-стабом (как в tests/test_photos.py).
"""

import hashlib
import http.server
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from agent.photos import PhotoLibrary, make_thumbnail, thumb_path
from agent.sync.storage import AgentStorage, StoredPhoto
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord
from tools.dev_operator_ui import _GRAY_JPEG

TOKEN = "agent-token-photos"
CENTER_JPEG = b"\xff\xd8\xff\xe0" + b"from-center-original" + b"\xff\xd9"
CENTER_THUMB = b"\xff\xd8\xff\xe0" + b"from-center-thumb" + b"\xff\xd9"


class _CenterStub(http.server.ThreadingHTTPServer):
    """Стаб центра: отдаёт снимок по токену агента, помнит запросы."""

    requests: list[tuple[str, str]]
    status: int = 200


def _handler() -> type[http.server.BaseHTTPRequestHandler]:
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            server: _CenterStub = self.server  # type: ignore[assignment]
            auth = self.headers.get("Authorization", "")
            server.requests.append((self.path, auth))
            if auth != f"Bearer {TOKEN}":
                self._send(401, b"")
                return
            if server.status != 200:
                self._send(server.status, b"")
                return
            body = CENTER_THUMB if "thumb=1" in self.path else CENTER_JPEG
            self._send(200, body)

        def _send(self, code: int, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return Handler


@pytest.fixture
def center() -> Iterator[_CenterStub]:
    server = _CenterStub(("127.0.0.1", 0), _handler())
    server.requests = []
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()
    server.server_close()


@pytest.fixture
def storage() -> Iterator[AgentStorage]:
    db = AgentStorage(":memory:")
    yield db
    db.close()


def _record(uuid: UUID) -> WeighingRecord:
    return WeighingRecord(
        uuid=uuid,
        operation=Operation.WEIGHING,
        code=ErrorCode.OK,
        massa=15000.0,
        stable=True,
        weighed_at=datetime.now(UTC),
        vehicle_number="01KG777AAA",
        source=WeighingSource.AIS,
    )


def _save_photo(storage: AgentStorage, photos_dir: Path, *, on_disk: bool = True) -> UUID:
    """Запись с одним снимком; ``on_disk=False`` — файл убран ретеншном."""
    uuid = uuid4()
    photos_dir.mkdir(parents=True, exist_ok=True)
    path = photos_dir / f"{uuid.hex}_photo1.jpeg"
    if on_disk:
        path.write_bytes(_GRAY_JPEG)
    storage.save_weighing(
        _record(uuid),
        [
            StoredPhoto(
                role=CameraRole.FRONT,
                path=str(path),
                sha256=hashlib.sha256(_GRAY_JPEG).hexdigest(),
                size_bytes=len(_GRAY_JPEG),
            )
        ],
    )
    return uuid


def _library(storage: AgentStorage, center: _CenterStub) -> PhotoLibrary:
    port = center.server_address[1]
    return PhotoLibrary(storage, base_url=f"http://127.0.0.1:{port}", token=TOKEN, timeout_s=3.0)


class TestLocalPhotos:
    def test_local_file_served_without_center(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Файл на месте → отдаём его и к центру не ходим вовсе."""
        uuid = _save_photo(storage, tmp_path)
        data = _library(storage, center).photo_bytes(uuid, CameraRole.FRONT)
        assert data == _GRAY_JPEG
        assert center.requests == [], "ходили в центр при живом локальном файле"

    def test_thumbnail_built_once_and_cached(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Миниатюра строится из оригинала, ложится рядом и переиспользуется."""
        uuid = _save_photo(storage, tmp_path)
        library = _library(storage, center)
        original = next(iter(tmp_path.glob("*_photo1.jpeg")))
        before = original.read_bytes()

        first = library.photo_bytes(uuid, CameraRole.FRONT, thumb=True)
        assert first is not None and first != before, "миниатюра совпала с оригиналом"
        assert thumb_path(original).is_file(), "миниатюра не сохранена рядом"
        assert original.read_bytes() == before, "оригинал изменён (правило №2)"

        thumb_path(original).write_bytes(b"\xff\xd8cached\xff\xd9")  # метка
        assert library.photo_bytes(uuid, CameraRole.FRONT, thumb=True) == b"\xff\xd8cached\xff\xd9"
        assert center.requests == []

    def test_unknown_role_and_record(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Роли нет в записи или записи нет вовсе → None, без запросов."""
        uuid = _save_photo(storage, tmp_path)
        library = _library(storage, center)
        assert library.photo_bytes(uuid, CameraRole.REAR) is None
        assert library.photo_bytes(uuid4(), CameraRole.FRONT) is None
        assert center.requests == []

    def test_roles_come_from_journal(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Роли берутся из журнала — даже когда файла уже нет на диске."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        assert _library(storage, center).roles_of(uuid) == [CameraRole.FRONT]


class TestCenterFallback:
    def test_missing_file_falls_back_to_center(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Файл убран ретеншном → снимок берётся из центра с токеном агента."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        data = _library(storage, center).photo_bytes(uuid, CameraRole.FRONT)
        assert data == CENTER_JPEG
        path, auth = center.requests[0]
        assert path == f"/agents/photos/{uuid}/front"
        assert auth == f"Bearer {TOKEN}"

    def test_thumb_requested_from_center(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Для строки журнала у центра просим именно миниатюру."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        data = _library(storage, center).photo_bytes(uuid, CameraRole.FRONT, thumb=True)
        assert data == CENTER_THUMB
        assert center.requests[0][0].endswith("?thumb=1")

    def test_center_refusal_is_not_an_error(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Центр ответил 404 → None, журнал покажет прочерк."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        center.status = 404
        assert _library(storage, center).photo_bytes(uuid, CameraRole.FRONT) is None

    def test_no_center_requests_while_offline(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Без связи в центр не ходим вовсе: журнал обновляется каждые 5 с,
        и ожидание таймаутов забило бы соединения браузера (ревью)."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        port = center.server_address[1]
        library = PhotoLibrary(
            storage,
            base_url=f"http://127.0.0.1:{port}",
            token=TOKEN,
            online=lambda: False,
            timeout_s=3.0,
        )
        assert library.photo_bytes(uuid, CameraRole.FRONT) is None
        assert center.requests == [], "ходили в центр в офлайне"

    def test_offline_center_is_not_an_error(self, storage: AgentStorage, tmp_path: Path) -> None:
        """Центр недоступен (офлайн объекта) → None, без исключений."""
        uuid = _save_photo(storage, tmp_path, on_disk=False)
        library = PhotoLibrary(storage, base_url="http://127.0.0.1:1", token=TOKEN, timeout_s=1.0)
        assert library.photo_bytes(uuid, CameraRole.FRONT) is None


class TestThumbnailGeneration:
    def test_thumbnail_smaller_than_original(self) -> None:
        """Миниатюра действительно уменьшает кадр и остаётся JPEG."""
        thumb = make_thumbnail(_GRAY_JPEG)
        assert thumb.startswith(b"\xff\xd8")
        assert len(thumb) <= len(_GRAY_JPEG) * 2  # заглушка крошечная, лишь бы не распухла

    def test_broken_jpeg_falls_back_to_original(
        self, storage: AgentStorage, center: _CenterStub, tmp_path: Path
    ) -> None:
        """Кадр не читается как изображение → отдаём как есть, а не пустоту."""
        uuid = uuid4()
        tmp_path.mkdir(parents=True, exist_ok=True)
        path = tmp_path / f"{uuid.hex}_photo1.jpeg"
        path.write_bytes(b"not-a-jpeg-at-all")
        storage.save_weighing(
            _record(uuid),
            [StoredPhoto(role=CameraRole.FRONT, path=str(path), sha256="0" * 64, size_bytes=17)],
        )
        assert _library(storage, center).photo_bytes(uuid, CameraRole.FRONT, thumb=True) == (
            b"not-a-jpeg-at-all"
        )
