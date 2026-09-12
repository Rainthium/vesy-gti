"""Тесты каталога инструментов центра и его раздачи агентам (0.4.33, урок Канта).

Покрытие:
- center/tools.py: выбор файла по имени (только валидные имена, только файлы,
  без обхода путей), sha256 и размер;
- center/tools_router.py: GET /agents/tools/<файл> через TestClient на отдельном
  FastAPI-приложении с фейковой session_factory и подменённым
  repo.authenticate_agent — 401 без/с чужим токеном, 404 для чужих имён,
  тело байт-в-байт и заголовки X-Sha256 / X-Size-Bytes при верном токене.
"""

import hashlib
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from center.db import repo
from center.tools import tool_by_filename
from center.tools_router import create_tools_router

AGENT_TOKEN = "tools-agent-token"
AUTH_HEADERS = {"Authorization": f"Bearer {AGENT_TOKEN}"}
FFMPEG_CONTENT = b"MZ-fake-ffmpeg-binary" * 500
FFMPEG_SHA256 = hashlib.sha256(FFMPEG_CONTENT).hexdigest()


def _seed_tools(tools_dir: Path) -> None:
    tools_dir.mkdir(exist_ok=True)
    (tools_dir / "ffmpeg.exe").write_bytes(FFMPEG_CONTENT)
    (tools_dir / "FFMPEG-LICENSE.txt").write_bytes(b"GPL")
    (tools_dir / "subdir.exe").mkdir()  # каталог с «файловым» именем — не инструмент


class TestToolByFilename:
    def test_existing_file_with_sha_and_size(self, tmp_path: Path) -> None:
        _seed_tools(tmp_path)
        tool = tool_by_filename(tmp_path, "ffmpeg.exe")
        assert tool is not None
        assert tool.filename == "ffmpeg.exe"
        assert tool.path == tmp_path / "ffmpeg.exe"
        assert tool.sha256 == FFMPEG_SHA256
        assert tool.size_bytes == len(FFMPEG_CONTENT)

    def test_missing_file_is_none(self, tmp_path: Path) -> None:
        _seed_tools(tmp_path)
        assert tool_by_filename(tmp_path, "nope.exe") is None

    def test_directory_is_none(self, tmp_path: Path) -> None:
        _seed_tools(tmp_path)
        assert tool_by_filename(tmp_path, "subdir.exe") is None

    @pytest.mark.parametrize(
        "filename",
        [
            "../ffmpeg.exe",
            "..",
            "a/b.exe",
            "a\\b.exe",
            ".hidden",
            "ffmpeg..exe",
            "",
            "x" * 65,
            "ffmpeg exe",
        ],
    )
    def test_foreign_names_rejected(self, tmp_path: Path, filename: str) -> None:
        _seed_tools(tmp_path)
        assert tool_by_filename(tmp_path, filename) is None

    def test_missing_dir_is_none(self, tmp_path: Path) -> None:
        assert tool_by_filename(tmp_path / "absent", "ffmpeg.exe") is None


class _FakeSession:
    """Фейковая сессия БД: только контекстный менеджер, запросов нет."""

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


@pytest.fixture
def tools_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient с маршрутом инструментов; аутентификация агента подменена."""
    _seed_tools(tmp_path)

    def fake_authenticate(session: Session, token: str) -> object | None:
        return SimpleNamespace(scale_id=8) if token == AGENT_TOKEN else None

    monkeypatch.setattr(repo, "authenticate_agent", fake_authenticate)
    factory = cast(Callable[[], Session], _FakeSession)
    app = FastAPI()
    app.include_router(create_tools_router(factory, tmp_path))
    return TestClient(app)


class TestToolsHttp:
    def test_no_token_401(self, tools_client: TestClient) -> None:
        response = tools_client.get("/agents/tools/ffmpeg.exe")
        assert response.status_code == 401
        assert response.content != FFMPEG_CONTENT

    def test_wrong_token_401(self, tools_client: TestClient) -> None:
        response = tools_client.get(
            "/agents/tools/ffmpeg.exe", headers={"Authorization": "Bearer wrong-token"}
        )
        assert response.status_code == 401

    def test_valid_token_downloads_with_checksum_headers(self, tools_client: TestClient) -> None:
        response = tools_client.get("/agents/tools/ffmpeg.exe", headers=AUTH_HEADERS)
        assert response.status_code == 200
        assert response.content == FFMPEG_CONTENT
        assert response.headers["X-Sha256"] == FFMPEG_SHA256
        assert response.headers["X-Size-Bytes"] == str(len(FFMPEG_CONTENT))
        assert response.headers["Content-Length"] == str(len(FFMPEG_CONTENT))

    def test_missing_tool_404(self, tools_client: TestClient) -> None:
        response = tools_client.get("/agents/tools/vlc.exe", headers=AUTH_HEADERS)
        assert response.status_code == 404

    def test_foreign_name_404(self, tools_client: TestClient) -> None:
        response = tools_client.get("/agents/tools/.hidden", headers=AUTH_HEADERS)
        assert response.status_code == 404

    def test_directory_404(self, tools_client: TestClient) -> None:
        response = tools_client.get("/agents/tools/subdir.exe", headers=AUTH_HEADERS)
        assert response.status_code == 404
