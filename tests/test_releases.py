"""Тесты раздачи релизов агента центром (автообновление, 10.08.2026).

Покрытие:
- center/releases.py: выбор актуального релиза (числовое сравнение версий),
  игнорирование посторонних файлов, sha256/size, защита от обхода путей,
  smoke кэша sha256 по ключу (имя, размер, mtime);
- center/releases_router.py: GET /agents/releases/<файл> через TestClient
  на отдельном FastAPI-приложении с фейковой session_factory и
  подменённым repo.authenticate_agent (живой PostgreSQL не нужен);
- AgentHub.send_update_command: доставка update_command подключённому
  агенту, ERR_AGENT_OFFLINE для офлайна и мёртвого линка;
- shared.messages: разбор update_command / update_status.
"""

import asyncio
import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

import center.releases as releases_module
from center.agents_ws.hub import AgentHub, AgentHubError
from center.db import repo
from center.releases import latest_release, release_by_filename
from center.releases_router import create_releases_router
from shared.enums import ErrorCode
from shared.messages import (
    UpdateCommand,
    UpdateStatus,
    parse_agent_message,
    parse_center_message,
)

AGENT_TOKEN = "release-agent-token"
AUTH_HEADERS = {"Authorization": f"Bearer {AGENT_TOKEN}"}

CONTENT_010 = b"release-0.1.0-content"
CONTENT_020 = b"release-0.2.0-content!!"
CONTENT_0100 = b"release-0.10.0-content!!!!!"


def _seed_releases(releases_dir: Path) -> None:
    """Выложить релизы 0.1.0/0.2.0/0.10.0 и посторонние файлы."""
    releases_dir.mkdir(exist_ok=True)
    (releases_dir / "ves-agent-0.1.0-win64.zip").write_bytes(CONTENT_010)
    (releases_dir / "ves-agent-0.2.0-win64.zip").write_bytes(CONTENT_020)
    (releases_dir / "ves-agent-0.10.0-win64.zip").write_bytes(CONTENT_0100)
    # посторонние файлы и каталог с релизным именем — игнорируются
    (releases_dir / "evil.zip").write_bytes(b"evil")
    (releases_dir / "ves-agent-x.zip").write_bytes(b"not-a-version")
    (releases_dir / "ves-agent-3.0.0-win64.zip.dir").mkdir()


# ---------------------------------------------------------------------------
# center/releases.py: выбор актуального релиза
# ---------------------------------------------------------------------------


class TestLatestRelease:
    def test_missing_dir_returns_none(self, tmp_path: Path) -> None:
        """Несуществующий каталог релизов → None (а не исключение)."""
        assert latest_release(tmp_path / "no-such-dir") is None

    def test_empty_dir_returns_none(self, tmp_path: Path) -> None:
        """Пустой каталог → None."""
        assert latest_release(tmp_path) is None

    def test_numeric_version_comparison(self, tmp_path: Path) -> None:
        """0.10.0 новее 0.2.0 (числовое сравнение; строковое выбрало бы 0.2.0)."""
        _seed_releases(tmp_path)
        release = latest_release(tmp_path)
        assert release is not None
        assert release.version == "0.10.0"
        assert release.filename == "ves-agent-0.10.0-win64.zip"
        assert release.path == tmp_path / "ves-agent-0.10.0-win64.zip"
        assert release.sha256 == hashlib.sha256(CONTENT_0100).hexdigest()
        assert release.size_bytes == len(CONTENT_0100)

    def test_only_foreign_files_returns_none(self, tmp_path: Path) -> None:
        """Каталог только с посторонними файлами → None."""
        (tmp_path / "evil.zip").write_bytes(b"evil")
        (tmp_path / "ves-agent-x.zip").write_bytes(b"x")
        assert latest_release(tmp_path) is None


class TestReleaseByFilename:
    def test_valid_filename_found(self, tmp_path: Path) -> None:
        """Валидное имя → релиз с корректными версией, sha256 и размером."""
        _seed_releases(tmp_path)
        release = release_by_filename(tmp_path, "ves-agent-0.2.0-win64.zip")
        assert release is not None
        assert release.version == "0.2.0"
        assert release.sha256 == hashlib.sha256(CONTENT_020).hexdigest()
        assert release.size_bytes == len(CONTENT_020)

    @pytest.mark.parametrize(
        "filename",
        [
            "../../etc/passwd",
            "evil.zip",
            "ves-agent-x.zip",
            "..%2F..%2Fetc%2Fpasswd",
            "ves-agent-0.1.0-win64.zip.exe",
        ],
    )
    def test_invalid_names_rejected(self, tmp_path: Path, filename: str) -> None:
        """Невалидные имена (в т.ч. обход путей) → None."""
        _seed_releases(tmp_path)
        assert release_by_filename(tmp_path, filename) is None

    def test_valid_name_missing_file(self, tmp_path: Path) -> None:
        """Валидное имя, но файла нет → None."""
        assert release_by_filename(tmp_path, "ves-agent-5.5.5-win64.zip") is None


class TestSha256Cache:
    def test_cache_hit_by_name_size_mtime(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Повторный вызов не пересчитывает sha256: подмена содержимого файла
        без смены размера и mtime отдаёт закэшированное значение (smoke)."""
        monkeypatch.setattr(releases_module, "_sha_cache", {})
        path = tmp_path / "ves-agent-1.0.0-win64.zip"
        path.write_bytes(b"A" * 100)
        first = release_by_filename(tmp_path, path.name)
        assert first is not None

        stat = path.stat()
        path.write_bytes(b"B" * 100)  # тот же размер, другое содержимое
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))  # вернуть mtime

        second = release_by_filename(tmp_path, path.name)
        assert second is not None
        assert second.sha256 == first.sha256, "значение не из кэша"
        # доказательство, что содержимое реально другое
        assert hashlib.sha256(b"B" * 100).hexdigest() != first.sha256


# ---------------------------------------------------------------------------
# HTTP-раздача: create_releases_router на отдельном приложении
# ---------------------------------------------------------------------------


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
def releases_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient с маршрутом релизов; аутентификация агента подменена."""
    _seed_releases(tmp_path)

    def fake_authenticate(session: Session, token: str) -> object | None:
        return SimpleNamespace(scale_id=7) if token == AGENT_TOKEN else None

    monkeypatch.setattr(repo, "authenticate_agent", fake_authenticate)
    factory = cast(Callable[[], Session], _FakeSession)
    app = FastAPI()
    app.include_router(create_releases_router(factory, tmp_path))
    return TestClient(app)


class TestReleasesHttp:
    def test_no_token_401(self, releases_client: TestClient) -> None:
        """Без Authorization → 401, тело релиза не отдаётся."""
        response = releases_client.get("/agents/releases/ves-agent-0.10.0-win64.zip")
        assert response.status_code == 401

    def test_wrong_token_401(self, releases_client: TestClient) -> None:
        """Неверный токен → 401."""
        response = releases_client.get(
            "/agents/releases/ves-agent-0.10.0-win64.zip",
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code == 401

    def test_valid_token_downloads_file(self, releases_client: TestClient) -> None:
        """Верный токен → 200, тело байт-в-байт, content-type application/zip."""
        response = releases_client.get(
            "/agents/releases/ves-agent-0.10.0-win64.zip", headers=AUTH_HEADERS
        )
        assert response.status_code == 200
        assert response.content == CONTENT_0100
        assert response.headers["content-type"] == "application/zip"

    def test_missing_release_404(self, releases_client: TestClient) -> None:
        """Валидное имя, но файла нет → 404."""
        response = releases_client.get(
            "/agents/releases/ves-agent-5.5.5-win64.zip", headers=AUTH_HEADERS
        )
        assert response.status_code == 404

    def test_foreign_filename_404(self, releases_client: TestClient) -> None:
        """Постороннее имя (evil.zip лежит в каталоге!) → 404, файл не отдаётся."""
        response = releases_client.get("/agents/releases/evil.zip", headers=AUTH_HEADERS)
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# AgentHub.send_update_command
# ---------------------------------------------------------------------------


class FakeLink:
    """Фейковое соединение агента: копит всё, что отправил центр."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


class DeadLink(FakeLink):
    """Соединение с умершим TCP: любая отправка падает."""

    async def send_text(self, data: str) -> None:
        raise RuntimeError("соединение закрыто")


def _make_command() -> UpdateCommand:
    return UpdateCommand(
        version="0.10.0",
        url_path="/agents/releases/ves-agent-0.10.0-win64.zip",
        sha256="a" * 64,
        size_bytes=123,
    )


class TestHubUpdateCommand:
    def test_connected_link_receives_update_command(self) -> None:
        """Подключённый агент получает JSON с type=update_command."""

        async def scenario() -> None:
            hub = AgentHub()
            link = FakeLink()
            hub.attach(1, link)
            command = _make_command()
            await hub.send_update_command(1, command)

            assert len(link.sent) == 1
            payload = json.loads(link.sent[0])
            assert payload["type"] == "update_command"
            assert payload["version"] == "0.10.0"
            assert payload["url_path"] == command.url_path
            assert payload["sha256"] == command.sha256
            assert payload["size_bytes"] == command.size_bytes
            # агент разберёт это сообщение штатным парсером
            parsed = parse_center_message(link.sent[0])
            assert isinstance(parsed, UpdateCommand)

        asyncio.run(scenario())

    def test_offline_scale_raises_agent_offline(self) -> None:
        """Неподключённые весы → AgentHubError с кодом ERR_AGENT_OFFLINE."""

        async def scenario() -> None:
            hub = AgentHub()
            with pytest.raises(AgentHubError) as excinfo:
                await hub.send_update_command(99, _make_command())
            assert excinfo.value.code is ErrorCode.ERR_AGENT_OFFLINE

        asyncio.run(scenario())

    def test_dead_link_raises_agent_offline(self) -> None:
        """Полуживой TCP (send_text падает) → тоже ERR_AGENT_OFFLINE."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, DeadLink())
            with pytest.raises(AgentHubError) as excinfo:
                await hub.send_update_command(1, _make_command())
            assert excinfo.value.code is ErrorCode.ERR_AGENT_OFFLINE

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# shared.messages: разбор новых типов сообщений
# ---------------------------------------------------------------------------


class TestMessagesParsing:
    def test_parse_center_message_update_command(self) -> None:
        """parse_center_message разбирает update_command в UpdateCommand."""
        raw = json.dumps(
            {
                "type": "update_command",
                "version": "0.10.0",
                "url_path": "/agents/releases/ves-agent-0.10.0-win64.zip",
                "sha256": "b" * 64,
                "size_bytes": 42,
            }
        )
        message = parse_center_message(raw)
        assert isinstance(message, UpdateCommand)
        assert message.version == "0.10.0"
        assert message.size_bytes == 42

    def test_parse_agent_message_update_status(self) -> None:
        """parse_agent_message разбирает update_status в UpdateStatus."""
        raw = json.dumps(
            {
                "type": "update_status",
                "agent_id": "agent-1",
                "version": "0.10.0",
                "ok": False,
                "error": "sha256 архива не совпал — обновление отвергнуто",
            }
        )
        message = parse_agent_message(raw)
        assert isinstance(message, UpdateStatus)
        assert message.ok is False
        assert message.error is not None and "sha256" in message.error

    def test_update_status_roundtrip(self) -> None:
        """model_dump_json ↔ parse_agent_message без потерь (путь агент→центр)."""
        status = UpdateStatus(agent_id="agent-1", version="0.10.0", ok=True)
        parsed = parse_agent_message(status.model_dump_json())
        assert parsed == status
