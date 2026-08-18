"""Тесты экрана «Релизы агентов» панели (маршруты /panel/releases, 18.08.2026).

Стенд как в test_center_panel (та же тестовая БД панели), но роутер собран
с каталогом релизов в tmp_path. Проверяется:
- доступ только администратору (диспетчер — 403, без сессии — на вход);
- каталог: файлы из каталога видны, канал назначается/переводится/
  отзывается кнопками, прежний stable уходит в архив;
- загрузка релиза: валидный zip ложится в каталог и в таблицу, мусорное
  имя / архив без exe / повтор версии — отказ с заметкой, файла нет;
- ручная команда из блока раскатки: агенту на связи уходит update_command
  с целью канала и заводится строка журнала (origin=manual); офлайн и
  отсутствие цели — заметка без команды;
- кнопка дашборда «Обновить до vX» показывает цель КАНАЛА агента.
"""

import io
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from starlette.middleware.sessions import SessionMiddleware

from center import rollout
from center.agents_ws.hub import AgentHub
from center.db.models import (
    Agent,
    AgentRelease,
    AgentStatus,
    AgentUpdate,
    ReleaseChannel,
    User,
    UserRole,
)
from center.web.router import create_panel_router
from shared.messages import UpdateCommand, parse_center_message
from tests.test_center_panel import (
    PANEL_LOGIN,
    PANEL_PASSWORD,
    _add_site_scale,
    _add_user,
    db,  # noqa: F401 — фикстуры БД панели
    panel_db_engine,  # noqa: F401
    panel_db_url,  # noqa: F401
)


@dataclass
class ReleasesEnv:
    client: TestClient
    factory: sessionmaker[Session]
    releases_dir: Path
    hub: AgentHub
    scale_id: int  # весы с агентом (Кызыл-Кыя)
    agent_id: int


@pytest.fixture
def env(db: sessionmaker[Session], tmp_path: Path) -> Iterator[ReleasesEnv]:  # noqa: F811
    releases_dir = tmp_path / "releases"
    releases_dir.mkdir()
    with db() as session:
        _add_user(session)
        _, scale = _add_site_scale(
            session, "kyzyl-kyia", "СВХ «Кызыл-Кыя»", "Весы SCS-80", with_agent=True
        )
        agent = session.execute(select(Agent).where(Agent.scale_id == scale.id)).scalar_one()
        agent.version = "0.4.18"
        agent.channel = ReleaseChannel.STABLE
        session.commit()
        ids = (scale.id, agent.id)
    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-secret", session_cookie="ves_test")
    hub = AgentHub()
    app.include_router(
        create_panel_router(db, hub, photos_dir=tmp_path / "photos", releases_dir=releases_dir)
    )
    client = TestClient(app)
    yield ReleasesEnv(client, db, releases_dir, hub, *ids)
    client.close()


def _login(env: ReleasesEnv, *, admin: bool = True) -> None:
    if admin:
        with env.factory() as session:
            user = session.execute(select(User).where(User.login == PANEL_LOGIN)).scalar_one()
            user.role = UserRole.ADMIN
            session.commit()
    response = env.client.post(
        "/panel/login",
        data={"login": PANEL_LOGIN, "password": PANEL_PASSWORD},
        follow_redirects=False,
    )
    assert response.status_code == 303


def _release_zip(*, with_exe: bool = True) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        if with_exe:
            bundle.writestr("app/ves-agent.exe", b"MZ new")
        bundle.writestr("app/_internal/x.pyd", b"x")
        bundle.writestr("install-service.bat", b"rem")
    return buffer.getvalue()


def _put_release(env: ReleasesEnv, version: str) -> None:
    (env.releases_dir / f"ves-agent-{version}-win64.zip").write_bytes(_release_zip())


class FakeLink:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


def _commands(link: FakeLink) -> list[UpdateCommand]:
    return [
        m for m in (parse_center_message(raw) for raw in link.sent) if isinstance(m, UpdateCommand)
    ]


class TestAccess:
    def test_anonymous_redirected(self, env: ReleasesEnv) -> None:
        response = env.client.get("/panel/releases", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    def test_dispatcher_forbidden(self, env: ReleasesEnv) -> None:
        _login(env, admin=False)
        assert env.client.get("/panel/releases").status_code == 403
        assert (
            env.client.post("/panel/releases/0.4.19/channel", data={"channel": "pilot"}).status_code
            == 403
        )
        assert env.client.post(f"/panel/releases/agents/{env.scale_id}/update").status_code == 403

    def test_dispatcher_has_no_tab(self, env: ReleasesEnv) -> None:
        _login(env, admin=False)
        page = env.client.get("/panel/").text
        assert 'href="/panel/releases"' not in page


class TestCatalogScreen:
    def test_lists_files_and_channels(self, env: ReleasesEnv) -> None:
        _put_release(env, "0.4.19")
        _put_release(env, "0.4.18")
        _login(env)
        page = env.client.get("/panel/releases").text
        assert 'href="/panel/releases"' in page  # вкладка в шапке
        assert "0.4.19" in page and "0.4.18" in page
        assert "не назначен" in page
        assert "СВХ «Кызыл-Кыя»" in page
        assert "Каналу релиз не назначен" in page

        response = env.client.post(
            "/panel/releases/0.4.19/channel", data={"channel": "pilot"}, follow_redirects=False
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/releases?note=")
        page = env.client.get("/panel/releases").text
        assert "Перевести в stable" in page
        assert "pilot 0.4.19" in page
        with env.factory() as session:
            row = session.execute(
                select(AgentRelease).where(AgentRelease.version == "0.4.19")
            ).scalar_one()
            assert row.channel is ReleaseChannel.PILOT and row.published_by == PANEL_LOGIN

        # 0.4.18 → stable, затем 0.4.19 → stable: 0.4.18 в архив
        env.client.post("/panel/releases/0.4.18/channel", data={"channel": "stable"})
        env.client.post("/panel/releases/0.4.19/channel", data={"channel": "stable"})
        with env.factory() as session:
            catalog = {r.version: r for r in rollout.release_catalog(session, env.releases_dir)}
        assert catalog["0.4.19"].channel is ReleaseChannel.STABLE
        assert catalog["0.4.18"].channel is None
        page = env.client.get("/panel/releases").text
        assert "архив" in page and "текущий stable" in page

        # отзыв
        env.client.post("/panel/releases/0.4.19/channel", data={"channel": "none"})
        with env.factory() as session:
            catalog = {r.version: r for r in rollout.release_catalog(session, env.releases_dir)}
        assert catalog["0.4.19"].channel is None

    def test_channel_without_file_rejected(self, env: ReleasesEnv) -> None:
        _login(env)
        response = env.client.post(
            "/panel/releases/9.9.9/channel", data={"channel": "pilot"}, follow_redirects=False
        )
        assert response.status_code == 303
        assert "нет в каталоге" in unquote(response.headers["location"])
        response = env.client.post(
            "/panel/releases/abc/channel", data={"channel": "pilot"}, follow_redirects=False
        )
        assert response.status_code == 303

    def test_notes_saved(self, env: ReleasesEnv) -> None:
        _put_release(env, "0.4.19")
        _login(env)
        env.client.post("/panel/releases/0.4.19/notes", data={"notes": "самопроверка и автооткат"})
        page = env.client.get("/panel/releases").text
        assert 'value="самопроверка и автооткат"' in page


class TestUpload:
    def test_valid_upload_stored_and_registered(self, env: ReleasesEnv) -> None:
        _login(env)
        payload = _release_zip()
        response = env.client.post(
            "/panel/releases/upload",
            files={"file": ("ves-agent-0.4.19-win64.zip", payload, "application/zip")},
            follow_redirects=False,
        )
        assert response.status_code == 303
        stored = env.releases_dir / "ves-agent-0.4.19-win64.zip"
        assert stored.read_bytes() == payload
        assert not any(p.name.startswith(".upload") for p in env.releases_dir.iterdir())
        with env.factory() as session:
            row = session.execute(
                select(AgentRelease).where(AgentRelease.version == "0.4.19")
            ).scalar_one()
            assert row.channel is None and row.size_bytes == len(payload)
        page = env.client.get("/panel/releases").text
        assert "0.4.19" in page and "В pilot" in page

    @pytest.mark.parametrize(
        ("filename", "payload"),
        [
            ("evil.zip", _release_zip()),
            ("ves-agent-0.4.19-win64.zip", _release_zip(with_exe=False)),
            ("ves-agent-0.4.19-win64.zip", b"not a zip"),
        ],
    )
    def test_bad_uploads_rejected(self, env: ReleasesEnv, filename: str, payload: bytes) -> None:
        _login(env)
        response = env.client.post(
            "/panel/releases/upload",
            files={"file": (filename, payload, "application/zip")},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert list(env.releases_dir.iterdir()) == []
        with env.factory() as session:
            assert session.execute(select(AgentRelease)).scalars().all() == []

    def test_traversal_name_lands_in_catalog(self, env: ReleasesEnv) -> None:
        """От имени файла берётся только basename — обход путей невозможен."""
        _login(env)
        env.client.post(
            "/panel/releases/upload",
            files={"file": ("../../ves-agent-0.4.19-win64.zip", _release_zip(), "application/zip")},
        )
        assert (env.releases_dir / "ves-agent-0.4.19-win64.zip").exists()
        assert not (env.releases_dir.parent / "ves-agent-0.4.19-win64.zip").exists()

    def test_duplicate_version_rejected(self, env: ReleasesEnv) -> None:
        _put_release(env, "0.4.19")
        original = (env.releases_dir / "ves-agent-0.4.19-win64.zip").read_bytes()
        _login(env)
        env.client.post(
            "/panel/releases/upload",
            files={"file": ("ves-agent-0.4.19-win64.zip", b"PK-other", "application/zip")},
        )
        assert (env.releases_dir / "ves-agent-0.4.19-win64.zip").read_bytes() == original


class TestManualCommand:
    def test_command_goes_to_online_agent(self, env: ReleasesEnv) -> None:
        _put_release(env, "0.4.19")
        _login(env)
        env.client.post("/panel/releases/0.4.19/channel", data={"channel": "stable"})
        link = FakeLink()
        env.hub.attach(env.scale_id, link)
        page = env.client.get("/panel/releases").text
        assert "Ждёт очереди" in page and "Обновить сейчас" in page

        response = env.client.post(
            f"/panel/releases/agents/{env.scale_id}/update", follow_redirects=False
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/releases?note=")
        commands = _commands(link)
        assert len(commands) == 1 and commands[0].version == "0.4.19"
        assert commands[0].url_path == "/agents/releases/ves-agent-0.4.19-win64.zip"
        with env.factory() as session:
            row = session.execute(select(AgentUpdate)).scalar_one()
            assert row.version == "0.4.19" and row.origin == rollout.ORIGIN_MANUAL
        page = env.client.get("/panel/releases").text
        assert "Команда отправлена" in page

    def test_offline_and_no_target(self, env: ReleasesEnv) -> None:
        _login(env)
        response = env.client.post(
            f"/panel/releases/agents/{env.scale_id}/update", follow_redirects=False
        )
        assert "релиз не назначен" in unquote(response.headers["location"])
        _put_release(env, "0.4.19")
        env.client.post("/panel/releases/0.4.19/channel", data={"channel": "stable"})
        response = env.client.post(
            f"/panel/releases/agents/{env.scale_id}/update", follow_redirects=False
        )
        assert "не в сети" in unquote(response.headers["location"])
        with env.factory() as session:
            assert session.execute(select(AgentUpdate)).scalars().all() == []
        page = env.client.get("/panel/releases").text
        assert "Офлайн · получит при связи" in page

    def test_dashboard_button_shows_channel_target(self, env: ReleasesEnv) -> None:
        _put_release(env, "0.4.19")
        _put_release(env, "0.4.20")
        _login(env)
        env.client.post("/panel/releases/0.4.19/channel", data={"channel": "stable"})
        env.client.post("/panel/releases/0.4.20/channel", data={"channel": "pilot"})
        env.hub.attach(env.scale_id, FakeLink())
        with env.factory() as session:
            agent = session.get(Agent, env.agent_id)
            assert agent is not None
            agent.status = AgentStatus.ONLINE
            agent.last_seen_at = datetime.now(UTC)
            session.commit()
        page = env.client.get("/panel/fragments/dashboard").text
        # агент канала stable видит stable-цель, не pilot
        assert "Обновить до v0.4.19" in page and "0.4.20" not in page
        # переключили канал агента на pilot со страницы релизов → цель pilot
        response = env.client.post(
            f"/panel/refs/agents/{env.agent_id}/channel",
            data={"channel": "pilot", "back": "releases"},
            follow_redirects=False,
        )
        assert response.headers["location"].startswith("/panel/releases?note=")
        page = env.client.get("/panel/fragments/dashboard").text
        assert "Обновить до v0.4.20" in page


class TestNoDowngrade:
    def test_manual_command_never_downgrades(self, env: ReleasesEnv) -> None:
        """Агент выше релиза канала: ручная команда (даже прямым POST) не шлётся."""
        _put_release(env, "0.4.17")
        _login(env)
        env.client.post("/panel/releases/0.4.17/channel", data={"channel": "stable"})
        link = FakeLink()
        env.hub.attach(env.scale_id, link)  # агент на 0.4.18
        response = env.client.post(
            f"/panel/releases/agents/{env.scale_id}/update", follow_redirects=False
        )
        assert "не новее" in unquote(response.headers["location"])
        assert _commands(link) == []
        with env.factory() as session:
            assert session.execute(select(AgentUpdate)).scalars().all() == []
        # на дашборде кнопки тоже нет
        page = env.client.get("/panel/fragments/dashboard").text
        assert "Обновить до v" not in page
