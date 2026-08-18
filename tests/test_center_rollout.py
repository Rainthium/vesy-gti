"""Тесты автовыката агентов по каналам (center/rollout.py, 18.08.2026).

Живой PostgreSQL (как test_center_events): своя одноразовая БД
ves_test_rollout_<pid> с миграциями. Каталог релизов — tmp_path с
файлами-заглушками (sha256/размер настоящие, содержимое произвольное:
оглавление здесь не проверяется). Хаб — настоящий AgentHub с фейковыми
линками (кому уходят команды — видно по накопленным сообщениям).

Инварианты:
- каталог = файлы ∪ строки; назначение канала снимает его с прежней
  версии того же канала; без файла канал не назначить;
- цель агента: релиз его канала, pilot без своего берёт stable;
- движок шлёт команду только агентам на связи с версией НИЖЕ цели, не
  больше max_in_flight за раз, повтор отказа — не раньше retry_after и не
  больше max_attempts, откат — терминален, зависшая команда — повтор;
- журнал: командa → started → installed (по hello или самопроверке),
  failed/rolled_back с событиями мониторинга; hello с прежней версией
  после «установлено» — откат без доклада; «уже установлена» — installed.
"""

import asyncio
import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center import rollout
from center.agents_ws.hub import AgentHub
from center.db.models import (
    Agent,
    AgentRelease,
    AgentUpdate,
    AgentUpdateStatus,
    MonitoringEvent,
    MonitoringSeverity,
    ReleaseChannel,
    Scale,
    ScaleKind,
    Site,
)
from center.db.session import database_url, make_session_factory
from center.releases import ReleaseError
from shared.messages import UpdateCommand, UpdateStatus, parse_center_message
from tests.test_center_db import ALL_TABLES, _upgrade_head

NOW = datetime(2026, 8, 18, 9, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def rollout_db_url() -> Iterator[URL]:
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip("PostgreSQL недоступен — тесты раскатки пропущены")
    db_name = f"ves_test_rollout_{os.getpid()}"
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    test_url = admin_url.set(database=db_name)
    _upgrade_head(test_url)
    yield test_url
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
    admin_engine.dispose()


@pytest.fixture(scope="session")
def rollout_db_engine(rollout_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(rollout_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def factory(rollout_db_engine: Engine) -> sessionmaker[Session]:
    with rollout_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    return make_session_factory(rollout_db_engine)


@pytest.fixture
def releases_dir(tmp_path: Path) -> Path:
    path = tmp_path / "releases"
    path.mkdir()
    return path


def _put_release(releases_dir: Path, version: str, content: bytes | None = None) -> bytes:
    payload = content if content is not None else f"release-{version}".encode()
    (releases_dir / f"ves-agent-{version}-win64.zip").write_bytes(payload)
    return payload


def _seed_agent(
    factory: sessionmaker[Session],
    *,
    code: str,
    version: str | None,
    channel: ReleaseChannel = ReleaseChannel.STABLE,
) -> tuple[int, int]:
    """Объект + весы + агент; вернуть (agent_id, scale_id)."""
    with factory() as session:
        site = Site(code=code, name=f"Объект {code}")
        session.add(site)
        session.flush()
        scale = Scale(site_id=site.id, name="Весы", kind=ScaleKind.STATIC, driver="cas22")
        session.add(scale)
        session.flush()
        agent = Agent(
            scale_id=scale.id, token_hash=f"hash-{code}", version=version, channel=channel
        )
        session.add(agent)
        session.flush()
        ids = (agent.id, scale.id)
        session.commit()
    return ids


class FakeLink:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


def _commands(link: FakeLink) -> list[UpdateCommand]:
    out: list[UpdateCommand] = []
    for raw in link.sent:
        message = parse_center_message(raw)
        if isinstance(message, UpdateCommand):
            out.append(message)
    return out


def _events(factory: sessionmaker[Session]) -> list[MonitoringEvent]:
    with factory() as session:
        rows = list(session.execute(select(MonitoringEvent).order_by(MonitoringEvent.id)).scalars())
        for row in rows:
            session.expunge(row)
        return rows


def _row(factory: sessionmaker[Session], agent_id: int, version: str) -> AgentUpdate | None:
    with factory() as session:
        row = rollout.get_agent_update(session, agent_id, version)
        if row is not None:
            session.expunge(row)
        return row


def _must_row(session: Session, agent_id: int, version: str) -> AgentUpdate:
    row = rollout.get_agent_update(session, agent_id, version)
    assert row is not None
    return row


# ---------------------------------------------------------------------------
# Каталог и каналы
# ---------------------------------------------------------------------------


class TestCatalog:
    def test_files_and_rows_merge(self, factory: sessionmaker[Session], releases_dir: Path) -> None:
        payload = _put_release(releases_dir, "0.4.19")
        _put_release(releases_dir, "0.4.18")
        with factory() as session:
            # строка без файла (файл удалили с диска) — видна, но present=False
            session.add(
                AgentRelease(
                    version="0.4.10",
                    channel=None,
                    file_path="ves-agent-0.4.10-win64.zip",
                    sha256="x" * 64,
                    notes="старый",
                )
            )
            session.commit()
            catalog = rollout.release_catalog(session, releases_dir)
        assert [r.version for r in catalog] == ["0.4.19", "0.4.18", "0.4.10"]
        newest = catalog[0]
        assert newest.present and newest.channel is None
        assert newest.sha256 == hashlib.sha256(payload).hexdigest()
        assert newest.size_bytes == len(payload)
        assert newest.released_at is not None  # mtime файла
        assert catalog[2].present is False and catalog[2].notes == "старый"

    def test_channel_moves_between_versions(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        _put_release(releases_dir, "0.4.18")
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.18", ReleaseChannel.STABLE, by="admin", now=NOW
            )
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.PILOT, by="admin", now=NOW
            )
            targets = rollout.channel_targets(rollout.release_catalog(session, releases_dir))
            assert targets[ReleaseChannel.PILOT].version == "0.4.19"
            assert targets[ReleaseChannel.STABLE].version == "0.4.18"
            # перевод 0.4.19 в stable снимает stable с 0.4.18 (архив)
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin", now=NOW
            )
            catalog = rollout.release_catalog(session, releases_dir)
            by_version = {r.version: r for r in catalog}
            assert by_version["0.4.19"].channel is ReleaseChannel.STABLE
            assert by_version["0.4.18"].channel is None
            assert by_version["0.4.18"].channel_changed_at == NOW
            targets = rollout.channel_targets(catalog)
            assert ReleaseChannel.PILOT not in targets
            # отзыв
            rollout.set_release_channel(session, releases_dir, "0.4.19", None, by="admin", now=NOW)
            assert rollout.channel_targets(rollout.release_catalog(session, releases_dir)) == {}

    def test_channel_requires_file(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        with factory() as session, pytest.raises(ReleaseError):
            rollout.set_release_channel(
                session, releases_dir, "9.9.9", ReleaseChannel.PILOT, by="admin"
            )

    def test_target_for(self, releases_dir: Path) -> None:
        pilot = rollout.ReleaseInfo(
            "0.4.19", "f", "s", 1, True, ReleaseChannel.PILOT, "", None, None, None
        )
        stable = rollout.ReleaseInfo(
            "0.4.18", "f", "s", 1, True, ReleaseChannel.STABLE, "", None, None, None
        )
        both = {ReleaseChannel.PILOT: pilot, ReleaseChannel.STABLE: stable}
        assert rollout.target_for(ReleaseChannel.PILOT, both) is pilot
        assert rollout.target_for(ReleaseChannel.STABLE, both) is stable
        only_stable = {ReleaseChannel.STABLE: stable}
        assert rollout.target_for(ReleaseChannel.PILOT, only_stable) is stable
        assert rollout.target_for(ReleaseChannel.STABLE, {}) is None

    def test_notes(self, factory: sessionmaker[Session], releases_dir: Path) -> None:
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_notes(
                session, releases_dir, "0.4.19", "  самопроверка и автооткат  ", by="admin"
            )
            catalog = rollout.release_catalog(session, releases_dir)
            assert catalog[0].notes == "самопроверка и автооткат"


# ---------------------------------------------------------------------------
# Журнал раскатки: переходы
# ---------------------------------------------------------------------------


class TestUpdateJournal:
    def test_command_started_installed_by_hello(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            row = rollout.mark_commanded(
                session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO, now=NOW
            )
            assert row.status is AgentUpdateStatus.COMMANDED and row.attempts == 1
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=True),
                now=NOW,
            )
            assert _must_row(session, agent_id, "0.4.19").status is (AgentUpdateStatus.STARTED)
            changed = rollout.note_agent_hello(session, agent_id, scale_id, "0.4.19", now=NOW)
            assert [r.status for r in changed] == [AgentUpdateStatus.INSTALLED]
            assert changed[0].note == rollout.NOTE_HELLO
        # 0.4.19 докладывает сам: ok-событие ждём от самопроверки, не от hello
        assert _events(factory) == []
        with factory() as session:
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a",
                    version="0.4.19",
                    ok=True,
                    stage="installed",
                    running_version="0.4.19",
                ),
                now=NOW,
            )
        final = _row(factory, agent_id, "0.4.19")
        assert final is not None
        assert final.status is AgentUpdateStatus.INSTALLED
        assert final.note == rollout.NOTE_SELF_CHECK
        events = _events(factory)
        assert len(events) == 1 and events[0].severity is MonitoringSeverity.OK
        assert "обновлён до 0.4.19, самопроверка пройдена" in events[0].message

    def test_self_check_installed_without_hello_first(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO)
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=True, stage="installed"),
            )
        events = _events(factory)
        assert len(events) == 1 and events[0].severity is MonitoringSeverity.OK
        assert "самопроверка пройдена" in events[0].message

    def test_failure_and_rollback_events(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO)
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=False, error="sha256 не совпал"),
            )
            row = _must_row(session, agent_id, "0.4.19")
            assert row.status is AgentUpdateStatus.FAILED and row.error == "sha256 не совпал"
            # повтор командой → снова commanded, попытка вторая
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_MANUAL)
            row = _must_row(session, agent_id, "0.4.19")
            assert row.status is AgentUpdateStatus.COMMANDED and row.attempts == 2
            assert row.origin == rollout.ORIGIN_MANUAL and row.error is None
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a",
                    version="0.4.19",
                    ok=False,
                    error="откат на 0.4.18: нет связи с центром за 120 с",
                    stage="rolled_back",
                    running_version="0.4.18",
                ),
            )
            row = _must_row(session, agent_id, "0.4.19")
            assert row.status is AgentUpdateStatus.ROLLED_BACK
            assert row.running_version == "0.4.18"
        events = _events(factory)
        assert [e.severity for e in events] == [MonitoringSeverity.WARNING] * 2
        assert "не выполнено — sha256 не совпал" in events[0].message
        assert "не удалось, откат на 0.4.18" in events[1].message
        assert events[1].kind == "update_failed"

    def test_report_without_command_creates_row(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            row = rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=True),
            )
            assert row.status is AgentUpdateStatus.STARTED
            assert row.origin == rollout.ORIGIN_MANUAL and row.attempts == 1

    def test_already_installed_error_means_installed(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.19")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_MANUAL)
            row = rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a", version="0.4.19", ok=False, error="версия 0.4.19 уже установлена"
                ),
            )
            assert row.status is AgentUpdateStatus.INSTALLED and row.error is None
        assert _events(factory) == []

    def test_hello_with_older_version_after_installed_is_rollback(
        self, factory: sessionmaker[Session]
    ) -> None:
        """Агент до 0.4.19 (сам не докладывает): версии 0.4.17 → 0.4.18."""
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.17")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.18", origin=rollout.ORIGIN_AUTO)
            rollout.note_agent_hello(session, agent_id, scale_id, "0.4.18")
            # повторный hello той же версии ничего не меняет
            assert rollout.note_agent_hello(session, agent_id, scale_id, "0.4.18") == []
            changed = rollout.note_agent_hello(session, agent_id, scale_id, "0.4.17")
            assert [r.status for r in changed] == [AgentUpdateStatus.ROLLED_BACK]
            assert "откат без доклада" in (changed[0].error or "")
        events = _events(factory)
        assert [e.severity for e in events] == [MonitoringSeverity.OK, MonitoringSeverity.WARNING]

    def test_hello_with_old_version_while_commanded_is_ignored(
        self, factory: sessionmaker[Session]
    ) -> None:
        """Реконнект старого агента до рестарта — не отказ и не установка."""
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO)
            assert rollout.note_agent_hello(session, agent_id, scale_id, "0.4.18") == []
            row = _must_row(session, agent_id, "0.4.19")
            assert row.status is AgentUpdateStatus.COMMANDED


# ---------------------------------------------------------------------------
# Движок
# ---------------------------------------------------------------------------


def _service(
    factory: sessionmaker[Session], hub: AgentHub, releases_dir: Path, **overrides: object
) -> rollout.RolloutService:
    fields: dict[str, object] = {"now": lambda: NOW}
    fields.update(overrides)
    return rollout.RolloutService(factory, hub, releases_dir, **fields)  # type: ignore[arg-type]


def _tick(service: rollout.RolloutService) -> list[rollout.RolloutPlan]:
    return asyncio.run(service.tick())


class TestRolloutService:
    def test_sends_only_to_online_agents_below_target(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        payload = _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        old_id, old_scale = _seed_agent(factory, code="old", version="0.4.18")
        _, current_scale = _seed_agent(factory, code="cur", version="0.4.19")
        _, newer_scale = _seed_agent(factory, code="new", version="0.4.20")
        _, offline_scale = _seed_agent(factory, code="off", version="0.4.18")
        _, unknown_scale = _seed_agent(factory, code="unk", version=None)
        hub = AgentHub()
        links = {}
        for scale_id in (old_scale, current_scale, newer_scale, unknown_scale):
            links[scale_id] = FakeLink()
            hub.attach(scale_id, links[scale_id])
        service = _service(factory, hub, releases_dir)

        sent = _tick(service)
        assert [(p.scale_id, p.release.version) for p in sent] == [(old_scale, "0.4.19")]
        commands = _commands(links[old_scale])
        assert len(commands) == 1
        assert commands[0].url_path == "/agents/releases/ves-agent-0.4.19-win64.zip"
        assert commands[0].sha256 == hashlib.sha256(payload).hexdigest()
        assert commands[0].size_bytes == len(payload)
        for scale_id in (current_scale, newer_scale, unknown_scale):
            assert _commands(links[scale_id]) == []
        row = _row(factory, old_id, "0.4.19")
        assert row is not None and row.status is AgentUpdateStatus.COMMANDED
        assert row.origin == rollout.ORIGIN_AUTO and row.attempts == 1
        # второй проход: команда в полёте — повтора нет
        assert _tick(service) == []
        assert len(_commands(links[old_scale])) == 1
        # офлайн-агент ничего не получил
        assert offline_scale not in links

    def test_pilot_channel_gets_pilot_release_and_falls_back_to_stable(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        _put_release(releases_dir, "0.4.19")
        _put_release(releases_dir, "0.4.20")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        _, pilot_scale = _seed_agent(
            factory, code="p", version="0.4.18", channel=ReleaseChannel.PILOT
        )
        _, stable_scale = _seed_agent(factory, code="s", version="0.4.18")
        hub = AgentHub()
        pilot_link, stable_link = FakeLink(), FakeLink()
        hub.attach(pilot_scale, pilot_link)
        hub.attach(stable_scale, stable_link)
        service = _service(factory, hub, releases_dir)
        sent = _tick(service)
        # без pilot-релиза оба берут stable 0.4.19
        assert sorted(p.release.version for p in sent) == ["0.4.19", "0.4.19"]
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.20", ReleaseChannel.PILOT, by="admin"
            )
        # у обоих команда 0.4.19 в полёте; pilot-агент «обновился» → hello 0.4.19
        with factory() as session:
            pilot_agent = session.execute(
                select(Agent).where(Agent.scale_id == pilot_scale)
            ).scalar_one()
            pilot_agent.version = "0.4.19"
            session.commit()
            # установка «давно» — окно устаканивания (10 мин) прошло
            rollout.note_agent_hello(
                session, pilot_agent.id, pilot_scale, "0.4.19", now=NOW - timedelta(minutes=15)
            )
        sent = _tick(service)
        assert [(p.scale_id, p.release.version) for p in sent] == [(pilot_scale, "0.4.20")]
        # stable-агент pilot-релиз не получает
        assert [c.version for c in _commands(stable_link)] == ["0.4.19"]

    def test_in_flight_limit(self, factory: sessionmaker[Session], releases_dir: Path) -> None:
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        hub = AgentHub()
        for i in range(5):
            _, scale_id = _seed_agent(factory, code=f"s{i}", version="0.4.18")
            hub.attach(scale_id, FakeLink())
        service = _service(factory, hub, releases_dir, max_in_flight=2)
        assert len(_tick(service)) == 2
        assert _tick(service) == []  # двое в полёте — лимит исчерпан
        # один отчитался installed → освободилось место для одного
        with factory() as session:
            row = session.execute(select(AgentUpdate).order_by(AgentUpdate.id)).scalars().first()
            assert row is not None
            row.status = AgentUpdateStatus.INSTALLED
            session.commit()
        assert len(_tick(service)) == 1

    def test_retry_policy(self, factory: sessionmaker[Session], releases_dir: Path) -> None:
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        hub = AgentHub()
        link = FakeLink()
        hub.attach(scale_id, link)
        clock = {"now": NOW}
        service = _service(
            factory,
            hub,
            releases_dir,
            now=lambda: clock["now"],
            max_attempts=3,
            retry_after=timedelta(minutes=30),
            stale_after=timedelta(minutes=30),
        )
        assert len(_tick(service)) == 1
        # отказ агента → повтор не раньше чем через 30 мин
        with factory() as session:
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=False, error="весы заняты"),
                now=NOW,
            )
        assert _tick(service) == []
        clock["now"] = NOW + timedelta(minutes=31)
        assert len(_tick(service)) == 1  # попытка 2
        row = _row(factory, agent_id, "0.4.19")
        assert row is not None and row.attempts == 2
        # зависшая команда без ответа → повтор после stale_after
        clock["now"] = NOW + timedelta(minutes=62)
        assert len(_tick(service)) == 1  # попытка 3
        # лимит попыток исчерпан
        with factory() as session:
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=False, error="весы заняты"),
                now=clock["now"],
            )
        clock["now"] = NOW + timedelta(hours=5)
        assert _tick(service) == []
        assert len(_commands(link)) == 3

    def test_rolled_back_is_terminal_until_manual(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        hub = AgentHub()
        link = FakeLink()
        hub.attach(scale_id, link)
        clock = {"now": NOW}
        service = _service(factory, hub, releases_dir, now=lambda: clock["now"])
        assert len(_tick(service)) == 1
        with factory() as session:
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a",
                    version="0.4.19",
                    ok=False,
                    error="откат",
                    stage="rolled_back",
                    running_version="0.4.18",
                ),
                now=NOW,
            )
        clock["now"] = NOW + timedelta(days=3)
        assert _tick(service) == []
        # человек нажал «Повторить» (mark_commanded manual) → команда в полёте,
        # движок её не дублирует
        with factory() as session:
            rollout.mark_commanded(
                session, agent_id, "0.4.19", origin=rollout.ORIGIN_MANUAL, now=clock["now"]
            )
        assert _tick(service) == []

    def test_no_targets_no_commands(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        _put_release(releases_dir, "0.4.19")  # файл есть, канал не назначен
        _, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        hub = AgentHub()
        link = FakeLink()
        hub.attach(scale_id, link)
        assert _tick(_service(factory, hub, releases_dir)) == []
        assert link.sent == []


# ---------------------------------------------------------------------------
# Правки по ревью 18.08.2026
# ---------------------------------------------------------------------------


class TestReviewFixes1808:
    def test_self_reporting_agent_rollback_yields_single_warning(
        self, factory: sessionmaker[Session]
    ) -> None:
        """Агент 0.4.19+: hello(new) → hello(old) → rolled_back даёт ОДНО событие
        (warning с причиной), а не три; ok-событие — только по самопроверке."""
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.19")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.20", origin=rollout.ORIGIN_AUTO)
            changed = rollout.note_agent_hello(session, agent_id, scale_id, "0.4.20")
            assert [r.status for r in changed] == [AgentUpdateStatus.INSTALLED]
            assert _events(factory) == []  # ok-событие по hello для 0.4.19+ не пишем
            changed = rollout.note_agent_hello(session, agent_id, scale_id, "0.4.19")
            assert [r.status for r in changed] == [AgentUpdateStatus.ROLLED_BACK]
            assert _events(factory) == []  # ждём доклад самого агента
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a",
                    version="0.4.20",
                    ok=False,
                    error="откат на 0.4.19: индикатор не шлёт данные за 90 с",
                    stage="rolled_back",
                    running_version="0.4.19",
                ),
            )
        events = _events(factory)
        assert len(events) == 1 and events[0].severity is MonitoringSeverity.WARNING
        assert "индикатор не шлёт данные" in events[0].message

    def test_self_reporting_agent_success_single_ok(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.19")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.20", origin=rollout.ORIGIN_AUTO)
            rollout.note_agent_hello(session, agent_id, scale_id, "0.4.20")
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.20", ok=True, stage="installed"),
            )
        events = _events(factory)
        assert [e.severity for e in events] == [MonitoringSeverity.OK]
        assert "самопроверка пройдена" in events[0].message

    def test_legacy_agent_events_by_hello(self, factory: sessionmaker[Session]) -> None:
        """Агент до 0.4.19 сам не докладывает — события по hello остаются."""
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.17")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.18", origin=rollout.ORIGIN_AUTO)
            rollout.note_agent_hello(session, agent_id, scale_id, "0.4.18")
            rollout.note_agent_hello(session, agent_id, scale_id, "0.4.17")
        assert [e.severity for e in _events(factory)] == [
            MonitoringSeverity.OK,
            MonitoringSeverity.WARNING,
        ]

    def test_already_running_is_not_a_failure(self, factory: sessionmaker[Session]) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO)
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=True),
            )
            row = rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(
                    agent_id="a", version="0.4.19", ok=False, error="обновление уже выполняется"
                ),
            )
            assert row.status is AgentUpdateStatus.STARTED and row.error is None
        assert _events(factory) == []

    def test_hello_promotes_failed_row_when_version_matches(
        self, factory: sessionmaker[Session]
    ) -> None:
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        with factory() as session:
            rollout.mark_commanded(session, agent_id, "0.4.19", origin=rollout.ORIGIN_AUTO)
            rollout.apply_update_status(
                session,
                agent_id,
                scale_id,
                UpdateStatus(agent_id="a", version="0.4.19", ok=False, error="что-то"),
            )
            changed = rollout.note_agent_hello(session, agent_id, scale_id, "0.4.19")
            assert [r.status for r in changed] == [AgentUpdateStatus.INSTALLED]
            assert changed[0].error is None

    def test_settle_window_blocks_next_version(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        """Только что обновившемуся агенту следующую версию не шлём 10 минут."""
        _put_release(releases_dir, "0.4.19")
        _put_release(releases_dir, "0.4.20")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")
        hub = AgentHub()
        link = FakeLink()
        hub.attach(scale_id, link)
        clock = {"now": NOW}
        service = _service(factory, hub, releases_dir, now=lambda: clock["now"])
        assert len(_tick(service)) == 1
        with factory() as session:
            agent = session.get(Agent, agent_id)
            assert agent is not None
            agent.version = "0.4.19"
            session.commit()
            rollout.note_agent_hello(session, agent_id, scale_id, "0.4.19", now=NOW)
            rollout.set_release_channel(
                session, releases_dir, "0.4.20", ReleaseChannel.STABLE, by="admin"
            )
        clock["now"] = NOW + timedelta(minutes=5)
        assert _tick(service) == []  # устаканивается
        clock["now"] = NOW + timedelta(minutes=11)
        sent = _tick(service)
        assert [p.release.version for p in sent] == ["0.4.20"]

    def test_send_failure_returns_attempt(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        """Строка отмечается ДО отправки; неотправленная команда не съедает попытку."""
        _put_release(releases_dir, "0.4.19")
        with factory() as session:
            rollout.set_release_channel(
                session, releases_dir, "0.4.19", ReleaseChannel.STABLE, by="admin"
            )
        agent_id, scale_id = _seed_agent(factory, code="a", version="0.4.18")

        class DeadLink:
            async def send_text(self, data: str) -> None:
                raise ConnectionError("линк умер")

        hub = AgentHub()
        hub.attach(scale_id, DeadLink())
        service = _service(factory, hub, releases_dir)
        assert _tick(service) == []
        row = _row(factory, agent_id, "0.4.19")
        assert row is not None
        assert row.status is AgentUpdateStatus.FAILED and row.attempts == 0
        assert row.error is not None and "не отправлена" in row.error

    def test_notes_and_withdraw_require_known_release(
        self, factory: sessionmaker[Session], releases_dir: Path
    ) -> None:
        with factory() as session:
            with pytest.raises(ReleaseError):
                rollout.set_release_notes(session, releases_dir, "9.9.9", "фантом", by="admin")
            with pytest.raises(ReleaseError):
                rollout.set_release_channel(session, releases_dir, "9.9.9", None, by="admin")
            assert session.execute(select(AgentRelease)).scalars().all() == []


class TestReleaseFiles:
    def test_non_canonical_names_ignored(self, releases_dir: Path) -> None:
        from center.releases import list_releases, parse_release_filename

        (releases_dir / "ves-agent-0.04.19-win64.zip").write_bytes(b"x")
        (releases_dir / "ves-agent-0.4.19-win64.zip").write_bytes(b"y")
        assert [r.version for r in list_releases(releases_dir)] == ["0.4.19"]
        assert parse_release_filename("ves-agent-0.04.19-win64.zip") is None
        assert parse_release_filename("ves-agent-0.4.19-win64.zip") == "0.4.19"

    def test_store_release_is_atomic_and_no_overwrite(
        self, releases_dir: Path, tmp_path: Path
    ) -> None:
        import io
        import zipfile

        from center.releases import store_release

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as bundle:
            bundle.writestr("app/ves-agent.exe", b"MZ")
        upload = tmp_path / "upload.tmp"  # другой каталог: копия в .part → link
        upload.write_bytes(buffer.getvalue())
        release = store_release(releases_dir, "ves-agent-0.4.19-win64.zip", upload)
        assert release.version == "0.4.19"
        assert not upload.exists()
        assert sorted(p.name for p in releases_dir.iterdir()) == ["ves-agent-0.4.19-win64.zip"]
        # повтор той же версии — отказ, файл цел
        upload.write_bytes(buffer.getvalue())
        with pytest.raises(ReleaseError):
            store_release(releases_dir, "ves-agent-0.4.19-win64.zip", upload)
        assert (releases_dir / "ves-agent-0.4.19-win64.zip").read_bytes() == buffer.getvalue()
        assert not any(p.name.startswith(".") for p in releases_dir.iterdir())
