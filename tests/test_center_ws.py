"""Тесты WebSocket-сервера агентов центра (center/agents_ws + center/db/repo).

Покрытие:
- AgentHub без сети (фейковые линки): attach/detach и вытеснение, круг
  send_weigh_request → resolve_result, тайм-аут, ERR_BUSY, очистка pending,
  fail_pending_for_scale бьёт только по своим весам, broadcast с умершим линком;
- repo на живом PostgreSQL: аутентификация по хешу токена, идемпотентная
  запись журнала с контрольной суммой, резолв tare_weighing_uuid, обновление
  реестра тар (OK/ERR_CAMERA — да, ERR_UNSTABLE и weighing — нет), досылка
  не по порядку не затирает актуальную тару, фильтр 3 месяцев, фото,
  тара по ПАРЕ голова+прицеп (решение 09.08.2026): регистрация под парой,
  find_active_tare только при совпадении обоих номеров, сосуществование
  тар одной головы с разными прицепами;
- WS-эндпоинт через TestClient: закрытие 4401 без/с неверным токеном,
  hello → tare_registry + статус online, журнал без дублей, offline_sync
  с ack на все uuid, живучесть после мусора, offline при разрыве,
  вытеснение старого соединения новым;
- репликация операторов (решение 10.08.2026): hub.send_operators (адресность,
  best-effort), repo.load_operators_for_scale (фильтры по роли/объекту,
  pw_hash байт-в-байт), после hello — tare_registry и следом
  operators_registry в фиксированном порядке;
- настройки весов из центра (решение 10.08.2026): repo.load_scale_settings
  (полный/частичный снимок, кривые thresholds, камеры только с URL,
  пусто → None), после hello — scale_config ТРЕТЬИМ и только при заданных
  настройках, hub.send_scale_config (адресность, best-effort), config_status
  от агента не рвёт соединение.

Инфраструктура БД повторяет tests/test_center_db.py (временная БД + миграции
alembic + TRUNCATE между тестами), но с собственным именем БД, чтобы не
конфликтовать с ней в одном прогоне.
"""

import asyncio
import hashlib
import json
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, func, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from starlette.testclient import WebSocketTestSession
from starlette.websockets import WebSocketDisconnect

from center.agents_ws.hub import AgentHub, AgentHubError
from center.agents_ws.router import create_agents_router
from center.db import repo
from center.db.models import (
    Agent,
    AgentStatus,
    Camera,
    Scale,
    ScaleKind,
    Site,
    TareRegistry,
    User,
    UserRole,
    Weighing,
    WeighingPhoto,
    weighing_checksum,
)
from center.db.session import database_url, make_session_factory
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import (
    ConfigStatus,
    EquipmentStatus,
    Hello,
    OfflineSync,
    OperatorRecord,
    OperatorsRegistryUpdate,
    PhotoMeta,
    ScaleConfigUpdate,
    ScaleSettingsPayload,
    TareRegistryUpdate,
    WeighingRecord,
    WeighRequest,
    WeighResult,
    parse_center_message,
)
from tests.test_center_db import ALL_TABLES, _upgrade_head

AGENT_TOKEN = "agent-secret-token-01"
AGENT_VERSION = "1.2.3"
AUTH_HEADERS = {"Authorization": f"Bearer {AGENT_TOKEN}"}

SHA_A = "a" * 64
SHA_B = "b" * 64


# ---------------------------------------------------------------------------
# Хелперы построения сообщений
# ---------------------------------------------------------------------------


def _make_record(**overrides: Any) -> WeighingRecord:
    """Типичная успешная запись взвешивания; overrides — точечные замены."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 15000.0,
        "stable": True,
        "weighed_at": datetime.now(UTC),
        "vehicle_number": "01KG123ABC",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _make_taring(**overrides: Any) -> WeighingRecord:
    """Успешное тарирование (обновляет реестр тар)."""
    fields: dict[str, Any] = {"operation": Operation.TARING, "massa": 7500.0}
    fields.update(overrides)
    return _make_record(**fields)


def _make_result(request_id: UUID | None = None, **overrides: Any) -> WeighResult:
    return WeighResult(request_id=request_id or uuid4(), record=_make_record(**overrides))


def _make_request(**overrides: Any) -> WeighRequest:
    fields: dict[str, Any] = {"request_id": uuid4(), "operation": Operation.WEIGHING}
    fields.update(overrides)
    return WeighRequest(**fields)


def _hello_json(version: str = AGENT_VERSION) -> str:
    equipment = EquipmentStatus(scale_status=ScaleStatus.OK)
    hello = Hello(agent_id="agent-1", version=version, driver="cas22", equipment=equipment)
    return hello.model_dump_json()


# ---------------------------------------------------------------------------
# Фейковые соединения для юнит-тестов хаба
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


# ---------------------------------------------------------------------------
# AgentHub: жизненный цикл соединений (без event loop)
# ---------------------------------------------------------------------------


class TestAgentHubConnections:
    def test_attach_first_returns_none(self) -> None:
        """Первое подключение весов ничего не вытесняет."""
        hub = AgentHub()
        assert hub.attach(1, FakeLink()) is None
        assert hub.connected(1)

    def test_attach_displaces_and_returns_old_link(self) -> None:
        """Новое соединение вытесняет старое; attach возвращает вытесненное."""
        hub = AgentHub()
        old, new = FakeLink(), FakeLink()
        hub.attach(1, old)
        displaced = hub.attach(1, new)
        assert displaced is old
        assert hub.connected(1)
        assert hub.connected_scale_ids() == [1]  # весы учтены один раз

    def test_detach_foreign_link_is_noop(self) -> None:
        """detach вытесненного (чужого) линка не снимает текущее соединение."""
        hub = AgentHub()
        old, new = FakeLink(), FakeLink()
        hub.attach(1, old)
        hub.attach(1, new)
        hub.detach(1, old)  # запоздалая уборка старого соединения
        assert hub.connected(1), "detach чужого линка снял живое соединение"

    def test_detach_current_link_disconnects(self) -> None:
        """detach текущего линка снимает соединение."""
        hub = AgentHub()
        link = FakeLink()
        hub.attach(1, link)
        hub.detach(1, link)
        assert not hub.connected(1)
        assert hub.connected_scale_ids() == []

    def test_equipment_stored_and_cleared_on_detach(self) -> None:
        """Самодиагностика живёт, пока живо соединение; detach чужого линка
        её не стирает (для дашборда панели)."""
        hub = AgentHub()
        link = FakeLink()
        hub.attach(1, link)
        hub.update_equipment(1, EquipmentStatus(scale_status=ScaleStatus.OK))
        equipment = hub.equipment(1)
        assert equipment is not None and equipment.scale_status is ScaleStatus.OK

        hub.detach(1, FakeLink())  # чужой (вытесненный) линк
        assert hub.equipment(1) is not None
        hub.detach(1, link)  # текущее соединение умерло — статусы стухли
        assert hub.equipment(1) is None

    def test_connected_scale_ids_lists_all(self) -> None:
        """connected_scale_ids перечисляет все подключённые весы."""
        hub = AgentHub()
        hub.attach(1, FakeLink())
        hub.attach(7, FakeLink())
        assert sorted(hub.connected_scale_ids()) == [1, 7]
        assert not hub.connected(3)


# ---------------------------------------------------------------------------
# AgentHub: команды взвешивания (чистый asyncio, без сети)
# ---------------------------------------------------------------------------


class TestAgentHubWeighCommands:
    def test_offline_agent_raises_agent_offline(self) -> None:
        """Агент не подключён → AgentHubError с кодом ERR_AGENT_OFFLINE."""

        async def scenario() -> None:
            hub = AgentHub()
            with pytest.raises(AgentHubError) as excinfo:
                await hub.send_weigh_request(1, _make_request())
            assert excinfo.value.code is ErrorCode.ERR_AGENT_OFFLINE

        asyncio.run(scenario())

    def test_success_roundtrip(self) -> None:
        """Команда уходит агенту; resolve_result будит ожидание тем же результатом."""

        async def scenario() -> None:
            hub = AgentHub()
            link = FakeLink()
            hub.attach(1, link)
            request = _make_request()
            task = asyncio.create_task(hub.send_weigh_request(1, request))
            await asyncio.sleep(0)  # даём команде отправиться и встать в pending

            # агенту ушёл корректный weigh_request с тем же request_id
            assert len(link.sent) == 1
            sent = parse_center_message(link.sent[0])
            assert isinstance(sent, WeighRequest)
            assert sent.request_id == request.request_id

            result = _make_result(request.request_id)
            assert hub.resolve_result(result) is True
            received = await task
            assert received is result  # тот же объект, без пересборки

            # pending очищен: повторный resolve никого не находит
            assert hub.resolve_result(result) is False

        asyncio.run(scenario())

    def test_timeout_raises_internal_and_clears_pending(self) -> None:
        """Никто не отвечает за timeout_s → ERR_INTERNAL, pending очищается."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, FakeLink())
            request = _make_request()
            with pytest.raises(AgentHubError) as excinfo:
                await hub.send_weigh_request(1, request, timeout_s=0.1)
            assert excinfo.value.code is ErrorCode.ERR_INTERNAL
            # поздний ответ после тайм-аута уже никого не будит
            assert hub.resolve_result(_make_result(request.request_id)) is False

        asyncio.run(scenario())

    def test_duplicate_request_id_raises_busy(self) -> None:
        """Повторная команда с тем же request_id, пока первая в полёте → ERR_BUSY."""

        async def scenario() -> None:
            hub = AgentHub()
            link = FakeLink()
            hub.attach(1, link)
            request = _make_request()
            task = asyncio.create_task(hub.send_weigh_request(1, request))
            await asyncio.sleep(0)

            with pytest.raises(AgentHubError) as excinfo:
                await hub.send_weigh_request(1, request)
            assert excinfo.value.code is ErrorCode.ERR_BUSY
            # дубль не отправлялся агенту
            assert len(link.sent) == 1

            # первая команда не пострадала и завершается успешно
            result = _make_result(request.request_id)
            assert hub.resolve_result(result) is True
            assert await task is result

        asyncio.run(scenario())

    def test_resolve_result_without_waiter_returns_false(self) -> None:
        """«Ничей» результат (после рестарта центра) → False, без исключений."""
        hub = AgentHub()
        assert hub.resolve_result(_make_result()) is False

    def test_resolve_result_twice_second_is_false(self) -> None:
        """Повторный resolve той же команды (future уже done) → False."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, FakeLink())
            request = _make_request()
            task = asyncio.create_task(hub.send_weigh_request(1, request))
            await asyncio.sleep(0)

            result = _make_result(request.request_id)
            assert hub.resolve_result(result) is True
            # задача ещё не возобновилась — запись в pending, но future done
            assert hub.resolve_result(_make_result(request.request_id)) is False
            await task

        asyncio.run(scenario())

    def test_fail_pending_only_affects_target_scale(self) -> None:
        """Разрыв одних весов не задевает команды других весов."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, FakeLink())
            hub.attach(2, FakeLink())
            request1, request2 = _make_request(), _make_request()
            task1 = asyncio.create_task(hub.send_weigh_request(1, request1))
            task2 = asyncio.create_task(hub.send_weigh_request(2, request2))
            await asyncio.sleep(0)

            hub.fail_pending_for_scale(1, "связь потеряна")

            with pytest.raises(AgentHubError) as excinfo:
                await task1
            assert excinfo.value.code is ErrorCode.ERR_AGENT_OFFLINE

            # команда вторых весов жива и завершается успешно
            result2 = _make_result(request2.request_id)
            assert hub.resolve_result(result2) is True
            assert await task2 is result2

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# AgentHub: рассылка реестра тарирований
# ---------------------------------------------------------------------------


class TestAgentHubBroadcast:
    def test_broadcast_reaches_all_links(self) -> None:
        """Снимок реестра уходит всем подключённым агентам."""

        async def scenario() -> None:
            hub = AgentHub()
            links = [FakeLink(), FakeLink(), FakeLink()]
            for scale_id, link in enumerate(links, start=1):
                hub.attach(scale_id, link)
            update = TareRegistryUpdate(records=[])
            sent = await hub.broadcast_tare_registry(update)
            assert sent == 3
            for link in links:
                assert len(link.sent) == 1
                assert json.loads(link.sent[0])["type"] == "tare_registry"

        asyncio.run(scenario())

    def test_dead_link_does_not_block_others(self) -> None:
        """Умерший линк (send_text бросает) не мешает рассылке остальным."""

        async def scenario() -> None:
            hub = AgentHub()
            alive1, dead, alive2 = FakeLink(), DeadLink(), FakeLink()
            hub.attach(1, alive1)
            hub.attach(2, dead)
            hub.attach(3, alive2)
            sent = await hub.broadcast_tare_registry(TareRegistryUpdate(records=[]))
            assert sent == 2  # умерший в счётчик не попал
            assert len(alive1.sent) == 1
            assert len(alive2.sent) == 1

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Инфраструктура БД: временная БД + миграции (подход tests/test_center_db.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ws_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_ws_<pid> с миграциями; имя не пересекается
    с БД test_center_db, чтобы модули не мешали друг другу в одном прогоне."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты WS-сервера центра пропущены"
        )

    db_name = f"ves_test_ws_{os.getpid()}"
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
def ws_db_engine(ws_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(ws_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


def _truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))


def _seed_graph(session: Session) -> tuple[int, int]:
    """Объект + весы + агент с известным токеном; вернуть (scale_id, agent_id)."""
    site = Site(code="test-site", name="Тестовый объект")
    session.add(site)
    session.flush()
    scale = Scale(site_id=site.id, name="Весы", kind=ScaleKind.STATIC, driver="cas22")
    session.add(scale)
    session.flush()
    agent = Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(AGENT_TOKEN))
    session.add(agent)
    session.flush()
    session.commit()
    return scale.id, agent.id


@pytest.fixture
def repo_env(ws_db_engine: Engine) -> Iterator[tuple[Session, int, int]]:
    """Чистая БД + посев графа; отдаёт (session, scale_id, agent_id)."""
    _truncate_all(ws_db_engine)
    factory = make_session_factory(ws_db_engine)
    session = factory()
    scale_id, agent_id = _seed_graph(session)
    yield session, scale_id, agent_id
    session.rollback()
    session.close()


# ---------------------------------------------------------------------------
# repo: аутентификация
# ---------------------------------------------------------------------------


class TestRepoAuth:
    def test_hash_agent_token_is_sha256_hex(self) -> None:
        """Хеш токена — sha256 hexdigest (правило №7: сырой токен не храним)."""
        assert repo.hash_agent_token("abc") == hashlib.sha256(b"abc").hexdigest()
        digest = repo.hash_agent_token(AGENT_TOKEN)
        assert len(digest) == 64
        assert set(digest) <= set("0123456789abcdef")

    def test_authenticate_agent_valid_token(self, repo_env: tuple[Session, int, int]) -> None:
        """Верный токен находит агента и его весы."""
        session, scale_id, agent_id = repo_env
        agent = repo.authenticate_agent(session, AGENT_TOKEN)
        assert agent is not None
        assert agent.id == agent_id
        assert agent.scale_id == scale_id

    def test_authenticate_agent_wrong_token(self, repo_env: tuple[Session, int, int]) -> None:
        """Неверный токен → None."""
        session, _, _ = repo_env
        assert repo.authenticate_agent(session, "wrong-token") is None

    def test_authenticate_agent_empty_token(self, repo_env: tuple[Session, int, int]) -> None:
        """Пустой токен → None (sha256('') не должен совпасть ни с кем)."""
        session, _, _ = repo_env
        assert repo.authenticate_agent(session, "") is None


# ---------------------------------------------------------------------------
# repo: журнал взвешиваний
# ---------------------------------------------------------------------------


def _weighing_by_uuid(session: Session, record_uuid: UUID) -> Weighing:
    session.expire_all()
    return session.execute(select(Weighing).where(Weighing.uuid == record_uuid)).scalar_one()


def _weighing_by_uuid_or_none(session: Session, record_uuid: UUID) -> Weighing | None:
    session.expire_all()
    return session.execute(
        select(Weighing).where(Weighing.uuid == record_uuid)
    ).scalar_one_or_none()


def _weighing_count(session: Session, record_uuid: UUID) -> int:
    count = session.execute(
        select(func.count()).select_from(Weighing).where(Weighing.uuid == record_uuid)
    ).scalar_one()
    return int(count)


class TestRepoSaveWeighing:
    def test_new_record_saved_with_checksum(self, repo_env: tuple[Session, int, int]) -> None:
        """Новая запись → True; строка в weighings с корректной checksum."""
        session, scale_id, _ = repo_env
        record = _make_record()
        assert repo.save_weighing_record(session, scale_id, record) is True

        row = _weighing_by_uuid(session, record.uuid)
        assert row.scale_id == scale_id
        assert row.massa == record.massa
        assert row.vehicle_number == record.vehicle_number
        assert row.checksum == weighing_checksum(
            uuid=record.uuid,
            operation=record.operation.value,
            code=record.code.value,
            massa=record.massa,
            weighed_at=record.weighed_at,
            vehicle_number=record.vehicle_number,
            source=record.source.value,
            photo_sha256s=[],
        )

    def test_duplicate_uuid_is_idempotent(self, repo_env: tuple[Session, int, int]) -> None:
        """Повтор того же uuid (досылка) → False, дублей в журнале нет."""
        session, scale_id, _ = repo_env
        record = _make_record()
        assert repo.save_weighing_record(session, scale_id, record) is True
        assert repo.save_weighing_record(session, scale_id, record) is False
        assert _weighing_count(session, record.uuid) == 1

    def test_tare_weighing_uuid_resolved_to_id(self, repo_env: tuple[Session, int, int]) -> None:
        """tare_weighing_uuid резолвится в id записи тарирования."""
        session, scale_id, _ = repo_env
        taring = _make_taring()
        repo.save_weighing_record(session, scale_id, taring)
        taring_row = _weighing_by_uuid(session, taring.uuid)

        brutto = _make_record(
            tare_weighing_uuid=taring.uuid, tare_value=7500.0, netto=15000.0 - 7500.0
        )
        assert repo.save_weighing_record(session, scale_id, brutto) is True
        row = _weighing_by_uuid(session, brutto.uuid)
        assert row.tare_weighing_id == taring_row.id
        assert row.netto == 7500.0

    def test_unknown_tare_uuid_saved_with_none(self, repo_env: tuple[Session, int, int]) -> None:
        """Несуществующий uuid тары → tare_weighing_id None, запись сохраняется."""
        session, scale_id, _ = repo_env
        record = _make_record(tare_weighing_uuid=uuid4())
        assert repo.save_weighing_record(session, scale_id, record) is True
        row = _weighing_by_uuid(session, record.uuid)
        assert row.tare_weighing_id is None

    def test_photos_saved_and_bound_to_checksum(self, repo_env: tuple[Session, int, int]) -> None:
        """Фото пишутся в weighing_photos; их sha входят в контрольную сумму."""
        session, scale_id, _ = repo_env
        photos = [
            PhotoMeta(role=CameraRole.FRONT, filename="front.jpeg", sha256=SHA_A, size_bytes=100),
            PhotoMeta(role=CameraRole.REAR, filename="rear.jpeg", sha256=SHA_B, size_bytes=200),
        ]
        record = _make_record()
        assert repo.save_weighing_record(session, scale_id, record, photos) is True

        row = _weighing_by_uuid(session, record.uuid)
        photo_rows = (
            session.execute(
                select(WeighingPhoto)
                .where(WeighingPhoto.weighing_id == row.id)
                .order_by(WeighingPhoto.id)
            )
            .scalars()
            .all()
        )
        # пути канонические (центр игнорирует имена файлов агента)
        assert record.weighed_at is not None
        day = record.weighed_at.strftime("%Y/%m/%d")
        assert [(p.role, p.path, p.sha256, p.size_bytes) for p in photo_rows] == [
            (CameraRole.FRONT, f"/vesy/{day}/{record.uuid.hex}_photo1.jpeg", SHA_A, 100),
            (CameraRole.REAR, f"/vesy/{day}/{record.uuid.hex}_photo2.jpeg", SHA_B, 200),
        ]
        with_photos = weighing_checksum(
            uuid=record.uuid,
            operation=record.operation.value,
            code=record.code.value,
            massa=record.massa,
            weighed_at=record.weighed_at,
            vehicle_number=record.vehicle_number,
            source=record.source.value,
            photo_sha256s=[SHA_A, SHA_B],
        )
        assert row.checksum == with_photos, "checksum не включает sha фото"


# ---------------------------------------------------------------------------
# repo: реестр тарирований
# ---------------------------------------------------------------------------


def _tare_row(
    session: Session, vehicle_number: str, trailer_number: str = ""
) -> TareRegistry | None:
    session.expire_all()
    return session.get(TareRegistry, (vehicle_number, trailer_number))


class TestRepoTareRegistry:
    def test_taring_ok_updates_registry(self, repo_env: tuple[Session, int, int]) -> None:
        """Успешное тарирование попадает в реестр активных тар."""
        session, scale_id, _ = repo_env
        taring = _make_taring()
        repo.save_weighing_record(session, scale_id, taring)
        tare = _tare_row(session, "01KG123ABC")
        assert tare is not None
        assert tare.tare_value == 7500.0
        assert tare.weighing_id == _weighing_by_uuid(session, taring.uuid).id

    def test_taring_refusal_not_saved_at_all(self, repo_env: tuple[Session, int, int]) -> None:
        """Отказы (code != OK) в журнал не пишутся вовсе (решение 10.08.2026):
        ни записи, ни тары — агент такие операции не выполняет."""
        session, scale_id, _ = repo_env
        for code in (ErrorCode.ERR_CAMERA, ErrorCode.ERR_UNSTABLE):
            taring = _make_taring(code=code, massa=None, stable=False)
            assert repo.save_weighing_record(session, scale_id, taring) is False
            assert _weighing_by_uuid_or_none(session, taring.uuid) is None
        assert _tare_row(session, "01KG123ABC") is None

    def test_weighing_does_not_touch_registry(self, repo_env: tuple[Session, int, int]) -> None:
        """Обычное взвешивание (не тарирование) реестр не меняет."""
        session, scale_id, _ = repo_env
        repo.save_weighing_record(session, scale_id, _make_record())
        assert _tare_row(session, "01KG123ABC") is None

    def test_newer_taring_replaces_older(self, repo_env: tuple[Session, int, int]) -> None:
        """Новое тарирование того же номера замещает старую тару."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        older = _make_taring(massa=7000.0, weighed_at=now - timedelta(days=10))
        newer = _make_taring(massa=8000.0, weighed_at=now - timedelta(days=1))
        repo.save_weighing_record(session, scale_id, older)
        repo.save_weighing_record(session, scale_id, newer)
        tare = _tare_row(session, "01KG123ABC")
        assert tare is not None
        assert tare.tare_value == 8000.0
        assert tare.weighing_id == _weighing_by_uuid(session, newer.uuid).id

    def test_out_of_order_sync_keeps_latest_tare(self, repo_env: tuple[Session, int, int]) -> None:
        """Досылка не по порядку: раннее тарирование, пришедшее позже,
        не затирает актуальную тару (where tared_at <= excluded)."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        newer = _make_taring(massa=8000.0, weighed_at=now - timedelta(days=1))
        older = _make_taring(massa=7000.0, weighed_at=now - timedelta(days=10))
        repo.save_weighing_record(session, scale_id, newer)  # свежая тара пришла первой
        repo.save_weighing_record(session, scale_id, older)  # запоздавшая старая
        tare = _tare_row(session, "01KG123ABC")
        assert tare is not None
        assert tare.tare_value == 8000.0, "старое тарирование затёрло актуальную тару"
        assert tare.weighing_id == _weighing_by_uuid(session, newer.uuid).id

    def test_load_tare_registry_filters_expired(self, repo_env: tuple[Session, int, int]) -> None:
        """Реплицируются только действующие тары (моложе 3 календарных месяцев)."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        fresh = _make_taring(vehicle_number="01KG111AAA", weighed_at=now - timedelta(days=5))
        expired = _make_taring(
            vehicle_number="01KG222BBB", massa=9000.0, weighed_at=now - timedelta(days=200)
        )
        repo.save_weighing_record(session, scale_id, fresh)
        repo.save_weighing_record(session, scale_id, expired)

        records = repo.load_tare_registry(session)
        assert [r.vehicle_number for r in records] == ["01KG111AAA"]
        assert records[0].tare_value == 7500.0
        assert records[0].weighing_uuid == fresh.uuid

    def test_find_active_tare(self, repo_env: tuple[Session, int, int]) -> None:
        """find_active_tare: действующая → запись; просроченная/неизвестная → None."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        fresh = _make_taring(vehicle_number="01KG111AAA", weighed_at=now - timedelta(days=5))
        expired = _make_taring(
            vehicle_number="01KG222BBB", massa=9000.0, weighed_at=now - timedelta(days=200)
        )
        repo.save_weighing_record(session, scale_id, fresh)
        repo.save_weighing_record(session, scale_id, expired)

        found = repo.find_active_tare(session, "01KG111AAA")
        assert found is not None
        assert found.tare_value == 7500.0
        assert found.weighing_uuid == fresh.uuid

        assert repo.find_active_tare(session, "01KG222BBB") is None  # просрочена
        assert repo.find_active_tare(session, "01KG999XYZ") is None  # неизвестна

    def test_taring_with_trailer_registered_under_pair(
        self, repo_env: tuple[Session, int, int]
    ) -> None:
        """Тарирование сцепки попадает в реестр под парой голова+прицеп,
        а не под одной головой (решение 09.08.2026; '' = без прицепа)."""
        session, scale_id, _ = repo_env
        taring = _make_taring(trailer_number="BD123AB")
        repo.save_weighing_record(session, scale_id, taring)

        tare = _tare_row(session, "01KG123ABC", "BD123AB")
        assert tare is not None
        assert tare.tare_value == 7500.0
        assert tare.weighing_id == _weighing_by_uuid(session, taring.uuid).id
        # под соло-ключом (без прицепа) записи нет
        assert _tare_row(session, "01KG123ABC") is None

    def test_find_active_tare_matches_full_pair_only(
        self, repo_env: tuple[Session, int, int]
    ) -> None:
        """find_active_tare: тара подставляется только при совпадении ОБОИХ
        номеров; соло-тара действует только для машины без прицепа."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        pair = _make_taring(
            vehicle_number="01KG111AAA",
            trailer_number="BD123AB",
            weighed_at=now - timedelta(days=5),
        )
        solo = _make_taring(
            vehicle_number="01KG222BBB", massa=6800.0, weighed_at=now - timedelta(days=5)
        )
        repo.save_weighing_record(session, scale_id, pair)
        repo.save_weighing_record(session, scale_id, solo)

        # чужой прицеп и запрос без прицепа — действующей тары нет
        assert repo.find_active_tare(session, "01KG111AAA", "XX999YY") is None
        assert repo.find_active_tare(session, "01KG111AAA") is None
        # совпавшая пара — тара найдена, прицеп восстановлен в записи
        found = repo.find_active_tare(session, "01KG111AAA", "BD123AB")
        assert found is not None
        assert found.tare_value == 7500.0
        assert found.trailer_number == "BD123AB"
        assert found.weighing_uuid == pair.uuid
        # соло-тара не находится при запросе с прицепом, но действует без него
        assert repo.find_active_tare(session, "01KG222BBB", "BD123AB") is None
        solo_found = repo.find_active_tare(session, "01KG222BBB")
        assert solo_found is not None
        assert solo_found.tare_value == 6800.0
        assert solo_found.trailer_number is None

    def test_same_head_different_trailers_coexist(self, repo_env: tuple[Session, int, int]) -> None:
        """Составной PK (vehicle, trailer): тары одной головы с разными
        прицепами сосуществуют — новое тарирование одной сцепки не затирает
        тару другой."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        first = _make_taring(
            trailer_number="BD111AA", massa=7000.0, weighed_at=now - timedelta(days=3)
        )
        second = _make_taring(
            trailer_number="BD222BB", massa=8200.0, weighed_at=now - timedelta(days=1)
        )
        repo.save_weighing_record(session, scale_id, first)
        repo.save_weighing_record(session, scale_id, second)

        one = _tare_row(session, "01KG123ABC", "BD111AA")
        other = _tare_row(session, "01KG123ABC", "BD222BB")
        assert one is not None and one.tare_value == 7000.0
        assert other is not None and other.tare_value == 8200.0
        assert one.weighing_id == _weighing_by_uuid(session, first.uuid).id
        assert other.weighing_id == _weighing_by_uuid(session, second.uuid).id


# ---------------------------------------------------------------------------
# WS-эндпоинт: TestClient + временная БД
# ---------------------------------------------------------------------------


@dataclass
class WsEnv:
    """Собранное окружение WS-теста: приложение, хаб и посеянные id."""

    client: TestClient
    hub: AgentHub
    factory: Callable[[], Session]
    scale_id: int
    agent_id: int


@pytest.fixture
def ws_env(ws_db_engine: Engine) -> Iterator[WsEnv]:
    """Чистая БД, посев site+scale+agent, приложение с маршрутом /agents/ws."""
    _truncate_all(ws_db_engine)
    factory = make_session_factory(ws_db_engine)
    with factory() as session:
        scale_id, agent_id = _seed_graph(session)
    hub = AgentHub()
    app = FastAPI()
    app.include_router(create_agents_router(hub, factory))
    yield WsEnv(TestClient(app), hub, factory, scale_id, agent_id)


def _agent_state(env: WsEnv) -> tuple[AgentStatus, str | None, datetime | None]:
    """Текущие статус/версия/last_seen_at агента из БД (свежая сессия)."""
    with env.factory() as session:
        agent = session.get(Agent, env.agent_id)
        assert agent is not None
        return agent.status, agent.version, agent.last_seen_at


def _count_by_uuid(env: WsEnv, record_uuid: UUID) -> int:
    with env.factory() as session:
        return _weighing_count(session, record_uuid)


def _wait_until(predicate: Callable[[], bool], timeout: float = 10.0) -> bool:
    """Подождать условие (финализация серверных задач) без сна вслепую.

    Потолок щедрый: на нагруженных раннерах CI финализация (detach +
    запись offline в БД) занимает секунды; быстрые машины выходят сразу.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _hello_and_registry(ws: WebSocketTestSession) -> dict[str, Any]:
    """hello → ответные tare_registry + operators_registry (порядок фиксирован
    циклом приёма); также барьер: предыдущие сообщения обработаны. Возвращает
    снимок реестра тарирований."""
    ws.send_text(_hello_json())
    tares: dict[str, Any] = json.loads(ws.receive_text())
    assert tares["type"] == "tare_registry"
    operators: dict[str, Any] = json.loads(ws.receive_text())
    assert operators["type"] == "operators_registry"
    return tares


def _seed_tare(env: WsEnv, vehicle_number: str = "01KG555TT") -> WeighingRecord:
    """Посеять действующую тару через журнал (как её создал бы агент)."""
    taring = _make_taring(
        vehicle_number=vehicle_number, weighed_at=datetime.now(UTC) - timedelta(days=2)
    )
    with env.factory() as session:
        repo.save_weighing_record(session, env.scale_id, taring)
    return taring


class TestAgentsWsAuth:
    def test_no_authorization_closed_4401(self, ws_env: WsEnv) -> None:
        """Без Authorization соединение закрывается кодом 4401; БД не тронута."""
        with (
            pytest.raises(WebSocketDisconnect) as excinfo,
            ws_env.client.websocket_connect("/agents/ws"),
        ):
            pass
        assert excinfo.value.code == 4401
        status, version, last_seen_at = _agent_state(ws_env)
        assert status is AgentStatus.OFFLINE
        assert version is None
        assert last_seen_at is None

    def test_wrong_token_closed_4401(self, ws_env: WsEnv) -> None:
        """Неверный токен → 4401; статус агента не менялся."""
        headers = {"Authorization": "Bearer wrong-token"}
        with (
            pytest.raises(WebSocketDisconnect) as excinfo,
            ws_env.client.websocket_connect("/agents/ws", headers=headers),
        ):
            pass
        assert excinfo.value.code == 4401
        status, _, last_seen_at = _agent_state(ws_env)
        assert status is AgentStatus.OFFLINE
        assert last_seen_at is None


class TestAgentsWsSession:
    def test_hello_receives_tare_registry_and_goes_online(self, ws_env: WsEnv) -> None:
        """Верный токен: приём; после hello приходит реестр тар; статус online."""
        taring = _seed_tare(ws_env)
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            registry = _hello_and_registry(ws)
            assert [r["vehicle_number"] for r in registry["records"]] == ["01KG555TT"]
            assert registry["records"][0]["weighing_uuid"] == str(taring.uuid)

            status, version, last_seen_at = _agent_state(ws_env)
            assert status is AgentStatus.ONLINE
            assert version == AGENT_VERSION  # версия из hello записана
            assert last_seen_at is not None
            assert ws_env.hub.connected(ws_env.scale_id)

    def test_hello_stores_equipment_and_disconnect_clears(self, ws_env: WsEnv) -> None:
        """hello кладёт самодиагностику в хаб (дашборд), разрыв — очищает."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            _hello_and_registry(ws)
            equipment = ws_env.hub.equipment(ws_env.scale_id)
            assert equipment is not None and equipment.scale_status is ScaleStatus.OK
        assert _wait_until(lambda: ws_env.hub.equipment(ws_env.scale_id) is None)

    def test_disconnect_sets_agent_offline(self, ws_env: WsEnv) -> None:
        """Разрыв соединения переводит агента в offline и снимает линк из хаба."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            _hello_and_registry(ws)
        assert _wait_until(lambda: _agent_state(ws_env)[0] is AgentStatus.OFFLINE)
        assert _wait_until(lambda: not ws_env.hub.connected(ws_env.scale_id))

    def test_garbage_message_keeps_connection_alive(self, ws_env: WsEnv) -> None:
        """Мусор (не-JSON и неизвестный тип) не рвёт соединение: следующие
        сообщения обрабатываются."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text("это вообще не json {{{")
            ws.send_text('{"type": "alien_probe"}')
            # после мусора цикл жив: hello обрабатывается и приходит реестр
            registry = _hello_and_registry(ws)
            assert registry["records"] == []


class TestAgentsWsWeighResult:
    def test_weigh_result_saved_to_journal(self, ws_env: WsEnv) -> None:
        """weigh_result от агента попадает в журнал центра."""
        record = _make_record()
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(WeighResult(request_id=uuid4(), record=record).model_dump_json())
            _hello_and_registry(ws)  # барьер: weigh_result обработан
        assert _count_by_uuid(ws_env, record.uuid) == 1
        with ws_env.factory() as session:
            row = _weighing_by_uuid(session, record.uuid)
            assert row.scale_id == ws_env.scale_id
            assert len(row.checksum) == 64

    def test_duplicate_weigh_result_no_duplicates(self, ws_env: WsEnv) -> None:
        """Повторная отправка того же weigh_result не создаёт дублей."""
        record = _make_record()
        payload = WeighResult(request_id=uuid4(), record=record).model_dump_json()
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(payload)
            ws.send_text(payload)
            _hello_and_registry(ws)
        assert _count_by_uuid(ws_env, record.uuid) == 1

    def test_taring_result_broadcasts_registry(self, ws_env: WsEnv) -> None:
        """Успешное тарирование по команде → реестр обновлён и разослан агентам."""
        taring = _make_taring(vehicle_number="01KG777CC")
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(WeighResult(request_id=uuid4(), record=taring).model_dump_json())
            broadcast = json.loads(ws.receive_text())
            assert broadcast["type"] == "tare_registry"
            numbers = [r["vehicle_number"] for r in broadcast["records"]]
            assert numbers == ["01KG777CC"]

    def test_weigh_result_photos_persisted(self, ws_env: WsEnv) -> None:
        """Метаданные фото из weigh_result попадают в weighing_photos и в
        контрольную сумму записи (правило №2: строка weighings неизменяема,
        поэтому привязать фото позже было бы невозможно)."""
        record = _make_record()
        photo = PhotoMeta(role=CameraRole.FRONT, filename="front.jpeg", sha256=SHA_A, size_bytes=1)
        record = record.model_copy(update={"photos": [photo]})
        result = WeighResult(request_id=uuid4(), record=record)
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(result.model_dump_json())
            _hello_and_registry(ws)
        with ws_env.factory() as session:
            row = _weighing_by_uuid(session, record.uuid)
            photo_shas = (
                session.execute(
                    select(WeighingPhoto.sha256).where(WeighingPhoto.weighing_id == row.id)
                )
                .scalars()
                .all()
            )
        assert photo_shas == [SHA_A], "фото из weigh_result не записаны в weighing_photos"


class TestAgentsWsOfflineSync:
    def test_offline_sync_acks_all_and_no_duplicates(self, ws_env: WsEnv) -> None:
        """Досылка 3 записей (2 новых + 1 повтор): ack со ВСЕМИ uuid, журнал без
        дублей, тарирование из досылки — в реестре."""
        repeated = _make_record()
        with ws_env.factory() as session:
            repo.save_weighing_record(session, ws_env.scale_id, repeated)  # уже была в журнале

        new_weighing = _make_record(source=WeighingSource.LOCAL_OFFLINE)
        new_taring = _make_taring(
            vehicle_number="01KG777CC",
            source=WeighingSource.LOCAL_OFFLINE,
            weighed_at=datetime.now(UTC) - timedelta(hours=1),
        )
        sync = OfflineSync(agent_id="agent-1", records=[new_weighing, new_taring, repeated])

        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(sync.model_dump_json())
            ack = json.loads(ws.receive_text())
            assert ack["type"] == "offline_sync_ack"
            # ack и за новые, и за повтор — иначе агент не пометит их synced
            assert set(ack["accepted_uuids"]) == {
                str(new_weighing.uuid),
                str(new_taring.uuid),
                str(repeated.uuid),
            }
            # тарирование в досылке → рассылка обновлённого реестра
            broadcast = json.loads(ws.receive_text())
            assert broadcast["type"] == "tare_registry"
            assert [r["vehicle_number"] for r in broadcast["records"]] == ["01KG777CC"]

        for record_uuid in (new_weighing.uuid, new_taring.uuid, repeated.uuid):
            assert _count_by_uuid(ws_env, record_uuid) == 1
        with ws_env.factory() as session:
            tare = _tare_row(session, "01KG777CC")
            assert tare is not None
            assert tare.tare_value == 7500.0


class TestAgentsWsDisplacement:
    def test_second_connection_displaces_first(self, ws_env: WsEnv) -> None:
        """Второе соединение того же агента вытесняет первое в хабе.

        Фактическое поведение центра: старое соединение НЕ закрывается — его
        цикл приёма продолжает работать (hello по нему всё ещё отвечает), но
        в hub команды маршрутизируются только в новое соединение."""
        ws1 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
        ws1.__enter__()
        try:
            _hello_and_registry(ws1)
            ws2 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
            ws2.__enter__()
            try:
                _hello_and_registry(ws2)
                # весы учтены в хабе ровно один раз
                assert ws_env.hub.connected_scale_ids() == [ws_env.scale_id]
                # старое соединение живо и всё ещё обслуживается циклом приёма
                _hello_and_registry(ws1)
            finally:
                ws2.__exit__(None, None, None)
        finally:
            ws1.__exit__(None, None, None)

    def test_old_connection_close_keeps_current_connection_registered(self, ws_env: WsEnv) -> None:
        """Закрытие вытесненного соединения не снимает новое из хаба
        (detach чужого линка — no-op)."""
        ws1 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
        ws1.__enter__()
        _hello_and_registry(ws1)
        ws2 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
        ws2.__enter__()
        try:
            _hello_and_registry(ws2)
            ws1.__exit__(None, None, None)  # старое соединение умирает
            # новое соединение остаётся зарегистрированным
            assert not _wait_until(
                lambda: not ws_env.hub.connected(ws_env.scale_id), timeout=0.5
            ), "смерть вытесненного соединения сняла живое из хаба"
            # и продолжает обслуживаться
            _hello_and_registry(ws2)
        finally:
            ws2.__exit__(None, None, None)

    def test_old_connection_death_keeps_agent_online(self, ws_env: WsEnv) -> None:
        """Смерть вытесненного соединения НЕ трогает живого агента: статус
        остаётся online, команды через новое соединение не прерываются
        (finally старого соединения проверяет, было ли оно текущим)."""
        ws1 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
        ws1.__enter__()
        _hello_and_registry(ws1)
        ws2 = ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS)
        ws2.__enter__()
        try:
            _hello_and_registry(ws2)
            assert _agent_state(ws_env)[0] is AgentStatus.ONLINE
            ws1.__exit__(None, None, None)  # старое соединение умирает
            became_offline = _wait_until(
                lambda: _agent_state(ws_env)[0] is AgentStatus.OFFLINE, timeout=1.0
            )
            assert not became_offline, (
                "закрытие вытесненного соединения сбросило статус агента в offline "
                "при живом втором соединении"
            )
        finally:
            ws2.__exit__(None, None, None)


# ---------------------------------------------------------------------------
# AgentHub: адресная отправка снимка операторов (send_operators)
# ---------------------------------------------------------------------------


class TestAgentHubSendOperators:
    def test_connected_agent_receives_snapshot(self) -> None:
        """Подключённый агент получает снимок; send_operators → True."""

        async def scenario() -> None:
            hub = AgentHub()
            target, bystander = FakeLink(), FakeLink()
            hub.attach(1, target)
            hub.attach(2, bystander)
            update = OperatorsRegistryUpdate(
                records=[OperatorRecord(login="op1", pw_hash="pbkdf2$1$aa$bb")]
            )
            assert await hub.send_operators(1, update) is True
            assert len(target.sent) == 1
            parsed = parse_center_message(target.sent[0])
            assert isinstance(parsed, OperatorsRegistryUpdate)
            assert parsed.records[0].login == "op1"
            # отправка адресная: соседние весы снимок НЕ получают
            assert bystander.sent == []

        asyncio.run(scenario())

    def test_offline_agent_returns_false(self) -> None:
        """Агент офлайн → False, исключение не бросается (best-effort)."""

        async def scenario() -> None:
            hub = AgentHub()
            assert await hub.send_operators(99, OperatorsRegistryUpdate(records=[])) is False

        asyncio.run(scenario())

    def test_dead_link_returns_false_without_raise(self) -> None:
        """Умерший TCP (send_text бросает) → False, а не исключение наружу."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, DeadLink())
            assert await hub.send_operators(1, OperatorsRegistryUpdate(records=[])) is False

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# repo: снимок операторов для реплики на агента (load_operators_for_scale)
# ---------------------------------------------------------------------------

# готовый хеш в формате shared.passwords: пароль по каналу не ходит,
# репо обязан отдать значение из БД байт-в-байт, не пересчитывая
OPERATOR_PW_HASH = "pbkdf2$200000$" + "ab" * 16 + "$" + "cd" * 32


def _seed_user(
    session: Session,
    login: str,
    *,
    role: UserRole = UserRole.OPERATOR,
    site_id: int | None = None,
    is_active: bool = True,
    full_name: str = "",
    pw_hash: str = OPERATOR_PW_HASH,
) -> User:
    """Пользователь центра (по умолчанию — активный оператор без привязки)."""
    user = User(
        login=login,
        pw_hash=pw_hash,
        full_name=full_name,
        role=role,
        site_id=site_id,
        is_active=is_active,
    )
    session.add(user)
    session.commit()
    return user


def _site_of_scale(session: Session, scale_id: int) -> int:
    scale = session.get(Scale, scale_id)
    assert scale is not None
    return scale.site_id


def _seed_other_site(session: Session) -> int:
    """Второй объект (для чужих операторов)."""
    site = Site(code="other-site", name="Чужой объект")
    session.add(site)
    session.commit()
    return site.id


class TestRepoLoadOperators:
    def test_own_site_operator_included(self, repo_env: tuple[Session, int, int]) -> None:
        """Оператор, привязанный к объекту этих весов, входит в снимок
        со всеми полями."""
        session, scale_id, _ = repo_env
        site_id = _site_of_scale(session, scale_id)
        _seed_user(session, "op.own", site_id=site_id, full_name="Свой Оператор")
        records = repo.load_operators_for_scale(session, scale_id)
        assert [(r.login, r.full_name, r.is_active) for r in records] == [
            ("op.own", "Свой Оператор", True)
        ]

    def test_foreign_site_operator_excluded(self, repo_env: tuple[Session, int, int]) -> None:
        """Оператор чужого объекта в снимок этих весов не попадает."""
        session, scale_id, _ = repo_env
        other_site_id = _seed_other_site(session)
        _seed_user(session, "op.foreign", site_id=other_site_id)
        assert repo.load_operators_for_scale(session, scale_id) == []

    def test_unbound_operator_included(self, repo_env: tuple[Session, int, int]) -> None:
        """Оператор без привязки (site_id NULL) работает на всех объектах."""
        session, scale_id, _ = repo_env
        _seed_user(session, "op.everywhere", site_id=None)
        records = repo.load_operators_for_scale(session, scale_id)
        assert [r.login for r in records] == ["op.everywhere"]

    def test_disabled_operator_included_with_flag(self, repo_env: tuple[Session, int, int]) -> None:
        """Отключённая учётка входит в снимок с is_active=False — агент
        должен заблокировать и офлайн-вход, а не «не знать» о блокировке."""
        session, scale_id, _ = repo_env
        site_id = _site_of_scale(session, scale_id)
        _seed_user(session, "op.blocked", site_id=site_id, is_active=False)
        records = repo.load_operators_for_scale(session, scale_id)
        assert [(r.login, r.is_active) for r in records] == [("op.blocked", False)]

    def test_admin_and_dispatcher_excluded(self, repo_env: tuple[Session, int, int]) -> None:
        """Не-операторы (admin/dispatcher) не реплицируются, даже привязанные
        к объекту весов: на весовом ПК им делать нечего."""
        session, scale_id, _ = repo_env
        site_id = _site_of_scale(session, scale_id)
        _seed_user(session, "chief", role=UserRole.ADMIN, site_id=site_id)
        _seed_user(session, "dispatcher", role=UserRole.DISPATCHER, site_id=site_id)
        _seed_user(session, "unbound.admin", role=UserRole.ADMIN, site_id=None)
        assert repo.load_operators_for_scale(session, scale_id) == []

    def test_unknown_scale_returns_empty(self, repo_env: tuple[Session, int, int]) -> None:
        """Несуществующие весы → пустой снимок, а не падение."""
        session, _, _ = repo_env
        _seed_user(session, "op.everywhere", site_id=None)
        assert repo.load_operators_for_scale(session, 987654) == []

    def test_pw_hash_passed_verbatim(self, repo_env: tuple[Session, int, int]) -> None:
        """pw_hash в снимке равен хешу из БД байт-в-байт: пароль не
        пересчитывается и не подменяется."""
        session, scale_id, _ = repo_env
        _seed_user(session, "op.hash", site_id=None, pw_hash=OPERATOR_PW_HASH)
        records = repo.load_operators_for_scale(session, scale_id)
        assert records[0].pw_hash == OPERATOR_PW_HASH

    def test_sorted_by_login(self, repo_env: tuple[Session, int, int]) -> None:
        """Снимок отсортирован по логину — порядок детерминирован."""
        session, scale_id, _ = repo_env
        site_id = _site_of_scale(session, scale_id)
        _seed_user(session, "zeta.op", site_id=site_id)
        _seed_user(session, "alpha.op", site_id=None)
        _seed_user(session, "mid.op", site_id=site_id)
        records = repo.load_operators_for_scale(session, scale_id)
        assert [r.login for r in records] == ["alpha.op", "mid.op", "zeta.op"]


# ---------------------------------------------------------------------------
# WS-эндпоинт: репликация операторов после hello
# ---------------------------------------------------------------------------


class TestAgentsWsOperatorsRegistry:
    def test_hello_sends_tares_then_operators_in_order(self, ws_env: WsEnv) -> None:
        """После hello агенту уходят ДВА снимка подряд в фиксированном
        порядке: сначала tare_registry, затем operators_registry с
        операторами именно этого объекта."""
        with ws_env.factory() as session:
            site_id = _site_of_scale(session, ws_env.scale_id)
            _seed_user(session, "op.own", site_id=site_id, full_name="Свой Оператор")
            _seed_user(session, "op.blocked", site_id=site_id, is_active=False)
            other_site_id = _seed_other_site(session)
            _seed_user(session, "op.foreign", site_id=other_site_id)

        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello_json())
            first = json.loads(ws.receive_text())
            assert first["type"] == "tare_registry", "первым должен идти реестр тар"
            second = json.loads(ws.receive_text())
            assert second["type"] == "operators_registry", "вторым должен идти снимок операторов"
            by_login = {r["login"]: r for r in second["records"]}
            # свои операторы (включая заблокированного) есть, чужого нет
            assert set(by_login) == {"op.own", "op.blocked"}
            assert by_login["op.own"]["full_name"] == "Свой Оператор"
            assert by_login["op.own"]["is_active"] is True
            assert by_login["op.own"]["pw_hash"] == OPERATOR_PW_HASH
            assert by_login["op.blocked"]["is_active"] is False

    def test_hello_without_operators_sends_empty_snapshot(self, ws_env: WsEnv) -> None:
        """Операторов нет → после hello всё равно приходит пустой снимок
        (агент очистит устаревшую реплику)."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello_json())
            assert json.loads(ws.receive_text())["type"] == "tare_registry"
            operators = json.loads(ws.receive_text())
            assert operators["type"] == "operators_registry"
            assert operators["records"] == []

    def test_repeated_hello_resends_both_registries(self, ws_env: WsEnv) -> None:
        """Каждый hello (переподключение агента) заново раздаёт оба снимка."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            _hello_and_registry(ws)
            # второй hello в том же соединении — снова пара снимков по порядку
            ws.send_text(_hello_json())
            assert json.loads(ws.receive_text())["type"] == "tare_registry"
            assert json.loads(ws.receive_text())["type"] == "operators_registry"


# ---------------------------------------------------------------------------
# repo: снимок настроек весов для доставки агенту (load_scale_settings)
# ---------------------------------------------------------------------------

# полный набор параметров цикла, как его сохраняет страница настроек
CYCLE_THRESHOLDS: dict[str, float] = {
    "zero_threshold_kg": 150.0,
    "vehicle_threshold_kg": 600.0,
    "zero_timeout_s": 10.0,
    "vehicle_timeout_s": 90.0,
    "stable_duration_s": 5.0,
    "stable_timeout_s": 30.0,
    "no_data_timeout_s": 5.0,
}


def _set_scale_settings(
    session: Session,
    scale_id: int,
    *,
    thresholds: dict[str, Any] | None = None,
    port_cfg: dict[str, Any] | None = None,
) -> None:
    """Записать thresholds/port_cfg прямо в строку весов (как это делает
    save_scale_settings, но без его валидаций — для кривых данных)."""
    scale = session.get(Scale, scale_id)
    assert scale is not None
    scale.thresholds = thresholds
    scale.port_cfg = port_cfg
    session.commit()


def _add_camera(
    session: Session,
    scale_id: int,
    role: CameraRole,
    *,
    snapshot_url: str | None = None,
    rtsp_url: str | None = None,
) -> None:
    session.add(Camera(scale_id=scale_id, role=role, snapshot_url=snapshot_url, rtsp_url=rtsp_url))
    session.commit()


class TestRepoLoadScaleSettings:
    def test_nothing_configured_returns_none(self, repo_env: tuple[Session, int, int]) -> None:
        """В центре ничего не задано → None (агент живёт локальным конфигом)."""
        session, scale_id, _ = repo_env
        assert repo.load_scale_settings(session, scale_id) is None

    def test_unknown_scale_returns_none(self, repo_env: tuple[Session, int, int]) -> None:
        """Несуществующие весы → None, а не падение."""
        session, _, _ = repo_env
        assert repo.load_scale_settings(session, 987654) is None

    def test_full_settings_loaded(self, repo_env: tuple[Session, int, int]) -> None:
        """Полный набор: цикл, порт со скоростью и камеры с URL."""
        session, scale_id, _ = repo_env
        _set_scale_settings(
            session,
            scale_id,
            thresholds=dict(CYCLE_THRESHOLDS),
            port_cfg={"port": "COM11", "baudrate": 19200},
        )
        _add_camera(
            session,
            scale_id,
            CameraRole.FRONT,
            snapshot_url="http://u:p@10.0.0.5/front",
            rtsp_url="rtsp://u:p@10.0.0.5/stream",
        )
        _add_camera(session, scale_id, CameraRole.REAR, rtsp_url="rtsp://u:p@10.0.0.6/rear")

        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.cycle is not None
        assert settings.cycle.model_dump() == CYCLE_THRESHOLDS
        assert settings.scale_port == "COM11"
        assert settings.baudrate == 19200
        assert settings.cameras is not None
        assert [(c.role, c.snapshot_url, c.rtsp_url) for c in settings.cameras] == [
            (CameraRole.FRONT, "http://u:p@10.0.0.5/front", "rtsp://u:p@10.0.0.5/stream"),
            (CameraRole.REAR, None, "rtsp://u:p@10.0.0.6/rear"),
        ]

    def test_cycle_only(self, repo_env: tuple[Session, int, int]) -> None:
        """Задан только цикл: порт и камеры — None."""
        session, scale_id, _ = repo_env
        _set_scale_settings(session, scale_id, thresholds=dict(CYCLE_THRESHOLDS))
        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.cycle is not None
        assert settings.scale_port is None
        assert settings.baudrate is None
        assert settings.cameras is None

    def test_port_only(self, repo_env: tuple[Session, int, int]) -> None:
        """Задан только порт: цикл и камеры — None."""
        session, scale_id, _ = repo_env
        _set_scale_settings(session, scale_id, port_cfg={"port": "COM7", "baudrate": 9600})
        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.cycle is None
        assert settings.scale_port == "COM7"
        assert settings.baudrate == 9600
        assert settings.cameras is None

    def test_port_without_baudrate(self, repo_env: tuple[Session, int, int]) -> None:
        """port_cfg без скорости: порт есть, baudrate None (скорость локальная)."""
        session, scale_id, _ = repo_env
        _set_scale_settings(session, scale_id, port_cfg={"port": "COM7"})
        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.scale_port == "COM7"
        assert settings.baudrate is None

    def test_camera_without_urls_excluded(self, repo_env: tuple[Session, int, int]) -> None:
        """Камера из справочника без единого URL в снимок не входит; если
        других настроек нет — снимок пустой (None)."""
        session, scale_id, _ = repo_env
        _add_camera(session, scale_id, CameraRole.FRONT)  # URL не заданы
        assert repo.load_scale_settings(session, scale_id) is None

    def test_camera_with_url_only_is_enough(self, repo_env: tuple[Session, int, int]) -> None:
        """Одна камера с URL — уже непустой снимок (только cameras)."""
        session, scale_id, _ = repo_env
        _add_camera(session, scale_id, CameraRole.FRONT)  # без URL — отсеется
        _add_camera(session, scale_id, CameraRole.REAR, snapshot_url="http://c/rear")
        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.cycle is None and settings.scale_port is None
        assert settings.cameras is not None
        assert [(c.role, c.snapshot_url) for c in settings.cameras] == [
            (CameraRole.REAR, "http://c/rear")
        ]

    def test_corrupt_thresholds_skipped(self, repo_env: tuple[Session, int, int]) -> None:
        """Кривой JSON в thresholds пропускается с warning: если больше ничего
        не задано — None, а не падение всей раздачи настроек."""
        session, scale_id, _ = repo_env
        _set_scale_settings(session, scale_id, thresholds={"garbage": True})
        assert repo.load_scale_settings(session, scale_id) is None

    def test_corrupt_thresholds_do_not_hide_port(self, repo_env: tuple[Session, int, int]) -> None:
        """Кривой цикл не мешает доставке остальных настроек (порта)."""
        session, scale_id, _ = repo_env
        _set_scale_settings(
            session,
            scale_id,
            thresholds={"zero_threshold_kg": "мусор"},
            port_cfg={"port": "COM7", "baudrate": 9600},
        )
        settings = repo.load_scale_settings(session, scale_id)
        assert settings is not None
        assert settings.cycle is None
        assert settings.scale_port == "COM7"


# ---------------------------------------------------------------------------
# WS-эндпоинт: доставка scale_config после hello
# ---------------------------------------------------------------------------


class TestAgentsWsScaleConfig:
    def test_hello_sends_scale_config_third_when_configured(self, ws_env: WsEnv) -> None:
        """Настройки заданы: после hello агент получает ТРИ снимка в
        фиксированном порядке — тары, операторы, затем scale_config."""
        with ws_env.factory() as session:
            _set_scale_settings(
                session,
                ws_env.scale_id,
                thresholds=dict(CYCLE_THRESHOLDS),
                port_cfg={"port": "COM11", "baudrate": 19200},
            )
            _add_camera(session, ws_env.scale_id, CameraRole.FRONT, snapshot_url="http://c/front")

        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            _hello_and_registry(ws)  # первые два: tare_registry + operators_registry
            third = json.loads(ws.receive_text())
            assert third["type"] == "scale_config", "третьим должен идти scale_config"
            settings = third["settings"]
            assert settings["cycle"] == CYCLE_THRESHOLDS
            assert settings["scale_port"] == "COM11"
            assert settings["baudrate"] == 19200
            assert [c["role"] for c in settings["cameras"]] == ["front"]
            assert settings["cameras"][0]["snapshot_url"] == "http://c/front"

    def test_hello_without_settings_sends_no_scale_config(self, ws_env: WsEnv) -> None:
        """Настроек в центре нет: scale_config после hello НЕ отправляется —
        следующий hello сразу отвечает реестром тар."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            _hello_and_registry(ws)
            # если бы scale_config был отправлен, он лежал бы в буфере первым
            ws.send_text(_hello_json())
            after = json.loads(ws.receive_text())
            assert after["type"] == "tare_registry", (
                f"после hello без настроек пришло лишнее сообщение: {after['type']}"
            )
            assert json.loads(ws.receive_text())["type"] == "operators_registry"

    def test_repeated_hello_resends_scale_config(self, ws_env: WsEnv) -> None:
        """Каждый hello раздаёт настройки заново (переподключение агента)."""
        with ws_env.factory() as session:
            _set_scale_settings(session, ws_env.scale_id, thresholds=dict(CYCLE_THRESHOLDS))
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            for _ in range(2):
                _hello_and_registry(ws)
                config = json.loads(ws.receive_text())
                assert config["type"] == "scale_config"
                assert config["settings"]["cycle"] == CYCLE_THRESHOLDS

    def test_config_status_from_agent_keeps_connection(self, ws_env: WsEnv) -> None:
        """config_status от агента (ok и откат) логируется, соединение живо."""
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(ConfigStatus(ok=True).model_dump_json())
            ws.send_text(
                ConfigStatus(ok=False, rolled_back=True, error="индикатор молчит").model_dump_json()
            )
            # цикл приёма жив: hello после отчётов обслуживается
            registry = _hello_and_registry(ws)
            assert registry["records"] == []


# ---------------------------------------------------------------------------
# AgentHub: адресная отправка настроек весов (send_scale_config)
# ---------------------------------------------------------------------------


def _make_scale_config_update() -> ScaleConfigUpdate:
    return ScaleConfigUpdate(settings=ScaleSettingsPayload(scale_port="COM11", baudrate=19200))


class TestAgentHubSendScaleConfig:
    def test_connected_agent_receives_config(self) -> None:
        """Подключённый агент получает настройки; соседние весы — нет."""

        async def scenario() -> None:
            hub = AgentHub()
            target, bystander = FakeLink(), FakeLink()
            hub.attach(1, target)
            hub.attach(2, bystander)
            assert await hub.send_scale_config(1, _make_scale_config_update()) is True
            assert len(target.sent) == 1
            parsed = parse_center_message(target.sent[0])
            assert isinstance(parsed, ScaleConfigUpdate)
            assert parsed.settings.scale_port == "COM11"
            assert parsed.settings.baudrate == 19200
            # отправка адресная: настройки одних весов не уходят другим
            assert bystander.sent == []

        asyncio.run(scenario())

    def test_offline_agent_returns_false(self) -> None:
        """Агент офлайн → False без исключений (best-effort: получит при hello)."""

        async def scenario() -> None:
            hub = AgentHub()
            assert await hub.send_scale_config(99, _make_scale_config_update()) is False

        asyncio.run(scenario())

    def test_dead_link_returns_false_without_raise(self) -> None:
        """Умерший TCP (send_text бросает) → False, а не исключение наружу."""

        async def scenario() -> None:
            hub = AgentHub()
            hub.attach(1, DeadLink())
            assert await hub.send_scale_config(1, _make_scale_config_update()) is False

        asyncio.run(scenario())
