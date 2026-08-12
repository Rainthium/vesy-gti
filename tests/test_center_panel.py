"""Тесты веб-панели диспетчера центра (center/web) и админ-CLI (tools).

Покрытие:
- queries.py на живом PostgreSQL: verify_user (пароль/неактивный/чужой логин),
  dashboard_scales (агент есть/нет, самая свежая операция), journal_page
  (фильтры по объекту, датам с границами суток, номеру ТС/прицепа, источнику;
  пагинация и порядок «новые первыми»), weighing_card (фото, связанная тара,
  сторно, несуществующий id), tare_list (просроченные скрыты, поиск),
  tare_expires_at (+3 календарных месяца с поджатием дня), refs_data;
- маршруты панели через TestClient: редиректы 303 без входа, вход/выход,
  экраны с данными посева, фильтр журнала, пагинация за пределами данных,
  карточка записи с контрольной суммой, отдача фото по сессии и защита
  от path traversal;
- tools/center_admin.py subprocess'ом: --help, create-site/scale/agent
  (токен печатается один раз, в БД только хеш), отказ от короткого пароля;
- tools/seed_demo_center.py: импорт, посев чистой БД, отказ на непустой БД.

Проверяется только СОДЕРЖИМОЕ страниц (тексты данных, атрибуты контракта),
не вёрстка: шаблоны параллельно правит другой исполнитель.

Инфраструктура БД — по образцу tests/test_center_ws.py: одноразовая БД
ves_test_panel_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import importlib
import os
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool
from starlette.middleware.sessions import SessionMiddleware

from center.agents_ws.hub import AgentHub
from center.db import repo
from center.db.models import (
    Agent,
    AgentStatus,
    AuditLog,
    Camera,
    Scale,
    ScaleKind,
    Site,
    User,
    UserRole,
    Weighing,
    WeighingPhoto,
)
from center.db.session import database_url, make_session_factory
from center.web import queries
from center.web.router import create_panel_router
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import (
    CameraStatus,
    EquipmentStatus,
    LogTailRequest,
    LogTailResponse,
    PhotoMeta,
    WeighingRecord,
    parse_center_message,
)
from shared.passwords import verify_password
from tests.test_center_db import ALL_TABLES, _upgrade_head

BISHKEK = ZoneInfo("Asia/Bishkek")
REPO_ROOT = Path(__file__).resolve().parents[1]

PANEL_LOGIN = "dispatcher"
PANEL_PASSWORD = "panel-pass-123"

SHA_A = "a" * 64
SHA_B = "b" * 64


# ---------------------------------------------------------------------------
# Инфраструктура: временная БД + миграции
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def panel_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_panel_<pid>; имя не пересекается с другими
    модулями тестов, чтобы не мешать им в одном прогоне."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты панели центра пропущены"
        )

    db_name = f"ves_test_panel_{os.getpid()}"
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
def panel_db_engine(panel_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(panel_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


def _truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))


@pytest.fixture
def db(panel_db_engine: Engine) -> Iterator[sessionmaker[Session]]:
    """Чистая БД; отдаёт фабрику сессий."""
    _truncate_all(panel_db_engine)
    yield make_session_factory(panel_db_engine)


@pytest.fixture
def db_session(db: sessionmaker[Session]) -> Iterator[Session]:
    session = db()
    yield session
    session.rollback()
    session.close()


# ---------------------------------------------------------------------------
# Посев данных
# ---------------------------------------------------------------------------


def _add_user(
    session: Session,
    login: str = PANEL_LOGIN,
    password: str = PANEL_PASSWORD,
    *,
    is_active: bool = True,
    full_name: str = "Айгуль Диспетчер",
) -> User:
    # хеш через shared.passwords: функции создания пользователя в queries нет
    from shared.passwords import hash_password

    user = User(
        login=login,
        pw_hash=hash_password(password),
        full_name=full_name,
        role=UserRole.DISPATCHER,
        is_active=is_active,
    )
    session.add(user)
    session.commit()
    return user


def _add_site_scale(
    session: Session, code: str, site_name: str, scale_name: str, *, with_agent: bool = False
) -> tuple[Site, Scale]:
    site = Site(code=code, name=site_name)
    session.add(site)
    session.flush()
    scale = Scale(site_id=site.id, name=scale_name, kind=ScaleKind.STATIC, driver="cas22")
    session.add(scale)
    session.flush()
    if with_agent:
        session.add(Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(f"tok-{code}")))
    session.commit()
    return site, scale


def _insert_weighing(
    session: Session,
    scale_id: int,
    *,
    created_at: datetime,
    vehicle: str | None = None,
    trailer: str | None = None,
    massa: float | None = 15000.0,
    operation: Operation = Operation.WEIGHING,
    code: ErrorCode = ErrorCode.OK,
    source: WeighingSource = WeighingSource.AIS,
    storno_of: int | None = None,
) -> Weighing:
    """Прямая вставка строки журнала с управляемым created_at
    (repo.save_weighing_record ставит created_at = сейчас, для тестов
    фильтра по датам этого недостаточно)."""
    row = Weighing(
        uuid=uuid4(),
        scale_id=scale_id,
        operation=operation,
        code=code,
        massa=massa,
        stable=True,
        weighed_at=created_at,
        created_at=created_at,
        vehicle_number=vehicle,
        trailer_number=trailer,
        source=source,
        storno_of=storno_of,
        checksum="0" * 64,
    )
    session.add(row)
    session.commit()
    return row


def _make_record(**overrides: Any) -> WeighingRecord:
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 15000.0,
        "stable": True,
        "weighed_at": datetime.now(UTC),
        "vehicle_number": "01KG777AAA",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _make_taring(**overrides: Any) -> WeighingRecord:
    fields: dict[str, Any] = {"operation": Operation.TARING, "massa": 7500.0}
    fields.update(overrides)
    return _make_record(**fields)


# ---------------------------------------------------------------------------
# queries.verify_user
# ---------------------------------------------------------------------------


class TestVerifyUser:
    def test_correct_password_returns_user(self, db_session: Session) -> None:
        """Верная пара логин/пароль → объект User."""
        _add_user(db_session)
        user = queries.verify_user(db_session, PANEL_LOGIN, PANEL_PASSWORD)
        assert user is not None
        assert user.login == PANEL_LOGIN
        assert user.full_name == "Айгуль Диспетчер"

    def test_wrong_password_returns_none(self, db_session: Session) -> None:
        """Неверный пароль → None."""
        _add_user(db_session)
        assert queries.verify_user(db_session, PANEL_LOGIN, "wrong-password") is None

    def test_inactive_user_returns_none(self, db_session: Session) -> None:
        """Отключённый пользователь не входит даже с верным паролем."""
        _add_user(db_session, is_active=False)
        assert queries.verify_user(db_session, PANEL_LOGIN, PANEL_PASSWORD) is None

    def test_unknown_login_returns_none(self, db_session: Session) -> None:
        """Несуществующий логин → None (без исключений)."""
        _add_user(db_session)
        assert queries.verify_user(db_session, "nobody", PANEL_PASSWORD) is None


# ---------------------------------------------------------------------------
# queries.dashboard_scales
# ---------------------------------------------------------------------------


class TestDashboardScales:
    def test_scales_with_and_without_agent(self, db_session: Session) -> None:
        """Весы с агентом и без; отсутствие агента не роняет сводку."""
        _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1", with_agent=True)
        _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 2", with_agent=False)
        cards = queries.dashboard_scales(db_session)
        assert len(cards) == 2
        by_site = {card.site.code: card for card in cards}
        assert by_site["a-site"].agent is not None
        assert by_site["b-site"].agent is None

    def test_last_weighing_is_freshest(self, db_session: Session) -> None:
        """last_weighing — самая свежая запись весов; на пустых весах None."""
        _, scale1 = _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1")
        _, scale2 = _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 2")
        now = datetime.now(UTC)
        _insert_weighing(db_session, scale1.id, created_at=now - timedelta(hours=2))
        fresh = _insert_weighing(db_session, scale1.id, created_at=now - timedelta(minutes=5))
        cards = {card.scale.id: card for card in queries.dashboard_scales(db_session)}
        last = cards[scale1.id].last_weighing
        assert last is not None
        assert last.uuid == fresh.uuid
        assert cards[scale2.id].last_weighing is None


# ---------------------------------------------------------------------------
# queries.journal_page
# ---------------------------------------------------------------------------


@dataclass
class JournalSeed:
    """Посев журнала для тестов фильтров: два объекта, четыре записи."""

    site_a: Site
    site_b: Site
    scale_a: Scale
    scale_b: Scale
    w1: Weighing  # 05.08 00:00+06, 01KG777AAA/01KG500AB, ais
    w2: Weighing  # 05.08 23:59:59+06, 28BAHE03KG, local_offline
    w3: Weighing  # 06.08 00:00+06, T076AB40UZ/0108BA40, ais
    w4: Weighing  # 06.08 00:00+06 (тот же момент, id больше), 05KG123BBB, ais


@pytest.fixture
def journal_seed(db_session: Session) -> JournalSeed:
    site_a, scale_a = _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1")
    site_b, scale_b = _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 2")
    d5 = datetime(2026, 8, 5, 0, 0, 0, tzinfo=BISHKEK)
    w1 = _insert_weighing(
        db_session, scale_a.id, created_at=d5, vehicle="01KG777AAA", trailer="01KG500AB"
    )
    w2 = _insert_weighing(
        db_session,
        scale_a.id,
        created_at=datetime(2026, 8, 5, 23, 59, 59, tzinfo=BISHKEK),
        vehicle="28BAHE03KG",
        source=WeighingSource.LOCAL_OFFLINE,
    )
    d6 = datetime(2026, 8, 6, 0, 0, 0, tzinfo=BISHKEK)
    w3 = _insert_weighing(
        db_session, scale_b.id, created_at=d6, vehicle="T076AB40UZ", trailer="0108BA40"
    )
    w4 = _insert_weighing(db_session, scale_b.id, created_at=d6, vehicle="05KG123BBB")
    return JournalSeed(site_a, site_b, scale_a, scale_b, w1, w2, w3, w4)


def _uuids(rows: list[tuple[Weighing, Scale, Site]]) -> list[Any]:
    return [row[0].uuid for row in rows]


class TestJournalPage:
    def test_no_filters_returns_all_newest_first(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """Без фильтров — все записи, новые первыми; при равном created_at
        первым идёт больший id (стабильный порядок)."""
        s = journal_seed
        rows, total = queries.journal_page(db_session, queries.JournalFilters())
        assert total == 4
        assert _uuids(rows) == [s.w4.uuid, s.w3.uuid, s.w2.uuid, s.w1.uuid]

    def test_filter_by_site(self, db_session: Session, journal_seed: JournalSeed) -> None:
        """site_id оставляет только записи весов этого объекта."""
        s = journal_seed
        rows, total = queries.journal_page(db_session, queries.JournalFilters(site_id=s.site_a.id))
        assert total == 2
        assert set(_uuids(rows)) == {s.w1.uuid, s.w2.uuid}

    def test_filter_by_scale(self, db_session: Session, journal_seed: JournalSeed) -> None:
        """scale_id сужает до конкретных весов."""
        s = journal_seed
        rows, total = queries.journal_page(
            db_session, queries.JournalFilters(scale_id=s.scale_b.id)
        )
        assert total == 2
        assert set(_uuids(rows)) == {s.w3.uuid, s.w4.uuid}

    def test_date_from_boundary_inclusive(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """date_from включает записи ровно в полночь этой даты."""
        s = journal_seed
        rows, total = queries.journal_page(
            db_session,
            queries.JournalFilters(date_from=datetime(2026, 8, 5, tzinfo=BISHKEK)),
        )
        assert total == 4  # w1 ровно в 00:00 5-го включена
        rows, total = queries.journal_page(
            db_session,
            queries.JournalFilters(date_from=datetime(2026, 8, 6, tzinfo=BISHKEK)),
        )
        assert total == 2
        assert set(_uuids(rows)) == {s.w3.uuid, s.w4.uuid}

    def test_date_to_includes_whole_day(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """date_to включает весь день до 23:59:59, но не полночь следующего."""
        s = journal_seed
        rows, total = queries.journal_page(
            db_session,
            queries.JournalFilters(date_to=datetime(2026, 8, 5, tzinfo=BISHKEK)),
        )
        assert total == 2, "конец суток date_to обрезан или захватил следующий день"
        assert set(_uuids(rows)) == {s.w1.uuid, s.w2.uuid}

    def test_date_range_single_day(self, db_session: Session, journal_seed: JournalSeed) -> None:
        """Диапазон «один день» отдаёт ровно записи этого дня."""
        s = journal_seed
        day = datetime(2026, 8, 6, tzinfo=BISHKEK)
        rows, total = queries.journal_page(
            db_session, queries.JournalFilters(date_from=day, date_to=day)
        )
        assert total == 2
        assert set(_uuids(rows)) == {s.w3.uuid, s.w4.uuid}

    def test_vehicle_substring_case_insensitive(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """Подстрока номера в нижнем регистре и с пробелами находит запись."""
        s = journal_seed
        rows, total = queries.journal_page(db_session, queries.JournalFilters(vehicle="  77aa  "))
        assert total == 1
        assert _uuids(rows) == [s.w1.uuid]

    def test_vehicle_matches_trailer_too(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """Поиск по номеру ищет и в номере прицепа."""
        s = journal_seed
        rows, total = queries.journal_page(db_session, queries.JournalFilters(vehicle="0108ba"))
        assert total == 1
        assert _uuids(rows) == [s.w3.uuid]

    def test_filter_by_source(self, db_session: Session, journal_seed: JournalSeed) -> None:
        """Фильтр по источнику: офлайн-записи отделяются от АИС."""
        s = journal_seed
        rows, total = queries.journal_page(
            db_session, queries.JournalFilters(source=WeighingSource.LOCAL_OFFLINE)
        )
        assert total == 1
        assert _uuids(rows) == [s.w2.uuid]
        _, total_ais = queries.journal_page(
            db_session, queries.JournalFilters(source=WeighingSource.AIS)
        )
        assert total_ais == 3

    def test_pagination_offset_and_stable_total(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """total не зависит от страницы; offset выдаёт следующую страницу."""
        s = journal_seed
        page1, total1 = queries.journal_page(
            db_session, queries.JournalFilters(), limit=2, offset=0
        )
        page2, total2 = queries.journal_page(
            db_session, queries.JournalFilters(), limit=2, offset=2
        )
        assert total1 == total2 == 4
        assert _uuids(page1) == [s.w4.uuid, s.w3.uuid]
        assert _uuids(page2) == [s.w2.uuid, s.w1.uuid]

    def test_offset_beyond_data_is_empty(
        self, db_session: Session, journal_seed: JournalSeed
    ) -> None:
        """Страница за пределами данных — пустой список, total прежний."""
        rows, total = queries.journal_page(
            db_session, queries.JournalFilters(), limit=50, offset=100
        )
        assert rows == []
        assert total == 4


# ---------------------------------------------------------------------------
# queries.weighing_card
# ---------------------------------------------------------------------------


class TestWeighingCard:
    def test_missing_id_returns_none(self, db_session: Session) -> None:
        """Несуществующий id → None (маршрут превратит в 404)."""
        assert queries.weighing_card(db_session, 987654) is None

    def test_photos_ordered_by_role(self, db_session: Session) -> None:
        """Фото в карточке отсортированы по роли (front раньше rear)
        независимо от порядка вставки."""
        _, scale = _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1")
        record = _make_record()
        photos = [
            PhotoMeta(role=CameraRole.REAR, filename="r.jpeg", sha256=SHA_B, size_bytes=2),
            PhotoMeta(role=CameraRole.FRONT, filename="f.jpeg", sha256=SHA_A, size_bytes=1),
        ]
        repo.save_weighing_record(db_session, scale.id, record, photos)
        row = db_session.execute(select(Weighing).where(Weighing.uuid == record.uuid)).scalar_one()
        card = queries.weighing_card(db_session, row.id)
        assert card is not None
        assert [photo.role for photo in card.photos] == [CameraRole.FRONT, CameraRole.REAR]
        assert [photo.sha256 for photo in card.photos] == [SHA_A, SHA_B]

    def test_tare_weighing_linked(self, db_session: Session) -> None:
        """Карточка брутто ссылается на запись тарирования (tare_weighing)."""
        _, scale = _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1")
        taring = _make_taring()
        repo.save_weighing_record(db_session, scale.id, taring)
        brutto = _make_record(tare_weighing_uuid=taring.uuid, tare_value=7500.0, netto=7500.0)
        repo.save_weighing_record(db_session, scale.id, brutto)

        brutto_row = db_session.execute(
            select(Weighing).where(Weighing.uuid == brutto.uuid)
        ).scalar_one()
        card = queries.weighing_card(db_session, brutto_row.id)
        assert card is not None
        assert card.tare_weighing is not None
        assert card.tare_weighing.uuid == taring.uuid
        assert card.storno_of is None

    def test_storno_of_linked(self, db_session: Session) -> None:
        """Сторно-запись ссылается на исходную (storno_of)."""
        _, scale = _add_site_scale(db_session, "a-site", "СВХ «А»", "Весы 1")
        now = datetime.now(UTC)
        original = _insert_weighing(db_session, scale.id, created_at=now, vehicle="01KG777AAA")
        storno = _insert_weighing(
            db_session, scale.id, created_at=now, vehicle="01KG777AAA", storno_of=original.id
        )
        card = queries.weighing_card(db_session, storno.id)
        assert card is not None
        assert card.storno_of is not None
        assert card.storno_of.uuid == original.uuid


# ---------------------------------------------------------------------------
# queries.tare_list
# ---------------------------------------------------------------------------


class TestTareList:
    def _seed(self, session: Session) -> Scale:
        _, scale = _add_site_scale(session, "a-site", "СВХ «А»", "Весы 1")
        now = datetime.now(UTC)
        fresh = _make_taring(vehicle_number="01KG111AAA", weighed_at=now - timedelta(days=5))
        expired = _make_taring(
            vehicle_number="01KG222BBB", massa=9000.0, weighed_at=now - timedelta(days=200)
        )
        repo.save_weighing_record(session, scale.id, fresh)
        repo.save_weighing_record(session, scale.id, expired)
        return scale

    def test_expired_tares_hidden(self, db_session: Session) -> None:
        """Тары старше 3 календарных месяцев не показываются и не в total."""
        self._seed(db_session)
        rows, total = queries.tare_list(db_session)
        assert total == 1
        assert [row[0].vehicle_number for row in rows] == ["01KG111AAA"]

    def test_search_by_number_substring(self, db_session: Session) -> None:
        """Поиск по подстроке номера (нижний регистр допустим)."""
        scale = self._seed(db_session)
        other = _make_taring(
            vehicle_number="05KG999ZZZ", weighed_at=datetime.now(UTC) - timedelta(days=1)
        )
        repo.save_weighing_record(db_session, scale.id, other)
        rows, total = queries.tare_list(db_session, search="999zz")
        assert total == 1
        assert [row[0].vehicle_number for row in rows] == ["05KG999ZZZ"]

    def test_newest_first(self, db_session: Session) -> None:
        """Действующие тары отсортированы по свежести тарирования."""
        scale = self._seed(db_session)
        newer = _make_taring(
            vehicle_number="05KG999ZZZ", weighed_at=datetime.now(UTC) - timedelta(days=1)
        )
        repo.save_weighing_record(db_session, scale.id, newer)
        rows, _ = queries.tare_list(db_session)
        assert [row[0].vehicle_number for row in rows] == ["05KG999ZZZ", "01KG111AAA"]

    def test_filter_by_site_and_scale(self, db_session: Session) -> None:
        """Двое весов на одном объекте: фильтры делят реестр (11.08.2026)."""
        site, scale_1 = _add_site_scale(db_session, "kant", "СВХ «КАНТ»", "Весы 1")
        scale_2 = Scale(site_id=site.id, name="Весы 2", kind=ScaleKind.STATIC, driver="cas22")
        db_session.add(scale_2)
        db_session.flush()
        _, other_scale = _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 1")
        now = datetime.now(UTC)
        for scale, number in (
            (scale_1, "01KG111AAA"),
            (scale_2, "02KG222BBB"),
            (other_scale, "03KG333CCC"),
        ):
            repo.save_weighing_record(
                db_session,
                scale.id,
                _make_taring(vehicle_number=number, weighed_at=now - timedelta(days=1)),
            )

        rows, total = queries.tare_list(db_session, site_id=site.id)
        assert total == 2
        assert {row[0].vehicle_number for row in rows} == {"01KG111AAA", "02KG222BBB"}

        rows, total = queries.tare_list(db_session, scale_id=scale_2.id)
        assert total == 1
        assert [row[0].vehicle_number for row in rows] == ["02KG222BBB"]


# ---------------------------------------------------------------------------
# queries.tare_expires_at (чистая функция, без БД)
# ---------------------------------------------------------------------------


class TestTareExpiresAt:
    def test_end_of_month_clamped(self) -> None:
        """31 августа + 3 месяца → 30 ноября (в ноябре нет 31-го)."""
        tared = datetime(2026, 8, 31, 12, 30, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2026, 11, 30, 12, 30, tzinfo=UTC)

    def test_year_rollover_with_clamp(self) -> None:
        """30 ноября + 3 месяца → 28 февраля следующего (невисокосного) года."""
        tared = datetime(2026, 11, 30, 8, 0, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2027, 2, 28, 8, 0, tzinfo=UTC)

    def test_year_rollover_leap_february(self) -> None:
        """29 ноября 2027 + 3 месяца → 29 февраля 2028 (високосный год)."""
        tared = datetime(2027, 11, 29, 8, 0, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2028, 2, 29, 8, 0, tzinfo=UTC)

    def test_mid_month_day_preserved(self) -> None:
        """День 1–27 должен сохраняться: 15 января + 3 месяца → 15 апреля.

        Проверка исправленного поведения tare_expires_at.
        и функция сваливается в ветку ``day=28`` — срок действия завышается
        (15 января «действует» до 28 апреля вместо 15 апреля).
        """
        tared = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2026, 4, 15, 10, 0, tzinfo=UTC)

    def test_first_day_of_month_preserved(self) -> None:
        """1 марта + 3 месяца → 1 июня (тот же баг, крайний случай day=1)."""
        tared = datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)

    def test_day_28_kept_as_is(self) -> None:
        """28 число сохраняется во всех месяцах."""
        tared = datetime(2026, 11, 28, 8, 0, tzinfo=UTC)
        assert queries.tare_expires_at(tared) == datetime(2027, 2, 28, 8, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# queries.refs_data
# ---------------------------------------------------------------------------


class TestRefsData:
    def test_lists_composition(self, db_session: Session) -> None:
        """Справочники собирают объекты, весы, камеры и агентов полностью."""
        site_a, scale_a = _add_site_scale(
            db_session, "a-site", "СВХ «А»", "Весы 1", with_agent=True
        )
        site_b, scale_b = _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 2")
        db_session.add(Camera(scale_id=scale_a.id, role=CameraRole.FRONT, rtsp_url="rtsp://cam/1"))
        db_session.commit()

        refs = queries.refs_data(db_session)
        assert [site.code for site in refs.sites] == ["a-site", "b-site"]
        assert {(scale.id, site.id) for scale, site in refs.scales} == {
            (scale_a.id, site_a.id),
            (scale_b.id, site_b.id),
        }
        assert [(camera.role, scale.id, site.id) for camera, scale, site in refs.cameras] == [
            (CameraRole.FRONT, scale_a.id, site_a.id)
        ]
        assert [(scale.id, site.id) for _agent, scale, site in refs.agents] == [
            (scale_a.id, site_a.id)
        ]

    def test_site_filter_narrows_everything_but_sites(self, db_session: Session) -> None:
        """site_id сужает весы/камеры/агентов до объекта; sites — полный
        (нужен селекторам фильтра и форм)."""
        site_a, scale_a = _add_site_scale(
            db_session, "a-site", "СВХ «А»", "Весы 1", with_agent=True
        )
        _, scale_b = _add_site_scale(db_session, "b-site", "СВХ «Б»", "Весы 2", with_agent=True)
        db_session.add(Camera(scale_id=scale_a.id, role=CameraRole.FRONT, rtsp_url="rtsp://a/1"))
        db_session.add(Camera(scale_id=scale_b.id, role=CameraRole.FRONT, rtsp_url="rtsp://b/1"))
        db_session.commit()

        refs = queries.refs_data(db_session, site_a.id)
        assert [site.code for site in refs.sites] == ["a-site", "b-site"]
        assert [scale.id for scale, _ in refs.scales] == [scale_a.id]
        assert [scale.id for _, scale, _site in refs.cameras] == [scale_a.id]
        assert [scale.id for _, scale, _site in refs.agents] == [scale_a.id]

    def test_empty_db_gives_empty_lists(self, db_session: Session) -> None:
        """Пустая БД → пустые справочники, без ошибок."""
        refs = queries.refs_data(db_session)
        assert refs.sites == []
        assert refs.scales == []
        assert refs.cameras == []
        assert refs.agents == []

    def test_scales_of_one_site_ordered_by_name(self, db_session: Session) -> None:
        """Порядок весов внутри объекта задан именем: сортировки только по
        объекту не хватало — строки прыгали между обновлениями (11.08.2026)."""
        site, _ = _add_site_scale(db_session, "kant", "СВХ «КАНТ»", "Весы Б")
        db_session.add(Scale(site_id=site.id, name="Весы А", kind=ScaleKind.STATIC, driver="cas22"))
        db_session.commit()
        refs = queries.refs_data(db_session)
        assert [scale.name for scale, _site in refs.scales] == ["Весы А", "Весы Б"]


# ---------------------------------------------------------------------------
# Маршруты панели (TestClient)
# ---------------------------------------------------------------------------


@dataclass
class PanelEnv:
    """Окружение маршрутных тестов: клиент, фабрика сессий, каталог фото."""

    client: TestClient
    factory: sessionmaker[Session]
    photos_dir: Path
    scale_id: int
    weighing_id: int  # запись 01KG777AAA с тарой и фото
    taring_id: int
    hub: AgentHub


@pytest.fixture
def panel_env(db: sessionmaker[Session], tmp_path: Path) -> Iterator[PanelEnv]:
    """Посев панели: пользователь, два объекта, журнал, тара, фото; приложение
    как в center/app.py — SessionMiddleware + create_panel_router."""
    photos_dir = tmp_path / "photos"
    photos_dir.mkdir()
    with db() as session:
        _add_user(session)
        _, scale_a = _add_site_scale(
            session, "kyzyl-kyia", "СВХ «Кызыл-Кыя»", "Весы SCS-80", with_agent=True
        )
        _, scale_b = _add_site_scale(session, "kant", "СВХ «КАНТ»", "Весы SCS-80 22-3")
        taring = _make_taring(
            vehicle_number="01KG555TTT", weighed_at=datetime.now(UTC) - timedelta(days=2)
        )
        repo.save_weighing_record(session, scale_a.id, taring)
        brutto = _make_record(
            vehicle_number="01KG777AAA",
            trailer_number="01KG500AB",
            tare_weighing_uuid=taring.uuid,
            tare_value=7500.0,
            netto=7500.0,
        )
        photos = [
            PhotoMeta(role=CameraRole.FRONT, filename="f.jpeg", sha256=SHA_A, size_bytes=3),
        ]
        repo.save_weighing_record(session, scale_a.id, brutto, photos)
        other = _make_record(vehicle_number="28BAHE03KG")
        repo.save_weighing_record(session, scale_b.id, other)
        weighing_id = session.execute(
            select(Weighing.id).where(Weighing.uuid == brutto.uuid)
        ).scalar_one()
        taring_id = session.execute(
            select(Weighing.id).where(Weighing.uuid == taring.uuid)
        ).scalar_one()
        scale_id = scale_a.id

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-secret", session_cookie="ves_test")
    hub = AgentHub()
    app.include_router(create_panel_router(db, hub, photos_dir=photos_dir))
    client = TestClient(app)
    yield PanelEnv(client, db, photos_dir, scale_id, weighing_id, taring_id, hub)
    client.close()


def _login(env: PanelEnv) -> None:
    response = env.client.post(
        "/panel/login",
        data={"login": PANEL_LOGIN, "password": PANEL_PASSWORD},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/panel/"


PROTECTED_PATHS = [
    "/panel/",
    "/panel/journal",
    "/panel/tares",
    "/panel/refs",
    "/panel/journal/1",
    "/panel/photos/x.jpeg",
    "/panel/fragments/dashboard",
]


class TestPanelAuthRedirects:
    @pytest.mark.parametrize("path", PROTECTED_PATHS)
    def test_unauthenticated_redirects_to_login(self, panel_env: PanelEnv, path: str) -> None:
        """Любой экран без сессии → 303 на /panel/login."""
        response = panel_env.client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/panel/login"


class TestPanelLogin:
    def test_login_page_renders(self, panel_env: PanelEnv) -> None:
        """Форма входа доступна без сессии и содержит поля login/password."""
        response = panel_env.client.get("/panel/login")
        assert response.status_code == 200
        assert 'name="login"' in response.text
        assert 'name="password"' in response.text

    def test_wrong_password_shows_error_no_session(self, panel_env: PanelEnv) -> None:
        """Неверный пароль: форма с ошибкой, сессия не создаётся."""
        response = panel_env.client.post(
            "/panel/login",
            data={"login": PANEL_LOGIN, "password": "totally-wrong"},
            follow_redirects=False,
        )
        assert response.status_code == 200
        assert "Неверный логин или пароль" in response.text
        after = panel_env.client.get("/panel/", follow_redirects=False)
        assert after.status_code == 303, "после неудачного входа экран остался доступен"

    def test_successful_login_opens_screens(self, panel_env: PanelEnv) -> None:
        """Верный пароль → 303 на дашборд; экраны 200 и содержат данные посева."""
        _login(panel_env)

        dashboard = panel_env.client.get("/panel/")
        assert dashboard.status_code == 200
        assert "СВХ «Кызыл-Кыя»" in dashboard.text
        assert "СВХ «КАНТ»" in dashboard.text

        journal = panel_env.client.get("/panel/journal")
        assert journal.status_code == 200
        assert "01KG777AAA" in journal.text
        assert "28BAHE03KG" in journal.text

        tares = panel_env.client.get("/panel/tares")
        assert tares.status_code == 200
        assert "01KG555TTT" in tares.text

        refs = panel_env.client.get("/panel/refs")
        assert refs.status_code == 200
        assert "Весы SCS-80" in refs.text


class TestPanelJournalRoutes:
    def test_vehicle_filter_narrows_rows(self, panel_env: PanelEnv) -> None:
        """Фильтр vehicle оставляет только совпавшие записи."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal", params={"vehicle": "777aaa"})
        assert response.status_code == 200
        assert "01KG777AAA" in response.text
        assert "28BAHE03KG" not in response.text

    def test_page_beyond_data_renders_empty(self, panel_env: PanelEnv) -> None:
        """page=2 при малом числе записей — пустая таблица, не падение."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal", params={"page": 2})
        assert response.status_code == 200
        assert "01KG777AAA" not in response.text

    @pytest.mark.parametrize("page", [0, -5])
    def test_non_positive_page_does_not_crash(self, panel_env: PanelEnv, page: int) -> None:
        """Нулевая/отрицательная страница трактуется как первая."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal", params={"page": page})
        assert response.status_code == 200
        assert "01KG777AAA" in response.text

    def test_scale_filter_narrows_rows(self, panel_env: PanelEnv) -> None:
        """Фильтр по весам: записи соседних весов не показываются."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal", params={"scale_id": panel_env.scale_id})
        assert response.status_code == 200
        assert "01KG777AAA" in response.text
        assert "28BAHE03KG" not in response.text

    def test_scale_of_other_site_is_dropped(self, panel_env: PanelEnv) -> None:
        """Весы чужого объекта в фильтре сбрасываются, а не дают пустой список
        (иначе после смены объекта экран молча пустеет)."""
        _login(panel_env)
        with panel_env.factory() as session:
            site_id = session.get(Scale, panel_env.scale_id).site_id  # type: ignore[union-attr]
            other_scale_id = session.execute(
                select(Scale.id).where(Scale.site_id != site_id)
            ).scalar_one()
        response = panel_env.client.get(
            "/panel/journal", params={"site_id": site_id, "scale_id": other_scale_id}
        )
        assert response.status_code == 200
        assert "01KG777AAA" in response.text  # показан выбранный объект
        assert "28BAHE03KG" not in response.text

    @pytest.mark.parametrize(
        "params",
        [
            {"site_id": "", "scale_id": "", "vehicle": "", "source": ""},
            {"site_id": "", "scale_id": "", "page": "2"},
            {"site_id": "abc", "scale_id": "%"},
            # id в БД — int4: без верхней границы был бы 500 из драйвера
            {"site_id": "2147483648", "scale_id": "99999999999999999999"},
            {"site_id": "0"},
        ],
    )
    def test_empty_and_garbage_filters_render(
        self, panel_env: PanelEnv, params: dict[str, str]
    ) -> None:
        """Форма шлёт пустые значения селектов («Все объекты»), а ссылки
        пагинации — пустые фильтры: это должна быть страница, а не 422."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal", params=params)
        assert response.status_code == 200

    def test_scale_selector_narrowed_by_site(self, panel_env: PanelEnv) -> None:
        """Выбран объект → в селекторе весов только его весы, без подписи
        объекта; без объекта — все весы с подписью объекта."""
        _login(panel_env)
        with panel_env.factory() as session:
            site_id = session.get(Scale, panel_env.scale_id).site_id  # type: ignore[union-attr]
        all_sites = panel_env.client.get("/panel/journal").text
        assert "СВХ «Кызыл-Кыя» · Весы SCS-80" in all_sites
        assert "СВХ «КАНТ» · Весы SCS-80 22-3" in all_sites

        one_site = panel_env.client.get("/panel/journal", params={"site_id": site_id}).text
        assert "СВХ «КАНТ» · Весы SCS-80 22-3" not in one_site
        assert f'value="{panel_env.scale_id}"' in one_site

    def test_tares_empty_filters_render(self, panel_env: PanelEnv) -> None:
        """Тот же случай на экране тарирований: «Найти» с «Все объекты»."""
        _login(panel_env)
        response = panel_env.client.get(
            "/panel/tares", params={"site_id": "", "scale_id": "", "search": ""}
        )
        assert response.status_code == 200
        assert "01KG555TTT" in response.text

    def test_tares_filtered_by_scale(self, panel_env: PanelEnv) -> None:
        """Реестр тарирований тоже фильтруется по весам."""
        _login(panel_env)
        response = panel_env.client.get("/panel/tares", params={"scale_id": panel_env.scale_id})
        assert response.status_code == 200
        assert "01KG555TTT" in response.text
        with panel_env.factory() as session:
            other_scale_id = session.execute(
                select(Scale.id).where(Scale.id != panel_env.scale_id)
            ).scalar_one()
        response = panel_env.client.get("/panel/tares", params={"scale_id": other_scale_id})
        assert response.status_code == 200
        assert "01KG555TTT" not in response.text


class TestPanelCard:
    def test_card_shows_record_and_checksum(self, panel_env: PanelEnv) -> None:
        """Карточка: номер ТС, прицеп и контрольная сумма записи на странице."""
        _login(panel_env)
        with panel_env.factory() as session:
            checksum = session.get(Weighing, panel_env.weighing_id).checksum  # type: ignore[union-attr]
        response = panel_env.client.get(f"/panel/journal/{panel_env.weighing_id}")
        assert response.status_code == 200
        assert "01KG777AAA" in response.text
        assert "01KG500AB" in response.text
        assert checksum in response.text, "контрольной суммы нет на карточке"
        # ссылка на связанное тарирование ведёт на его карточку
        assert f"/panel/journal/{panel_env.taring_id}" in response.text

    def test_missing_record_404(self, panel_env: PanelEnv) -> None:
        """Несуществующий id → 404 (а не 500 и не пустая страница)."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal/987654")
        assert response.status_code == 404


class TestPanelPhotos:
    def test_photo_served_by_session(self, panel_env: PanelEnv) -> None:
        """Файл из photos_dir отдаётся вошедшему пользователю как JPEG."""
        _login(panel_env)
        target = panel_env.photos_dir / "vesy" / "2026" / "08" / "07"
        target.mkdir(parents=True)
        payload = b"\xff\xd8\xff\xe0 fake jpeg"
        (target / "x.jpeg").write_bytes(payload)
        response = panel_env.client.get("/panel/photos/vesy/2026/08/07/x.jpeg")
        assert response.status_code == 200
        assert response.content == payload
        assert response.headers["content-type"] == "image/jpeg"

    def test_traversal_outside_photos_dir_404(self, panel_env: PanelEnv) -> None:
        """../ (percent-encoded) не выводит за пределы photos_dir."""
        _login(panel_env)
        secret = panel_env.photos_dir.parent / "secret.txt"
        secret.write_text("секрет вне каталога фото")
        response = panel_env.client.get("/panel/photos/..%2Fsecret.txt")
        assert response.status_code == 404, "path traversal вышел за пределы photos_dir"

    def test_missing_photo_404(self, panel_env: PanelEnv) -> None:
        """Несуществующий файл → 404."""
        _login(panel_env)
        response = panel_env.client.get("/panel/photos/vesy/none.jpeg")
        assert response.status_code == 404


class TestPanelLogout:
    def test_logout_drops_session(self, panel_env: PanelEnv) -> None:
        """logout → 303 на форму входа; экраны снова требуют вход."""
        _login(panel_env)
        assert panel_env.client.get("/panel/").status_code == 200
        response = panel_env.client.post("/panel/logout", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/panel/login"
        after = panel_env.client.get("/panel/journal", follow_redirects=False)
        assert after.status_code == 303


# ---------------------------------------------------------------------------
# tools/center_admin.py и tools/seed_demo_center.py (subprocess)
# ---------------------------------------------------------------------------


def _run_tool(
    module: str, args: list[str], db_url: URL | None = None, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Запуск CLI-инструмента отдельным процессом.

    start_new_session=True отвязывает процесс от терминала: getpass не сможет
    открыть /dev/tty и упадёт в чтение stdin — пароль подаётся через input.
    """
    env = dict(os.environ)
    if db_url is not None:
        env["DATABASE_URL"] = db_url.render_as_string(hide_password=False)
    return subprocess.run(
        [sys.executable, "-m", module, *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=REPO_ROOT,
        input=input_text,
        start_new_session=True,
        timeout=120,
    )


class TestCenterAdminCli:
    def test_help_exits_zero(self) -> None:
        """--help работает без БД и перечисляет команды."""
        result = _run_tool("tools.center_admin", ["--help"])
        assert result.returncode == 0
        for command in ("create-user", "create-site", "create-scale", "create-agent", "list"):
            assert command in result.stdout

    def test_create_site_scale_agent_flow(
        self, panel_db_url: URL, db: sessionmaker[Session]
    ) -> None:
        """create-site/scale/agent создают объекты; токен агента печатается
        один раз, в БД хранится только его sha256-хеш."""
        result = _run_tool(
            "tools.center_admin",
            ["create-site", "--code", "kyzyl-kyia", "--name", "СВХ «Кызыл-Кыя»"],
            db_url=panel_db_url,
        )
        assert result.returncode == 0, result.stderr

        result = _run_tool(
            "tools.center_admin",
            [
                "create-scale",
                "--site",
                "kyzyl-kyia",
                "--name",
                "Весы SCS-80",
                "--legacy-ip",
                "192.168.150.185",
                "--legacy-port",
                "8087",
                "--legacy-autoscale",
                "2",
            ],
            db_url=panel_db_url,
        )
        assert result.returncode == 0, result.stderr

        with db() as session:
            scale = session.execute(select(Scale)).scalar_one()
            assert scale.name == "Весы SCS-80"
            assert scale.legacy_ip == "192.168.150.185"
            site = session.execute(select(Site)).scalar_one()
            assert site.code == "kyzyl-kyia"

        result = _run_tool(
            "tools.center_admin",
            ["create-agent", "--scale-id", str(scale.id)],
            db_url=panel_db_url,
        )
        assert result.returncode == 0, result.stderr
        # токен — единственная непустая строка с отступом после заголовка
        token_lines = [line.strip() for line in result.stdout.splitlines() if line.startswith("  ")]
        assert token_lines, f"токен не напечатан: {result.stdout!r}"
        token = token_lines[0]
        with db() as session:
            agent = session.execute(select(Agent)).scalar_one()
            assert agent.token_hash == repo.hash_agent_token(token)
            assert token not in agent.token_hash, "сырой токен попал в БД"

    def test_create_scale_unknown_site_fails(
        self, panel_db_url: URL, db: sessionmaker[Session]
    ) -> None:
        """create-scale с неизвестным кодом объекта завершается ошибкой."""
        result = _run_tool(
            "tools.center_admin",
            ["create-scale", "--site", "no-such", "--name", "Весы"],
            db_url=panel_db_url,
        )
        assert result.returncode != 0
        assert "не найден" in result.stderr

    def test_create_user_short_password_rejected(
        self, panel_db_url: URL, db: sessionmaker[Session]
    ) -> None:
        """Пароль короче 8 символов → отказ, пользователь не создан."""
        result = _run_tool(
            "tools.center_admin",
            ["create-user", "--login", "d.ivanov"],
            db_url=panel_db_url,
            input_text="short\n",
        )
        assert result.returncode != 0
        assert "короче 8" in result.stderr
        with db() as session:
            assert session.execute(select(User)).scalar_one_or_none() is None

    def test_create_user_success_via_stdin(
        self, panel_db_url: URL, db: sessionmaker[Session]
    ) -> None:
        """Нормальный пароль через stdin: пользователь создан, хеш проверяем."""
        result = _run_tool(
            "tools.center_admin",
            ["create-user", "--login", "d.ivanov", "--role", "dispatcher"],
            db_url=panel_db_url,
            input_text="good-password-9\n",
        )
        assert result.returncode == 0, result.stderr
        assert "создан" in result.stdout
        with db() as session:
            user = session.execute(select(User)).scalar_one()
            assert user.login == "d.ivanov"
            assert user.role is UserRole.DISPATCHER
            assert verify_password("good-password-9", user.pw_hash)


class TestSeedDemoCenter:
    def test_module_importable(self) -> None:
        """Импорт модуля не выполняет посев (main под guard'ом)."""
        module = importlib.import_module("tools.seed_demo_center")
        assert hasattr(module, "main")

    def test_refuses_non_empty_db(self, panel_db_url: URL, db: sessionmaker[Session]) -> None:
        """На непустой БД сеятель отказывается (защита боевой базы)."""
        with db() as session:
            _add_site_scale(session, "busy", "СВХ «Занято»", "Весы")
        result = _run_tool("tools.seed_demo_center", [], db_url=panel_db_url)
        assert result.returncode != 0
        assert "БД не пуста" in result.stderr
        with db() as session:
            # посев не прошёл: остался только наш объект
            assert session.execute(select(Site)).scalar_one().code == "busy"

    def test_seeds_empty_db(self, panel_db_url: URL, db: sessionmaker[Session]) -> None:
        """На чистой БД посев проходит: объекты, пользователь demo, журнал, тары."""
        result = _run_tool("tools.seed_demo_center", [], db_url=panel_db_url)
        assert result.returncode == 0, result.stderr
        with db() as session:
            sites = list(session.execute(select(Site)).scalars())
            assert len(sites) == 3
            demo = session.execute(select(User).where(User.login == "demo")).scalar_one()
            assert verify_password("demo1234", demo.pw_hash)
            _, journal_total = queries.journal_page(session, queries.JournalFilters())
            assert journal_total == 40  # 6 тарирований + 34 взвешивания
            _, tares_total = queries.tare_list(session)
            assert tares_total == 6


# ---------------------------------------------------------------------------
# Дашборд: состояние индикатора и камер (запрос Игоря 09.08.2026)
# ---------------------------------------------------------------------------


def _set_agent_online(env: PanelEnv) -> None:
    with env.factory() as session:
        agent_id = session.execute(
            select(Agent.id).where(Agent.scale_id == env.scale_id)
        ).scalar_one()
        repo.set_agent_status(session, agent_id, AgentStatus.ONLINE)


class TestDashboardEquipment:
    def test_indicator_and_each_camera_shown(self, panel_env: PanelEnv) -> None:
        """Все проблемы видны на одном экране: индикатор и КАЖДАЯ камера
        из последнего heartbeat, недоступная камера — красным."""
        _login(panel_env)
        _set_agent_online(panel_env)
        panel_env.hub.update_equipment(
            panel_env.scale_id,
            EquipmentStatus(
                scale_status=ScaleStatus.OK,
                current_weight=12340.0,
                stable=True,
                cameras=[
                    CameraStatus(role=CameraRole.FRONT, available=True),
                    CameraStatus(role=CameraRole.REAR, available=False),
                ],
            ),
        )
        page = panel_env.client.get("/panel/").text
        assert "Индикатор: ОК" in page
        assert "Камера перед: ОК" in page
        assert "Камера зад: недоступна" in page

    def test_indicator_no_data_is_error(self, panel_env: PanelEnv) -> None:
        """Индикатор без данных — ошибка на карточке (dot err)."""
        _login(panel_env)
        _set_agent_online(panel_env)
        panel_env.hub.update_equipment(
            panel_env.scale_id, EquipmentStatus(scale_status=ScaleStatus.NO_DATA)
        )
        page = panel_env.client.get("/panel/").text
        assert "Индикатор: нет данных" in page
        assert "Камеры: нет данных" in page  # cameras пуст — честное «нет данных»

    def test_equipment_hidden_for_offline_agent(self, panel_env: PanelEnv) -> None:
        """Для офлайн-агента стухшие статусы оборудования не показываются."""
        _login(panel_env)
        panel_env.hub.update_equipment(
            panel_env.scale_id, EquipmentStatus(scale_status=ScaleStatus.OK)
        )
        page = panel_env.client.get("/panel/").text
        assert "Индикатор" not in page


# ---------------------------------------------------------------------------
# Разграничение видимости по объекту (решение 11.08.2026, перед тиражом)
# ---------------------------------------------------------------------------


def _bind_user_to_site(env: PanelEnv, site_code: str) -> None:
    """Привязать пользователя панели к объекту (как это делает админ)."""
    with env.factory() as session:
        site_id = session.execute(select(Site.id).where(Site.code == site_code)).scalar_one()
        user = session.execute(select(User).where(User.login == PANEL_LOGIN)).scalar_one()
        user.site_id = site_id
        session.commit()


def _make_admin(env: PanelEnv) -> None:
    with env.factory() as session:
        user = session.execute(select(User).where(User.login == PANEL_LOGIN)).scalar_one()
        user.role = UserRole.ADMIN
        session.commit()


class TestSiteScope:
    def test_dashboard_shows_only_own_site(self, panel_env: PanelEnv) -> None:
        """Диспетчер объекта видит на дашборде только свои весы."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        page = panel_env.client.get("/panel/").text
        assert "Кызыл-Кыя" in page
        assert "КАНТ" not in page

    def test_journal_hides_other_sites(self, panel_env: PanelEnv) -> None:
        """В журнале не видно записей чужого объекта — даже без фильтра."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        page = panel_env.client.get("/panel/journal").text
        assert "01KG777AAA" in page  # своя запись
        assert "28BAHE03KG" not in page  # запись соседнего объекта

    def test_journal_filter_cannot_widen_scope(self, panel_env: PanelEnv) -> None:
        """Подстановка чужого site_id в адрес не расширяет видимость."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        with panel_env.factory() as session:
            other_id = session.execute(select(Site.id).where(Site.code == "kant")).scalar_one()
        page = panel_env.client.get(f"/panel/journal?site_id={other_id}").text
        assert "28BAHE03KG" not in page, "фильтром удалось выйти за свой объект"

    def test_selectors_offer_only_own_site(self, panel_env: PanelEnv) -> None:
        """В селекторах фильтров остаётся только свой объект."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        page = panel_env.client.get("/panel/journal").text
        assert "СВХ «КАНТ»" not in page

    def test_foreign_record_card_is_404(self, panel_env: PanelEnv) -> None:
        """Карточка чужой записи недоступна по прямой ссылке."""
        _login(panel_env)
        with panel_env.factory() as session:
            foreign_id = session.execute(
                select(Weighing.id).where(Weighing.vehicle_number == "28BAHE03KG")
            ).scalar_one()
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        assert panel_env.client.get(f"/panel/journal/{foreign_id}").status_code == 404
        # своя запись по-прежнему открывается
        assert panel_env.client.get(f"/panel/journal/{panel_env.weighing_id}").status_code == 200

    def test_tares_limited_to_own_site(self, panel_env: PanelEnv) -> None:
        """Реестр тарирований тоже ограничен своим объектом."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kant")
        page = panel_env.client.get("/panel/tares").text
        assert "01KG555TTT" not in page, "видна тара чужого объекта"

    def test_refs_limited_to_own_site(self, panel_env: PanelEnv) -> None:
        """Справочники показывают только свой объект."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        page = panel_env.client.get("/panel/refs").text
        assert "СВХ «КАНТ»" not in page

    def test_user_without_site_sees_everything(self, panel_env: PanelEnv) -> None:
        """Диспетчер без привязки (головной офис) видит все объекты."""
        _login(panel_env)
        page = panel_env.client.get("/panel/").text
        assert "Кызыл-Кыя" in page and "КАНТ" in page

    def test_admin_sees_everything_despite_binding(self, panel_env: PanelEnv) -> None:
        """Админ видит систему целиком, даже будучи привязан к объекту."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        _make_admin(panel_env)
        page = panel_env.client.get("/panel/").text
        assert "Кызыл-Кыя" in page and "КАНТ" in page

    def test_binding_applies_without_relogin(self, panel_env: PanelEnv) -> None:
        """Привязка применяется сразу: сессия не кэширует права."""
        _login(panel_env)
        assert "КАНТ" in panel_env.client.get("/panel/").text
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        assert "КАНТ" not in panel_env.client.get("/panel/").text


# ---------------------------------------------------------------------------
# Выгрузка журнала в CSV (architecture §4.3) и пагинация тарирований
# ---------------------------------------------------------------------------


class TestJournalExport:
    def test_csv_has_bom_and_semicolons(self, panel_env: PanelEnv) -> None:
        """Файл открывается в Excel двойным щелчком: BOM + точка с запятой."""
        _login(panel_env)
        response = panel_env.client.get("/panel/journal/export.csv")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert "attachment" in response.headers["content-disposition"]
        assert ".csv" in response.headers["content-disposition"]
        text = response.content.decode("utf-8")
        assert text.startswith("\ufeff"), "нет BOM — Excel испортит кириллицу"
        # запятые в шапке есть («Брутто, кг»), поэтому проверяем состав колонок
        header = text.lstrip("\ufeff").splitlines()[0].split(";")
        assert header[:4] == ["Дата и время", "Объект", "Весы", "Номер ТС"]
        assert len(header) == 11

    def test_rows_contain_journal_data(self, panel_env: PanelEnv) -> None:
        """В выгрузке есть записи с номерами, объектом и весами."""
        _login(panel_env)
        text = panel_env.client.get("/panel/journal/export.csv").content.decode("utf-8")
        assert "01KG777AAA" in text
        assert "СВХ «Кызыл-Кыя»" in text
        assert "Весы SCS-80" in text
        assert "Взвешивание" in text

    def test_filters_apply(self, panel_env: PanelEnv) -> None:
        """Выгрузка идёт под теми же фильтрами, что и экран."""
        _login(panel_env)
        text = panel_env.client.get("/panel/journal/export.csv?vehicle=28BAHE").content.decode(
            "utf-8"
        )
        assert "28BAHE03KG" in text
        assert "01KG777AAA" not in text

    def test_scope_applies(self, panel_env: PanelEnv) -> None:
        """Ограниченный объектом пользователь не выгрузит чужие записи."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        text = panel_env.client.get("/panel/journal/export.csv").content.decode("utf-8")
        assert "01KG777AAA" in text
        assert "28BAHE03KG" not in text

    def test_requires_login(self, panel_env: PanelEnv) -> None:
        """Без входа выгрузка недоступна."""
        response = panel_env.client.get("/panel/journal/export.csv", follow_redirects=False)
        assert response.status_code == 303

    def test_formula_injection_neutralised(self, panel_env: PanelEnv) -> None:
        """Номер вида «=1+1» не должен вычисляться при открытии в Excel.

        Номера приходят из АИС и ручного ввода оператора, поэтому значение
        экранируется апострофом (находка ревью 11.08.2026).
        """
        _login(panel_env)
        with panel_env.factory() as session:
            repo.save_weighing_record(
                session, panel_env.scale_id, _make_record(vehicle_number="=1+1")
            )
        text = panel_env.client.get("/panel/journal/export.csv").content.decode("utf-8")
        assert ";'=1+1;" in text
        assert ";=1+1;" not in text

    def test_truncation_is_announced(self, panel_env: PanelEnv, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Обрезанная по потолку выгрузка честно говорит об этом в файле."""
        import center.web.router as router_module

        monkeypatch.setattr(router_module, "EXPORT_LIMIT", 1)
        _login(panel_env)
        text = panel_env.client.get("/panel/journal/export.csv").content.decode("utf-8")
        assert "ВНИМАНИЕ" in text and "сузьте фильтры" in text


class TestTaresPagination:
    def test_pagination_block_appears(self, panel_env: PanelEnv) -> None:
        """При числе тар больше страницы появляется переключатель страниц.

        Раньше маршрут считал pages, но в шаблоне блока не было — хвост
        реестра был недостижим (находка ревью 11.08.2026).
        """
        _login(panel_env)
        with panel_env.factory() as session:
            scale_id = panel_env.scale_id
            for i in range(55):
                taring = _make_taring(
                    vehicle_number=f"01KG{i:03d}PG",
                    weighed_at=datetime.now(UTC) - timedelta(hours=i + 1),
                )
                repo.save_weighing_record(session, scale_id, taring)

        page1 = panel_env.client.get("/panel/tares").text
        assert 'href="/panel/tares?page=2' in page1, "нет перехода на вторую страницу"
        page2 = panel_env.client.get("/panel/tares?page=2").text
        assert page2 != page1
        # хвост реестра доступен: на второй странице есть записи
        assert "Действует" in page2


# ---------------------------------------------------------------------------
# Журнал агента в панели (удалённая диагностика, 11.08.2026)
# ---------------------------------------------------------------------------


class _LogLink:
    """Фейковое соединение агента: отвечает на запрос журнала."""

    def __init__(self, hub: AgentHub, scale_id: int, lines: list[str]) -> None:
        self._hub = hub
        self._scale_id = scale_id
        self._lines = lines
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)
        request = parse_center_message(data)
        assert isinstance(request, LogTailRequest)
        self._hub.resolve_log_tail(
            LogTailResponse(
                request_id=request.request_id,
                agent_id="agent-1",
                lines=self._lines,
                location="C:/vesy-agent/logs/agent.log",
            ),
            scale_id=self._scale_id,
        )


def _set_agent_version(env: PanelEnv, version: str) -> None:
    with env.factory() as session:
        agent = session.execute(select(Agent).where(Agent.scale_id == env.scale_id)).scalar_one()
        agent.version = version
        session.commit()


class TestAgentLogPage:
    def test_admin_sees_log_lines(self, panel_env: PanelEnv) -> None:
        """Админ открывает страницу — центр спрашивает агента и показывает строки."""
        _login(panel_env)
        _make_admin(panel_env)
        _set_agent_version(panel_env, "0.4.5")
        panel_env.hub.attach(
            panel_env.scale_id, _LogLink(panel_env.hub, panel_env.scale_id, ["строка журнала"])
        )
        page = panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log").text
        assert "строка журнала" in page
        assert "agent.log" in page

    def test_log_content_escaped(self, panel_env: PanelEnv) -> None:
        """Лог — данные: разметка из строк не исполняется."""
        _login(panel_env)
        _make_admin(panel_env)
        _set_agent_version(panel_env, "0.4.5")
        panel_env.hub.attach(
            panel_env.scale_id,
            _LogLink(panel_env.hub, panel_env.scale_id, ["<script>alert(1)</script>"]),
        )
        page = panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log").text
        assert "<script>alert(1)</script>" not in page
        assert "&lt;script&gt;" in page

    def test_view_is_audited(self, panel_env: PanelEnv) -> None:
        """Каждый просмотр журнала объекта попадает в аудит — по логину."""
        _login(panel_env)
        _make_admin(panel_env)
        _set_agent_version(panel_env, "0.4.5")
        panel_env.hub.attach(
            panel_env.scale_id, _LogLink(panel_env.hub, panel_env.scale_id, ["строка"])
        )
        panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log")
        with panel_env.factory() as session:
            entry = session.execute(
                select(AuditLog).where(AuditLog.action == "agent_log_view")
            ).scalar_one()
        assert entry.actor == f"panel:{PANEL_LOGIN}", "в аудите должен быть логин"
        assert entry.details == {"scale_id": panel_env.scale_id}

    def test_dispatcher_forbidden(self, panel_env: PanelEnv) -> None:
        """Диспетчеру журнал агента недоступен (403)."""
        _login(panel_env)
        response = panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log")
        assert response.status_code == 403

    def test_old_agent_explained(self, panel_env: PanelEnv) -> None:
        """Агент старой версии запроса не получает — панель объясняет почему."""
        _login(panel_env)
        _make_admin(panel_env)
        _set_agent_version(panel_env, "0.4.4")
        link = _LogLink(panel_env.hub, panel_env.scale_id, ["не должно уехать"])
        panel_env.hub.attach(panel_env.scale_id, link)
        page = panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log").text
        assert "0.4.5" in page and "обновите" in page.lower()
        assert link.sent == [], "старому агенту ушёл запрос журнала"

    def test_offline_agent_explained(self, panel_env: PanelEnv) -> None:
        """Агент не в сети — понятное объяснение вместо ожидания."""
        _login(panel_env)
        _make_admin(panel_env)
        _set_agent_version(panel_env, "0.4.5")
        page = panel_env.client.get(f"/panel/scales/{panel_env.scale_id}/log").text
        assert "не в сети" in page

    def test_unknown_scale_404(self, panel_env: PanelEnv) -> None:
        """Несуществующие весы — 404."""
        _login(panel_env)
        _make_admin(panel_env)
        assert panel_env.client.get("/panel/scales/9999/log").status_code == 404


class TestUpdateAgentPermissions:
    def test_dispatcher_cannot_update_agent(self, panel_env: PanelEnv) -> None:
        """Обновление агента — сервисное действие с простоем весов: только админ.

        Раньше маршрут требовал лишь входа в панель, и диспетчер чужого
        объекта мог перезапустить службу подбором scale_id (находка ревью).
        """
        _login(panel_env)
        response = panel_env.client.post(
            f"/panel/scales/{panel_env.scale_id}/update-agent", follow_redirects=False
        )
        assert response.status_code == 403

    def test_admin_can_update_agent(self, panel_env: PanelEnv) -> None:
        """Админу маршрут доступен (релизов нет — вернётся заметка)."""
        _login(panel_env)
        _make_admin(panel_env)
        response = panel_env.client.post(
            f"/panel/scales/{panel_env.scale_id}/update-agent", follow_redirects=False
        )
        assert response.status_code == 303


class TestDisabledUserLosesAccess:
    def test_disabling_user_does_not_widen_visibility(self, panel_env: PanelEnv) -> None:
        """Отключение учётки при живой сессии закрывает панель, а НЕ открывает
        её целиком (находка ревью 11.08.2026: fail-open в правах)."""
        _login(panel_env)
        _bind_user_to_site(panel_env, "kyzyl-kyia")
        assert "КАНТ" not in panel_env.client.get("/panel/").text
        with panel_env.factory() as session:
            user = session.execute(select(User).where(User.login == PANEL_LOGIN)).scalar_one()
            user.is_active = False
            session.commit()
        response = panel_env.client.get("/panel/", follow_redirects=False)
        assert response.status_code == 303
        export = panel_env.client.get("/panel/journal/export.csv", follow_redirects=False)
        assert export.status_code == 303


class TestPanelPhotoScope:
    def test_foreign_photo_not_served(self, panel_env: PanelEnv) -> None:
        """Снимок чужого объекта не отдаётся даже по прямому пути."""
        _login(panel_env)
        with panel_env.factory() as session:
            path = session.execute(select(WeighingPhoto.path)).scalars().first()
        assert path is not None
        file = panel_env.photos_dir / path.lstrip("/")
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b"\xff\xd8jpeg\xff\xd9")

        thumb = path.replace(".jpeg", "_thumb.jpeg")
        thumb_file = panel_env.photos_dir / thumb.lstrip("/")
        thumb_file.write_bytes(b"\xff\xd8thumb\xff\xd9")

        assert panel_env.client.get(f"/panel/photos{path}").status_code == 200
        _bind_user_to_site(panel_env, "kyzyl-kyia")  # свой объект: и кадр, и миниатюра
        assert panel_env.client.get(f"/panel/photos{path}").status_code == 200
        assert panel_env.client.get(f"/panel/photos{thumb}").status_code == 200, (
            "миниатюра своего объекта не отдалась — в списках были бы пустые квадраты"
        )
        _bind_user_to_site(panel_env, "kant")  # объект, которому снимок не принадлежит
        assert panel_env.client.get(f"/panel/photos{path}").status_code == 404
        assert panel_env.client.get(f"/panel/photos{thumb}").status_code == 404
