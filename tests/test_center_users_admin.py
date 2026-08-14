"""Тесты экрана администрирования пользователей панели (center/web/users_admin).

Покрытие:
- users_list: порядок (активные сверху, затем по логину), подтянутый объект;
- create_user: успех (хеш через shared.passwords), дубль логина, кривой логин
  (пустой/пробелы/длиннее 64), короткий пароль, запрет admin/admin,
  несуществующий site_id;
- update_user: смена роли/ФИО/объекта, сброс объекта, защита последнего
  активного администратора от разжалования, отсутствующий пользователь;
- set_password: смена пароля (старый перестаёт подходить), короткий пароль;
- toggle_active: отключение/включение, запрет самоотключения и отключения
  последнего активного админа, отключённый не проходит verify_user;
- is_active_admin: актуальная проверка прав по БД;
- маршруты /panel/users*: 303 без сессии, 403 для диспетчера и для
  разжалованного при живой сессии, страница и мутации для админа,
  вкладка «Пользователи» только у роли admin;
- рассылка операторов агентам (_push_operators): персональные снимки после
  каждой успешной мутации create/edit/password/toggle, тишина при ошибке;
- блок «Учётки на агентах»: снимки operators_report на странице, фильтры
  экрана действуют и на блок (роль и «— все —» неприменимы), перехват
  местной учётки кнопкой (block_agent_operator + POST agent-block) с
  защитой легитимных логинов.

Инфраструктура БД — по образцу tests/test_center_panel.py: одноразовая БД
ves_test_users_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, select, text, update
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool
from starlette.middleware.sessions import SessionMiddleware

from center.agents_ws.hub import AgentHub
from center.db.models import AgentOperator, Scale, ScaleKind, Site, User, UserRole
from center.db.session import database_url, make_session_factory
from center.web import queries, users_admin
from center.web.router import create_panel_router
from shared.passwords import hash_password, verify_password
from tests.test_center_db import ALL_TABLES, _upgrade_head

ADMIN_LOGIN = "chief"
ADMIN_PASSWORD = "admin-pass-123"
DISPATCHER_LOGIN = "dispatcher"
DISPATCHER_PASSWORD = "disp-pass-123"


# ---------------------------------------------------------------------------
# Инфраструктура: временная БД + миграции
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def users_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_users_<pid>; имя не пересекается с другими
    модулями тестов, чтобы не мешать им в одном прогоне."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты администрирования пользователей пропущены"
        )

    db_name = f"ves_test_users_{os.getpid()}"
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
def users_db_engine(users_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(users_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def db(users_db_engine: Engine) -> Iterator[sessionmaker[Session]]:
    """Чистая БД; отдаёт фабрику сессий."""
    with users_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    yield make_session_factory(users_db_engine)


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
    login: str,
    password: str = "default-pass-1",
    *,
    role: UserRole = UserRole.DISPATCHER,
    site_id: int | None = None,
    is_active: bool = True,
    full_name: str = "",
) -> User:
    user = User(
        login=login,
        pw_hash=hash_password(password),
        full_name=full_name,
        role=role,
        site_id=site_id,
        is_active=is_active,
    )
    session.add(user)
    session.commit()
    return user


def _add_site(session: Session, code: str = "kant", name: str = "СВХ «КАНТ»") -> Site:
    site = Site(code=code, name=name)
    session.add(site)
    session.commit()
    return site


def _get_user(session: Session, login: str) -> User:
    return session.execute(select(User).where(User.login == login)).scalar_one()


# ---------------------------------------------------------------------------
# users_list
# ---------------------------------------------------------------------------


class TestUsersList:
    def test_active_first_then_by_login(self, db_session: Session) -> None:
        """Активные сверху (в алфавите логинов), отключённые ниже."""
        _add_user(db_session, "zoya")
        _add_user(db_session, "anna", is_active=False)
        _add_user(db_session, "boris")
        rows = users_admin.users_list(db_session)
        assert [user.login for user, _ in rows] == ["boris", "zoya", "anna"]

    def test_site_joined(self, db_session: Session) -> None:
        """Объект пользователя подтянут; без объекта — None (не падение)."""
        site = _add_site(db_session)
        _add_user(db_session, "with.site", site_id=site.id)
        _add_user(db_session, "without.site")
        rows = {user.login: joined for user, joined in users_admin.users_list(db_session)}
        assert rows["with.site"] is not None
        assert rows["with.site"].code == "kant"
        assert rows["without.site"] is None


class TestUsersListFilters:
    """Фильтры списка учёток (запрос Игоря 11.08.2026)."""

    def _seed(self, session: Session) -> Site:
        site = _add_site(session)
        _add_user(session, "igor", role=UserRole.ADMIN, full_name="Игорь Петрухин")
        _add_user(
            session,
            "kyzylweight",
            role=UserRole.OPERATOR,
            site_id=site.id,
            full_name="Оператор Кызыл-Кия",
        )
        _add_user(session, "d.ivanov", full_name="Иванов Д.", is_active=False)
        return site

    def _logins(self, rows: list[tuple[User, Site | None]]) -> list[str]:
        return [user.login for user, _ in rows]

    def test_search_matches_login_and_name_case_insensitive(self, db_session: Session) -> None:
        """Подстрока без учёта регистра ищет и в логине, и в ФИО."""
        self._seed(db_session)
        assert self._logins(users_admin.users_list(db_session, search="KYZYL")) == ["kyzylweight"]
        assert self._logins(users_admin.users_list(db_session, search="петрухин")) == ["igor"]
        assert self._logins(users_admin.users_list(db_session, search="  weight ")) == [
            "kyzylweight"
        ]

    def test_filter_by_role(self, db_session: Session) -> None:
        self._seed(db_session)
        assert self._logins(users_admin.users_list(db_session, role=UserRole.OPERATOR)) == [
            "kyzylweight"
        ]

    def test_filter_by_site_and_without_site(self, db_session: Session) -> None:
        """Конкретный объект и «без привязки» (все объекты) — разные фильтры."""
        site = self._seed(db_session)
        assert self._logins(users_admin.users_list(db_session, site_id=site.id)) == ["kyzylweight"]
        assert self._logins(users_admin.users_list(db_session, without_site=True)) == [
            "igor",
            "d.ivanov",
        ]

    def test_filter_by_status(self, db_session: Session) -> None:
        self._seed(db_session)
        assert self._logins(users_admin.users_list(db_session, active=False)) == ["d.ivanov"]
        assert "d.ivanov" not in self._logins(users_admin.users_list(db_session, active=True))

    def test_search_escapes_like_wildcards(self, db_session: Session) -> None:
        """'_' и '%' ищутся литерально: '_' — допустимый символ логина,
        а '%' не должен возвращать всех."""
        self._seed(db_session)
        _add_user(db_session, "d_underscore", full_name="Подчёркнутый")
        assert self._logins(users_admin.users_list(db_session, search="d_und")) == ["d_underscore"]
        # 'd_' литерально не совпадает с 'd.' в d.ivanov
        assert "d.ivanov" not in self._logins(users_admin.users_list(db_session, search="d_"))
        assert users_admin.users_list(db_session, search="%") == []

    def test_filters_combine(self, db_session: Session) -> None:
        """Фильтры складываются: активный оператор объекта + поиск."""
        site = self._seed(db_session)
        rows = users_admin.users_list(
            db_session, search="кызыл", role=UserRole.OPERATOR, site_id=site.id, active=True
        )
        assert self._logins(rows) == ["kyzylweight"]
        assert users_admin.users_list(db_session, search="кызыл", role=UserRole.ADMIN) == []


# ---------------------------------------------------------------------------
# create_user
# ---------------------------------------------------------------------------


class TestCreateUser:
    def test_success_hashes_password(self, db_session: Session) -> None:
        """Успех: None, в БД bcrypt-хеш (не сырой пароль), поля сохранены."""
        site = _add_site(db_session)
        error = users_admin.create_user(
            db_session,
            login="  d.ivanov  ",
            password="strong-pass-9",
            full_name="  Иванов Д.  ",
            role=UserRole.OPERATOR,
            site_id=site.id,
        )
        assert error is None
        user = _get_user(db_session, "d.ivanov")
        assert user.pw_hash != "strong-pass-9", "пароль сохранён открытым текстом"
        assert verify_password("strong-pass-9", user.pw_hash)
        assert user.full_name == "Иванов Д."
        assert user.role is UserRole.OPERATOR
        assert user.site_id == site.id
        assert user.is_active

    def test_duplicate_login_rejected(self, db_session: Session) -> None:
        """Повтор логина → ошибка, второй пользователь не создан."""
        _add_user(db_session, "d.ivanov")
        error = users_admin.create_user(
            db_session,
            login="d.ivanov",
            password="strong-pass-9",
            full_name="Дубль",
            role=UserRole.DISPATCHER,
            site_id=None,
        )
        assert error is not None
        assert "занят" in error
        count = len(db_session.execute(select(User)).scalars().all())
        assert count == 1

    @pytest.mark.parametrize(
        "login",
        ["", "   ", "два слова", "с\tтабом", "x" * 65, "x');alert(1)//", "кириллица"],
        ids=["empty", "spaces-only", "inner-space", "tab", "too-long", "js-quote", "cyrillic"],
    )
    def test_bad_login_rejected(self, db_session: Session, login: str) -> None:
        """Логин вне белого списка [a-zA-Z0-9._-]{1,64} отклоняется, БД не
        тронута (кавычки в логине — вектор инъекции в разметку панели)."""
        error = users_admin.create_user(
            db_session,
            login=login,
            password="strong-pass-9",
            full_name="",
            role=UserRole.DISPATCHER,
            site_id=None,
        )
        assert error is not None
        assert db_session.execute(select(User)).scalar_one_or_none() is None

    def test_short_password_rejected(self, db_session: Session) -> None:
        """Пароль из 7 символов → ошибка «короче 8»."""
        error = users_admin.create_user(
            db_session,
            login="d.ivanov",
            password="1234567",
            full_name="",
            role=UserRole.DISPATCHER,
            site_id=None,
        )
        assert error is not None
        assert "короче 8" in error
        assert db_session.execute(select(User)).scalar_one_or_none() is None

    def test_admin_admin_forbidden(self, db_session: Session) -> None:
        """Пара admin/admin запрещена правилом проекта №7 (даже с пробелами
        вокруг логина — он нормализуется до проверки)."""
        error = users_admin.create_user(
            db_session,
            login=" admin ",
            password="admin",
            full_name="",
            role=UserRole.ADMIN,
            site_id=None,
        )
        assert error is not None
        assert "admin/admin" in error
        assert db_session.execute(select(User)).scalar_one_or_none() is None

    def test_unknown_site_rejected(self, db_session: Session) -> None:
        """Несуществующий site_id → «объект не найден», без IntegrityError."""
        error = users_admin.create_user(
            db_session,
            login="d.ivanov",
            password="strong-pass-9",
            full_name="",
            role=UserRole.DISPATCHER,
            site_id=987654,
        )
        assert error == "объект не найден"
        assert db_session.execute(select(User)).scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# update_user
# ---------------------------------------------------------------------------


class TestUpdateUser:
    def test_change_role_name_site(self, db_session: Session) -> None:
        """Смена ФИО/роли/объекта проходит одной операцией."""
        site = _add_site(db_session)
        user = _add_user(db_session, "d.ivanov", full_name="Старое имя")
        error = users_admin.update_user(
            db_session,
            user.id,
            full_name="  Новое имя  ",
            role=UserRole.OPERATOR,
            site_id=site.id,
        )
        assert error is None
        db_session.refresh(user)
        assert user.full_name == "Новое имя"
        assert user.role is UserRole.OPERATOR
        assert user.site_id == site.id

    def test_reset_site_to_none(self, db_session: Session) -> None:
        """site_id=None отвязывает пользователя от объекта."""
        site = _add_site(db_session)
        user = _add_user(db_session, "d.ivanov", site_id=site.id)
        error = users_admin.update_user(
            db_session, user.id, full_name="", role=UserRole.DISPATCHER, site_id=None
        )
        assert error is None
        db_session.refresh(user)
        assert user.site_id is None

    def test_demote_last_active_admin_blocked(self, db_session: Session) -> None:
        """Единственного активного админа нельзя разжаловать (защита от
        локаута); отключённый второй админ не считается."""
        admin = _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        _add_user(db_session, "old.admin", role=UserRole.ADMIN, is_active=False)
        error = users_admin.update_user(
            db_session, admin.id, full_name="", role=UserRole.DISPATCHER, site_id=None
        )
        assert error is not None
        assert "последнего" in error
        db_session.refresh(admin)
        assert admin.role is UserRole.ADMIN, "роль всё же снята"

    def test_demote_allowed_with_second_active_admin(self, db_session: Session) -> None:
        """При втором активном админе разжалование разрешено."""
        admin = _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        _add_user(db_session, "second.admin", role=UserRole.ADMIN)
        error = users_admin.update_user(
            db_session, admin.id, full_name="", role=UserRole.DISPATCHER, site_id=None
        )
        assert error is None
        db_session.refresh(admin)
        assert admin.role is UserRole.DISPATCHER

    def test_missing_user(self, db_session: Session) -> None:
        """Несуществующий user_id → «пользователь не найден»."""
        error = users_admin.update_user(
            db_session, 987654, full_name="", role=UserRole.DISPATCHER, site_id=None
        )
        assert error == "пользователь не найден"

    def test_unknown_site_rejected(self, db_session: Session) -> None:
        """Несуществующий объект при правке → ошибка, поля не изменены."""
        user = _add_user(db_session, "d.ivanov", full_name="Как было")
        error = users_admin.update_user(
            db_session, user.id, full_name="Новое", role=UserRole.OPERATOR, site_id=987654
        )
        assert error == "объект не найден"
        db_session.refresh(user)
        assert user.full_name == "Как было"
        assert user.role is UserRole.DISPATCHER


# ---------------------------------------------------------------------------
# set_password
# ---------------------------------------------------------------------------


class TestSetPassword:
    def test_success_replaces_hash(self, db_session: Session) -> None:
        """Новый пароль подходит, старый перестаёт (и через verify_user)."""
        user = _add_user(db_session, "d.ivanov", "old-pass-123")
        error = users_admin.set_password(db_session, user.id, "new-pass-456")
        assert error is None
        db_session.refresh(user)
        assert verify_password("new-pass-456", user.pw_hash)
        assert not verify_password("old-pass-123", user.pw_hash)
        assert queries.verify_user(db_session, "d.ivanov", "new-pass-456") is not None
        assert queries.verify_user(db_session, "d.ivanov", "old-pass-123") is None

    def test_short_password_rejected(self, db_session: Session) -> None:
        """Короткий пароль → ошибка, старый хеш сохранён."""
        user = _add_user(db_session, "d.ivanov", "old-pass-123")
        old_hash = user.pw_hash
        error = users_admin.set_password(db_session, user.id, "short")
        assert error is not None
        assert "короче 8" in error
        db_session.refresh(user)
        assert user.pw_hash == old_hash

    def test_admin_admin_forbidden(self, db_session: Session) -> None:
        """Пользователю с логином admin нельзя сбросить пароль на admin."""
        user = _add_user(db_session, "admin", "old-pass-123", role=UserRole.ADMIN)
        error = users_admin.set_password(db_session, user.id, "admin")
        assert error is not None
        assert "admin/admin" in error

    def test_missing_user(self, db_session: Session) -> None:
        """Несуществующий user_id → «пользователь не найден»."""
        assert users_admin.set_password(db_session, 987654, "new-pass-456") == (
            "пользователь не найден"
        )


# ---------------------------------------------------------------------------
# toggle_active
# ---------------------------------------------------------------------------


class TestToggleActive:
    def test_disable_then_enable(self, db_session: Session) -> None:
        """Отключение и повторное включение диспетчера админом."""
        user = _add_user(db_session, DISPATCHER_LOGIN)
        assert users_admin.toggle_active(db_session, user.id, actor_login=ADMIN_LOGIN) is None
        db_session.refresh(user)
        assert not user.is_active
        assert users_admin.toggle_active(db_session, user.id, actor_login=ADMIN_LOGIN) is None
        db_session.refresh(user)
        assert user.is_active

    def test_self_disable_blocked(self, db_session: Session) -> None:
        """Актор не может отключить сам себя (даже при втором админе)."""
        admin = _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        _add_user(db_session, "second.admin", role=UserRole.ADMIN)
        error = users_admin.toggle_active(db_session, admin.id, actor_login=ADMIN_LOGIN)
        assert error is not None
        assert "самого себя" in error
        db_session.refresh(admin)
        assert admin.is_active

    def test_last_active_admin_blocked(self, db_session: Session) -> None:
        """Последнего активного админа не отключить даже другим актором."""
        admin = _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        _add_user(db_session, DISPATCHER_LOGIN)
        error = users_admin.toggle_active(db_session, admin.id, actor_login=DISPATCHER_LOGIN)
        assert error is not None
        assert "последнего" in error
        db_session.refresh(admin)
        assert admin.is_active

    def test_admin_disable_allowed_with_second_active(self, db_session: Session) -> None:
        """При двух активных админах одного можно отключить."""
        _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        second = _add_user(db_session, "second.admin", role=UserRole.ADMIN)
        error = users_admin.toggle_active(db_session, second.id, actor_login=ADMIN_LOGIN)
        assert error is None
        db_session.refresh(second)
        assert not second.is_active

    def test_disabled_user_fails_verify_user(self, db_session: Session) -> None:
        """Отключённая учётка сразу теряет вход в панель."""
        user = _add_user(db_session, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        assert queries.verify_user(db_session, DISPATCHER_LOGIN, DISPATCHER_PASSWORD) is not None
        assert users_admin.toggle_active(db_session, user.id, actor_login=ADMIN_LOGIN) is None
        assert queries.verify_user(db_session, DISPATCHER_LOGIN, DISPATCHER_PASSWORD) is None

    def test_missing_user(self, db_session: Session) -> None:
        """Несуществующий user_id → «пользователь не найден»."""
        assert users_admin.toggle_active(db_session, 987654, actor_login=ADMIN_LOGIN) == (
            "пользователь не найден"
        )


# ---------------------------------------------------------------------------
# is_active_admin
# ---------------------------------------------------------------------------


class TestIsActiveAdmin:
    def test_matrix(self, db_session: Session) -> None:
        """True только для активного админа; отключённый/диспетчер/чужой — нет."""
        _add_user(db_session, ADMIN_LOGIN, role=UserRole.ADMIN)
        _add_user(db_session, "off.admin", role=UserRole.ADMIN, is_active=False)
        _add_user(db_session, DISPATCHER_LOGIN)
        assert users_admin.is_active_admin(db_session, ADMIN_LOGIN)
        assert not users_admin.is_active_admin(db_session, "off.admin")
        assert not users_admin.is_active_admin(db_session, DISPATCHER_LOGIN)
        assert not users_admin.is_active_admin(db_session, "nobody")


# ---------------------------------------------------------------------------
# Маршруты /panel/users* (TestClient)
# ---------------------------------------------------------------------------


@dataclass
class UsersEnv:
    """Окружение маршрутных тестов: клиент, фабрика сессий, id пользователей."""

    client: TestClient
    factory: sessionmaker[Session]
    admin_id: int
    dispatcher_id: int


@pytest.fixture
def users_env(db: sessionmaker[Session], tmp_path: Path) -> Iterator[UsersEnv]:
    """Приложение как в center/app.py: SessionMiddleware + create_panel_router;
    посев — активный админ и диспетчер."""
    with db() as session:
        admin = _add_user(
            session, ADMIN_LOGIN, ADMIN_PASSWORD, role=UserRole.ADMIN, full_name="Главный Админ"
        )
        dispatcher = _add_user(
            session, DISPATCHER_LOGIN, DISPATCHER_PASSWORD, full_name="Айгуль Диспетчер"
        )
        admin_id, dispatcher_id = admin.id, dispatcher.id

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-secret", session_cookie="ves_test")
    app.include_router(create_panel_router(db, AgentHub(), photos_dir=tmp_path))
    client = TestClient(app)
    yield UsersEnv(client, db, admin_id, dispatcher_id)
    client.close()


def _login(env: UsersEnv, login: str, password: str) -> None:
    response = env.client.post(
        "/panel/login", data={"login": login, "password": password}, follow_redirects=False
    )
    assert response.status_code == 303, "вход не удался"


def _note_from(response: object) -> str:
    """Флеш-заметка из Location редиректа на /panel/users?note=..."""
    location = response.headers["location"]  # type: ignore[attr-defined]
    parts = urlsplit(location)
    assert parts.path == "/panel/users", f"редирект не на экран пользователей: {location}"
    notes = parse_qs(parts.query).get("note", [])
    assert notes, f"note отсутствует в редиректе: {location}"
    return unquote(notes[0])


USERS_MUTATIONS = [
    ("/panel/users/create", {"login": "x", "password": "strong-pass-9"}),
    ("/panel/users/1/edit", {"full_name": "", "role": "dispatcher", "site_id": ""}),
    ("/panel/users/1/password", {"password": "strong-pass-9"}),
    ("/panel/users/1/toggle", {}),
    ("/panel/users/agent-block", {"scale_id": "1", "login": "x.y"}),
]


class TestUsersRoutesAccess:
    def test_get_without_session_redirects(self, users_env: UsersEnv) -> None:
        """GET /panel/users без сессии → 303 на форму входа."""
        response = users_env.client.get("/panel/users", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    @pytest.mark.parametrize(("path", "data"), USERS_MUTATIONS)
    def test_post_without_session_redirects(
        self, users_env: UsersEnv, path: str, data: dict[str, str]
    ) -> None:
        """POST-мутации без сессии → 303 на форму входа, не 500."""
        response = users_env.client.post(path, data=data, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    def test_dispatcher_gets_403_on_page(self, users_env: UsersEnv) -> None:
        """Диспетчер с живой сессией получает 403 на экран пользователей."""
        _login(users_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        response = users_env.client.get("/panel/users", follow_redirects=False)
        assert response.status_code == 403

    @pytest.mark.parametrize(("path", "data"), USERS_MUTATIONS)
    def test_dispatcher_gets_403_on_mutations(
        self, users_env: UsersEnv, path: str, data: dict[str, str]
    ) -> None:
        """Диспетчер не может выполнять POST-мутации (403, БД не тронута)."""
        _login(users_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        response = users_env.client.post(path, data=data, follow_redirects=False)
        assert response.status_code == 403

    def test_demoted_admin_loses_access_with_live_session(self, users_env: UsersEnv) -> None:
        """Права проверяются по БД: разжалованный при живой сессии → 403."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        assert users_env.client.get("/panel/users").status_code == 200
        with users_env.factory() as session:
            session.execute(
                update(User).where(User.id == users_env.admin_id).values(role=UserRole.DISPATCHER)
            )
            session.commit()
        response = users_env.client.get("/panel/users", follow_redirects=False)
        assert response.status_code == 403, "разжалованный админ сохранил экран по сессии"


class TestBlockAgentOperator:
    """Перехват местной учётки центром — кнопка «Заблокировать» блока
    «Учётки на агентах» (запрос Игоря 14.08.2026)."""

    def _seed_scale(self, env: UsersEnv) -> int:
        with env.factory() as session:
            site = Site(code="kyzyl-kyia", name="СВХ «Кызыл-Кыя»")
            session.add(site)
            session.flush()
            scale = Scale(
                site_id=site.id, name="Весы SCS-80", kind=ScaleKind.STATIC, driver="cas22"
            )
            session.add(scale)
            session.commit()
            return scale.id

    def test_creates_disabled_double_bound_to_site(self, users_env: UsersEnv) -> None:
        """Двойник: роль оператор, отключён, привязан к объекту весов."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            assert users_admin.block_agent_operator(session, scale_id, "local.backdoor") is None
            user = session.execute(select(User).where(User.login == "local.backdoor")).scalar_one()
            scale = session.get(Scale, scale_id)
            assert scale is not None
            assert user.role is UserRole.OPERATOR
            assert user.is_active is False
            assert user.site_id == scale.site_id
            assert user.full_name == "Блокировка местной учётки"

    def test_repeat_block_does_not_duplicate(self, users_env: UsersEnv) -> None:
        """Повторный перехват того же логина не плодит двойников."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            assert users_admin.block_agent_operator(session, scale_id, "local.backdoor") is None
            assert users_admin.block_agent_operator(session, scale_id, "local.backdoor") is None
            doubles = (
                session.execute(select(User).where(User.login == "local.backdoor")).scalars().all()
            )
            assert len(doubles) == 1

    def test_panel_user_login_not_touched(self, users_env: UsersEnv) -> None:
        """Логин пользователя панели перехватить нельзя — и он не отключается."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            error = users_admin.block_agent_operator(session, scale_id, ADMIN_LOGIN)
            assert error is not None and "занят пользователем панели" in error
            admin = session.execute(select(User).where(User.login == ADMIN_LOGIN)).scalar_one()
            assert admin.is_active

    def test_operator_of_other_site_not_touched(self, users_env: UsersEnv) -> None:
        """Логин оператора другого объекта не перехватывается."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            other = Site(code="kant", name="СВХ «КАНТ»")
            session.add(other)
            session.flush()
            session.add(
                User(
                    login="op.kant",
                    pw_hash=hash_password("op-pass-123"),
                    role=UserRole.OPERATOR,
                    site_id=other.id,
                )
            )
            session.commit()
            error = users_admin.block_agent_operator(session, scale_id, "op.kant")
            assert error is not None and "другого объекта" in error
            user = session.execute(select(User).where(User.login == "op.kant")).scalar_one()
            assert user.is_active

    def test_active_operator_of_same_site_kept_active(self, users_env: UsersEnv) -> None:
        """Действующий оператор объекта кнопкой НЕ отключается: свежий снимок
        сам вернёт центровую учётку поверх местной копии."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            scale = session.get(Scale, scale_id)
            assert scale is not None
            session.add(
                User(
                    login="op.local",
                    pw_hash=hash_password("op-pass-123"),
                    role=UserRole.OPERATOR,
                    site_id=scale.site_id,
                )
            )
            session.commit()
            assert users_admin.block_agent_operator(session, scale_id, "op.local") is None
            user = session.execute(select(User).where(User.login == "op.local")).scalar_one()
            assert user.is_active, "боевой оператор не должен гаснуть кнопкой перехвата"

    def test_invalid_login_and_missing_scale_rejected(self, users_env: UsersEnv) -> None:
        """Логин вне белого списка и несуществующие весы — внятные ошибки."""
        scale_id = self._seed_scale(users_env)
        with users_env.factory() as session:
            error = users_admin.block_agent_operator(session, scale_id, "весовщик")
            assert error is not None and "правила центра" in error
            assert users_admin.block_agent_operator(session, 99999, "x.y") == "весы не найдены"

    def test_route_creates_double_and_notes(self, users_env: UsersEnv) -> None:
        """POST agent-block: 303 с заметкой, двойник в БД отключён.

        Хаб тестов пуст — агент «не на связи»: заметка честно говорит,
        что реплика доедет при подключении (замечание ревью — не обещать
        доставку офлайн-агенту)."""
        scale_id = self._seed_scale(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            "/panel/users/agent-block",
            data={"scale_id": str(scale_id), "login": "local.backdoor"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        note = _note_from(response)
        assert "готово" in note and "перекрыта" in note
        assert "не на связи" in note
        with users_env.factory() as session:
            user = session.execute(select(User).where(User.login == "local.backdoor")).scalar_one()
            assert not user.is_active

    def test_route_error_becomes_note(self, users_env: UsersEnv) -> None:
        """Ошибка перехвата уезжает флеш-заметкой, а не 500."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            "/panel/users/agent-block",
            data={"scale_id": "99999", "login": "x.y"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "перехват не выполнен" in _note_from(response)


class TestUsersAgentOperatorsFilters:
    """Фильтры экрана действуют и на блок «Учётки на агентах»
    (пожелание Игоря 14.08.2026, второй виток)."""

    def _seed_two_sites(self, env: UsersEnv) -> None:
        with env.factory() as session:
            now = datetime.now(UTC)
            for code, name, scale_name, login, full_name, active in [
                ("kyzyl-kyia", "СВХ «Кызыл-Кыя»", "Весы SCS-80", "kk.op", "Оператор КК", True),
                ("kant", "СВХ «КАНТ»", "Весы 22-3", "kant.op", "Оператор Канта", False),
            ]:
                site = Site(code=code, name=name)
                session.add(site)
                session.flush()
                scale = Scale(
                    site_id=site.id, name=scale_name, kind=ScaleKind.STATIC, driver="cas22"
                )
                session.add(scale)
                session.flush()
                session.add(
                    AgentOperator(
                        scale_id=scale.id,
                        login=login,
                        full_name=full_name,
                        is_active=active,
                        from_center=False,
                        reported_at=now,
                    )
                )
            session.commit()

    def test_search_narrows_block(self, users_env: UsersEnv) -> None:
        self._seed_two_sites(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users", params={"search": "kant.op"}).text
        assert "kant.op" in page
        assert "kk.op" not in page
        assert "Учётки на агентах (1)" in page

    def test_site_filter_narrows_block(self, users_env: UsersEnv) -> None:
        self._seed_two_sites(users_env)
        with users_env.factory() as session:
            kant_id = session.execute(select(Site.id).where(Site.code == "kant")).scalar_one()
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users", params={"site": str(kant_id)}).text
        assert "kant.op" in page
        assert "kk.op" not in page

    def test_status_filter_applies_to_block(self, users_env: UsersEnv) -> None:
        self._seed_two_sites(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users", params={"status": "disabled"}).text
        assert "kant.op" in page
        assert "kk.op" not in page

    def test_role_and_no_site_do_not_hide_block(self, users_env: UsersEnv) -> None:
        """Роль и «— все —» к учёткам агентов неприменимы: блок показывается
        целиком, а не пустеет."""
        self._seed_two_sites(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get(
            "/panel/users", params={"role": "dispatcher", "site": "none"}
        ).text
        assert "kk.op" in page
        assert "kant.op" in page

    def test_filtered_empty_state(self, users_env: UsersEnv) -> None:
        """Пустой результат фильтра объясняется фильтром, а не «агенты
        не прислали» (снимки-то есть)."""
        self._seed_two_sites(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users", params={"search": "nomatch"}).text
        assert "Под фильтры не попала ни одна учётка" in page
        assert "Агенты ещё не прислали" not in page


class TestUsersAgentOperatorsBlock:
    """Блок «Учётки на агентах» на экране «Пользователи»: снимки
    operators_report, локальные учётки помечены (запрос Игоря 14.08.2026)."""

    def _seed_snapshot(self, env: UsersEnv) -> None:
        with env.factory() as session:
            site = Site(code="kyzyl-kyia", name="СВХ «Кызыл-Кыя»")
            session.add(site)
            session.flush()
            scale = Scale(
                site_id=site.id, name="Весы SCS-80", kind=ScaleKind.STATIC, driver="cas22"
            )
            session.add(scale)
            session.flush()
            now = datetime.now(UTC)
            session.add(
                AgentOperator(
                    scale_id=scale.id,
                    login="c.operator",
                    full_name="Из Центра",
                    is_active=True,
                    from_center=True,
                    reported_at=now,
                )
            )
            session.add(
                AgentOperator(
                    scale_id=scale.id,
                    login="local.backdoor",
                    is_active=True,
                    from_center=False,
                    reported_at=now,
                )
            )
            session.commit()

    def test_block_shows_local_and_center_operators(self, users_env: UsersEnv) -> None:
        """Обе учётки на странице; локальная — с пилюлей «заведена на месте»."""
        self._seed_snapshot(users_env)
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users").text
        assert "Учётки на агентах (2)" in page
        assert "local.backdoor" in page
        assert "заведена на месте" in page
        assert "c.operator" in page
        assert ">из центра</span>" in page

    def test_empty_snapshot_shows_hint(self, users_env: UsersEnv) -> None:
        """Без отчётов агентов блок объясняет, почему пуст (нужен 0.4.14)."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        page = users_env.client.get("/panel/users").text
        assert "Агенты ещё не прислали снимки учёток" in page


class TestUsersRoutesAdmin:
    def test_admin_sees_page_with_logins(self, users_env: UsersEnv) -> None:
        """Админ видит экран: оба логина и ФИО на странице."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.get("/panel/users")
        assert response.status_code == 200
        assert ADMIN_LOGIN in response.text
        assert DISPATCHER_LOGIN in response.text
        assert "Айгуль Диспетчер" in response.text

    def test_create_via_post(self, users_env: UsersEnv) -> None:
        """POST create создаёт пользователя, редиректит с note «создан»."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            "/panel/users/create",
            data={
                "login": "new.user",
                "password": "strong-pass-9",
                "full_name": "Новый Оператор",
                "role": "operator",
                "site_id": "",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "создан" in _note_from(response)
        with users_env.factory() as session:
            user = _get_user(session, "new.user")
            assert user.role is UserRole.OPERATOR
            assert verify_password("strong-pass-9", user.pw_hash)
        page = users_env.client.get("/panel/users")
        assert "new.user" in page.text

    def test_create_error_shown_as_note(self, users_env: UsersEnv) -> None:
        """Ошибка мутации не 500, а note в редиректе (короткий пароль)."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            "/panel/users/create",
            data={"login": "new.user", "password": "short"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "короче 8" in _note_from(response)
        with users_env.factory() as session:
            assert (
                session.execute(select(User).where(User.login == "new.user")).scalar_one_or_none()
                is None
            )

    def test_toggle_self_returns_error_note(self, users_env: UsersEnv) -> None:
        """Самоотключение через POST — note об ошибке, учётка активна."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            f"/panel/users/{users_env.admin_id}/toggle", follow_redirects=False
        )
        assert response.status_code == 303
        assert "самого себя" in _note_from(response)
        with users_env.factory() as session:
            assert session.get(User, users_env.admin_id).is_active  # type: ignore[union-attr]

    def test_toggle_dispatcher_via_post(self, users_env: UsersEnv) -> None:
        """Отключение диспетчера админом через POST проходит."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = users_env.client.post(
            f"/panel/users/{users_env.dispatcher_id}/toggle", follow_redirects=False
        )
        assert response.status_code == 303
        assert "изменён" in _note_from(response)
        with users_env.factory() as session:
            assert not session.get(User, users_env.dispatcher_id).is_active  # type: ignore[union-attr]

    def test_users_tab_visible_only_for_admin(self, users_env: UsersEnv) -> None:
        """Вкладка «Пользователи» в шапке есть у админа и скрыта у диспетчера."""
        _login(users_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        admin_page = users_env.client.get("/panel/")
        assert admin_page.status_code == 200
        assert "/panel/users" in admin_page.text

        users_env.client.post("/panel/logout", follow_redirects=False)
        _login(users_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        dispatcher_page = users_env.client.get("/panel/")
        assert dispatcher_page.status_code == 200
        assert "/panel/users" not in dispatcher_page.text


# ---------------------------------------------------------------------------
# Рассылка снимков операторов агентам после мутаций (_push_operators)
# ---------------------------------------------------------------------------


OPERATOR_LOGIN = "scale.operator"
OPERATOR_PASSWORD = "oper-pass-123"


class RecordingLink:
    """Фейковое соединение агента (протокол AgentLink): копит сообщения центра."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)

    def snapshots(self) -> list[dict[str, Any]]:
        """Разобранные сообщения (при мутациях ждём только operators_registry)."""
        return [json.loads(item) for item in self.sent]

    def last_logins(self) -> dict[str, dict[str, Any]]:
        """Записи последнего снимка операторов по логину."""
        snapshot = self.snapshots()[-1]
        assert snapshot["type"] == "operators_registry"
        return {record["login"]: record for record in snapshot["records"]}


@dataclass
class PushEnv:
    """Окружение тестов рассылки: панель + хаб с двумя «подключёнными»
    агентами разных объектов."""

    client: TestClient
    factory: sessionmaker[Session]
    site_id: int  # объект первых весов (к нему привязан посеянный оператор)
    other_site_id: int
    link: RecordingLink  # агент весов объекта site_id
    other_link: RecordingLink  # агент весов другого объекта
    operator_id: int


@pytest.fixture
def push_env(db: sessionmaker[Session], tmp_path: Path) -> Iterator[PushEnv]:
    """Приложение панели как в panel_env + реальный AgentHub с фейковыми
    линками, привязанными к весам двух разных объектов."""
    hub = AgentHub()
    with db() as session:
        _add_user(session, ADMIN_LOGIN, ADMIN_PASSWORD, role=UserRole.ADMIN)
        site = _add_site(session)
        other_site = _add_site(session, code="osh", name="СВХ «ОШ»")
        scale = Scale(site_id=site.id, name="Весы КАНТ", kind=ScaleKind.STATIC, driver="cas22")
        other_scale = Scale(
            site_id=other_site.id, name="Весы ОШ", kind=ScaleKind.STATIC, driver="cas22"
        )
        session.add_all([scale, other_scale])
        session.commit()
        # версии агентов ≥ 0.4.0: гейт снимков с секретами пропускает push
        from center.db import repo as center_repo
        from center.db.models import Agent

        session.add_all(
            [
                Agent(
                    scale_id=scale.id,
                    token_hash=center_repo.hash_agent_token("tok-push-a"),
                    version="0.4.0",
                ),
                Agent(
                    scale_id=other_scale.id,
                    token_hash=center_repo.hash_agent_token("tok-push-b"),
                    version="0.4.0",
                ),
            ]
        )
        session.commit()
        operator = _add_user(
            session,
            OPERATOR_LOGIN,
            OPERATOR_PASSWORD,
            role=UserRole.OPERATOR,
            site_id=site.id,
            full_name="Оператор Весов",
        )
        site_id, other_site_id = site.id, other_site.id
        scale_id, other_scale_id, operator_id = scale.id, other_scale.id, operator.id

    link, other_link = RecordingLink(), RecordingLink()
    hub.attach(scale_id, link)
    hub.attach(other_scale_id, other_link)

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-secret", session_cookie="ves_test")
    app.include_router(create_panel_router(db, hub, photos_dir=tmp_path))
    client = TestClient(app)
    yield PushEnv(client, db, site_id, other_site_id, link, other_link, operator_id)
    client.close()


def _login_push(env: PushEnv) -> None:
    response = env.client.post(
        "/panel/login",
        data={"login": ADMIN_LOGIN, "password": ADMIN_PASSWORD},
        follow_redirects=False,
    )
    assert response.status_code == 303, "вход админа не удался"


class TestOperatorsPushOnMutations:
    def test_create_operator_pushes_personal_snapshots(self, push_env: PushEnv) -> None:
        """Создание оператора объекта → каждому подключённому агенту уходит
        ЕГО персональный снимок: свой агент видит новый логин, чужой — нет."""
        _login_push(push_env)
        response = push_env.client.post(
            "/panel/users/create",
            data={
                "login": "new.oper",
                "password": "strong-pass-9",
                "full_name": "Новый Оператор",
                "role": "operator",
                "site_id": str(push_env.site_id),
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "создан" in _note_from(response)

        # оба агента получили ровно по одному снимку operators_registry
        assert len(push_env.link.sent) == 1
        assert len(push_env.other_link.sent) == 1
        own = push_env.link.last_logins()
        assert set(own) == {OPERATOR_LOGIN, "new.oper"}
        assert own["new.oper"]["full_name"] == "Новый Оператор"
        assert own["new.oper"]["is_active"] is True
        # у агента чужого объекта — свой снимок, без операторов этого объекта
        assert push_env.other_link.last_logins() == {}

    def test_create_unbound_operator_reaches_all_agents(self, push_env: PushEnv) -> None:
        """Оператор без привязки (site_id пустой) попадает в снимки ВСЕХ агентов."""
        _login_push(push_env)
        response = push_env.client.post(
            "/panel/users/create",
            data={
                "login": "roaming.oper",
                "password": "strong-pass-9",
                "role": "operator",
                "site_id": "",
            },
            follow_redirects=False,
        )
        assert "создан" in _note_from(response)
        assert "roaming.oper" in push_env.link.last_logins()
        assert "roaming.oper" in push_env.other_link.last_logins()

    def test_create_dispatcher_pushes_snapshot_without_him(self, push_env: PushEnv) -> None:
        """Мутация над не-оператором тоже рассылает снимки, но диспетчер
        в реплику операторов не попадает."""
        _login_push(push_env)
        response = push_env.client.post(
            "/panel/users/create",
            data={
                "login": "new.dispatcher",
                "password": "strong-pass-9",
                "role": "dispatcher",
                "site_id": "",
            },
            follow_redirects=False,
        )
        assert "создан" in _note_from(response)
        assert len(push_env.link.sent) == 1
        assert "new.dispatcher" not in push_env.link.last_logins()

    def test_toggle_operator_pushes_inactive(self, push_env: PushEnv) -> None:
        """Блокировка оператора → агенту сразу уходит снимок с is_active=False
        (офлайн-вход блокируется без ожидания hello)."""
        _login_push(push_env)
        response = push_env.client.post(
            f"/panel/users/{push_env.operator_id}/toggle", follow_redirects=False
        )
        assert "изменён" in _note_from(response)
        own = push_env.link.last_logins()
        assert own[OPERATOR_LOGIN]["is_active"] is False

    def test_password_change_pushes_fresh_hash(self, push_env: PushEnv) -> None:
        """Смена пароля → в снимке новый pw_hash (равный хешу из БД),
        старый пароль по реплике перестаёт подходить."""
        with push_env.factory() as session:
            old_hash = _get_user(session, OPERATOR_LOGIN).pw_hash
        _login_push(push_env)
        response = push_env.client.post(
            f"/panel/users/{push_env.operator_id}/password",
            data={"password": "new-oper-pass-77"},
            follow_redirects=False,
        )
        assert "пароль изменён" in _note_from(response)
        pushed_hash = push_env.link.last_logins()[OPERATOR_LOGIN]["pw_hash"]
        assert pushed_hash != old_hash, "в снимок ушёл старый хеш"
        with push_env.factory() as session:
            db_hash = _get_user(session, OPERATOR_LOGIN).pw_hash
        assert pushed_hash == db_hash, "хеш в снимке не совпал с хешем в БД"
        assert verify_password("new-oper-pass-77", pushed_hash)
        assert not verify_password(OPERATOR_PASSWORD, pushed_hash)

    def test_edit_moves_operator_between_sites(self, push_env: PushEnv) -> None:
        """Перевод оператора на другой объект → он исчезает из снимка
        старого агента и появляется в снимке нового."""
        _login_push(push_env)
        response = push_env.client.post(
            f"/panel/users/{push_env.operator_id}/edit",
            data={
                "full_name": "Оператор Весов",
                "role": "operator",
                "site_id": str(push_env.other_site_id),
            },
            follow_redirects=False,
        )
        assert "сохранены" in _note_from(response)
        assert OPERATOR_LOGIN not in push_env.link.last_logins()
        assert OPERATOR_LOGIN in push_env.other_link.last_logins()

    @pytest.mark.parametrize(
        ("path", "data", "note_part"),
        [
            (
                "/panel/users/create",
                {"login": OPERATOR_LOGIN, "password": "strong-pass-9", "role": "operator"},
                "занят",
            ),
            ("/panel/users/create", {"login": "x.y", "password": "short"}, "короче 8"),
            ("/panel/users/987654/toggle", {}, "не найден"),
            ("/panel/users/987654/password", {"password": "strong-pass-9"}, "не найден"),
        ],
        ids=["duplicate-login", "short-password", "toggle-missing", "password-missing"],
    )
    def test_failed_mutation_sends_nothing(
        self, push_env: PushEnv, path: str, data: dict[str, str], note_part: str
    ) -> None:
        """Неуспешная мутация (дубль логина и пр.) — рассылки НЕТ: агентам
        не должен уходить снимок, если состояние учёток не изменилось."""
        _login_push(push_env)
        response = push_env.client.post(path, data=data, follow_redirects=False)
        assert note_part in _note_from(response)
        assert push_env.link.sent == [], "рассылка ушла при неуспешной мутации"
        assert push_env.other_link.sent == []
