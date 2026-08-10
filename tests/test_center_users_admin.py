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
  вкладка «Пользователи» только у роли admin.

Инфраструктура БД — по образцу tests/test_center_panel.py: одноразовая БД
ves_test_users_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
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
from center.db.models import Site, User, UserRole
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
]


class TestUsersRoutesAccess:
    def test_get_without_session_redirects(self, users_env: UsersEnv) -> None:
        """GET /panel/users без сессии → 303 на форму входа."""
        response = users_env.client.get("/panel/users", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/panel/login"

    @pytest.mark.parametrize(("path", "data"), USERS_MUTATIONS)
    def test_post_without_session_redirects(
        self, users_env: UsersEnv, path: str, data: dict[str, str]
    ) -> None:
        """POST-мутации без сессии → 303 на форму входа, не 500."""
        response = users_env.client.post(path, data=data, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"] == "/panel/login"

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
