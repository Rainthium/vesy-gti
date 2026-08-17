"""Тесты редактирования справочников из панели (center/web/refs_admin).

Покрытие:
- create_site: успех с нормализацией к lower-case, дубль кода (в т.ч. другим
  регистром), кривые коды (кириллица, пробелы, спецсимволы, длина > 32);
- update_site: переименование; кода в сигнатуре нет — код прежний; не найден;
- create_scale/update_scale: полный legacy-маршрут и без него, частичный
  маршрут → ошибка, кривой IP, порт вне 1..65535, дубль маршрута (create и
  update на занятый), кривой драйвер, несуществующий объект;
- upsert_camera: создание/обновление по (весы, роль) без второй строки,
  независимость ролей, кривые схемы URL, очистка пустыми строками → None,
  несуществующие весы;
- create_agent/reissue_agent_token/set_agent_channel: токен наружу один раз,
  в БД только sha256, один агент на весы, перевыпуск обрывает старый токен;
- маршруты /panel/refs*: 303 без сессии, 403 диспетчеру на мутации при
  доступном GET, мутации админа с note, токен агента через одноразовый
  flash в сессии (не в URL);
- save_scale_settings и маршруты /panel/refs/scales/{id}/settings
  (решение 10.08.2026): валидации цикла/порта/скорости, пустой порт →
  port_cfg None, страница только для админа (403 диспетчеру и на GET),
  404 на несуществующие весы, note о доставке агенту (офлайн-хвост),
  push настроек при сохранении камеры.

Инфраструктура БД — по образцу tests/test_center_users_admin.py: одноразовая
БД ves_test_refs_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

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
    Camera,
    ReleaseChannel,
    Scale,
    ScaleKind,
    Site,
    User,
    UserRole,
)
from center.db.session import database_url, make_session_factory
from center.web import refs_admin
from center.web.router import create_panel_router
from shared.enums import CameraRole
from shared.messages import CycleSettings
from shared.passwords import hash_password
from tests.test_center_db import ALL_TABLES, _upgrade_head

ADMIN_LOGIN = "chief"
ADMIN_PASSWORD = "admin-pass-123"
DISPATCHER_LOGIN = "dispatcher"
DISPATCHER_PASSWORD = "disp-pass-123"

# токен агента — secrets.token_urlsafe(32): base64url без паддинга
TOKEN_RE = re.compile(r"<code>([A-Za-z0-9_-]{30,})</code>")


# ---------------------------------------------------------------------------
# Инфраструктура: временная БД + миграции
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def refs_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_refs_<pid>; имя не пересекается с другими
    модулями тестов, чтобы не мешать им в одном прогоне."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты редактирования справочников пропущены"
        )

    db_name = f"ves_test_refs_{os.getpid()}"
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
def refs_db_engine(refs_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(refs_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def db(refs_db_engine: Engine) -> Iterator[sessionmaker[Session]]:
    """Чистая БД; отдаёт фабрику сессий."""
    with refs_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    yield make_session_factory(refs_db_engine)


@pytest.fixture
def db_session(db: sessionmaker[Session]) -> Iterator[Session]:
    session = db()
    yield session
    session.rollback()
    session.close()


# ---------------------------------------------------------------------------
# Посев данных
# ---------------------------------------------------------------------------


def _add_site(session: Session, code: str = "kant", name: str = "СВХ «КАНТ»") -> Site:
    site = Site(code=code, name=name)
    session.add(site)
    session.commit()
    return site


def _add_scale(
    session: Session,
    site_id: int,
    name: str = "Весы SCS-80",
    *,
    driver: str = "cas22",
    legacy_ip: str | None = None,
    legacy_port: int | None = None,
    legacy_autoscale: int | None = None,
) -> Scale:
    scale = Scale(
        site_id=site_id,
        name=name,
        kind=ScaleKind.STATIC,
        driver=driver,
        legacy_ip=legacy_ip,
        legacy_port=legacy_port,
        legacy_autoscale=legacy_autoscale,
    )
    session.add(scale)
    session.commit()
    return scale


def _add_user(
    session: Session,
    login: str,
    password: str,
    *,
    role: UserRole = UserRole.DISPATCHER,
    full_name: str = "",
) -> User:
    user = User(login=login, pw_hash=hash_password(password), full_name=full_name, role=role)
    session.add(user)
    session.commit()
    return user


def _cameras_of(session: Session, scale_id: int) -> list[Camera]:
    return list(session.execute(select(Camera).where(Camera.scale_id == scale_id)).scalars().all())


# ---------------------------------------------------------------------------
# create_site
# ---------------------------------------------------------------------------


class TestCreateSite:
    def test_success_normalizes_lowercase(self, db_session: Session) -> None:
        """Код нормализуется: пробелы срезаны, регистр приведён к нижнему."""
        error = refs_admin.create_site(
            db_session, code="  KYZYL-Kyia  ", name="  СВХ «Кызыл-Кыя»  "
        )
        assert error is None
        site = db_session.execute(select(Site)).scalar_one()
        assert site.code == "kyzyl-kyia"
        assert site.name == "СВХ «Кызыл-Кыя»"

    def test_duplicate_code_rejected(self, db_session: Session) -> None:
        """Дубль кода (даже другим регистром) → ошибка, второй объект не создан."""
        _add_site(db_session, "kant")
        error = refs_admin.create_site(db_session, code="KANT", name="Дубль")
        assert error is not None
        assert "занят" in error
        assert len(db_session.execute(select(Site)).scalars().all()) == 1

    @pytest.mark.parametrize(
        "code",
        ["", "   ", "кызыл", "kyzyl kyia", "kyzyl_kyia", "-kant", "a" * 33, "kant!", "a;b"],
        ids=[
            "empty",
            "spaces-only",
            "cyrillic",
            "inner-space",
            "underscore",
            "leading-dash",
            "too-long-33",
            "bang",
            "semicolon",
        ],
    )
    def test_bad_code_rejected(self, db_session: Session, code: str) -> None:
        """Код вне слага [a-z0-9][a-z0-9-]{0,31} отклоняется, БД не тронута
        (код входит в пути хранения фото — мусор туда нельзя)."""
        error = refs_admin.create_site(db_session, code=code, name="Объект")
        assert error is not None
        assert db_session.execute(select(Site)).scalar_one_or_none() is None

    def test_max_length_32_accepted(self, db_session: Session) -> None:
        """Ровно 32 символа — верхняя граница слага, допустима."""
        assert refs_admin.create_site(db_session, code="a" * 32, name="Длинный код") is None

    def test_empty_name_rejected(self, db_session: Session) -> None:
        """Название из пробелов → ошибка, объект не создан."""
        error = refs_admin.create_site(db_session, code="kant", name="   ")
        assert error is not None
        assert "пустое" in error
        assert db_session.execute(select(Site)).scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# update_site
# ---------------------------------------------------------------------------


class TestUpdateSite:
    def test_rename_keeps_code(self, db_session: Session) -> None:
        """Переименование меняет только название; код прежний (параметра кода
        в сигнатуре нет — он входит в канонические пути фото)."""
        site = _add_site(db_session, "kant", "Старое имя")
        error = refs_admin.update_site(db_session, site.id, name="  Новое имя  ")
        assert error is None
        db_session.refresh(site)
        assert site.name == "Новое имя"
        assert site.code == "kant", "код объекта изменился при переименовании"

    def test_empty_name_rejected(self, db_session: Session) -> None:
        """Пустое название → ошибка, старое имя сохранено."""
        site = _add_site(db_session, "kant", "Как было")
        error = refs_admin.update_site(db_session, site.id, name="   ")
        assert error is not None
        db_session.refresh(site)
        assert site.name == "Как было"

    def test_missing_site(self, db_session: Session) -> None:
        """Несуществующий site_id → «объект не найден»."""
        assert refs_admin.update_site(db_session, 987654, name="Имя") == "объект не найден"


# ---------------------------------------------------------------------------
# create_scale
# ---------------------------------------------------------------------------


class TestCreateScale:
    def test_success_full_legacy(self, db_session: Session) -> None:
        """Полный legacy-маршрут (ip+port+autoscale) сохраняется целиком."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="  Весы SCS-80  ",
            kind=ScaleKind.STATIC,
            driver="  CAS22  ",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is None
        scale = db_session.execute(select(Scale)).scalar_one()
        assert scale.name == "Весы SCS-80"
        assert scale.driver == "cas22", "драйвер не нормализован к lower-case"
        assert scale.legacy_ip == "192.168.158.20"
        assert scale.legacy_port == 2020
        assert scale.legacy_autoscale == 1

    def test_success_without_legacy(self, db_session: Session) -> None:
        """Без маршрута АИС все три legacy-поля — None."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session, site_id=site.id, name="Весы", kind=ScaleKind.DYNAMIC, driver="cas22"
        )
        assert error is None
        scale = db_session.execute(select(Scale)).scalar_one()
        assert scale.legacy_ip is None
        assert scale.legacy_port is None
        assert scale.legacy_autoscale is None

    def test_ais_route_saved_with_default_scale_no(self, db_session: Session) -> None:
        """Привязка АИС v2: идентификатор СВХ как есть (нули значимы), № весов по умолчанию 1."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_object=" 0014 ",
        )
        assert error is None
        scale = db_session.execute(select(Scale)).scalar_one()
        assert scale.ais_object == "0014"
        assert scale.ais_scale_no == 1

    def test_ais_route_duplicate_rejected(self, db_session: Session) -> None:
        """Одна пара «объект АИС + № весов» — одни весы; другой номер — можно (Кант)."""
        site = _add_site(db_session)
        assert (
            refs_admin.create_scale(
                db_session,
                site_id=site.id,
                name="Весы 1",
                kind=ScaleKind.STATIC,
                driver="cas22",
                ais_object="0002",
                ais_scale_no=1,
            )
            is None
        )
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы 1 дубль",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_object="0002",
            ais_scale_no=1,
        )
        assert error == "такая привязка АИС (объект + № весов) уже назначена другим весам"
        assert (
            refs_admin.create_scale(
                db_session,
                site_id=site.id,
                name="Весы 2",
                kind=ScaleKind.STATIC,
                driver="cas22",
                ais_object="0002",
                ais_scale_no=2,
            )
            is None
        )

    def test_ais_scale_no_without_object_rejected(self, db_session: Session) -> None:
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_scale_no=2,
        )
        assert error == "привязка АИС: укажите идентификатор СВХ"

    def test_two_scales_without_legacy_allowed(self, db_session: Session) -> None:
        """Пустой маршрут не считается дублем: несколько весов без legacy."""
        site = _add_site(db_session)
        for name in ("Весы 1", "Весы 2"):
            assert (
                refs_admin.create_scale(
                    db_session, site_id=site.id, name=name, kind=ScaleKind.STATIC, driver="cas22"
                )
                is None
            )
        assert len(db_session.execute(select(Scale)).scalars().all()) == 2

    @pytest.mark.parametrize(
        ("ip", "port", "autoscale"),
        [
            ("192.168.158.20", None, None),
            ("", 2020, None),
            ("", None, 1),
            ("192.168.158.20", 2020, None),
            ("192.168.158.20", None, 1),
            ("", 2020, 1),
        ],
        ids=["ip-only", "port-only", "autoscale-only", "no-autoscale", "no-port", "no-ip"],
    )
    def test_partial_legacy_rejected(
        self, db_session: Session, ip: str, port: int | None, autoscale: int | None
    ) -> None:
        """Частично заполненный маршрут не находился бы v1-маршрутизацией —
        ошибка «заполните вместе», весы не созданы."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip=ip,
            legacy_port=port,
            legacy_autoscale=autoscale,
        )
        assert error is not None
        assert "вместе" in error
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    @pytest.mark.parametrize(
        "ip",
        [
            "10.0.0",
            "10.0.0.0.1",
            "abc",
            "192.168.1.x",
            "192.168.1.1 ",
            "1234.1.1.1",
            "999.999.999.999",
            "192.168.1.256",
        ],
        ids=[
            "three-octets",
            "five-octets",
            "letters",
            "mixed",
            "inner-space",
            "long-octet",
            "octets-over-255",
            "octet-256",
        ],
    )
    def test_bad_ip_rejected(self, db_session: Session, ip: str) -> None:
        """IP не по формату N.N.N.N → ошибка (пробелы по краям срезаются,
        внутри — нет)."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip=ip if ip != "192.168.1.1 " else "192.168.1. 1",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "IP" in error
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    @pytest.mark.parametrize("autoscale", [0, -2, 1000], ids=["zero", "neg", "over-999"])
    def test_autoscale_out_of_range_rejected(self, db_session: Session, autoscale: int) -> None:
        """autoscale вне 1..999 → ошибка, весы не созданы."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=autoscale,
        )
        assert error is not None
        assert "autoscale" in error
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    @pytest.mark.parametrize("port", [0, -1, 65536, 100000], ids=["zero", "neg", "65536", "huge"])
    def test_port_out_of_range_rejected(self, db_session: Session, port: int) -> None:
        """Порт вне 1..65535 → ошибка, весы не созданы."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="192.168.158.20",
            legacy_port=port,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "порт" in error
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    def test_duplicate_route_rejected(self, db_session: Session) -> None:
        """Занятый маршрут (ip+port+autoscale) → «уже назначен другим весам»,
        вторые весы не созданы, сессия остаётся рабочей."""
        site = _add_site(db_session)
        _add_scale(
            db_session,
            site.id,
            "Первые",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        error = refs_admin.create_scale(
            db_session,
            site_id=site.id,
            name="Вторые",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "уже назначен" in error
        assert len(db_session.execute(select(Scale)).scalars().all()) == 1

    @pytest.mark.parametrize(
        "driver",
        ["", "   ", "cas-22", "cas 22", "кас22", "x" * 33, "cas22;rm"],
        ids=["empty", "spaces", "dash", "space", "cyrillic", "too-long-33", "semicolon"],
    )
    def test_bad_driver_rejected(self, db_session: Session, driver: str) -> None:
        """Драйвер вне [a-z0-9_]{1,32} отклоняется (это имя модуля
        agent/drivers/*), весы не созданы."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session, site_id=site.id, name="Весы", kind=ScaleKind.STATIC, driver=driver
        )
        assert error is not None
        assert "драйвер" in error
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    def test_unknown_site_rejected(self, db_session: Session) -> None:
        """Несуществующий site_id → «объект не найден», без IntegrityError."""
        error = refs_admin.create_scale(
            db_session, site_id=987654, name="Весы", kind=ScaleKind.STATIC, driver="cas22"
        )
        assert error == "объект не найден"
        assert db_session.execute(select(Scale)).scalar_one_or_none() is None

    def test_empty_name_rejected(self, db_session: Session) -> None:
        """Пустое название весов → ошибка."""
        site = _add_site(db_session)
        error = refs_admin.create_scale(
            db_session, site_id=site.id, name="  ", kind=ScaleKind.STATIC, driver="cas22"
        )
        assert error is not None
        assert "пустое" in error


# ---------------------------------------------------------------------------
# update_scale
# ---------------------------------------------------------------------------


class TestUpdateScale:
    def test_success_sets_full_legacy(self, db_session: Session) -> None:
        """Правка: имя, тип, драйвер и полный маршрут сохраняются."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id, "Старые")
        error = refs_admin.update_scale(
            db_session,
            scale.id,
            name="Новые",
            kind=ScaleKind.PLATFORM,
            driver="CAS22",
            legacy_ip="10.1.1.1",
            legacy_port=2021,
            legacy_autoscale=2,
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.name == "Новые"
        assert scale.kind is ScaleKind.PLATFORM
        assert scale.driver == "cas22"
        assert (scale.legacy_ip, scale.legacy_port, scale.legacy_autoscale) == ("10.1.1.1", 2021, 2)

    def test_success_clears_legacy(self, db_session: Session) -> None:
        """Пустые legacy-поля при правке снимают маршрут (все три → None)."""
        site = _add_site(db_session)
        scale = _add_scale(
            db_session, site.id, legacy_ip="10.1.1.1", legacy_port=2021, legacy_autoscale=2
        )
        error = refs_admin.update_scale(
            db_session, scale.id, name="Весы", kind=ScaleKind.STATIC, driver="cas22"
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.legacy_ip is None
        assert scale.legacy_port is None
        assert scale.legacy_autoscale is None

    def test_partial_legacy_rejected(self, db_session: Session) -> None:
        """Частичный маршрут при правке → ошибка, поля не изменены."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id, "Как было")
        error = refs_admin.update_scale(
            db_session,
            scale.id,
            name="Новое имя",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="10.1.1.1",
        )
        assert error is not None
        assert "вместе" in error
        db_session.refresh(scale)
        assert scale.name == "Как было"
        assert scale.legacy_ip is None

    def test_bad_ip_rejected(self, db_session: Session) -> None:
        """Кривой IP при правке → ошибка, маршрут не тронут."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.update_scale(
            db_session,
            scale.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="not-an-ip",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "IP" in error
        db_session.refresh(scale)
        assert scale.legacy_ip is None

    def test_port_out_of_range_rejected(self, db_session: Session) -> None:
        """Порт 65536 при правке → ошибка."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.update_scale(
            db_session,
            scale.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="10.1.1.1",
            legacy_port=65536,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "порт" in error

    def test_move_to_taken_route_rejected(self, db_session: Session) -> None:
        """Перевод весов на маршрут, занятый другими, → ошибка, откат:
        собственный маршрут весов не изменился."""
        site = _add_site(db_session)
        _add_scale(
            db_session,
            site.id,
            "Первые",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        second = _add_scale(db_session, site.id, "Вторые")
        error = refs_admin.update_scale(
            db_session,
            second.id,
            name="Вторые",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is not None
        assert "уже назначен" in error
        db_session.refresh(second)
        assert second.legacy_ip is None, "занятый маршрут всё же присвоен"

    def test_keep_own_route_allowed(self, db_session: Session) -> None:
        """Пересохранение весов с их же маршрутом — не конфликт."""
        site = _add_site(db_session)
        scale = _add_scale(
            db_session, site.id, legacy_ip="192.168.158.20", legacy_port=2020, legacy_autoscale=1
        )
        error = refs_admin.update_scale(
            db_session,
            scale.id,
            name="Весы",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip="192.168.158.20",
            legacy_port=2020,
            legacy_autoscale=1,
        )
        assert error is None

    def test_bad_driver_rejected(self, db_session: Session) -> None:
        """Кривой драйвер при правке → ошибка, поля не тронуты."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id, driver="cas22")
        error = refs_admin.update_scale(
            db_session, scale.id, name="Весы", kind=ScaleKind.STATIC, driver="bad driver"
        )
        assert error is not None
        db_session.refresh(scale)
        assert scale.driver == "cas22"

    def test_missing_scale(self, db_session: Session) -> None:
        """Несуществующий scale_id → «весы не найдены»."""
        error = refs_admin.update_scale(
            db_session, 987654, name="Весы", kind=ScaleKind.STATIC, driver="cas22"
        )
        assert error == "весы не найдены"


# ---------------------------------------------------------------------------
# upsert_camera
# ---------------------------------------------------------------------------


class TestUpsertCamera:
    def test_create_then_update_same_role(self, db_session: Session) -> None:
        """Повторный upsert той же роли правит существующую строку —
        второй записи (scale_id, role) не появляется."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.upsert_camera(
            db_session,
            scale_id=scale.id,
            role=CameraRole.FRONT,
            snapshot_url="http://user:pass@10.0.0.5/snap",
            rtsp_url="rtsp://user:pass@10.0.0.5/stream",
        )
        assert error is None
        error = refs_admin.upsert_camera(
            db_session,
            scale_id=scale.id,
            role=CameraRole.FRONT,
            snapshot_url="https://10.0.0.9/snap2",
            rtsp_url="rtsp://10.0.0.9/stream2",
        )
        assert error is None
        cameras = _cameras_of(db_session, scale.id)
        assert len(cameras) == 1, "upsert создал вторую строку той же роли"
        assert cameras[0].snapshot_url == "https://10.0.0.9/snap2"
        assert cameras[0].rtsp_url == "rtsp://10.0.0.9/stream2"

    def test_roles_independent(self, db_session: Session) -> None:
        """ПЕРЕД и ЗАД — независимые строки: правка одной не трогает другую."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        assert (
            refs_admin.upsert_camera(
                db_session,
                scale_id=scale.id,
                role=CameraRole.FRONT,
                snapshot_url="http://10.0.0.5/front",
                rtsp_url="",
            )
            is None
        )
        assert (
            refs_admin.upsert_camera(
                db_session,
                scale_id=scale.id,
                role=CameraRole.REAR,
                snapshot_url="http://10.0.0.5/rear",
                rtsp_url="",
            )
            is None
        )
        cameras = {camera.role: camera for camera in _cameras_of(db_session, scale.id)}
        assert len(cameras) == 2
        assert cameras[CameraRole.FRONT].snapshot_url == "http://10.0.0.5/front"
        assert cameras[CameraRole.REAR].snapshot_url == "http://10.0.0.5/rear"

    @pytest.mark.parametrize(
        ("snapshot_url", "rtsp_url"),
        [
            ("ftp://10.0.0.5/snap", ""),
            ("rtsp://10.0.0.5/snap", ""),
            ("10.0.0.5/snap", ""),
            ("", "http://10.0.0.5/stream"),
            ("", "rtp://10.0.0.5/stream"),
            ("", "10.0.0.5/stream"),
        ],
        ids=["snap-ftp", "snap-rtsp", "snap-no-scheme", "rtsp-http", "rtsp-rtp", "rtsp-no-scheme"],
    )
    def test_bad_url_scheme_rejected(
        self, db_session: Session, snapshot_url: str, rtsp_url: str
    ) -> None:
        """Схемы вне http(s):// для snapshot и rtsp:// для RTSP отклоняются,
        камера не создаётся."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.upsert_camera(
            db_session,
            scale_id=scale.id,
            role=CameraRole.FRONT,
            snapshot_url=snapshot_url,
            rtsp_url=rtsp_url,
        )
        assert error is not None
        assert _cameras_of(db_session, scale.id) == []

    def test_clear_urls_with_empty_strings(self, db_session: Session) -> None:
        """Пустые строки очищают URL до None; строка камеры остаётся."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        refs_admin.upsert_camera(
            db_session,
            scale_id=scale.id,
            role=CameraRole.FRONT,
            snapshot_url="http://10.0.0.5/snap",
            rtsp_url="rtsp://10.0.0.5/stream",
        )
        error = refs_admin.upsert_camera(
            db_session, scale_id=scale.id, role=CameraRole.FRONT, snapshot_url="  ", rtsp_url=""
        )
        assert error is None
        cameras = _cameras_of(db_session, scale.id)
        assert len(cameras) == 1
        assert cameras[0].snapshot_url is None
        assert cameras[0].rtsp_url is None

    def test_missing_scale(self, db_session: Session) -> None:
        """Несуществующие весы → «весы не найдены», камера не создана."""
        error = refs_admin.upsert_camera(
            db_session,
            scale_id=987654,
            role=CameraRole.FRONT,
            snapshot_url="http://10.0.0.5/snap",
            rtsp_url="",
        )
        assert error == "весы не найдены"
        assert db_session.execute(select(Camera)).scalar_one_or_none() is None


# ---------------------------------------------------------------------------
# Агенты: create_agent / reissue_agent_token / set_agent_channel
# ---------------------------------------------------------------------------


class TestAgents:
    def test_create_returns_token_stores_hash(self, db_session: Session) -> None:
        """Успех: токен возвращён, в БД только sha256 (правило №7), сам токен
        нигде в строках БД не лежит."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error, token = refs_admin.create_agent(
            db_session, scale_id=scale.id, channel=ReleaseChannel.PILOT
        )
        assert error is None
        assert token
        agent = db_session.execute(select(Agent)).scalar_one()
        assert agent.token_hash == repo.hash_agent_token(token)
        assert agent.token_hash != token, "в БД лежит сырой токен"
        assert agent.channel is ReleaseChannel.PILOT
        # токен действителен для аутентификации агента
        assert repo.authenticate_agent(db_session, token) is not None

    def test_second_agent_same_scale_rejected(self, db_session: Session) -> None:
        """Один агент на весы: повторное создание → ошибка, токен не выдан."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        refs_admin.create_agent(db_session, scale_id=scale.id, channel=ReleaseChannel.PILOT)
        error, token = refs_admin.create_agent(
            db_session, scale_id=scale.id, channel=ReleaseChannel.STABLE
        )
        assert error is not None
        assert "уже есть агент" in error
        assert token is None
        assert len(db_session.execute(select(Agent)).scalars().all()) == 1

    def test_create_missing_scale(self, db_session: Session) -> None:
        """Несуществующие весы → ошибка, токен не выдан."""
        error, token = refs_admin.create_agent(
            db_session, scale_id=987654, channel=ReleaseChannel.PILOT
        )
        assert error == "весы не найдены"
        assert token is None

    def test_reissue_invalidates_old_token(self, db_session: Session) -> None:
        """Перевыпуск: новый токен работает, старый хеш перестаёт совпадать —
        старый агент теряет связь сразу."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        _, old_token = refs_admin.create_agent(
            db_session, scale_id=scale.id, channel=ReleaseChannel.PILOT
        )
        assert old_token is not None
        agent = db_session.execute(select(Agent)).scalar_one()

        error, new_token = refs_admin.reissue_agent_token(db_session, agent.id)
        assert error is None
        assert new_token
        assert new_token != old_token
        db_session.refresh(agent)
        assert agent.token_hash == repo.hash_agent_token(new_token)
        assert agent.token_hash != repo.hash_agent_token(old_token)
        assert repo.authenticate_agent(db_session, new_token) is not None
        assert repo.authenticate_agent(db_session, old_token) is None, "старый токен ещё действует"

    def test_reissue_missing_agent(self, db_session: Session) -> None:
        """Несуществующий агент → «агент не найден», токен не выдан."""
        error, token = refs_admin.reissue_agent_token(db_session, 987654)
        assert error == "агент не найден"
        assert token is None

    def test_set_channel_roundtrip(self, db_session: Session) -> None:
        """Смена канала pilot → stable → pilot."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        refs_admin.create_agent(db_session, scale_id=scale.id, channel=ReleaseChannel.PILOT)
        agent = db_session.execute(select(Agent)).scalar_one()
        assert refs_admin.set_agent_channel(db_session, agent.id, ReleaseChannel.STABLE) is None
        # канал перечитывается из БД заново: refresh() не сбрасывает
        # mypy-сужение agent.channel до литерала после первого assert
        channel: ReleaseChannel = db_session.execute(select(Agent)).scalar_one().channel
        assert channel is ReleaseChannel.STABLE
        assert refs_admin.set_agent_channel(db_session, agent.id, ReleaseChannel.PILOT) is None
        channel = db_session.execute(select(Agent)).scalar_one().channel
        assert channel is ReleaseChannel.PILOT

    def test_set_channel_missing_agent(self, db_session: Session) -> None:
        """Несуществующий агент → «агент не найден»."""
        assert (
            refs_admin.set_agent_channel(db_session, 987654, ReleaseChannel.STABLE)
            == "агент не найден"
        )


# ---------------------------------------------------------------------------
# Маршруты /panel/refs* (TestClient)
# ---------------------------------------------------------------------------


@dataclass
class RefsEnv:
    """Окружение маршрутных тестов: клиент, фабрика сессий, id справочников."""

    client: TestClient
    factory: sessionmaker[Session]
    site_id: int
    scale_id: int


@pytest.fixture
def refs_env(db: sessionmaker[Session], tmp_path: Path) -> Iterator[RefsEnv]:
    """Приложение как в center/app.py: SessionMiddleware + create_panel_router;
    посев — админ, диспетчер, объект и весы без агента."""
    with db() as session:
        _add_user(session, ADMIN_LOGIN, ADMIN_PASSWORD, role=UserRole.ADMIN, full_name="Админ")
        _add_user(session, DISPATCHER_LOGIN, DISPATCHER_PASSWORD, full_name="Диспетчер")
        site = _add_site(session)
        scale = _add_scale(session, site.id)
        # камера в посеве: иначе строки таблицы камер на странице не
        # отрисовываются и ошибка в шаблоне прошла бы мимо тестов
        session.add(Camera(scale_id=scale.id, role=CameraRole.FRONT, snapshot_url="http://cam/1"))
        session.commit()
        site_id, scale_id = site.id, scale.id

    app = FastAPI()
    app.add_middleware(SessionMiddleware, secret_key="test-secret", session_cookie="ves_test")
    app.include_router(create_panel_router(db, AgentHub(), photos_dir=tmp_path))
    client = TestClient(app)
    yield RefsEnv(client, db, site_id, scale_id)
    client.close()


def _login(env: RefsEnv, login: str, password: str) -> None:
    response = env.client.post(
        "/panel/login", data={"login": login, "password": password}, follow_redirects=False
    )
    assert response.status_code == 303, "вход не удался"


def _note_from(response: object) -> str:
    """Флеш-заметка из Location редиректа на /panel/refs?note=..."""
    location = response.headers["location"]  # type: ignore[attr-defined]
    parts = urlsplit(location)
    assert parts.path == "/panel/refs", f"редирект не на экран справочников: {location}"
    notes = parse_qs(parts.query).get("note", [])
    assert notes, f"note отсутствует в редиректе: {location}"
    return unquote(notes[0])


# валидные данные форм: проверяем именно контроль доступа, а не валидацию
REFS_MUTATIONS = [
    ("/panel/refs/sites/create", {"code": "osh", "name": "СВХ «Ош»"}),
    ("/panel/refs/sites/1/edit", {"name": "Новое имя"}),
    (
        "/panel/refs/scales/create",
        {"site_id": "1", "name": "Весы", "kind": "static", "driver": "cas22"},
    ),
    ("/panel/refs/scales/1/edit", {"name": "Весы", "kind": "static", "driver": "cas22"}),
    ("/panel/refs/scales/1/camera", {"role": "front", "snapshot_url": "", "rtsp_url": ""}),
    ("/panel/refs/agents/create", {"scale_id": "1", "channel": "pilot"}),
    ("/panel/refs/agents/1/reissue-token", {}),
    ("/panel/refs/agents/1/channel", {"channel": "stable"}),
]


class TestRefsRoutesAccess:
    def test_get_without_session_redirects(self, refs_env: RefsEnv) -> None:
        """GET /panel/refs без сессии → 303 на форму входа."""
        response = refs_env.client.get("/panel/refs", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    @pytest.mark.parametrize(("path", "data"), REFS_MUTATIONS)
    def test_post_without_session_redirects(
        self, refs_env: RefsEnv, path: str, data: dict[str, str]
    ) -> None:
        """POST-мутации без сессии → 303 на форму входа, не 500."""
        response = refs_env.client.post(path, data=data, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    def test_dispatcher_reads_page_without_forms(self, refs_env: RefsEnv) -> None:
        """Диспетчеру экран доступен на чтение (200), но форм редактирования
        в HTML нет — они только при can_edit."""
        _login(refs_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        response = refs_env.client.get("/panel/refs")
        assert response.status_code == 200
        assert "Создать объект" not in response.text
        assert "/panel/refs/sites/create" not in response.text
        assert "Перевыпустить токен" not in response.text

    @pytest.mark.parametrize(("path", "data"), REFS_MUTATIONS)
    def test_dispatcher_gets_403_on_mutations(
        self, refs_env: RefsEnv, path: str, data: dict[str, str]
    ) -> None:
        """Диспетчер не может выполнять POST-мутации справочников (403)."""
        _login(refs_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        response = refs_env.client.post(path, data=data, follow_redirects=False)
        assert response.status_code == 403

    def test_dispatcher_mutations_leave_db_untouched(self, refs_env: RefsEnv) -> None:
        """После 403 диспетчера в БД нет новых объектов/весов/агентов."""
        _login(refs_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        for path, data in REFS_MUTATIONS:
            refs_env.client.post(path, data=data, follow_redirects=False)
        with refs_env.factory() as session:
            assert len(session.execute(select(Site)).scalars().all()) == 1
            assert len(session.execute(select(Scale)).scalars().all()) == 1
            assert session.execute(select(Agent)).scalar_one_or_none() is None


class TestRefsRoutesAdmin:
    def test_admin_sees_edit_forms(self, refs_env: RefsEnv) -> None:
        """Админ видит формы редактирования на странице справочников."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.get("/panel/refs")
        assert response.status_code == 200
        assert "Создать объект" in response.text
        assert "/panel/refs/sites/create" in response.text

    def test_agents_and_cameras_show_site(self, refs_env: RefsEnv) -> None:
        """В таблицах агентов и камер объект стоит рядом с весами: названия
        весов на разных объектах совпадают (11.08.2026)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.get("/panel/refs")
        assert response.status_code == 200
        assert "<th>Объект</th><th>Весы</th>" in response.text
        # строка камеры отрисована (посев содержит камеру) и подписана объектом
        assert f"cam-{refs_env.scale_id}-front" in response.text
        assert response.text.count("СВХ «КАНТ»") >= 3  # весы, агенты-раздел, камеры

    def test_create_site_via_post(self, refs_env: RefsEnv) -> None:
        """POST sites/create создаёт объект и редиректит с note «создан»."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/sites/create",
            data={"code": "OSH", "name": "СВХ «Ош»"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "создан" in _note_from(response)
        with refs_env.factory() as session:
            site = session.execute(select(Site).where(Site.code == "osh")).scalar_one()
            assert site.name == "СВХ «Ош»"
        page = refs_env.client.get("/panel/refs")
        assert "СВХ «Ош»" in page.text

    def test_create_site_error_shown_as_note(self, refs_env: RefsEnv) -> None:
        """Ошибка мутации — не 500, а note в редиректе (дубль кода)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/sites/create",
            data={"code": "kant", "name": "Дубль"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "занят" in _note_from(response)

    def test_site_edit_ignores_code_field(self, refs_env: RefsEnv) -> None:
        """Лишнее поле code в POST edit игнорируется: код объекта прежний."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/sites/{refs_env.site_id}/edit",
            data={"name": "Новое имя", "code": "hacked"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        with refs_env.factory() as session:
            site = session.get(Site, refs_env.site_id)
            assert site is not None
            assert site.code == "kant", "код объекта изменён через форму"
            assert site.name == "Новое имя"

    def test_agent_token_flash_shown_once_not_in_url(self, refs_env: RefsEnv) -> None:
        """Токен агента: НЕ в Location редиректа, показан один раз в блоке
        token-note следующего GET, при повторном GET исчезает (flash)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/agents/create",
            data={"scale_id": str(refs_env.scale_id), "channel": "pilot"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        location = response.headers["location"]
        # в редиректе только note, никаких других параметров с секретом
        assert set(parse_qs(urlsplit(location).query)) == {"note"}
        assert "агент создан" in _note_from(response)

        page = refs_env.client.get("/panel/refs")
        assert page.status_code == 200
        assert "token-note" in page.text, "блок токена не показан после создания агента"
        match = TOKEN_RE.search(page.text)
        assert match, "токен не найден в блоке token-note"
        token = match.group(1)
        assert token not in location, "секретный токен попал в URL редиректа"
        with refs_env.factory() as session:
            agent = session.execute(
                select(Agent).where(Agent.scale_id == refs_env.scale_id)
            ).scalar_one()
            assert agent.token_hash == repo.hash_agent_token(token)

        second_page = refs_env.client.get("/panel/refs")
        assert "token-note" not in second_page.text, "flash-токен показан повторно"
        assert token not in second_page.text

    def test_reissue_token_flash(self, refs_env: RefsEnv) -> None:
        """Перевыпуск через POST: новый токен во flash, старый хеш мёртв."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        refs_env.client.post(
            "/panel/refs/agents/create",
            data={"scale_id": str(refs_env.scale_id), "channel": "pilot"},
            follow_redirects=False,
        )
        first_page = refs_env.client.get("/panel/refs")
        first_match = TOKEN_RE.search(first_page.text)
        assert first_match
        old_token = first_match.group(1)
        with refs_env.factory() as session:
            agent_id = session.execute(select(Agent)).scalar_one().id

        response = refs_env.client.post(
            f"/panel/refs/agents/{agent_id}/reissue-token", follow_redirects=False
        )
        assert response.status_code == 303
        assert "перевыпущен" in _note_from(response)
        page = refs_env.client.get("/panel/refs")
        match = TOKEN_RE.search(page.text)
        assert match, "новый токен не показан после перевыпуска"
        new_token = match.group(1)
        assert new_token != old_token
        with refs_env.factory() as session:
            assert repo.authenticate_agent(session, new_token) is not None
            assert repo.authenticate_agent(session, old_token) is None

    def test_scale_create_and_agent_channel_via_post(self, refs_env: RefsEnv) -> None:
        """Создание весов и смена канала агента через POST-формы."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/scales/create",
            data={
                "site_id": str(refs_env.site_id),
                "name": "Весы №2",
                "kind": "dynamic",
                "driver": "cas22",
                "legacy_ip": "192.168.158.20",
                "legacy_port": "2020",
                "legacy_autoscale": "1",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "созданы" in _note_from(response)
        with refs_env.factory() as session:
            scale = session.execute(select(Scale).where(Scale.name == "Весы №2")).scalar_one()
            assert scale.kind is ScaleKind.DYNAMIC
            assert (scale.legacy_ip, scale.legacy_port, scale.legacy_autoscale) == (
                "192.168.158.20",
                2020,
                1,
            )

        refs_env.client.post(
            "/panel/refs/agents/create",
            data={"scale_id": str(refs_env.scale_id), "channel": "pilot"},
            follow_redirects=False,
        )
        with refs_env.factory() as session:
            agent_id = session.execute(select(Agent)).scalar_one().id
        response = refs_env.client.post(
            f"/panel/refs/agents/{agent_id}/channel",
            data={"channel": "stable"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "канал" in _note_from(response)
        with refs_env.factory() as session:
            agent = session.get(Agent, agent_id)
            assert agent is not None
            assert agent.channel is ReleaseChannel.STABLE

    def test_scale_form_garbage_port_is_note_not_500(self, refs_env: RefsEnv) -> None:
        """Нечисловой порт в форме → note об ошибке, весы не созданы."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/scales/create",
            data={
                "site_id": str(refs_env.site_id),
                "name": "Весы №2",
                "kind": "static",
                "driver": "cas22",
                "legacy_ip": "192.168.158.20",
                "legacy_port": "not-a-number",
                "legacy_autoscale": "1",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "числа" in _note_from(response)
        with refs_env.factory() as session:
            assert (
                session.execute(select(Scale).where(Scale.name == "Весы №2")).scalar_one_or_none()
                is None
            )

    def test_camera_upsert_via_post(self, refs_env: RefsEnv) -> None:
        """Сохранение камеры через POST: строка появляется, URL сохранены."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/camera",
            data={
                "role": "front",
                "snapshot_url": "http://user:pass@10.0.0.5/snap",
                "rtsp_url": "rtsp://user:pass@10.0.0.5/stream",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "камера" in _note_from(response)
        with refs_env.factory() as session:
            camera = session.execute(
                select(Camera).where(Camera.scale_id == refs_env.scale_id)
            ).scalar_one()
            assert camera.role is CameraRole.FRONT
            assert camera.snapshot_url == "http://user:pass@10.0.0.5/snap"


# ---------------------------------------------------------------------------
# save_scale_settings (страница настроек весов, решение Игоря 10.08.2026)
# ---------------------------------------------------------------------------


def _make_cycle(**overrides: float) -> CycleSettings:
    """Валидный полный набор параметров цикла; overrides — точечные замены."""
    fields: dict[str, float] = {
        "zero_threshold_kg": 150.0,
        "vehicle_threshold_kg": 600.0,
        "zero_timeout_s": 10.0,
        "vehicle_timeout_s": 90.0,
        "stable_duration_s": 5.0,
        "stable_timeout_s": 30.0,
        "no_data_timeout_s": 5.0,
    }
    fields.update(overrides)
    return CycleSettings(**fields)


class TestSaveScaleSettings:
    def test_success_saves_thresholds_and_port(self, db_session: Session) -> None:
        """Успех: thresholds — полный словарь цикла, port_cfg — порт и скорость."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        cycle = _make_cycle()
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=cycle, port="  COM11  ", baudrate=19200
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.thresholds == cycle.model_dump()
        assert scale.port_cfg == {"port": "COM11", "baudrate": 19200}

    def test_empty_port_clears_port_cfg(self, db_session: Session) -> None:
        """Пустой порт → port_cfg None (портом управляет локальный конфиг)."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        scale.port_cfg = {"port": "COM7", "baudrate": 9600}
        db_session.commit()
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=_make_cycle(), port="   ", baudrate=None
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.port_cfg is None
        assert scale.thresholds == _make_cycle().model_dump()

    def test_port_without_baudrate_defaults_9600(self, db_session: Session) -> None:
        """Порт без скорости: в port_cfg записывается дефолт 9600."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=_make_cycle(), port="COM11", baudrate=None
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.port_cfg == {"port": "COM11", "baudrate": 9600}

    @pytest.mark.parametrize(
        "field",
        [
            "zero_threshold_kg",
            "vehicle_threshold_kg",
            "zero_timeout_s",
            "vehicle_timeout_s",
            "stable_duration_s",
            "stable_timeout_s",
            "no_data_timeout_s",
        ],
    )
    @pytest.mark.parametrize("bad_value", [0.0, -1.0], ids=["zero", "negative"])
    def test_non_positive_cycle_value_rejected(
        self, db_session: Session, field: str, bad_value: float
    ) -> None:
        """Каждый параметр цикла обязан быть > 0; БД не тронута."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        cycle = _make_cycle(**{field: bad_value})
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=cycle, port="", baudrate=None
        )
        assert error is not None
        assert "больше нуля" in error
        db_session.refresh(scale)
        assert scale.thresholds is None

    @pytest.mark.parametrize("vehicle", [150.0, 100.0], ids=["equal", "below"])
    def test_vehicle_threshold_must_exceed_zero_threshold(
        self, db_session: Session, vehicle: float
    ) -> None:
        """Порог заезда должен быть СТРОГО больше порога пустых весов."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        cycle = _make_cycle(zero_threshold_kg=150.0, vehicle_threshold_kg=vehicle)
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=cycle, port="", baudrate=None
        )
        assert error is not None
        assert "порог заезда" in error
        db_session.refresh(scale)
        assert scale.thresholds is None

    def test_stable_duration_above_timeout_rejected(self, db_session: Session) -> None:
        """Время стабильности больше её таймаута → фиксация недостижима, ошибка."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        cycle = _make_cycle(stable_duration_s=31.0, stable_timeout_s=30.0)
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=cycle, port="", baudrate=None
        )
        assert error is not None
        assert "стабильности" in error

    def test_stable_duration_equal_timeout_allowed(self, db_session: Session) -> None:
        """Граница: duration == timeout допустимо."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        cycle = _make_cycle(stable_duration_s=30.0, stable_timeout_s=30.0)
        assert (
            refs_admin.save_scale_settings(
                db_session, scale.id, cycle=cycle, port="", baudrate=None
            )
            is None
        )

    def test_port_longer_64_rejected(self, db_session: Session) -> None:
        """Порт длиннее 64 символов отклоняется; ровно 64 — допустим."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=_make_cycle(), port="C" * 65, baudrate=None
        )
        assert error is not None
        assert "64" in error
        db_session.refresh(scale)
        assert scale.port_cfg is None
        assert (
            refs_admin.save_scale_settings(
                db_session, scale.id, cycle=_make_cycle(), port="C" * 64, baudrate=None
            )
            is None
        )

    @pytest.mark.parametrize(
        "baudrate", [299, 921601, 0, -9600], ids=["low", "high", "zero", "neg"]
    )
    def test_baudrate_out_of_range_rejected(self, db_session: Session, baudrate: int) -> None:
        """Скорость вне 300..921600 → ошибка, настройки не сохранены."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_settings(
            db_session, scale.id, cycle=_make_cycle(), port="COM11", baudrate=baudrate
        )
        assert error is not None
        assert "скорость" in error
        db_session.refresh(scale)
        assert scale.port_cfg is None

    @pytest.mark.parametrize("baudrate", [300, 921600], ids=["min", "max"])
    def test_baudrate_boundaries_accepted(self, db_session: Session, baudrate: int) -> None:
        """Границы диапазона скорости 300 и 921600 принимаются."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        assert (
            refs_admin.save_scale_settings(
                db_session, scale.id, cycle=_make_cycle(), port="COM11", baudrate=baudrate
            )
            is None
        )

    def test_missing_scale(self, db_session: Session) -> None:
        """Несуществующие весы → «весы не найдены»."""
        error = refs_admin.save_scale_settings(
            db_session, 987654, cycle=_make_cycle(), port="", baudrate=None
        )
        assert error == "весы не найдены"


# ---------------------------------------------------------------------------
# Маршруты /panel/refs/scales/{id}/settings
# ---------------------------------------------------------------------------


class TestSaveScaleVerification:
    """Свидетельство о поверке (весовая карточка, 13.08.2026)."""

    def test_success_saves_all_fields(self, db_session: Session) -> None:
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_verification(
            db_session,
            scale.id,
            number="  №3961  ",
            verified_on="2026-02-26",
            valid_until="2027-02-26",
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.verif_number == "№3961"
        assert scale.verif_date == date(2026, 2, 26)
        assert scale.verif_until == date(2027, 2, 26)

    def test_empty_number_clears_verification(self, db_session: Session) -> None:
        """Пустой номер очищает поверку целиком (на карточке — прочерк)."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        scale.verif_number = "№1"
        scale.verif_date = date(2026, 1, 1)
        scale.verif_until = date(2027, 1, 1)
        db_session.commit()
        error = refs_admin.save_scale_verification(
            db_session, scale.id, number="  ", verified_on="", valid_until=""
        )
        assert error is None
        db_session.refresh(scale)
        assert scale.verif_number is None
        assert scale.verif_date is None
        assert scale.verif_until is None

    def test_dates_without_number_rejected(self, db_session: Session) -> None:
        """Даты без номера — ошибка, а не молчаливая потеря дат."""
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_verification(
            db_session, scale.id, number="", verified_on="2026-02-26", valid_until=""
        )
        assert error is not None and "номер" in error

    def test_deadline_before_date_rejected(self, db_session: Session) -> None:
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_verification(
            db_session,
            scale.id,
            number="№3961",
            verified_on="2027-02-26",
            valid_until="2026-02-26",
        )
        assert error is not None and "раньше" in error

    def test_garbage_date_rejected(self, db_session: Session) -> None:
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_verification(
            db_session, scale.id, number="№3961", verified_on="26.02.2026", valid_until=""
        )
        assert error is not None and "ГГГГ-ММ-ДД" in error

    def test_too_long_number_rejected(self, db_session: Session) -> None:
        site = _add_site(db_session)
        scale = _add_scale(db_session, site.id)
        error = refs_admin.save_scale_verification(
            db_session, scale.id, number="№" * 65, verified_on="", valid_until=""
        )
        assert error is not None and "64" in error

    def test_missing_scale(self, db_session: Session) -> None:
        error = refs_admin.save_scale_verification(
            db_session, 987654, number="№3961", verified_on="", valid_until=""
        )
        assert error == "весы не найдены"


def _settings_form(**overrides: str) -> dict[str, str]:
    """Валидные данные формы настроек весов."""
    fields = {
        "zero_threshold_kg": "150",
        "vehicle_threshold_kg": "600",
        "zero_timeout_s": "10",
        "vehicle_timeout_s": "90",
        "stable_duration_s": "5",
        "stable_timeout_s": "30",
        "no_data_timeout_s": "5",
        "port": "COM11",
        "baudrate": "19200",
    }
    fields.update(overrides)
    return fields


def _settings_note(response: object, scale_id: int) -> str:
    """Флеш-заметка из редиректа обратно на страницу настроек весов."""
    location = response.headers["location"]  # type: ignore[attr-defined]
    parts = urlsplit(location)
    assert parts.path == f"/panel/refs/scales/{scale_id}/settings", (
        f"редирект не на страницу настроек: {location}"
    )
    notes = parse_qs(parts.query).get("note", [])
    assert notes, f"note отсутствует в редиректе: {location}"
    return unquote(notes[0])


class TestScaleSettingsRoutes:
    def test_admin_gets_page_with_defaults(self, refs_env: RefsEnv) -> None:
        """GET страница настроек: 200, поля цикла с дефолтами и пустой порт."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.get(f"/panel/refs/scales/{refs_env.scale_id}/settings")
        assert response.status_code == 200
        for field in (
            "zero_threshold_kg",
            "vehicle_threshold_kg",
            "zero_timeout_s",
            "vehicle_timeout_s",
            "stable_duration_s",
            "stable_timeout_s",
            "no_data_timeout_s",
            'name="port"',
            'name="baudrate"',
        ):
            assert field in response.text, f"на странице нет поля {field}"
        # дефолты цикла (пока центр не управляет) — из DEFAULT_CYCLE
        assert str(refs_admin.DEFAULT_CYCLE.zero_threshold_kg) in response.text

    def test_get_missing_scale_404(self, refs_env: RefsEnv) -> None:
        """GET несуществующих весов → 404."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.get("/panel/refs/scales/987654/settings")
        assert response.status_code == 404

    def test_get_without_session_redirects_to_login(self, refs_env: RefsEnv) -> None:
        """GET без сессии → 303 на форму входа."""
        response = refs_env.client.get(
            f"/panel/refs/scales/{refs_env.scale_id}/settings", follow_redirects=False
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/panel/login")

    def test_dispatcher_gets_403_on_get_and_post(self, refs_env: RefsEnv) -> None:
        """Страница настроек — только админам: диспетчеру 403 и на GET, и на POST."""
        _login(refs_env, DISPATCHER_LOGIN, DISPATCHER_PASSWORD)
        get_response = refs_env.client.get(f"/panel/refs/scales/{refs_env.scale_id}/settings")
        assert get_response.status_code == 403
        post_response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(),
            follow_redirects=False,
        )
        assert post_response.status_code == 403
        with refs_env.factory() as session:
            scale = session.get(Scale, refs_env.scale_id)
            assert scale is not None
            assert scale.thresholds is None, "диспетчер изменил настройки весов"

    def test_post_saves_and_redirects_with_note(self, refs_env: RefsEnv) -> None:
        """POST: настройки в БД, редирект на страницу настроек с note;
        агента в хабе нет → «агент офлайн, применятся при подключении»."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(),
            follow_redirects=False,
        )
        assert response.status_code == 303
        note = _settings_note(response, refs_env.scale_id)
        assert "настройки сохранены" in note
        assert "офлайн" in note
        with refs_env.factory() as session:
            scale = session.get(Scale, refs_env.scale_id)
            assert scale is not None
            assert scale.thresholds == {
                "zero_threshold_kg": 150.0,
                "vehicle_threshold_kg": 600.0,
                "zero_timeout_s": 10.0,
                "vehicle_timeout_s": 90.0,
                "stable_duration_s": 5.0,
                "stable_timeout_s": 30.0,
                "no_data_timeout_s": 5.0,
            }
            assert scale.port_cfg == {"port": "COM11", "baudrate": 19200}
        # сохранённые значения видны на странице при следующем GET
        page = refs_env.client.get(f"/panel/refs/scales/{refs_env.scale_id}/settings")
        assert "COM11" in page.text
        assert "19200" in page.text

    def test_post_validation_error_shown_as_note(self, refs_env: RefsEnv) -> None:
        """Ошибка валидации (порог заезда ниже порога пустых) → note, БД не тронута."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(vehicle_threshold_kg="100"),
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "порог заезда" in _settings_note(response, refs_env.scale_id)
        with refs_env.factory() as session:
            scale = session.get(Scale, refs_env.scale_id)
            assert scale is not None
            assert scale.thresholds is None

    def test_post_garbage_baudrate_is_note_not_500(self, refs_env: RefsEnv) -> None:
        """Нечисловая скорость → note об ошибке, не 500/422."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(baudrate="fast"),
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "число" in _settings_note(response, refs_env.scale_id)

    def test_post_missing_scale_redirects_with_error(self, refs_env: RefsEnv) -> None:
        """POST на несуществующие весы → note «весы не найдены», не 500."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            "/panel/refs/scales/987654/settings",
            data=_settings_form(),
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "весы не найдены" in _settings_note(response, 987654)

    def test_post_saves_verification_and_page_shows_it(self, refs_env: RefsEnv) -> None:
        """Поверка сохраняется той же формой настроек и видна при GET."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(
                verif_number="№3961", verif_date="2026-02-26", verif_until="2027-02-26"
            ),
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "настройки сохранены" in _settings_note(response, refs_env.scale_id)
        with refs_env.factory() as session:
            scale = session.get(Scale, refs_env.scale_id)
            assert scale is not None
            assert scale.verif_number == "№3961"
            assert scale.verif_date == date(2026, 2, 26)
            assert scale.verif_until == date(2027, 2, 26)
        page = refs_env.client.get(f"/panel/refs/scales/{refs_env.scale_id}/settings").text
        assert "№3961" in page
        assert "2026-02-26" in page
        assert "2027-02-26" in page

    def test_post_bad_verification_dates_is_note(self, refs_env: RefsEnv) -> None:
        """Срок раньше даты поверки: note честно разделяет судьбы — цикл и порт
        сохранены (и уходят агенту), поверка нет (замечание ревью 13.08.2026)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/settings",
            data=_settings_form(
                verif_number="№3961", verif_date="2027-02-26", verif_until="2026-02-26"
            ),
            follow_redirects=False,
        )
        assert response.status_code == 303
        note = _settings_note(response, refs_env.scale_id)
        assert "цикла и порт сохранены" in note
        assert "НЕ сохранено" in note
        assert "раньше" in note
        with refs_env.factory() as session:
            scale = session.get(Scale, refs_env.scale_id)
            assert scale is not None
            assert scale.verif_number is None
            assert scale.thresholds is not None  # цикл в БД, несмотря на ошибку поверки

    def test_camera_post_pushes_settings_note(self, refs_env: RefsEnv) -> None:
        """Сохранение камеры с URL — часть настроек: note содержит хвост
        доставки (агент офлайн в тестовом хабе)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/camera",
            data={"role": "front", "snapshot_url": "http://10.0.0.5/snap", "rtsp_url": ""},
            follow_redirects=False,
        )
        assert response.status_code == 303
        note = _note_from(response)
        assert "камера сохранена" in note
        assert "офлайн" in note

    def test_camera_post_without_settings_no_push_tail(self, refs_env: RefsEnv) -> None:
        """Камера без URL и без прочих настроек: снимок пуст — хвоста
        доставки в note нет (агенту нечего слать)."""
        _login(refs_env, ADMIN_LOGIN, ADMIN_PASSWORD)
        response = refs_env.client.post(
            f"/panel/refs/scales/{refs_env.scale_id}/camera",
            data={"role": "front", "snapshot_url": "", "rtsp_url": ""},
            follow_redirects=False,
        )
        assert response.status_code == 303
        note = _note_from(response)
        assert "камера сохранена" in note
        assert "офлайн" not in note
