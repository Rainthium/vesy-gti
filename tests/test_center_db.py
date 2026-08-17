"""Тесты схемы БД центра (center/db): миграция, модели, неизменяемость, checksum.

Покрытие:
- миграция alembic: upgrade head на чистой БД, повторный upgrade (no-op),
  цикл downgrade base → upgrade head, наличие триггеров неизменяемости;
- модели и констрейнты: полный граф site→scale→cameras→agent→weighing+фото+тара,
  round-trip enum'ов и TIMESTAMPTZ, уникальность, внешние ключи, самоссылки;
- неизменяемость (правило №2): триггеры блокируют UPDATE/DELETE weighings и
  weighing_photos прямым SQL; сторно новой записью проходит; tare_registry,
  audit_log, agents, users — обновляемы;
- weighing_checksum: детерминированность, чувствительность к каждому полю,
  независимость от порядка фото, None-поля, нормализация часового пояса.

Требуется живой PostgreSQL (docker-контейнер ves-postgres, порт 5443, ves/ves;
в CI адрес приходит через DATABASE_URL). Если сервер недоступен — тесты,
зависящие от БД, пропускаются с понятным сообщением; тесты weighing_checksum
и session.py от БД не зависят и выполняются всегда.

Очистка между тестами — TRUNCATE ... CASCADE: в PostgreSQL TRUNCATE не
вызывает строчные триггеры ON DELETE, поэтому защита weighings/weighing_photos
от удаления очистке не мешает и отключать триггеры не требуется (это осознанный
выбор вместо ALTER TABLE ... DISABLE TRIGGER USER / пересоздания БД).
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, inspect, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from center.db.models import (
    Agent,
    AgentStatus,
    Camera,
    ReleaseChannel,
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
from center.db.session import (
    DEV_DATABASE_URL,
    database_url,
    make_engine,
    make_session_factory,
)
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource

REPO_ROOT = Path(__file__).resolve().parents[1]

# Все 12 таблиц схемы центра (architecture §5 + мониторинг этапа 2)
ALL_TABLES = (
    "agent_operators",
    "agent_releases",
    "audit_log",
    "monitoring_events",
    "sites",
    "scales",
    "users",
    "agents",
    "cameras",
    "weighings",
    "tare_registry",
    "weighing_photos",
    "weighing_ais_refs",
)

HEAD_REVISION = (
    "f6a7b8c9d0e1"  # контракт v2 с АИС: привязка объектов, номера документов (17.08.2026)
)

SHA_A = "a" * 64
SHA_B = "b" * 64

BISHKEK = timezone(timedelta(hours=6))  # Asia/Bishkek: UTC+6 без переводов


# ---------------------------------------------------------------------------
# Инфраструктура: тестовая БД + миграции
# ---------------------------------------------------------------------------


@contextmanager
def _database_url_env(url: URL) -> Iterator[None]:
    """Подменяет DATABASE_URL на время работы alembic (env.py читает его)."""
    prev = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = url.render_as_string(hide_password=False)
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = prev


def _alembic_config() -> Config:
    """Конфиг alembic с абсолютными путями — не зависит от cwd pytest."""
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "center" / "db" / "migrations"))
    return cfg


def _upgrade_head(url: URL) -> None:
    with _database_url_env(url):
        command.upgrade(_alembic_config(), "head")


def _downgrade_base(url: URL) -> None:
    with _database_url_env(url):
        command.downgrade(_alembic_config(), "base")


@pytest.fixture(scope="session")
def db_url() -> Iterator[URL]:
    """Создаёт одноразовую БД ves_test_<pid>, прогоняет миграции, в конце дропает.

    Если PostgreSQL недоступен (нет докера локально) — пропускаем все
    зависящие от БД тесты, а не роняем их.
    """
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты схемы БД центра пропущены"
        )

    db_name = f"ves_test_{os.getpid()}"
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
def db_engine(db_url: URL) -> Iterator[Engine]:
    """Движок к тестовой БД; NullPool — чтобы не держать соединения между
    тестами (мешают DROP DATABASE и циклам downgrade/upgrade)."""
    engine = create_engine(db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(db_engine: Engine) -> Iterator[Session]:
    """ORM-сессия с очисткой таблиц ПЕРЕД тестом (см. докстринг модуля)."""
    with db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    factory = make_session_factory(db_engine)
    session = factory()
    yield session
    session.rollback()
    session.close()


# ---------------------------------------------------------------------------
# Хелперы: построение графа данных
# ---------------------------------------------------------------------------


def _make_site(**overrides: Any) -> Site:
    fields: dict[str, Any] = {"code": "kyzyl-kyia", "name": "СВХ «Кызыл-Кыя»"}
    fields.update(overrides)
    return Site(**fields)


def _make_scale(site_id: int, **overrides: Any) -> Scale:
    fields: dict[str, Any] = {
        "site_id": site_id,
        "name": "Весы SCS-80",
        "kind": ScaleKind.STATIC,
        "driver": "cas22",
        "port_cfg": {"port": "COM3", "baudrate": 9600},
        "thresholds": {"empty_max_kg": 200},
        "legacy_ip": "10.77.1.10",
        "legacy_port": 2020,
        "legacy_autoscale": 1,
    }
    fields.update(overrides)
    return Scale(**fields)


def _make_weighing(scale_id: int, **overrides: Any) -> Weighing:
    """Типичная успешная запись взвешивания с корректной контрольной суммой."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "scale_id": scale_id,
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 12340.0,
        "unit": "kg",
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 10, 30, 15, 123456, tzinfo=UTC),
        "vehicle_number": "01KG123ABC",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    fields.setdefault(
        "checksum",
        weighing_checksum(
            uuid=fields["uuid"],
            operation=fields["operation"].value,
            code=fields["code"].value,
            massa=fields["massa"],
            weighed_at=fields["weighed_at"],
            vehicle_number=fields["vehicle_number"],
            source=fields["source"].value,
            photo_sha256s=[SHA_A, SHA_B],
        ),
    )
    return Weighing(**fields)


def _insert_graph(session: Session) -> tuple[Site, Scale, Weighing]:
    """Полный граф: объект → весы → камеры → агент → взвешивание + фото + тара."""
    site = _make_site()
    session.add(site)
    session.flush()

    scale = _make_scale(site.id)
    session.add(scale)
    session.flush()

    session.add_all(
        [
            Camera(scale_id=scale.id, role=CameraRole.FRONT, snapshot_url="http://cam1/snap"),
            Camera(scale_id=scale.id, role=CameraRole.REAR, rtsp_url="rtsp://cam2/stream"),
            Agent(
                scale_id=scale.id,
                token_hash="f" * 64,
                version="1.0.0",
                channel=ReleaseChannel.PILOT,
                status=AgentStatus.ONLINE,
                last_seen_at=datetime(2026, 8, 7, 10, 0, tzinfo=UTC),
            ),
        ]
    )

    weighing = _make_weighing(scale.id)
    session.add(weighing)
    session.flush()

    session.add_all(
        [
            WeighingPhoto(
                weighing_id=weighing.id,
                role=CameraRole.FRONT,
                path=f"/vesy/2026/08/07/{weighing.uuid}_photo1.jpeg",
                sha256=SHA_A,
                size_bytes=123_456,
            ),
            WeighingPhoto(
                weighing_id=weighing.id,
                role=CameraRole.REAR,
                path=f"/vesy/2026/08/07/{weighing.uuid}_photo2.jpeg",
                sha256=SHA_B,
                size_bytes=234_567,
            ),
            TareRegistry(
                vehicle_number="01KG123ABC",
                weighing_id=weighing.id,
                tare_value=7500.0,
                tared_at=datetime(2026, 8, 1, 9, 0, tzinfo=UTC),
            ),
        ]
    )
    session.commit()
    return site, scale, weighing


def _table_names(engine: Engine) -> set[str]:
    return set(inspect(engine).get_table_names())


def _trigger_names(engine: Engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT tgname FROM pg_trigger WHERE NOT tgisinternal"))
        return {row[0] for row in rows}


# ---------------------------------------------------------------------------
# Миграция
# ---------------------------------------------------------------------------


class TestMigration:
    """alembic upgrade/downgrade и объекты, создаваемые миграцией."""

    def test_upgrade_creates_all_tables(self, db_engine: Engine) -> None:
        """upgrade head на чистой БД создаёт все таблицы ALL_TABLES + alembic_version."""
        tables = _table_names(db_engine)
        assert set(ALL_TABLES) <= tables
        assert "alembic_version" in tables

    def test_upgrade_installs_immutability_triggers(self, db_engine: Engine) -> None:
        """Триггеры неизменяемости стоят на weighings и weighing_photos."""
        triggers = _trigger_names(db_engine)
        assert {"weighings_immutable", "weighing_photos_immutable"} <= triggers

    def test_repeated_upgrade_is_noop(self, db_url: URL, db_engine: Engine) -> None:
        """Повторный upgrade head ничего не ломает и не меняет ревизию."""
        before = _table_names(db_engine)
        _upgrade_head(db_url)  # не должен упасть (например, на CREATE TABLE)
        assert _table_names(db_engine) == before
        with db_engine.connect() as conn:
            version = conn.execute(text("SELECT version_num FROM alembic_version")).scalar()
        assert version == HEAD_REVISION

    def test_downgrade_upgrade_cycle(self, db_url: URL, db_engine: Engine) -> None:
        """downgrade base убирает таблицы/триггеры/функцию; upgrade возвращает."""
        _downgrade_base(db_url)
        tables_after_down = _table_names(db_engine)
        assert not (set(ALL_TABLES) & tables_after_down), "downgrade не удалил таблицы"
        # функция триггера тоже должна быть удалена — иначе повторный upgrade упадёт
        with db_engine.connect() as conn:
            leftover = conn.execute(
                text("SELECT count(*) FROM pg_proc WHERE proname = 'forbid_weighing_mutation'")
            ).scalar()
        assert leftover == 0, "downgrade оставил функцию forbid_weighing_mutation"

        _upgrade_head(db_url)
        assert set(ALL_TABLES) <= _table_names(db_engine)
        assert {"weighings_immutable", "weighing_photos_immutable"} <= _trigger_names(db_engine)


# ---------------------------------------------------------------------------
# Модели: полный граф и round-trip типов
# ---------------------------------------------------------------------------


class TestModelGraph:
    def test_full_graph_roundtrip(self, db_session: Session) -> None:
        """Вставка полного графа и чтение назад: enum'ы и TIMESTAMPTZ."""
        _, scale, weighing = _insert_graph(db_session)
        weighing_id = weighing.id
        db_session.expire_all()  # заставляем перечитать всё из БД, а не из кеша

        w = db_session.get(Weighing, weighing_id)
        assert w is not None
        # enum'ы возвращаются членами Python-enum, а не строками
        assert w.operation is Operation.WEIGHING
        assert w.code is ErrorCode.OK
        assert w.source is WeighingSource.AIS
        assert w.stable is True
        assert w.massa == 12340.0
        assert w.vehicle_number == "01KG123ABC"
        # TIMESTAMPTZ: назад приходит aware-datetime того же момента
        assert w.weighed_at is not None
        assert w.weighed_at.tzinfo is not None
        assert w.weighed_at == datetime(2026, 8, 7, 10, 30, 15, 123456, tzinfo=UTC)
        assert w.created_at.tzinfo is not None

        cameras = db_session.query(Camera).order_by(Camera.id).all()
        assert [c.role for c in cameras] == [CameraRole.FRONT, CameraRole.REAR]

        agent = db_session.query(Agent).one()
        assert agent.scale_id == scale.id
        assert agent.channel is ReleaseChannel.PILOT
        assert agent.status is AgentStatus.ONLINE

        db_scale = db_session.get(Scale, scale.id)
        assert db_scale is not None
        assert db_scale.kind is ScaleKind.STATIC
        assert db_scale.port_cfg == {"port": "COM3", "baudrate": 9600}

        photos = db_session.query(WeighingPhoto).order_by(WeighingPhoto.id).all()
        assert [p.sha256 for p in photos] == [SHA_A, SHA_B]
        assert all(p.weighing_id == weighing_id for p in photos)

        tare = db_session.get(TareRegistry, ("01KG123ABC", ""))
        assert tare is not None
        assert tare.weighing_id == weighing_id
        assert tare.tare_value == 7500.0
        assert tare.tared_at.tzinfo is not None

    def test_weighing_self_references(self, db_session: Session) -> None:
        """tare_weighing_id и storno_of ссылаются на другие записи weighings."""
        _, scale, _ = _insert_graph(db_session)
        tare = _make_weighing(scale.id, uuid=uuid4(), operation=Operation.TARING, massa=7500.0)
        db_session.add(tare)
        db_session.commit()

        brutto = _make_weighing(
            scale.id,
            uuid=uuid4(),
            tare_weighing_id=tare.id,
            tare_value=7500.0,
            netto=12340.0 - 7500.0,
        )
        db_session.add(brutto)
        db_session.commit()

        storno = _make_weighing(scale.id, uuid=uuid4(), storno_of=brutto.id)
        db_session.add(storno)
        db_session.commit()

        db_session.expire_all()
        loaded = db_session.get(Weighing, storno.id)
        assert loaded is not None
        assert loaded.storno_of == brutto.id
        loaded_brutto = db_session.get(Weighing, brutto.id)
        assert loaded_brutto is not None
        assert loaded_brutto.tare_weighing_id == tare.id


# ---------------------------------------------------------------------------
# Констрейнты: уникальность и внешние ключи
# ---------------------------------------------------------------------------


class TestConstraints:
    def test_duplicate_site_code_rejected(self, db_session: Session) -> None:
        """Дубль sites.code запрещён."""
        db_session.add(_make_site())
        db_session.commit()
        db_session.add(_make_site(name="Другое имя"))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_duplicate_weighing_uuid_rejected(self, db_session: Session) -> None:
        """Дубль weighings.uuid запрещён (идемпотентность досылки)."""
        _, scale, weighing = _insert_graph(db_session)
        db_session.add(_make_weighing(scale.id, uuid=weighing.uuid))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_duplicate_camera_role_rejected(self, db_session: Session) -> None:
        """Две камеры одной роли на одних весах запрещены."""
        _insert_graph(db_session)  # уже содержит front+rear
        scale = db_session.query(Scale).one()
        db_session.add(Camera(scale_id=scale.id, role=CameraRole.FRONT))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_second_agent_per_scale_rejected(self, db_session: Session) -> None:
        """Одни весы = один агент: второй агент на те же весы запрещён."""
        _insert_graph(db_session)
        scale = db_session.query(Scale).one()
        db_session.add(Agent(scale_id=scale.id, token_hash="e" * 64))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_duplicate_legacy_route_rejected(self, db_session: Session) -> None:
        """Дубль legacy-маршрута (ip+port+autoscale) запрещён — иначе API v1
        не сможет однозначно выбрать весы."""
        site = _make_site()
        db_session.add(site)
        db_session.flush()
        db_session.add(_make_scale(site.id))
        db_session.commit()
        db_session.add(_make_scale(site.id, name="Весы-дубль"))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_weighing_requires_existing_scale(self, db_session: Session) -> None:
        """FK: взвешивание с несуществующим scale_id отклоняется."""
        db_session.add(_make_weighing(scale_id=999_999))
        with pytest.raises(IntegrityError):
            db_session.commit()

    def test_tare_registry_requires_existing_weighing(self, db_session: Session) -> None:
        """FK: запись реестра тары должна ссылаться на реальное взвешивание."""
        db_session.add(
            TareRegistry(
                vehicle_number="01KG999XYZ",
                weighing_id=999_999,
                tare_value=7000.0,
                tared_at=datetime(2026, 8, 1, tzinfo=UTC),
            )
        )
        with pytest.raises(IntegrityError):
            db_session.commit()


# ---------------------------------------------------------------------------
# Неизменяемость (правило №2): триггеры против прямого SQL
# ---------------------------------------------------------------------------


class TestImmutability:
    @pytest.mark.parametrize(
        "set_clause",
        [
            "massa = massa + 1",
            "code = 'ERR_INTERNAL'",
            "vehicle_number = 'ПОДМЕНА'",
        ],
        ids=["massa", "code", "vehicle_number"],
    )
    def test_update_weighing_forbidden(self, db_session: Session, set_clause: str) -> None:
        """UPDATE любого поля weighings блокируется триггером."""
        _, _, weighing = _insert_graph(db_session)
        with pytest.raises(DBAPIError) as excinfo:
            db_session.execute(
                text(f"UPDATE weighings SET {set_clause} WHERE id = :id"),
                {"id": weighing.id},
            )
        assert "неизменяемости" in str(excinfo.value)

    def test_delete_weighing_forbidden(self, db_session: Session) -> None:
        """DELETE из weighings блокируется триггером."""
        _, _, weighing = _insert_graph(db_session)
        with pytest.raises(DBAPIError) as excinfo:
            db_session.execute(text("DELETE FROM weighings WHERE id = :id"), {"id": weighing.id})
        assert "неизменяемости" in str(excinfo.value)

    def test_update_photo_forbidden(self, db_session: Session) -> None:
        """UPDATE weighing_photos блокируется: sha256 фото подменить нельзя."""
        _insert_graph(db_session)
        with pytest.raises(DBAPIError) as excinfo:
            db_session.execute(text("UPDATE weighing_photos SET sha256 = :sha"), {"sha": "0" * 64})
        assert "неизменяемости" in str(excinfo.value)

    def test_delete_photo_forbidden(self, db_session: Session) -> None:
        """DELETE из weighing_photos блокируется."""
        _insert_graph(db_session)
        with pytest.raises(DBAPIError) as excinfo:
            db_session.execute(text("DELETE FROM weighing_photos"))
        assert "неизменяемости" in str(excinfo.value)

    def test_storno_insert_allowed(self, db_session: Session) -> None:
        """Сторнирование — INSERT новой записи со storno_of — проходит."""
        _, scale, weighing = _insert_graph(db_session)
        storno = _make_weighing(scale.id, uuid=uuid4(), code=ErrorCode.OK, storno_of=weighing.id)
        db_session.add(storno)
        db_session.commit()
        db_session.expire_all()
        count = db_session.execute(
            text("SELECT count(*) FROM weighings WHERE storno_of = :id"), {"id": weighing.id}
        ).scalar()
        assert count == 1

    def test_tare_registry_is_updatable(self, db_session: Session) -> None:
        """tare_registry — снимок активной тары, а не журнал: UPDATE разрешён."""
        _insert_graph(db_session)
        db_session.execute(
            text("UPDATE tare_registry SET tare_value = :v WHERE vehicle_number = :n"),
            {"v": 7600.0, "n": "01KG123ABC"},
        )
        db_session.commit()
        db_session.expire_all()
        tare = db_session.get(TareRegistry, ("01KG123ABC", ""))
        assert tare is not None
        assert tare.tare_value == 7600.0

    def test_mutable_tables_not_caught_by_trigger(self, db_session: Session) -> None:
        """audit_log/agents/users обновляемы — триггер их не зацепил."""
        _insert_graph(db_session)
        db_session.execute(
            text(
                "INSERT INTO audit_log (actor, action, at, details) "
                "VALUES ('qa', 'test', now(), NULL)"
            )
        )
        db_session.add(
            User(login="qa_user", pw_hash="x" * 64, full_name="QA", role=UserRole.OPERATOR)
        )
        db_session.commit()

        db_session.execute(text("UPDATE audit_log SET action = 'test2'"))
        db_session.execute(text("UPDATE agents SET status = 'offline'"))
        db_session.execute(text("UPDATE users SET is_active = false"))
        db_session.commit()

        db_session.expire_all()
        agent = db_session.query(Agent).one()
        assert agent.status is AgentStatus.OFFLINE
        user = db_session.query(User).filter_by(login="qa_user").one()
        assert user.is_active is False


# ---------------------------------------------------------------------------
# weighing_checksum (чистая функция, БД не нужна)
# ---------------------------------------------------------------------------


def _checksum_kwargs(**overrides: Any) -> dict[str, Any]:
    """Базовый набор аргументов weighing_checksum; overrides — точечные замены."""
    fields: dict[str, Any] = {
        "uuid": UUID("12345678-1234-5678-1234-567812345678"),
        "operation": "weighing",
        "code": "OK",
        "massa": 12340.0,
        "weighed_at": datetime(2026, 8, 7, 10, 30, 15, tzinfo=UTC),
        "vehicle_number": "01KG123ABC",
        "source": "ais",
        "photo_sha256s": [SHA_A, SHA_B],
    }
    fields.update(overrides)
    return fields


class TestWeighingChecksum:
    def test_deterministic(self) -> None:
        """Одинаковые аргументы всегда дают один и тот же sha256."""
        first = weighing_checksum(**_checksum_kwargs())
        second = weighing_checksum(**_checksum_kwargs())
        assert first == second
        assert len(first) == 64
        assert set(first) <= set("0123456789abcdef")

    @pytest.mark.parametrize(
        "override",
        [
            {"uuid": UUID("87654321-4321-8765-4321-876543218765")},
            {"operation": "taring"},
            {"code": "ERR_CAMERA"},
            {"massa": 12340.001},
            {"massa": None},
            {"weighed_at": datetime(2026, 8, 7, 10, 30, 16, tzinfo=UTC)},
            {"vehicle_number": "01KG999XYZ"},
            {"source": "local_offline"},
            {"photo_sha256s": [SHA_A, "c" * 64]},
            {"photo_sha256s": [SHA_A]},
        ],
        ids=[
            "uuid",
            "operation",
            "code",
            "massa",
            "massa_none",
            "weighed_at",
            "vehicle_number",
            "source",
            "photo_sha",
            "photo_missing",
        ],
    )
    def test_sensitive_to_each_field(self, override: dict[str, Any]) -> None:
        """Изменение любого поля меняет контрольную сумму."""
        base = weighing_checksum(**_checksum_kwargs())
        changed = weighing_checksum(**_checksum_kwargs(**override))
        assert base != changed

    def test_photo_order_does_not_matter(self) -> None:
        """Порядок photo_sha256s не влияет — внутри сортировка."""
        forward = weighing_checksum(**_checksum_kwargs(photo_sha256s=[SHA_A, SHA_B]))
        backward = weighing_checksum(**_checksum_kwargs(photo_sha256s=[SHA_B, SHA_A]))
        assert forward == backward

    def test_none_fields_do_not_crash(self) -> None:
        """None-поля (ошибка без веса/номера/времени) не роняют функцию."""
        checksum = weighing_checksum(
            **_checksum_kwargs(massa=None, weighed_at=None, vehicle_number=None)
        )
        assert len(checksum) == 64

    def test_timezone_normalized(self) -> None:
        """Один и тот же момент в разных поясах даёт один хеш (нормализация UTC)."""
        utc_dt = datetime(2026, 8, 7, 10, 30, 15, tzinfo=UTC)
        bishkek_dt = utc_dt.astimezone(BISHKEK)
        assert utc_dt == bishkek_dt  # тот же момент, другое представление
        assert weighing_checksum(**_checksum_kwargs(weighed_at=utc_dt)) == weighing_checksum(
            **_checksum_kwargs(weighed_at=bishkek_dt)
        )


# ---------------------------------------------------------------------------
# center/db/session.py (БД не нужна: движок создаётся без подключения)
# ---------------------------------------------------------------------------


class TestSessionModule:
    def test_database_url_prefers_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """DATABASE_URL из окружения имеет приоритет над dev-значением."""
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://u:p@host:5/db")
        assert database_url() == "postgresql+psycopg://u:p@host:5/db"

    def test_database_url_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Без DATABASE_URL возвращается dev-адрес docker-compose (порт 5443)."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        assert database_url() == DEV_DATABASE_URL

    def test_make_engine_uses_given_url(self) -> None:
        """make_engine принимает явный URL и включает pool_pre_ping."""
        engine = make_engine("postgresql+psycopg://u:p@host:5/db")
        assert engine.url.database == "db"
        assert engine.pool._pre_ping is True
        engine.dispose()

    def test_session_factory_binds_engine(self) -> None:
        """Фабрика сессий привязана к движку и не сбрасывает объекты на commit."""
        engine = make_engine("postgresql+psycopg://u:p@host:5/db")
        factory = make_session_factory(engine)
        session = factory()
        assert session.get_bind() is engine
        assert session.expire_on_commit is False
        session.close()
        engine.dispose()


class TestReviewFixes:
    """Тесты исправлений по находкам ревью схемы."""

    def test_legacy_route_null_autoscale_still_unique(self, db_session: Session) -> None:
        """Частичный индекс: дубль маршрута ip+port с autoscale IS NULL отбивается."""
        site = Site(code="s-null-route", name="Тест")
        db_session.add(site)
        db_session.flush()
        db_session.add(
            Scale(
                site_id=site.id,
                name="В1",
                kind=ScaleKind.STATIC,
                driver="cas22",
                legacy_ip="10.0.0.1",
                legacy_port=8087,
                legacy_autoscale=None,
            )
        )
        db_session.flush()
        db_session.add(
            Scale(
                site_id=site.id,
                name="В2",
                kind=ScaleKind.STATIC,
                driver="cas22",
                legacy_ip="10.0.0.1",
                legacy_port=8087,
                legacy_autoscale=None,
            )
        )
        with pytest.raises(IntegrityError):
            db_session.flush()
        db_session.rollback()

    def test_scales_without_legacy_route_are_unlimited(self, db_session: Session) -> None:
        """Весы совсем без legacy-маршрута (все NULL) — сколько угодно."""
        site = Site(code="s-no-route", name="Тест")
        db_session.add(site)
        db_session.flush()
        for i in range(3):
            db_session.add(
                Scale(site_id=site.id, name=f"В{i}", kind=ScaleKind.STATIC, driver="cas22")
            )
        db_session.flush()

    def test_checksum_rejects_naive_weighed_at(self) -> None:
        """Naive-время машинозависимо — контрольная сумма его отвергает."""
        with pytest.raises(ValueError, match="timezone-aware"):
            weighing_checksum(
                uuid=uuid4(),
                operation="weighing",
                code="OK",
                massa=100.0,
                weighed_at=datetime(2026, 8, 7, 12, 0, 0),  # без пояса
                vehicle_number="X",
                source="ais",
                photo_sha256s=[],
            )


class TestAisV2Repo:
    """Контракт v2 с АИС (17.08.2026): последнее тарирование сцепки на момент,
    сохранение записи с номером АИС, конфликты номера."""

    @staticmethod
    def _taring(scale_id: int, moment: datetime, **overrides: Any) -> Weighing:
        fields: dict[str, Any] = {
            "operation": Operation.TARING,
            "massa": 15300.0,
            "weighed_at": moment,
            "vehicle_number": "01KG777AAA",
            "trailer_number": "01KG500AB",
        }
        fields.update(overrides)
        return _make_weighing(scale_id, **fields)

    def _scale(self, session: Session) -> Scale:
        site = _make_site()
        session.add(site)
        session.flush()
        scale = _make_scale(site.id)
        session.add(scale)
        session.flush()
        return scale

    def test_latest_taring_as_of_picks_latest_not_later_than_moment(
        self, db_session: Session
    ) -> None:
        from center.db import repo

        scale = self._scale(db_session)
        moment = datetime(2026, 8, 14, 9, 30, tzinfo=UTC)
        older = self._taring(scale.id, datetime(2026, 5, 1, 9, 0, tzinfo=UTC))
        latest = self._taring(scale.id, datetime(2026, 7, 1, 9, 0, tzinfo=UTC))
        boundary = self._taring(scale.id, moment)  # ровно в момент — подходит (<=)
        later = self._taring(scale.id, moment + timedelta(seconds=1))
        db_session.add_all([older, latest, boundary, later])
        db_session.flush()
        found = repo.latest_taring_as_of(db_session, "01KG777AAA", "01KG500AB", moment)
        assert found is not None and found.uuid == boundary.uuid
        found = repo.latest_taring_as_of(
            db_session, "01KG777AAA", "01KG500AB", moment - timedelta(seconds=1)
        )
        assert found is not None and found.uuid == latest.uuid

    def test_latest_taring_as_of_respects_coupling(self, db_session: Session) -> None:
        """Тара — свойство сцепки: другой прицеп или его отсутствие не подходят."""
        from center.db import repo

        scale = self._scale(db_session)
        moment = datetime(2026, 8, 14, 9, 30, tzinfo=UTC)
        with_trailer = self._taring(scale.id, datetime(2026, 7, 1, 9, 0, tzinfo=UTC))
        solo = self._taring(scale.id, datetime(2026, 7, 2, 9, 0, tzinfo=UTC), trailer_number=None)
        db_session.add_all([with_trailer, solo])
        db_session.flush()
        assert repo.latest_taring_as_of(db_session, "01KG777AAA", "01KG999ZZ", moment) is None
        found = repo.latest_taring_as_of(db_session, "01KG777AAA", None, moment)
        assert found is not None and found.uuid == solo.uuid
        found = repo.latest_taring_as_of(db_session, "01KG777AAA", " 01kg500ab ", moment)
        assert found is not None and found.uuid == with_trailer.uuid
        # пустая строка прицепа считается «без прицепа»
        found = repo.latest_taring_as_of(db_session, "01KG777AAA", "", moment)
        assert found is not None and found.uuid == solo.uuid

    def test_latest_taring_as_of_ignores_weighings_and_other_vehicles(
        self, db_session: Session
    ) -> None:
        from center.db import repo

        scale = self._scale(db_session)
        moment = datetime(2026, 8, 14, 9, 30, tzinfo=UTC)
        db_session.add_all(
            [
                _make_weighing(
                    scale.id,
                    weighed_at=datetime(2026, 7, 1, tzinfo=UTC),
                    vehicle_number="01KG777AAA",
                    trailer_number="01KG500AB",
                ),
                self._taring(
                    scale.id, datetime(2026, 7, 1, tzinfo=UTC), vehicle_number="01KG000BBB"
                ),
            ]
        )
        db_session.flush()
        assert repo.latest_taring_as_of(db_session, "01KG777AAA", "01KG500AB", moment) is None

    def test_save_with_taken_ais_ref_keeps_record_without_link(self, db_session: Session) -> None:
        """Занятый номер АИС не роняет сохранение: запись есть, связки нет."""
        from center.db import repo
        from shared.messages import WeighingRecord

        scale = self._scale(db_session)
        db_session.commit()

        def record() -> WeighingRecord:
            return WeighingRecord(
                uuid=uuid4(),
                operation=Operation.WEIGHING,
                code=ErrorCode.OK,
                massa=1000.0,
                weighed_at=datetime(2026, 8, 14, 9, 30, tzinfo=UTC),
                vehicle_number="01KG777AAA",
                source=WeighingSource.AIS,
            )

        first, second = record(), record()
        assert repo.save_weighing_record(db_session, scale.id, first, ais_ref="WEI000000001")
        assert repo.save_weighing_record(db_session, scale.id, second, ais_ref="WEI000000001")
        linked = repo.weighing_by_ais_ref(db_session, "WEI000000001")
        assert linked is not None and linked.uuid == first.uuid
        assert repo.ais_refs_for(db_session, [linked.id]) == {linked.id: "WEI000000001"}
        second_row = db_session.execute(
            select(Weighing).where(Weighing.uuid == second.uuid)
        ).scalar_one()
        assert repo.ais_refs_for(db_session, [second_row.id]) == {}
        # обратный вызов тем же номером на вторую — конфликт, на первую — same
        assert (
            repo.link_ais_ref(db_session, second_row, "WEI000000001", origin="callback")
            == "conflict"
        )
        assert repo.link_ais_ref(db_session, linked, "WEI000000001", origin="callback") == "same"
        assert (
            repo.link_ais_ref(db_session, second_row, "WEI000000002", origin="callback") == "linked"
        )
