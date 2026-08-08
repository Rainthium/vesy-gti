"""Тесты фото-конвейера: агент (очередь файлов) → центр (приём/хранение/раздача).

Покрытие:
- canonical_photo_path / save_weighing_record: канонический путь
  /vesy/ГГГГ/ММ/ДД/<uuid.hex>_photoN.jpeg, дата в UTC, weighed_at=None,
  имя файла агента игнорируется;
- POST /agents/photos/{uuid}/{role}: аутентификация агента, неизвестные
  роль/uuid, идемпотентный приём байт-в-байт (правило №2), миниатюра,
  409 при несовпадении sha, приём «не-JPEG» тела с совпавшим sha,
  path traversal через злонамеренный путь в БД;
- GET /vesy/...: сервисный токен, IP-allowlist, отдача байт-в-байт,
  миниатюра, 404, traversal, запись в audit_log;
- сторона агента (SQLite): photos_to_upload / mark_photo_uploaded /
  pending_records с фото / photo_meta, охрана переходов uploaded;
- PhotoUploader против HTTP-стаба: 204/404/409, пропавший файл,
  недоступный сервер.

Инфраструктура БД центра — как в tests/test_center_ws.py (временная БД
ves_test_photos_<pid> + миграции alembic + TRUNCATE между тестами).
PhotoUploader тестируется против маленького http.server-стаба (быстро и
стабильно, реальный uvicorn не нужен).
"""

import asyncio
import hashlib
import logging
import os
import socket
import sqlite3
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from agent.sync.photo_uploader import PhotoUploader
from agent.sync.storage import AgentStorage, StoredPhoto, photo_meta
from center.db import repo
from center.db.models import Agent, AuditLog, Scale, ScaleKind, Site, Weighing, WeighingPhoto
from center.db.session import database_url, make_session_factory
from center.photos.router import PhotosConfig, create_photos_router
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import PhotoMeta, WeighingRecord
from tests.test_center_db import ALL_TABLES, _upgrade_head
from tools.dev_operator_ui import _GRAY_JPEG

AGENT_TOKEN = "agent-photo-token-01"
AGENT_AUTH = {"Authorization": f"Bearer {AGENT_TOKEN}"}
SERVICE_TOKEN = "tok-ais"
SERVICE_AUTH = {"Authorization": f"Bearer {SERVICE_TOKEN}"}

GRAY_SHA = hashlib.sha256(_GRAY_JPEG).hexdigest()
BISHKEK = timezone(timedelta(hours=6))  # Asia/Bishkek, UTC+6


# ---------------------------------------------------------------------------
# Хелперы построения записей
# ---------------------------------------------------------------------------


def make_record(**overrides: Any) -> WeighingRecord:
    """Типичная успешная запись взвешивания; overrides — точечные замены."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 15000.0,
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 10, 30, 15, tzinfo=UTC),
        "vehicle_number": "01KG123ABC",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def gray_meta(role: CameraRole = CameraRole.FRONT, filename: str = "agent_front.jpeg") -> PhotoMeta:
    """Метаданные серого JPEG-заглушки (sha настоящего тела)."""
    return PhotoMeta(role=role, filename=filename, sha256=GRAY_SHA, size_bytes=len(_GRAY_JPEG))


# ---------------------------------------------------------------------------
# canonical_photo_path — чистая функция, БД не нужна
# ---------------------------------------------------------------------------


class TestCanonicalPhotoPath:
    def test_format_front_and_rear(self) -> None:
        """front → _photo1, rear → _photo2; uuid — hex без дефисов; дата ГГГГ/ММ/ДД."""
        record = make_record(weighed_at=datetime(2026, 8, 7, 10, 30, tzinfo=UTC))
        front = repo.canonical_photo_path(record, CameraRole.FRONT)
        rear = repo.canonical_photo_path(record, CameraRole.REAR)
        assert front == f"/vesy/2026/08/07/{record.uuid.hex}_photo1.jpeg"
        assert rear == f"/vesy/2026/08/07/{record.uuid.hex}_photo2.jpeg"
        assert "-" not in front.rsplit("/", 1)[1]  # hex, не str(uuid)

    def test_date_taken_in_utc(self) -> None:
        """Дата пути — из UTC-момента: 01.01 03:00 по Бишкеку = 31.12 21:00 UTC."""
        record = make_record(weighed_at=datetime(2026, 1, 1, 3, 0, tzinfo=BISHKEK))
        path = repo.canonical_photo_path(record, CameraRole.FRONT)
        assert path.startswith("/vesy/2025/12/31/")

    def test_weighed_at_none_uses_current_utc_date(self) -> None:
        """weighed_at=None → текущая дата UTC, функция не падает."""
        record = make_record(weighed_at=None)
        before = datetime.now(UTC).strftime("%Y/%m/%d")
        path = repo.canonical_photo_path(record, CameraRole.REAR)
        after = datetime.now(UTC).strftime("%Y/%m/%d")
        assert any(path == f"/vesy/{day}/{record.uuid.hex}_photo2.jpeg" for day in {before, after})


# ---------------------------------------------------------------------------
# Инфраструктура БД центра: временная БД + миграции (подход test_center_ws.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def photos_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_photos_<pid>; имя не пересекается с другими
    модулями, чтобы прогоны не мешали друг другу."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты фото-конвейера центра пропущены"
        )

    db_name = f"ves_test_photos_{os.getpid()}"
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
def photos_db_engine(photos_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(photos_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


class TestAlembicLoggingSideEffect:
    def test_migrations_do_not_disable_app_loggers(self, photos_db_url: URL) -> None:
        """БАГ: прогон миграций отключает уже созданные логгеры приложения.

        center/db/migrations/env.py вызывает fileConfig(config.config_file_name)
        с дефолтным disable_existing_loggers=True → все существующие на тот
        момент логгеры (в т.ч. agent.sync.photo_uploader и логгеры центра)
        получают disabled=True и замолкают навсегда. Ошибки 409 «хеш не
        совпал» — сигнал о повреждении фото-доказательства — молча пропадают.
        Ожидаемо: fileConfig(..., disable_existing_loggers=False)."""
        import agent.sync.photo_uploader as photo_uploader_module

        photo_uploader_module.logger.disabled = False  # исходное состояние
        _upgrade_head(photos_db_url)  # повторный прогон — no-op для схемы
        disabled_after = photo_uploader_module.logger.disabled
        # не оставляем последствия бага другим тестам прогона
        photo_uploader_module.logger.disabled = False
        assert not disabled_after, (
            "alembic env.py (fileConfig) отключил логгер agent.sync.photo_uploader"
        )


@dataclass
class PhotoEnv:
    """Окружение теста: фабрика сессий, посеянные id и каталог хранилища."""

    factory: Callable[[], Session]
    scale_id: int
    agent_id: int
    photos_dir: Path


@pytest.fixture
def photo_env(photos_db_engine: Engine, tmp_path: Path) -> PhotoEnv:
    """Чистая БД + объект/весы/агент; photos_dir — подкаталог tmp_path,
    чтобы контролировать «выход наружу» при traversal-проверках."""
    with photos_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    factory = make_session_factory(photos_db_engine)
    with factory() as session:
        site = Site(code="test-site", name="Тестовый объект")
        session.add(site)
        session.flush()
        scale = Scale(site_id=site.id, name="Весы", kind=ScaleKind.STATIC, driver="cas22")
        session.add(scale)
        session.flush()
        agent = Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(AGENT_TOKEN))
        session.add(agent)
        session.flush()
        scale_id, agent_id = scale.id, agent.id
        session.commit()
    return PhotoEnv(factory, scale_id, agent_id, tmp_path / "photos")


def _make_client(
    env: PhotoEnv,
    *,
    allowed_ips: frozenset[str] | None = None,
    raise_server_exceptions: bool = True,
) -> TestClient:
    """Приложение центра с одним фото-роутером."""
    config = PhotosConfig(
        photos_dir=env.photos_dir,
        service_tokens={SERVICE_TOKEN: "ais"},
        allowed_ips=allowed_ips,
    )
    app = FastAPI()
    app.include_router(create_photos_router(env.factory, config))
    return TestClient(app, raise_server_exceptions=raise_server_exceptions)


def _seed_weighing(env: PhotoEnv, photos: list[PhotoMeta], **overrides: Any) -> WeighingRecord:
    """Запись с метаданными фото — как её создала бы досылка от агента."""
    record = make_record(**overrides)
    with env.factory() as session:
        assert repo.save_weighing_record(session, env.scale_id, record, photos) is True
    return record


def _stored_path(env: PhotoEnv, record: WeighingRecord, role: CameraRole) -> Path:
    """Файловый путь фото внутри photos_dir по каноническому пути из БД."""
    db_path = repo.canonical_photo_path(record, role)
    return env.photos_dir / db_path.lstrip("/")


# ---------------------------------------------------------------------------
# save_weighing_record: канонические пути в БД
# ---------------------------------------------------------------------------


class TestSaveWeighingRecordPhotoPaths:
    def test_agent_filename_ignored_paths_canonical(self, photo_env: PhotoEnv) -> None:
        """Имя файла агента (даже злонамеренное) не влияет на путь в БД."""
        photos = [
            gray_meta(CameraRole.FRONT, filename="..\\..\\evil.jpeg"),
            gray_meta(CameraRole.REAR, filename="/etc/passwd"),
        ]
        record = _seed_weighing(photo_env, photos)
        with photo_env.factory() as session:
            row = session.execute(select(Weighing).where(Weighing.uuid == record.uuid)).scalar_one()
            paths = (
                session.execute(
                    select(WeighingPhoto.path)
                    .where(WeighingPhoto.weighing_id == row.id)
                    .order_by(WeighingPhoto.id)
                )
                .scalars()
                .all()
            )
        assert paths == [
            f"/vesy/2026/08/07/{record.uuid.hex}_photo1.jpeg",
            f"/vesy/2026/08/07/{record.uuid.hex}_photo2.jpeg",
        ]


# ---------------------------------------------------------------------------
# POST /agents/photos/{uuid}/{role}
# ---------------------------------------------------------------------------


class TestUploadPhotoAuth:
    def test_no_token_401(self, photo_env: PhotoEnv) -> None:
        """Без Authorization → 401."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        response = client.post(f"/agents/photos/{record.uuid}/front", content=_GRAY_JPEG)
        assert response.status_code == 401

    def test_wrong_token_401(self, photo_env: PhotoEnv) -> None:
        """Неверный токен агента → 401."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        response = client.post(
            f"/agents/photos/{record.uuid}/front",
            content=_GRAY_JPEG,
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code == 401

    def test_unknown_role_404(self, photo_env: PhotoEnv) -> None:
        """Роль вне CameraRole (side) → 404."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        response = client.post(
            f"/agents/photos/{record.uuid}/side", content=_GRAY_JPEG, headers=AGENT_AUTH
        )
        assert response.status_code == 404

    def test_unknown_uuid_404(self, photo_env: PhotoEnv) -> None:
        """Неизвестный uuid записи → 404 (агент повторит после досылки)."""
        client = _make_client(photo_env)
        response = client.post(
            f"/agents/photos/{uuid4()}/front", content=_GRAY_JPEG, headers=AGENT_AUTH
        )
        assert response.status_code == 404


class TestUploadPhoto:
    def test_valid_upload_stores_bytes_and_thumbnail(self, photo_env: PhotoEnv) -> None:
        """204; файл лежит по каноническому пути байт-в-байт; миниатюра —
        существует и начинается с JPEG-магии FF D8."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        response = client.post(
            f"/agents/photos/{record.uuid}/front", content=_GRAY_JPEG, headers=AGENT_AUTH
        )
        assert response.status_code == 204

        stored = _stored_path(photo_env, record, CameraRole.FRONT)
        assert stored.is_file()
        assert stored.read_bytes() == _GRAY_JPEG, "оригинал изменён (правило №2)"

        thumb = stored.with_name(stored.stem + "_thumb" + stored.suffix)
        assert thumb.is_file(), "миниатюра не создана"
        assert thumb.read_bytes()[:2] == b"\xff\xd8", "миниатюра — не JPEG"

    def test_repeated_upload_idempotent_no_rewrite(self, photo_env: PhotoEnv) -> None:
        """Повтор того же тела → 204, файл не перезаписывается (mtime тот же)."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        url = f"/agents/photos/{record.uuid}/front"
        assert client.post(url, content=_GRAY_JPEG, headers=AGENT_AUTH).status_code == 204

        stored = _stored_path(photo_env, record, CameraRole.FRONT)
        mtime_before = os.stat(stored).st_mtime_ns
        assert client.post(url, content=_GRAY_JPEG, headers=AGENT_AUTH).status_code == 204
        assert os.stat(stored).st_mtime_ns == mtime_before, "повторная загрузка тронула файл"

    def test_mismatched_body_409_file_untouched(self, photo_env: PhotoEnv) -> None:
        """Тело с другим sha → 409; ни до, ни после приёма файл не подменяется."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        url = f"/agents/photos/{record.uuid}/front"
        stored = _stored_path(photo_env, record, CameraRole.FRONT)

        # до первой корректной загрузки: 409 и файла нет
        assert client.post(url, content=b"tampered", headers=AGENT_AUTH).status_code == 409
        assert not stored.exists()

        assert client.post(url, content=_GRAY_JPEG, headers=AGENT_AUTH).status_code == 204
        # после: 409 и содержимое не подменено
        assert client.post(url, content=b"tampered", headers=AGENT_AUTH).status_code == 409
        assert stored.read_bytes() == _GRAY_JPEG

    def test_non_jpeg_body_with_matching_sha_accepted(self, photo_env: PhotoEnv) -> None:
        """Не-JPEG тело с совпавшим sha принимается (204): центр хранит
        байт-в-байт то, что зафиксировано в записи; валидация JPEG — не его
        дело. Миниатюра при этом не строится (Pillow не открыл мусор), но
        оригинал не теряется — фиксируем фактическое поведение."""
        garbage = b"definitely not a jpeg"
        meta = PhotoMeta(
            role=CameraRole.FRONT,
            filename="front.jpeg",
            sha256=hashlib.sha256(garbage).hexdigest(),
            size_bytes=len(garbage),
        )
        record = _seed_weighing(photo_env, [meta])
        client = _make_client(photo_env)
        response = client.post(
            f"/agents/photos/{record.uuid}/front", content=garbage, headers=AGENT_AUTH
        )
        assert response.status_code == 204
        stored = _stored_path(photo_env, record, CameraRole.FRONT)
        assert stored.read_bytes() == garbage
        thumb = stored.with_name(stored.stem + "_thumb" + stored.suffix)
        assert not thumb.exists()  # миниатюры нет — оригинал важнее

    def test_traversal_path_in_db_does_not_escape_photos_dir(self, photo_env: PhotoEnv) -> None:
        """Злонамеренный путь с ../ в БД: файл вне photos_dir НЕ создаётся.

        Фактическое поведение: _file_path бросает ValueError, обработчик её
        не ловит → HTTP 500 (агент будет бесконечно повторять). Главное
        свойство — выхода из хранилища нет — выполняется."""
        record = _seed_weighing(photo_env, [gray_meta()])
        with photo_env.factory() as session:
            row = session.execute(select(Weighing).where(Weighing.uuid == record.uuid)).scalar_one()
            # прямой обход save_weighing_record: путь с ../ в weighing_photos
            session.add(
                WeighingPhoto(
                    weighing_id=row.id,
                    role=CameraRole.REAR,
                    path="/vesy/../../evil_photo.jpeg",
                    sha256=GRAY_SHA,
                    size_bytes=len(_GRAY_JPEG),
                )
            )
            session.commit()

        client = _make_client(photo_env, raise_server_exceptions=False)
        response = client.post(
            f"/agents/photos/{record.uuid}/rear", content=_GRAY_JPEG, headers=AGENT_AUTH
        )
        assert response.status_code == 500  # фактическое поведение (ValueError наружу)
        # photos_dir = tmp_path/photos → ../../evil уехал бы в tmp_path
        escaped = photo_env.photos_dir.parent / "evil_photo.jpeg"
        assert not escaped.exists(), "traversal создал файл вне photos_dir"


# ---------------------------------------------------------------------------
# GET /vesy/...
# ---------------------------------------------------------------------------


def _uploaded_env(photo_env: PhotoEnv) -> tuple[TestClient, WeighingRecord, Path]:
    """Загрузить фото в хранилище и вернуть клиент с allowlist=None."""
    record = _seed_weighing(photo_env, [gray_meta()])
    client = _make_client(photo_env)
    response = client.post(
        f"/agents/photos/{record.uuid}/front", content=_GRAY_JPEG, headers=AGENT_AUTH
    )
    assert response.status_code == 204
    return client, record, _stored_path(photo_env, record, CameraRole.FRONT)


def _db_url_of(record: WeighingRecord, role: CameraRole = CameraRole.FRONT) -> str:
    return repo.canonical_photo_path(record, role)


class TestServePhotoAuth:
    def test_no_token_401(self, photo_env: PhotoEnv) -> None:
        """Без токена раздача закрыта."""
        client, record, _ = _uploaded_env(photo_env)
        assert client.get(_db_url_of(record)).status_code == 401

    def test_wrong_token_401(self, photo_env: PhotoEnv) -> None:
        """Неизвестный сервисный токен → 401."""
        client, record, _ = _uploaded_env(photo_env)
        response = client.get(_db_url_of(record), headers={"Authorization": "Bearer wrong-token"})
        assert response.status_code == 401

    def test_ip_not_in_allowlist_403(self, photo_env: PhotoEnv) -> None:
        """Токен верный, но IP не в allowlist → 403 (TestClient ходит с
        адреса testclient)."""
        _, record, _ = _uploaded_env(photo_env)
        client = _make_client(photo_env, allowed_ips=frozenset({"10.0.0.1"}))
        assert client.get(_db_url_of(record), headers=SERVICE_AUTH).status_code == 403

    def test_ip_in_allowlist_ok(self, photo_env: PhotoEnv) -> None:
        """IP из allowlist пропускается."""
        _, record, _ = _uploaded_env(photo_env)
        client = _make_client(photo_env, allowed_ips=frozenset({"testclient"}))
        assert client.get(_db_url_of(record), headers=SERVICE_AUTH).status_code == 200

    def test_allowlist_none_any_ip_ok(self, photo_env: PhotoEnv) -> None:
        """allowed_ips=None (dev) — любой IP допущен."""
        client, record, _ = _uploaded_env(photo_env)
        assert client.get(_db_url_of(record), headers=SERVICE_AUTH).status_code == 200


class TestServePhoto:
    def test_serves_original_byte_for_byte(self, photo_env: PhotoEnv) -> None:
        """200, image/jpeg, тело байт-в-байт."""
        client, record, _ = _uploaded_env(photo_env)
        response = client.get(_db_url_of(record), headers=SERVICE_AUTH)
        assert response.status_code == 200
        assert response.headers["content-type"] == "image/jpeg"
        assert response.content == _GRAY_JPEG

    def test_serves_thumbnail(self, photo_env: PhotoEnv) -> None:
        """Миниатюра *_thumb.jpeg отдаётся тем же маршрутом."""
        client, record, stored = _uploaded_env(photo_env)
        thumb_url = _db_url_of(record).replace("_photo1.jpeg", "_photo1_thumb.jpeg")
        response = client.get(thumb_url, headers=SERVICE_AUTH)
        assert response.status_code == 200
        thumb = stored.with_name(stored.stem + "_thumb" + stored.suffix)
        assert response.content == thumb.read_bytes()

    def test_missing_file_404(self, photo_env: PhotoEnv) -> None:
        """Файл не загружен агентом → 404."""
        record = _seed_weighing(photo_env, [gray_meta()])
        client = _make_client(photo_env)
        assert client.get(_db_url_of(record), headers=SERVICE_AUTH).status_code == 404

    def test_traversal_404_and_no_leak(self, photo_env: PhotoEnv) -> None:
        """%2e%2e-traversal не выводит из photos_dir: 404, секрет не отдан."""
        client, _, _ = _uploaded_env(photo_env)
        secret = photo_env.photos_dir.parent / "secret.txt"
        secret.write_text("top-secret")
        response = client.get("/vesy/%2e%2e/%2e%2e/secret.txt", headers=SERVICE_AUTH)
        assert response.status_code == 404
        assert b"top-secret" not in response.content

    def test_download_is_audited(self, photo_env: PhotoEnv) -> None:
        """Скачивание журналируется: actor=integrator:ais, action=photo_download,
        в details — путь и IP клиента."""
        client, record, _ = _uploaded_env(photo_env)
        url = _db_url_of(record)
        assert client.get(url, headers=SERVICE_AUTH).status_code == 200
        with photo_env.factory() as session:
            rows = (
                session.execute(select(AuditLog).where(AuditLog.action == "photo_download"))
                .scalars()
                .all()
            )
        assert len(rows) == 1
        entry = rows[0]
        assert entry.actor == "integrator:ais"
        assert entry.details is not None
        assert entry.details["path"] == url
        assert entry.details["ip"] == "testclient"


# ---------------------------------------------------------------------------
# Сторона агента: очередь фото в SQLite
# ---------------------------------------------------------------------------


def _photo_for(path: str, role: CameraRole = CameraRole.FRONT) -> StoredPhoto:
    return StoredPhoto(role=role, path=path, sha256=GRAY_SHA, size_bytes=len(_GRAY_JPEG))


class TestAgentPhotoQueue:
    def test_only_photos_of_synced_records(self) -> None:
        """В очередь попадают только фото ДОСЛАННЫХ записей (uploaded=0)."""
        storage = AgentStorage(":memory:")
        synced, unsynced = make_record(), make_record()
        storage.save_weighing(synced, [_photo_for("C:/photos/a.jpeg")])
        storage.save_weighing(unsynced, [_photo_for("C:/photos/b.jpeg")])
        storage.mark_synced([synced.uuid])

        batch = storage.photos_to_upload()
        assert [(u, p.path) for u, p in batch] == [(synced.uuid, "C:/photos/a.jpeg")]

    def test_order_old_first_and_limit(self) -> None:
        """Старые записи первыми; limit ограничивает порцию."""
        storage = AgentStorage(":memory:")
        first, second = make_record(), make_record()
        storage.save_weighing(first, [_photo_for("1.jpeg")])
        # created_at в ISO с микросекундами — принудительно разносим записи
        with storage._conn:
            storage._conn.execute(
                "UPDATE weighings_local SET synced = synced"  # no-op, только для наглядности
            )
        storage.save_weighing(second, [_photo_for("2.jpeg")])
        storage.mark_synced([second.uuid, first.uuid])

        batch = storage.photos_to_upload()
        assert [p.path for _, p in batch] == ["1.jpeg", "2.jpeg"]
        assert [p.path for _, p in storage.photos_to_upload(limit=1)] == ["1.jpeg"]

    def test_both_roles_ordered_front_then_rear(self) -> None:
        """Обе камеры одной записи в очереди: front раньше rear (по роли)."""
        storage = AgentStorage(":memory:")
        record = make_record()
        storage.save_weighing(
            record,
            [_photo_for("r.jpeg", CameraRole.REAR), _photo_for("f.jpeg", CameraRole.FRONT)],
        )
        storage.mark_synced([record.uuid])
        batch = storage.photos_to_upload()
        assert [p.role for _, p in batch] == [CameraRole.FRONT, CameraRole.REAR]

    def test_mark_photo_uploaded_removes_from_queue_idempotent(self) -> None:
        """mark_photo_uploaded убирает фото из очереди; повторный вызов — не ошибка."""
        storage = AgentStorage(":memory:")
        record = make_record()
        storage.save_weighing(
            record,
            [_photo_for("f.jpeg", CameraRole.FRONT), _photo_for("r.jpeg", CameraRole.REAR)],
        )
        storage.mark_synced([record.uuid])

        storage.mark_photo_uploaded(record.uuid, CameraRole.FRONT)
        assert [p.role for _, p in storage.photos_to_upload()] == [CameraRole.REAR]
        storage.mark_photo_uploaded(record.uuid, CameraRole.FRONT)  # идемпотентно
        storage.mark_photo_uploaded(record.uuid, CameraRole.REAR)
        assert storage.photos_to_upload() == []

    def test_uploaded_transitions_guarded_by_db(self) -> None:
        """Прямой SQL: переход 0→1 разрешён, откат 1→0 — IntegrityError
        (триггер), значение вне {0,1} — IntegrityError (CHECK)."""
        storage = AgentStorage(":memory:")
        record = make_record()
        storage.save_weighing(record, [_photo_for("f.jpeg")])
        key = (str(record.uuid), CameraRole.FRONT.value)

        with storage._conn:  # 0 → 1 разрешён
            storage._conn.execute(
                "UPDATE weighing_photos_local SET uploaded = 1"
                " WHERE weighing_uuid = ? AND role = ?",
                key,
            )
        with pytest.raises(sqlite3.IntegrityError), storage._conn:  # откат 1 → 0
            storage._conn.execute(
                "UPDATE weighing_photos_local SET uploaded = 0"
                " WHERE weighing_uuid = ? AND role = ?",
                key,
            )
        with pytest.raises(sqlite3.IntegrityError), storage._conn:  # мусорное значение
            storage._conn.execute(
                "UPDATE weighing_photos_local SET uploaded = 2"
                " WHERE weighing_uuid = ? AND role = ?",
                key,
            )

    def test_pending_records_carry_photo_meta(self) -> None:
        """pending_records несёт метаданные фото; filename — имя файла без пути."""
        storage = AgentStorage(":memory:")
        record = make_record()
        storage.save_weighing(
            record,
            [
                _photo_for("C:/ves/photos/2026/front_1.jpeg", CameraRole.FRONT),
                _photo_for("C:/ves/photos/2026/rear_1.jpeg", CameraRole.REAR),
            ],
        )
        pending = storage.pending_records()
        assert len(pending) == 1
        photos = pending[0].photos
        assert [(p.role, p.filename, p.sha256, p.size_bytes) for p in photos] == [
            (CameraRole.FRONT, "front_1.jpeg", GRAY_SHA, len(_GRAY_JPEG)),
            (CameraRole.REAR, "rear_1.jpeg", GRAY_SHA, len(_GRAY_JPEG)),
        ]

    def test_photo_meta_conversion(self) -> None:
        """photo_meta: StoredPhoto → PhotoMeta, путь срезается до имени файла."""
        stored = StoredPhoto(
            role=CameraRole.REAR,
            path="C:\\ves\\photos\\rear_2.jpeg" if os.name == "nt" else "/ves/photos/rear_2.jpeg",
            sha256=GRAY_SHA,
            size_bytes=42,
        )
        meta = photo_meta(stored)
        assert meta == PhotoMeta(
            role=CameraRole.REAR, filename="rear_2.jpeg", sha256=GRAY_SHA, size_bytes=42
        )


# ---------------------------------------------------------------------------
# PhotoUploader против HTTP-стаба
# ---------------------------------------------------------------------------


class _StubServer(ThreadingHTTPServer):
    """HTTP-стаб центра: отвечает заданным кодом и копит входящие запросы."""

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, _StubHandler)
        self.requests: list[dict[str, Any]] = []
        self.response_status: int = 204


class _StubHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        server = cast(_StubServer, self.server)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        server.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
        self.send_response(server.response_status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        """Молчим: стаб не должен сорить в вывод pytest."""


@pytest.fixture
def stub_server() -> Iterator[_StubServer]:
    server = _StubServer(("127.0.0.1", 0))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _storage_with_photo(
    tmp_path: Path, *, file_exists: bool = True
) -> tuple[AgentStorage, WeighingRecord]:
    """Хранилище с одной досланной записью и одним фото на диске."""
    storage = AgentStorage(":memory:")
    record = make_record()
    photo_file = tmp_path / "front.jpeg"
    if file_exists:
        photo_file.write_bytes(_GRAY_JPEG)
    storage.save_weighing(record, [_photo_for(str(photo_file))])
    storage.mark_synced([record.uuid])
    return storage, record


def _make_uploader(storage: AgentStorage, base_url: str) -> PhotoUploader:
    return PhotoUploader(storage, base_url=base_url, token=AGENT_TOKEN, timeout_s=3.0)


class TestPhotoUploader:
    def test_success_204_uploads_and_marks(self, tmp_path: Path, stub_server: _StubServer) -> None:
        """204: корректные URL/заголовки/тело; фото помечено и ушло из очереди."""
        storage, record = _storage_with_photo(tmp_path)
        port = stub_server.server_address[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 1
        assert storage.photos_to_upload() == [], "принятое фото осталось в очереди"

        assert len(stub_server.requests) == 1
        sent = stub_server.requests[0]
        assert sent["path"] == f"/agents/photos/{record.uuid}/front"
        assert sent["headers"]["Authorization"] == f"Bearer {AGENT_TOKEN}"
        assert sent["headers"]["Content-Type"] == "image/jpeg"
        assert sent["body"] == _GRAY_JPEG  # файл байт-в-байт

    def test_404_keeps_photo_in_queue(self, tmp_path: Path, stub_server: _StubServer) -> None:
        """404 (центр ещё не знает запись) → фото остаётся, повтор позже."""
        storage, _ = _storage_with_photo(tmp_path)
        stub_server.response_status = 404
        port = stub_server.server_address[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert len(storage.photos_to_upload()) == 1

    def test_409_keeps_photo_and_logs_error(
        self, tmp_path: Path, stub_server: _StubServer, caplog: pytest.LogCaptureFixture
    ) -> None:
        """409 (хеш не совпал) → фото остаётся, в логе ERROR (молча терять
        фото-доказательство нельзя)."""
        storage, _ = _storage_with_photo(tmp_path)
        stub_server.response_status = 409
        port = stub_server.server_address[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        # обход бага: миграции alembic (fileConfig в env.py) отключают уже
        # созданные логгеры — см. TestAlembicLoggingSideEffect
        logging.getLogger("agent.sync.photo_uploader").disabled = False
        with caplog.at_level(logging.ERROR, logger="agent.sync.photo_uploader"):
            assert asyncio.run(uploader.upload_once()) == 0
        assert len(storage.photos_to_upload()) == 1
        assert any(
            r.levelno == logging.ERROR and "отверг" in r.getMessage() for r in caplog.records
        )

    def test_missing_file_does_not_crash(self, tmp_path: Path, stub_server: _StubServer) -> None:
        """Файл пропал с диска → без исключений, запросов нет, фото в очереди."""
        storage, _ = _storage_with_photo(tmp_path, file_exists=False)
        port = stub_server.server_address[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert stub_server.requests == []  # к центру даже не ходили
        assert len(storage.photos_to_upload()) == 1

    def test_unreachable_server_returns_zero(self, tmp_path: Path) -> None:
        """Закрытый порт → upload_once возвращает 0 без исключений."""
        storage, _ = _storage_with_photo(tmp_path)
        with socket.socket() as probe:  # свободный порт, на котором никто не слушает
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert len(storage.photos_to_upload()) == 1
