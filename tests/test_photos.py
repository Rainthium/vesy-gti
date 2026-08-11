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
- очередь загрузки с повторами (11.08.2026): пауза после неудачи растёт
  вдвое до потолка, застрявший снимок уступает голову очереди свежему,
  но НИКОГДА не выбрасывается; статистика очереди photo_queue_stats;
- PhotoUploader против HTTP-стаба: 204/404/409, пропавший файл,
  недоступный сервер, учёт попыток, единственное предупреждение о
  застрявшем снимке и периодическая сводка по очереди.

Инфраструктура БД центра — как в tests/test_center_ws.py (временная БД
ves_test_photos_<pid> + миграции alembic + TRUNCATE между тестами).
PhotoUploader тестируется против маленького http.server-стаба (быстро и
стабильно, реальный uvicorn не нужен).
"""

import asyncio
import contextlib
import hashlib
import logging
import os
import socket
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

import agent.sync.photo_uploader as photo_uploader_module
from agent.sync.photo_uploader import (
    STUCK_AFTER_ATTEMPTS,
    PhotoUploader,
)
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
# Очередь загрузки с повторами: паузы, ротация, «не сдаёмся» (11.08.2026)
# ---------------------------------------------------------------------------

QUEUE_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)  # опорный момент тестов очереди
RETRY_BASE_S = 15.0
RETRY_MAX_S = 1800.0


def _photo_row(
    storage: AgentStorage, weighing_uuid: UUID, role: CameraRole = CameraRole.FRONT
) -> sqlite3.Row:
    """Служебные поля снимка напрямую из БД (attempts, паузы, отметки)."""
    row: sqlite3.Row | None = storage._conn.execute(
        "SELECT * FROM weighing_photos_local WHERE weighing_uuid = ? AND role = ?",
        (str(weighing_uuid), role.value),
    ).fetchone()
    assert row is not None, "строка снимка исчезла из журнала"
    return row


def _pause_seconds(storage: AgentStorage, weighing_uuid: UUID, since: datetime) -> float:
    """Назначенная пауза до следующей попытки, в секундах от ``since``."""
    next_attempt_at = str(_photo_row(storage, weighing_uuid)["next_attempt_at"])
    return (datetime.fromisoformat(next_attempt_at) - since).total_seconds()


def _queued_photo(storage: AgentStorage, path: str, role: CameraRole = CameraRole.FRONT) -> UUID:
    """Досланная запись с одним снимком в очереди загрузки."""
    record = make_record()
    storage.save_weighing(record, [_photo_for(path, role)])
    storage.mark_synced([record.uuid])
    time.sleep(0.002)  # created_at записей должен различаться (порядок очереди)
    return record.uuid


def _fail(storage: AgentStorage, weighing_uuid: UUID, *, now: datetime, times: int = 1) -> int:
    attempts = 0
    for _ in range(times):
        attempts = storage.mark_photo_failed(
            weighing_uuid,
            CameraRole.FRONT,
            base_delay_s=RETRY_BASE_S,
            max_delay_s=RETRY_MAX_S,
            now=now,
        )
    return attempts


class TestPhotoRetryQueue:
    """mark_photo_failed / photos_to_upload: паузы и порядок выборки."""

    def test_absurd_pause_from_clock_jump_is_retried(self) -> None:
        """Часы ПК ушли вперёд → пауза оказалась в далёком будущем.

        Часы весовых ПК никто не обслуживает (agent/clock.py), и снимок с
        такой отметкой не уехал бы никогда. Отметку дальше предельной
        паузы считаем следом скачка и берём снимок в работу снова
        (замечание ревью 11.08.2026).
        """
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        # неудача при часах, убежавших на десять лет вперёд
        _fail(storage, uuid, now=QUEUE_NOW + timedelta(days=3650))

        assert storage.photos_to_upload(now=QUEUE_NOW) == [], "без потолка пауза считается честной"
        assert len(storage.photos_to_upload(now=QUEUE_NOW, max_pause_s=RETRY_MAX_S * 2)) == 1
        storage.close()

    def test_normal_pause_respected_with_max_pause(self) -> None:
        """Обычная пауза при заданном потолке по-прежнему соблюдается."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=QUEUE_NOW)

        assert storage.photos_to_upload(now=QUEUE_NOW, max_pause_s=RETRY_MAX_S * 2) == []
        assert (
            len(
                storage.photos_to_upload(
                    now=QUEUE_NOW + timedelta(seconds=15), max_pause_s=RETRY_MAX_S * 2
                )
            )
            == 1
        )
        storage.close()

    def test_failed_photo_hidden_until_pause_ends(self) -> None:
        """После неудачи снимок пропадает из порции до конца паузы; момент
        назначенной попытки включительно — снова в очереди."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        assert _fail(storage, uuid, now=QUEUE_NOW) == 1

        assert storage.photos_to_upload(now=QUEUE_NOW) == []
        assert storage.photos_to_upload(now=QUEUE_NOW + timedelta(seconds=14)) == []
        assert len(storage.photos_to_upload(now=QUEUE_NOW + timedelta(seconds=15))) == 1
        storage.close()

    def test_pause_doubles_up_to_ceiling(self) -> None:
        """Пауза удваивается с каждой неудачей и упирается в max_delay_s."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        delays = []
        for expected_attempts in range(1, 10):
            assert _fail(storage, uuid, now=QUEUE_NOW) == expected_attempts
            delays.append(_pause_seconds(storage, uuid, QUEUE_NOW))
        # 15 с, 30 с, 1 мин … 30 мин и дальше ровно потолок
        assert delays == [15, 30, 60, 120, 240, 480, 960, 1800, 1800]
        storage.close()

    def test_stuck_photo_yields_head_of_queue_to_fresh_one(self) -> None:
        """Главное свойство ротации: вечно падающий снимок не держит голову
        очереди — свежий уезжает первым, даже когда пауза битого истекла."""
        storage = AgentStorage(":memory:")
        stuck = _queued_photo(storage, "stuck.jpeg")
        _fail(storage, stuck, now=QUEUE_NOW, times=3)
        fresh = _queued_photo(storage, "fresh.jpeg")  # запись СВЕЖЕЕ битой

        later = QUEUE_NOW + timedelta(hours=1)  # пауза битого давно прошла
        batch = storage.photos_to_upload(now=later)
        assert [p.path for _, p in batch] == ["fresh.jpeg", "stuck.jpeg"]
        # порция на одно фото достаётся свежему снимку, а не битому
        assert [u for u, _ in storage.photos_to_upload(limit=1, now=later)] == [fresh]
        storage.close()

    def test_photo_is_never_dropped_from_queue(self) -> None:
        """Сколько бы попыток ни было, снимок остаётся в очереди: это
        доказательство операции, терять его нельзя."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        assert _fail(storage, uuid, now=QUEUE_NOW, times=50) == 50

        after_pause = QUEUE_NOW + timedelta(seconds=RETRY_MAX_S)
        assert [u for u, _ in storage.photos_to_upload(now=after_pause)] == [uuid]
        storage.close()

    def test_parallel_failures_lose_no_attempts(self) -> None:
        """Попытки считаются под локом: загрузчик работает в отдельном потоке
        рядом с потоком цикла взвешивания, потерянных обновлений быть не должно."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        errors: list[Exception] = []

        def worker() -> None:
            try:
                for _ in range(10):
                    _fail(storage, uuid, now=QUEUE_NOW)
            except Exception as exc:  # ошибку потока показываем в отчёте теста
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert _photo_row(storage, uuid)["attempts"] == 80
        storage.close()

    def test_unknown_photo_returns_zero_attempts(self) -> None:
        """Неудача по исчезнувшей строке не падает и не считается попыткой."""
        storage = AgentStorage(":memory:")
        assert (
            storage.mark_photo_failed(
                uuid4(), CameraRole.FRONT, base_delay_s=RETRY_BASE_S, max_delay_s=RETRY_MAX_S
            )
            == 0
        )
        storage.close()

    def test_upload_clears_pause_and_stamps_time(self) -> None:
        """Успех после неудач: снимок уходит из очереди, пауза снята,
        проставлено время подтверждения (по нему считает ретеншн)."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=QUEUE_NOW, times=2)
        storage.mark_photo_uploaded(uuid, CameraRole.FRONT, now=QUEUE_NOW)

        row = _photo_row(storage, uuid)
        assert row["uploaded"] == 1
        assert row["next_attempt_at"] is None
        assert row["uploaded_at"] == QUEUE_NOW.isoformat()
        assert storage.photos_to_upload(now=QUEUE_NOW + timedelta(days=1)) == []
        storage.close()

    def test_repeat_upload_keeps_first_confirmation_time(self) -> None:
        """Повторная пометка не сдвигает uploaded_at: срок ретеншна считается
        от первого подтверждения, а не от последнего вызова."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        storage.mark_photo_uploaded(uuid, CameraRole.FRONT, now=QUEUE_NOW)
        storage.mark_photo_uploaded(uuid, CameraRole.FRONT, now=QUEUE_NOW + timedelta(days=30))
        assert _photo_row(storage, uuid)["uploaded_at"] == QUEUE_NOW.isoformat()
        storage.close()

    def test_pause_survives_thousands_of_attempts(self) -> None:
        """Снимок, падающий месяцами, не роняет учёт попыток.

        При потолке 30 минут 1025 неудач набегают примерно за три недели
        офлайна или битого файла — на объекте это реально. Показатель
        двойки ограничен, иначе множитель переставал помещаться во float
        и mark_photo_failed бросал OverflowError (баг найден qa-tester
        11.08.2026): пауза не продлевалась, и снимок снова опрашивался
        каждые 5 секунд.
        """
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        with storage._conn:  # быстрая перемотка: 1024 неудачи уже позади
            storage._conn.execute(
                "UPDATE weighing_photos_local SET attempts = 1024 WHERE weighing_uuid = ?",
                (str(uuid),),
            )
        try:
            assert _fail(storage, uuid, now=QUEUE_NOW) == 1025
            assert _pause_seconds(storage, uuid, QUEUE_NOW) == RETRY_MAX_S
        finally:
            storage.close()

    def test_pause_compares_moments_not_strings(self) -> None:
        """Тот же момент в бишкекском поясе — пауза ещё идёт.

        next_attempt_at сравнивается в SQL как текст, поэтому все служебные
        времена приводятся к UTC: иначе «12:00+00:00» и равный ему
        «18:00+06:00» давали бы разный результат и снимок с паузой
        выдавался бы к загрузке раньше срока (баг найден qa-tester).
        """
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=QUEUE_NOW)
        try:
            assert storage.photos_to_upload(now=QUEUE_NOW.astimezone(BISHKEK)) == []
        finally:
            storage.close()

    @pytest.mark.parametrize(
        "moment",
        [
            QUEUE_NOW,
            QUEUE_NOW.astimezone(BISHKEK),  # тот же момент в бишкекском поясе
            QUEUE_NOW.replace(tzinfo=None),  # наивное время — тоже UTC
        ],
        ids=["utc", "bishkek", "naive"],
    )
    def test_pause_written_in_utc(self, moment: datetime) -> None:
        """Пауза всегда пишется в UTC, каким бы поясом ни пришёл момент:
        строки в БД сравниваются посимвольно, разнобой ломал бы очередь."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=moment)

        next_attempt_at = str(_photo_row(storage, uuid)["next_attempt_at"])
        assert next_attempt_at == (QUEUE_NOW + timedelta(seconds=RETRY_BASE_S)).isoformat()
        assert storage.photos_to_upload(now=QUEUE_NOW + timedelta(seconds=14)) == []
        assert len(storage.photos_to_upload(now=QUEUE_NOW + timedelta(seconds=15))) == 1
        storage.close()


class TestPhotoQueueStats:
    """photo_queue_stats: сводка «сколько ждёт и сколько застряло»."""

    def test_counts_pending_and_stuck(self) -> None:
        """Застрявшим считается снимок с attempts >= порога; остальные —
        просто в ожидании."""
        storage = AgentStorage(":memory:")
        stuck = _queued_photo(storage, "stuck.jpeg")
        _fail(storage, stuck, now=QUEUE_NOW, times=5)
        _queued_photo(storage, "fresh.jpeg")

        assert storage.photo_queue_stats(stuck_after=5) == (2, 1)
        assert storage.photo_queue_stats(stuck_after=6) == (2, 0)
        storage.close()

    def test_paused_photo_still_counted(self) -> None:
        """Снимок в паузе не виден очереди, но в сводке остаётся — иначе
        диспетчер решил бы, что всё уехало."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=datetime.now(UTC))
        assert storage.photos_to_upload() == []
        assert storage.photo_queue_stats() == (1, 0)
        storage.close()

    def test_uploaded_and_unsynced_are_not_counted(self) -> None:
        """Сводка про очередь загрузки: принятые центром и ещё не досланные
        записи в неё не входят."""
        storage = AgentStorage(":memory:")
        uploaded = _queued_photo(storage, "done.jpeg")
        storage.mark_photo_uploaded(uploaded, CameraRole.FRONT, now=QUEUE_NOW)
        unsynced = make_record()
        storage.save_weighing(unsynced, [_photo_for("pending.jpeg")])

        assert storage.photo_queue_stats() == (0, 0)
        storage.close()

    def test_empty_queue_gives_zeros(self) -> None:
        """Пустая очередь — (0, 0), а не падение на SUM(NULL)."""
        storage = AgentStorage(":memory:")
        assert storage.photo_queue_stats() == (0, 0)
        storage.close()

    def test_default_threshold_matches_uploader_constant(self) -> None:
        """Порог «застрял» в хранилище и в загрузчике — одно и то же число:
        снимок, о котором предупредил загрузчик, виден и в сводке."""
        storage = AgentStorage(":memory:")
        uuid = _queued_photo(storage, "f.jpeg")
        _fail(storage, uuid, now=QUEUE_NOW, times=STUCK_AFTER_ATTEMPTS)
        assert storage.photo_queue_stats() == (1, 1)
        storage.close()


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


def _later(seconds: float = 60.0) -> datetime:
    """Момент после паузы повтора: неудачное фото снова видно очереди."""
    return datetime.now(UTC) + timedelta(seconds=seconds)


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
        """404 (центр ещё не знает запись) → фото остаётся, повтор после паузы."""
        storage, _ = _storage_with_photo(tmp_path)
        stub_server.response_status = 404
        port = stub_server.server_address[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert storage.photos_to_upload() == [], "неудачное фото не ушло в паузу"
        assert len(storage.photos_to_upload(now=_later())) == 1

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
        assert len(storage.photos_to_upload(now=_later())) == 1
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
        assert len(storage.photos_to_upload(now=_later())) == 1

    def test_unreachable_server_returns_zero(self, tmp_path: Path) -> None:
        """Закрытый порт → upload_once возвращает 0 без исключений."""
        storage, _ = _storage_with_photo(tmp_path)
        with socket.socket() as probe:  # свободный порт, на котором никто не слушает
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        uploader = _make_uploader(storage, f"http://127.0.0.1:{port}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert len(storage.photos_to_upload(now=_later())) == 1


# ---------------------------------------------------------------------------
# PhotoUploader: повторы, ротация очереди и сводки в лог
# ---------------------------------------------------------------------------


@pytest.fixture
def uploader_logger() -> logging.Logger:
    """Логгер загрузчика с принудительно снятым disabled.

    Обход бага миграций alembic (fileConfig в env.py глушит уже созданные
    логгеры) — см. TestAlembicLoggingSideEffect.
    """
    logger = logging.getLogger("agent.sync.photo_uploader")
    logger.disabled = False
    return logger


def _instant_uploader(storage: AgentStorage, base_url: str, *, batch: int = 4) -> PhotoUploader:
    """Загрузчик без пауз между попытками: в тестах важен порядок, не время."""
    return PhotoUploader(
        storage,
        base_url=base_url,
        token=AGENT_TOKEN,
        timeout_s=3.0,
        batch=batch,
        retry_base_s=0.0,
        retry_max_s=0.0,
    )


def _dead_port() -> int:
    """Порт, на котором заведомо никто не слушает (центр недоступен)."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class TestPhotoUploaderRetries:
    """Учёт попыток, ротация очереди и предупреждение о застрявшем снимке."""

    def test_failure_counts_attempt(self, tmp_path: Path, stub_server: _StubServer) -> None:
        """Каждая неудача — попытка в БД (по ней растёт пауза и порядок)."""
        storage, record = _storage_with_photo(tmp_path)
        stub_server.response_status = 500
        uploader = _instant_uploader(storage, f"http://127.0.0.1:{stub_server.server_address[1]}")

        assert asyncio.run(uploader.upload_once()) == 0
        assert _photo_row(storage, record.uuid)["attempts"] == 1
        assert asyncio.run(uploader.upload_once()) == 0
        assert _photo_row(storage, record.uuid)["attempts"] == 2

    def test_stuck_photo_warned_exactly_once(
        self,
        tmp_path: Path,
        stub_server: _StubServer,
        uploader_logger: logging.Logger,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Про застрявший снимок предупреждаем ровно один раз (на пятой
        попытке), дальше он молча ждёт длинных пауз."""
        storage, record = _storage_with_photo(tmp_path)
        stub_server.response_status = 409  # битый файл: так будет всегда
        uploader = _instant_uploader(storage, f"http://127.0.0.1:{stub_server.server_address[1]}")

        with caplog.at_level(logging.WARNING, logger="agent.sync.photo_uploader"):
            for _ in range(8):
                assert asyncio.run(uploader.upload_once()) == 0

        stuck_warnings = [r for r in caplog.records if "не уходит в центр" in r.getMessage()]
        assert len(stuck_warnings) == 1, "предупреждение о застрявшем фото дублируется"
        assert stuck_warnings[0].levelno == logging.WARNING
        assert f"({STUCK_AFTER_ATTEMPTS} попыток)" in stuck_warnings[0].getMessage()
        assert str(record.uuid) in stuck_warnings[0].getMessage()
        # и снимок по-прежнему в очереди: сдаваться нельзя
        assert _photo_row(storage, record.uuid)["attempts"] == 8
        assert len(storage.photos_to_upload(now=_later())) == 1

    def test_fresh_photo_overtakes_broken_one(
        self, tmp_path: Path, stub_server: _StubServer
    ) -> None:
        """Битый снимок (файл пропал) не занимает порцию каждые 5 секунд:
        следующая порция достаётся свежему снимку, и тот уезжает в центр."""
        storage = AgentStorage(":memory:")
        broken_path = str(tmp_path / "потерян.jpeg")  # файла на диске нет
        broken = _queued_photo(storage, broken_path)
        fresh_file = tmp_path / "fresh.jpeg"
        fresh_file.write_bytes(_GRAY_JPEG)
        fresh = _queued_photo(storage, str(fresh_file))
        uploader = _instant_uploader(
            storage, f"http://127.0.0.1:{stub_server.server_address[1]}", batch=1
        )

        assert asyncio.run(uploader.upload_once()) == 0  # порция ушла на битый снимок
        assert asyncio.run(uploader.upload_once()) == 1  # свежий обогнал битый

        assert [r["path"] for r in stub_server.requests] == [f"/agents/photos/{fresh}/front"]
        assert [p.path for _, p in storage.photos_to_upload(now=_later())] == [broken_path]
        assert _photo_row(storage, broken)["uploaded"] == 0
        storage.close()

    def test_never_gives_up_and_uploads_after_recovery(
        self, tmp_path: Path, stub_server: _StubServer
    ) -> None:
        """Двадцать неудач подряд не выбрасывают снимок: как только центр
        ожил, доказательство операции уезжает."""
        storage, record = _storage_with_photo(tmp_path)
        stub_server.response_status = 500
        uploader = _instant_uploader(storage, f"http://127.0.0.1:{stub_server.server_address[1]}")

        for _ in range(20):
            assert asyncio.run(uploader.upload_once()) == 0
        assert _photo_row(storage, record.uuid)["attempts"] == 20

        stub_server.response_status = 204
        assert asyncio.run(uploader.upload_once()) == 1
        assert storage.photos_to_upload(now=_later()) == []
        assert _photo_row(storage, record.uuid)["uploaded_at"] is not None

    def test_failure_does_not_abort_rest_of_batch(
        self, tmp_path: Path, stub_server: _StubServer
    ) -> None:
        """Битый снимок в голове порции не отменяет загрузку остальных:
        неудача одного файла не задерживает уже готовые."""
        storage = AgentStorage(":memory:")
        broken_path = str(tmp_path / "потерян.jpeg")  # файла на диске нет
        _queued_photo(storage, broken_path)
        for name in ("a.jpeg", "b.jpeg"):
            good = tmp_path / name
            good.write_bytes(_GRAY_JPEG)
            _queued_photo(storage, str(good))
        uploader = _instant_uploader(
            storage, f"http://127.0.0.1:{stub_server.server_address[1]}", batch=3
        )

        assert asyncio.run(uploader.upload_once()) == 2
        assert [p.path for _, p in storage.photos_to_upload(now=_later())] == [broken_path]
        storage.close()


class TestPhotoUploaderQueueStats:
    """Периодическая сводка по очереди (STATS_EVERY_CYCLES)."""

    def test_stats_warn_about_stuck_photos_in_loop(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        uploader_logger: logging.Logger,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Цикл run() периодически напоминает о застрявших снимках."""
        storage, record = _storage_with_photo(tmp_path)
        with storage._conn:  # снимок уже застрял к моменту запуска
            storage._conn.execute(
                "UPDATE weighing_photos_local SET attempts = ? WHERE weighing_uuid = ?",
                (STUCK_AFTER_ATTEMPTS, str(record.uuid)),
            )
        monkeypatch.setattr(photo_uploader_module, "STATS_EVERY_CYCLES", 1)
        uploader = PhotoUploader(
            storage,
            base_url=f"http://127.0.0.1:{_dead_port()}",
            token=AGENT_TOKEN,
            timeout_s=1.0,
            interval_s=0.01,
            retry_base_s=0.0,
            retry_max_s=0.0,
        )

        async def scenario() -> None:
            task = asyncio.create_task(uploader.run())
            deadline = time.monotonic() + 5
            while not any("застряло" in r.getMessage() for r in caplog.records):
                assert time.monotonic() < deadline, "сводка по очереди не появилась"
                await asyncio.sleep(0.01)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        with caplog.at_level(logging.INFO, logger="agent.sync.photo_uploader"):
            asyncio.run(asyncio.wait_for(scenario(), timeout=10))

        summary = next(r for r in caplog.records if "застряло" in r.getMessage())
        assert summary.levelno == logging.WARNING
        assert "1 в ожидании, из них застряло 1" in summary.getMessage()

    def test_stats_info_when_nothing_stuck(
        self,
        tmp_path: Path,
        uploader_logger: logging.Logger,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Пока попыток мало — спокойное INFO без слова «застряло»."""
        storage, _ = _storage_with_photo(tmp_path)
        uploader = _instant_uploader(storage, f"http://127.0.0.1:{_dead_port()}")

        with caplog.at_level(logging.INFO, logger="agent.sync.photo_uploader"):
            asyncio.run(uploader._log_queue_stats())

        messages = [r.getMessage() for r in caplog.records if "очередь фото" in r.getMessage()]
        assert messages == ["очередь фото: 1 в ожидании"]

    def test_no_stats_for_empty_queue(
        self, uploader_logger: logging.Logger, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Пустая очередь — тишина в логе (не засоряем журнал службы)."""
        storage = AgentStorage(":memory:")
        uploader = _instant_uploader(storage, f"http://127.0.0.1:{_dead_port()}")

        with caplog.at_level(logging.INFO, logger="agent.sync.photo_uploader"):
            asyncio.run(uploader._log_queue_stats())

        assert not [r for r in caplog.records if "очередь фото" in r.getMessage()]
        storage.close()
