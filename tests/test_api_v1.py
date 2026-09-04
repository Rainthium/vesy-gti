"""Тесты совместимого API v1 центра (center/api_v1 + center/app).

Закрепляют контракт docs/contracts/ais-api-v1.md:
- схемы: bishkek_iso (пояс +06:00, naive=UTC, timespec seconds),
  WeighV1Request (лишние поля игнорируются, operation по умолчанию weighing);
- POST /api/v1/weigh: любой исход — HTTP 200 с полем code (кроме
  нечитаемого JSON → 422); авторизация по учётке из ApiV1Config;
  маршрутизация по legacy-адресу (ip + autoscale [+ port], NULL-port
  матчится при любом порте запроса); ERR_AGENT_OFFLINE без линка;
- состав ответа: успех (шесть базовых полей + tare/tare_datetime/netto),
  no_valid_tare без действующей тары, тарирование БЕЗ полей тары,
  ошибки цикла (включая ERR_CAMERA с 09.08.2026) — только {code,
  message}, фото из БД, нормализация номера ТС, тайм-аут операции;
- audit_log: запись weigh_request_v1 без пароля, с code и record_uuid;
- create_app: сборка приложения и конфиг из переменных окружения;
- tools/ais_client: импорт и --help (без сети).

Агент имитируется НЕ через WebSocket, а напрямую через AgentHub:
фейковый линк ловит weigh_request и отвечает hub.resolve_result(...)
прямо из send_text (pending уже зарегистрирован к этому моменту).

Инфраструктура БД — по образцу tests/test_center_ws.py: одноразовая БД
ves_test_apiv1_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import json
import os
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center.agents_ws.hub import AgentHub
from center.api_v1.router import ApiV1Config, create_api_v1_router
from center.api_v1.schemas import WeighV1Request, bishkek_iso
from center.app import create_app
from center.db import repo
from center.db.models import AuditLog, MonitoringEvent, Scale, ScaleKind, Site
from center.db.session import database_url, make_session_factory
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import (
    PhotoMeta,
    WeighingRecord,
    WeighRequest,
    WeighResult,
    parse_center_message,
)
from tests.test_center_db import ALL_TABLES, _upgrade_head

REPO_ROOT = Path(__file__).resolve().parents[1]

# НЕ-дефолтная сервисная учётка: проверяем, что ApiV1Config реально применяется
V1_USERNAME = "ais-gate"
V1_PASSWORD = "v1-secret-2026"
CFG = ApiV1Config(legacy_username=V1_USERNAME, legacy_password=V1_PASSWORD, weigh_timeout_s=5.0)

LEGACY_IP = "192.168.150.185"
LEGACY_PORT = 8087
LEGACY_AUTOSCALE = 2
NULLPORT_AUTOSCALE = 3

SHA_A = "a" * 64
SHA_B = "b" * 64

# Ровно шесть базовых полей текущего контракта (правило проекта №1)
BASE_SUCCESS_KEYS = {
    "code",
    "massa",
    "weighing_datetime",
    "unit_meas",
    "front_image",
    "rear_image",
}
# + согласованные новые поля тары для operation=weighing
WEIGHING_SUCCESS_KEYS = BASE_SUCCESS_KEYS | {"tare", "tare_datetime", "netto"}


def _utc_now_seconds() -> datetime:
    """Текущий момент UTC без микросекунд (bishkek_iso отдаёт timespec=seconds)."""
    return datetime.now(UTC).replace(microsecond=0)


# ---------------------------------------------------------------------------
# Схемы: bishkek_iso (без БД)
# ---------------------------------------------------------------------------


class TestBishkekIso:
    def test_utc_moment_shifted_plus_six(self) -> None:
        """UTC-момент → строка с поясом +06:00 и сдвигом на 6 часов."""
        moment = datetime(2026, 8, 7, 9, 59, 26, tzinfo=UTC)
        assert bishkek_iso(moment) == "2026-08-07T15:59:26+06:00"

    def test_naive_treated_as_utc(self) -> None:
        """Naive-время трактуется как UTC (в БД время хранится в UTC)."""
        assert bishkek_iso(datetime(2026, 8, 7, 9, 59, 26)) == "2026-08-07T15:59:26+06:00"

    def test_none_returns_none(self) -> None:
        assert bishkek_iso(None) is None

    def test_timespec_seconds_no_microseconds(self) -> None:
        """Формат timespec=seconds: микросекунды отбрасываются."""
        moment = datetime(2026, 8, 7, 9, 59, 26, 987654, tzinfo=UTC)
        result = bishkek_iso(moment)
        assert result == "2026-08-07T15:59:26+06:00"
        assert result is not None and "." not in result

    def test_date_rollover_across_midnight(self) -> None:
        """Сдвиг +6 часов переносит дату через полночь."""
        moment = datetime(2026, 8, 7, 20, 30, 0, tzinfo=UTC)
        assert bishkek_iso(moment) == "2026-08-08T02:30:00+06:00"

    def test_non_utc_timezone_converted(self) -> None:
        """Момент с другим поясом конвертируется в бишкекский корректно."""
        moment = datetime(2026, 8, 7, 11, 59, 26, tzinfo=timezone(timedelta(hours=2)))
        assert bishkek_iso(moment) == "2026-08-07T15:59:26+06:00"


# ---------------------------------------------------------------------------
# Схемы: WeighV1Request (без БД)
# ---------------------------------------------------------------------------


def _request_payload(**overrides: Any) -> dict[str, Any]:
    """Базовый запрос АИС по контракту; overrides — точечные замены."""
    payload: dict[str, Any] = {
        "ip_address": LEGACY_IP,
        "port": LEGACY_PORT,
        "username": V1_USERNAME,
        "password": V1_PASSWORD,
        "autoscale": LEGACY_AUTOSCALE,
    }
    payload.update(overrides)
    return payload


class TestWeighV1RequestSchema:
    def test_extra_fields_ignored(self) -> None:
        """Старый клиент может слать что угодно — неизвестные поля игнорируются."""
        request = WeighV1Request.model_validate(
            _request_payload(unexpected="x", legacy_flag=1, comment="мусор")
        )
        assert request.ip_address == LEGACY_IP
        assert not hasattr(request, "unexpected")

    def test_missing_operation_defaults_to_weighing(self) -> None:
        """Отсутствие operation трактуется как weighing (контракт, до подтверждения АИС)."""
        request = WeighV1Request.model_validate(_request_payload())
        assert request.operation is Operation.WEIGHING

    def test_taring_operation_parsed(self) -> None:
        request = WeighV1Request.model_validate(_request_payload(operation="taring"))
        assert request.operation is Operation.TARING

    def test_missing_vehicle_number_is_none(self) -> None:
        """Отсутствие номера ТС допустимо: поле None, операция выполнима."""
        request = WeighV1Request.model_validate(_request_payload())
        assert request.vehicle_number is None
        assert request.trailer_number is None

    def test_operator_parsed_and_optional(self) -> None:
        """ФИО оператора (контракт 13.08.2026): принимается, без него — None."""
        request = WeighV1Request.model_validate(
            _request_payload(operator="Акимов Нурлан Боронбаевич")
        )
        assert request.operator == "Акимов Нурлан Боронбаевич"
        assert WeighV1Request.model_validate(_request_payload()).operator is None


# ---------------------------------------------------------------------------
# Фейковые линки агента (имитация через хаб, без WebSocket)
# ---------------------------------------------------------------------------


class SilentLink:
    """Линк, который принимает команды, но никогда не отвечает (тайм-аут)."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


class ScriptedAgentLink(SilentLink):
    """Имитация агента: на weigh_request сразу отвечает заготовленной записью.

    resolve_result вызывается прямо из send_text: pending уже
    зарегистрирован хабом до отправки команды, поэтому ранний ответ
    корректно будит ожидание (wait_for на done-future).
    """

    def __init__(
        self,
        hub: AgentHub,
        scale_id: int,
        record: WeighingRecord | None = None,
        photos: list[PhotoMeta] | None = None,
    ) -> None:
        super().__init__()
        self.hub = hub
        self.scale_id = scale_id
        self.record = record
        self.photos = photos or []
        self.requests: list[WeighRequest] = []

    async def send_text(self, data: str) -> None:
        await super().send_text(data)
        message = parse_center_message(data)
        if isinstance(message, WeighRequest):
            self.requests.append(message)
            if self.record is not None:
                record = self.record.model_copy(update={"photos": self.photos})
                result = WeighResult(request_id=message.request_id, record=record)
                self.hub.resolve_result(result, scale_id=self.scale_id)


def _make_record(**overrides: Any) -> WeighingRecord:
    """Типичная успешная запись взвешивания от агента."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 43310.0,
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 9, 59, 26, tzinfo=UTC),
        "vehicle_number": "01KG777AAA",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _make_taring(**overrides: Any) -> WeighingRecord:
    """Успешное тарирование (попадает в реестр тар при сохранении)."""
    fields: dict[str, Any] = {
        "operation": Operation.TARING,
        "massa": 15300.0,
        "weighed_at": _utc_now_seconds() - timedelta(days=2),
    }
    fields.update(overrides)
    return _make_record(**fields)


# ---------------------------------------------------------------------------
# Инфраструктура БД: временная БД + миграции (образец tests/test_center_ws.py)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def apiv1_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_apiv1_<pid>; имя не пересекается с другими
    модулями тестов, чтобы не мешать им в одном прогоне."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты API v1 центра пропущены"
        )

    db_name = f"ves_test_apiv1_{os.getpid()}"
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
def apiv1_db_engine(apiv1_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(apiv1_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


def _truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))


def _seed_scales(factory: sessionmaker[Session]) -> tuple[int, int]:
    """Объект + двое весов с legacy-маршрутами; вернуть (main_id, nullport_id).

    Основные весы — точный адрес UniServer Кызыл-Кыи (ip+port+autoscale);
    вторые — с legacy_port=NULL (матчатся по ip+autoscale при любом порте).
    """
    with factory() as session:
        site = Site(code="test-site", name="Тестовый объект")
        session.add(site)
        session.flush()
        main = Scale(
            site_id=site.id,
            name="Весы АИС",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip=LEGACY_IP,
            legacy_port=LEGACY_PORT,
            legacy_autoscale=LEGACY_AUTOSCALE,
        )
        nullport = Scale(
            site_id=site.id,
            name="Весы без legacy-порта",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip=LEGACY_IP,
            legacy_port=None,
            legacy_autoscale=NULLPORT_AUTOSCALE,
        )
        session.add_all([main, nullport])
        session.flush()
        ids = (main.id, nullport.id)
        session.commit()
    return ids


@dataclass
class ApiEnv:
    """Собранное окружение теста API v1."""

    app: FastAPI
    client: TestClient
    hub: AgentHub
    factory: sessionmaker[Session]
    scale_id: int  # legacy 192.168.150.185:8087 autoscale=2
    nullport_scale_id: int  # legacy_port=NULL, autoscale=3
    config: ApiV1Config = field(default=CFG)


def _build_env(engine: Engine, config: ApiV1Config) -> ApiEnv:
    """Чистая БД + посев маршрутов + приложение с роутером v1."""
    _truncate_all(engine)
    factory = make_session_factory(engine)
    scale_id, nullport_scale_id = _seed_scales(factory)
    hub = AgentHub()
    app = FastAPI()
    app.include_router(create_api_v1_router(hub, factory, config))
    return ApiEnv(app, TestClient(app), hub, factory, scale_id, nullport_scale_id, config)


@pytest.fixture
def api_env(apiv1_db_engine: Engine) -> ApiEnv:
    return _build_env(apiv1_db_engine, CFG)


def _seed_taring(
    env: ApiEnv,
    vehicle_number: str = "01KG777AAA",
    *,
    massa: float = 15300.0,
    weighed_at: datetime | None = None,
    trailer_number: str | None = None,
) -> WeighingRecord:
    """Посеять тарирование через журнал (как его записал бы WS-сервер)."""
    taring = _make_taring(vehicle_number=vehicle_number, massa=massa, trailer_number=trailer_number)
    if weighed_at is not None:
        taring = taring.model_copy(update={"weighed_at": weighed_at})
    with env.factory() as session:
        repo.save_weighing_record(session, env.scale_id, taring)
    return taring


def _audit_details(env: ApiEnv) -> list[tuple[str, dict[str, Any]]]:
    """Пары (actor, details) записей audit_log действия weigh_request_v1."""
    with env.factory() as session:
        rows = (
            session.execute(
                select(AuditLog).where(AuditLog.action == "weigh_request_v1").order_by(AuditLog.id)
            )
            .scalars()
            .all()
        )
        return [(row.actor, dict(row.details or {})) for row in rows]


def _post(env: ApiEnv, **overrides: Any) -> Any:
    return env.client.post("/api/v1/weigh", json=_request_payload(**overrides))


# ---------------------------------------------------------------------------
# /api/v1/weigh: валидация и авторизация
# ---------------------------------------------------------------------------


class TestWeighValidationAndAuth:
    def test_broken_json_returns_422(self, api_env: ApiEnv) -> None:
        """Нечитаемый JSON — единственный случай не-200 (контракт)."""
        response = api_env.client.post(
            "/api/v1/weigh",
            content=b'{"ip_address": "192.168.150.185", "port": ',
            headers={"Content-Type": "application/json"},
        )
        assert response.status_code == 422

    def test_missing_required_field_returns_422(self, api_env: ApiEnv) -> None:
        """Без обязательного поля (ip_address) запрос не разбирается → 422."""
        payload = _request_payload()
        del payload["ip_address"]
        response = api_env.client.post("/api/v1/weigh", json=payload)
        assert response.status_code == 422

    def test_wrong_password_http200_err_internal(self, api_env: ApiEnv) -> None:
        """Неверный пароль → HTTP 200 + ERR_INTERNAL + message про учётные данные."""
        response = _post(api_env, password="wrong")
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "ERR_INTERNAL"
        assert "учётные" in data["message"]

    def test_wrong_username_err_internal(self, api_env: ApiEnv) -> None:
        response = _post(api_env, username="intruder")
        assert response.status_code == 200
        assert response.json()["code"] == "ERR_INTERNAL"

    def test_default_admin_admin_rejected_with_custom_config(self, api_env: ApiEnv) -> None:
        """Учётка admin/admin не проходит: применяются креды из ApiV1Config,
        а не захардкоженные дефолты."""
        response = _post(api_env, username="admin", password="admin")
        assert response.status_code == 200
        assert response.json()["code"] == "ERR_INTERNAL"

    def test_non_ascii_credentials_still_http200(self, apiv1_db_engine: Engine) -> None:
        """Регрессионная защита: не-ASCII учётные данные не роняют эндпоинт.

        Раньше hmac.compare_digest(str, str) бросал TypeError на кириллице
        (HTTP 500); теперь сравнение байтовое — контрактный HTTP 200 + code."""
        env = _build_env(apiv1_db_engine, CFG)
        client = TestClient(env.app, raise_server_exceptions=False)
        response = client.post(
            "/api/v1/weigh", json=_request_payload(username="админ", password="пароль")
        )
        assert response.status_code == 200, (
            "не-ASCII учётные данные уронили эндпоинт вместо ERR_INTERNAL"
        )
        assert response.json()["code"] == "ERR_INTERNAL"


# ---------------------------------------------------------------------------
# /api/v1/weigh: маршрутизация и доступность агента
# ---------------------------------------------------------------------------


class TestWeighRouting:
    def test_unknown_route_err_internal_with_message(self, api_env: ApiEnv) -> None:
        """Неизвестный legacy-адрес → ERR_INTERNAL + message; HTTP 200."""
        response = _post(api_env, ip_address="10.9.9.9")
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "ERR_INTERNAL"
        assert data["message"]

    def test_unknown_autoscale_err_internal(self, api_env: ApiEnv) -> None:
        """ip известен, autoscale — нет → маршрут не найден."""
        response = _post(api_env, autoscale=99)
        assert response.json()["code"] == "ERR_INTERNAL"

    def test_unknown_route_written_to_audit(self, api_env: ApiEnv) -> None:
        """Отказ маршрутизации журналируется в audit_log."""
        _post(api_env, ip_address="10.9.9.9")
        entries = _audit_details(api_env)
        assert len(entries) == 1
        actor, details = entries[0]
        assert actor == f"ais:{V1_USERNAME}"
        assert details["code"] == "ERR_INTERNAL"
        assert details["record_uuid"] is None

    def test_null_legacy_port_matches_any_request_port(self, api_env: ApiEnv) -> None:
        """legacy_port=NULL в БД: маршрут матчится по ip+autoscale при любом
        port из запроса (контракт: ключ маршрутизации — ip + autoscale)."""
        record = _make_record(vehicle_number=None)
        link = ScriptedAgentLink(api_env.hub, api_env.nullport_scale_id, record)
        api_env.hub.attach(api_env.nullport_scale_id, link)

        response = _post(api_env, autoscale=NULLPORT_AUTOSCALE, port=59999)
        assert response.json()["code"] == "OK"
        assert len(link.requests) == 1

    def test_command_routed_to_matching_scale_only(self, api_env: ApiEnv) -> None:
        """Команда уходит именно весам своего маршрута, а не всем подряд."""
        main_link = ScriptedAgentLink(api_env.hub, api_env.scale_id, _make_record())
        other_link = ScriptedAgentLink(api_env.hub, api_env.nullport_scale_id, _make_record())
        api_env.hub.attach(api_env.scale_id, main_link)
        api_env.hub.attach(api_env.nullport_scale_id, other_link)

        assert _post(api_env).json()["code"] == "OK"
        assert len(main_link.requests) == 1
        assert len(other_link.requests) == 0

    def test_agent_offline_err_agent_offline(self, api_env: ApiEnv) -> None:
        """Маршрут известен, но линка в хабе нет → ERR_AGENT_OFFLINE."""
        response = _post(api_env)
        assert response.status_code == 200
        data = response.json()
        assert data["code"] == "ERR_AGENT_OFFLINE"
        assert data["message"]


# ---------------------------------------------------------------------------
# /api/v1/weigh: состав ответа
# ---------------------------------------------------------------------------


class TestWeighSuccessResponse:
    def _attach(self, env: ApiEnv, record: WeighingRecord) -> ScriptedAgentLink:
        link = ScriptedAgentLink(env.hub, env.scale_id, record)
        env.hub.attach(env.scale_id, link)
        return link

    def test_success_with_agent_tare_full_contract_shape(self, api_env: ApiEnv) -> None:
        """Успех: агент вернул тару/нетто и ссылку на тарирование → ответ
        содержит ровно контрактный набор полей, даты — с поясом +06:00."""
        taring = _seed_taring(api_env)  # тарирование в журнале: uuid резолвится
        record = _make_record(
            tare_value=15300.0,
            netto=43310.0 - 15300.0,
            tare_weighing_uuid=taring.uuid,
        )
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert set(data) == WEIGHING_SUCCESS_KEYS
        assert data["code"] == "OK"
        assert data["massa"] == 43310.0
        assert data["weighing_datetime"] == "2026-08-07T15:59:26+06:00"
        assert data["unit_meas"] == "kg"
        assert data["tare"] == 15300.0
        assert data["netto"] == 28010.0
        # tare_datetime — из записи тарирования, бишкекское с поясом
        assert data["tare_datetime"] == bishkek_iso(taring.weighed_at)
        assert data["tare_datetime"] is not None and data["tare_datetime"].endswith("+06:00")
        # запись агента не сохранялась с фото → путей нет
        assert data["front_image"] is None
        assert data["rear_image"] is None
        assert "no_valid_tare" not in data

    def test_center_computes_tare_when_agent_omits(self, api_env: ApiEnv) -> None:
        """Агент не заполнил тару/нетто, но номер известен и тара в реестре
        действует → центр досчитывает сам (правило №4)."""
        taring = _seed_taring(api_env, massa=15300.0)
        record = _make_record(tare_value=None, netto=None, tare_weighing_uuid=None)
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert data["code"] == "OK"
        assert data["tare"] == 15300.0
        assert data["netto"] == 43310.0 - 15300.0
        assert data["tare_datetime"] == bishkek_iso(taring.weighed_at)
        assert "no_valid_tare" not in data

    def test_center_computes_tare_for_matching_pair(self, api_env: ApiEnv) -> None:
        """Фолбэк центра учитывает прицеп (решение 09.08.2026): тара той же
        СЦЕПКИ голова+прицеп подставляется, нетто досчитывается."""
        taring = _seed_taring(api_env, massa=15300.0, trailer_number="BD123AB")
        record = _make_record(
            trailer_number="BD123AB", tare_value=None, netto=None, tare_weighing_uuid=None
        )
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA", trailer_number="BD123AB").json()
        assert data["code"] == "OK"
        assert data["tare"] == 15300.0
        assert data["netto"] == 43310.0 - 15300.0
        assert data["tare_datetime"] == bishkek_iso(taring.weighed_at)
        assert "no_valid_tare" not in data

    def test_center_tare_of_other_trailer_gives_no_valid_tare(self, api_env: ApiEnv) -> None:
        """Фолбэк центра: в реестре тара другого прицепа → действующей тары
        нет, no_valid_tare = true (смена прицепа = новое тарирование)."""
        _seed_taring(api_env, trailer_number="OLD01AB")
        record = _make_record(trailer_number="NEW02CD", tare_value=None, netto=None)
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA", trailer_number="NEW02CD").json()
        assert data["code"] == "OK"
        assert data["tare"] is None
        assert data["netto"] is None
        assert data["no_valid_tare"] is True

    def test_weighing_without_vehicle_no_valid_tare(self, api_env: ApiEnv) -> None:
        """Без номера ТС тару подставить не из чего: tare/netto = null +
        no_valid_tare = true (контракт)."""
        record = _make_record(vehicle_number=None, tare_value=None, netto=None)
        self._attach(api_env, record)

        data = _post(api_env).json()
        assert data["code"] == "OK"
        assert data["tare"] is None
        assert data["netto"] is None
        assert data["no_valid_tare"] is True

    def test_expired_tare_gives_no_valid_tare(self, api_env: ApiEnv) -> None:
        """Тара старше 3 месяцев недействительна → нетто не считается."""
        _seed_taring(api_env, weighed_at=_utc_now_seconds() - timedelta(days=200))
        record = _make_record(tare_value=None, netto=None)
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert data["code"] == "OK"
        assert data["tare"] is None
        assert data["netto"] is None
        assert data["no_valid_tare"] is True

    def test_taring_response_has_no_tare_fields(self, api_env: ApiEnv) -> None:
        """Тарирование: в JSON нет полей tare/tare_datetime/netto/no_valid_tare
        (они только для operation=weighing)."""
        record = _make_taring(vehicle_number="01KG777AAA")
        self._attach(api_env, record)

        data = _post(api_env, operation="taring", vehicle_number="01KG777AAA").json()
        assert set(data) == BASE_SUCCESS_KEYS
        assert data["code"] == "OK"
        assert data["massa"] == 15300.0

    def test_cycle_error_returns_code_and_message_only(self, api_env: ApiEnv) -> None:
        """Ошибка цикла (ERR_VEHICLE_TIMEOUT): только {code, message}, без massa."""
        record = _make_record(
            code=ErrorCode.ERR_VEHICLE_TIMEOUT,
            massa=None,
            stable=False,
            message="АТС не заехало за отведённое время",
        )
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert data == {
            "code": "ERR_VEHICLE_TIMEOUT",
            "message": "АТС не заехало за отведённое время",
        }

    def test_err_camera_is_error_without_weight(self, api_env: ApiEnv) -> None:
        """ERR_CAMERA: операция не проведена (решение 09.08.2026) —
        только {code, message}, веса и фото в ответе нет."""
        record = _make_record(
            code=ErrorCode.ERR_CAMERA,
            massa=None,
            stable=False,
            weighed_at=None,
            message="операция не проведена, камера недоступна: rear",
        )
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert data == {
            "code": "ERR_CAMERA",
            "message": "операция не проведена, камера недоступна: rear",
        }

    def test_photo_paths_taken_from_db(self, api_env: ApiEnv) -> None:
        """Фото записи (front/rear) отдаются путями из БД."""
        record = _make_record(vehicle_number=None)
        photos = [
            PhotoMeta(
                role=CameraRole.FRONT,
                filename="/vesy/2026/08/07/aaa_photo1.jpeg",
                sha256=SHA_A,
                size_bytes=100,
            ),
            PhotoMeta(
                role=CameraRole.REAR,
                filename="/vesy/2026/08/07/aaa_photo2.jpeg",
                sha256=SHA_B,
                size_bytes=200,
            ),
        ]
        # запись с фото уже в журнале (её сохранил бы WS-приём weigh_result)
        with api_env.factory() as session:
            repo.save_weighing_record(session, api_env.scale_id, record, photos)
        self._attach(api_env, record)

        data = _post(api_env).json()
        assert data["code"] == "OK"
        # пути канонические — формирует центр из uuid и даты записи
        assert record.weighed_at is not None
        day = record.weighed_at.strftime("%Y/%m/%d")
        assert data["front_image"] == f"/vesy/{day}/{record.uuid.hex}_photo1.jpeg"
        assert data["rear_image"] == f"/vesy/{day}/{record.uuid.hex}_photo2.jpeg"

    def test_vehicle_number_normalized_before_agent(self, api_env: ApiEnv) -> None:
        """Номер нормализуется (upper, без пробелов по краям) до отправки агенту."""
        link = self._attach(api_env, _make_record())
        _post(api_env, vehicle_number=" 01kg777aaa ", trailer_number=" 01kg500ab ")
        assert len(link.requests) == 1
        command = link.requests[0]
        assert command.vehicle_number == "01KG777AAA"
        assert command.trailer_number == "01KG500AB"
        assert command.operation is Operation.WEIGHING

    def test_operator_passed_to_agent(self, api_env: ApiEnv) -> None:
        """ФИО оператора из запроса АИС уезжает агенту в команде (контракт
        13.08.2026): лишние пробелы схлопываются, регистр сохраняется."""
        link = self._attach(api_env, _make_record())
        _post(api_env, operator="  Акимов   Нурлан  Боронбаевич ")
        assert len(link.requests) == 1
        assert link.requests[0].operator == "Акимов Нурлан Боронбаевич"

    def test_operator_trimmed_to_column_width(self, api_env: ApiEnv) -> None:
        """Оператор длиннее колонки БД (200) обрезается, а не роняет запись."""
        link = self._attach(api_env, _make_record())
        _post(api_env, operator="Ф" * 250)
        assert link.requests[0].operator == "Ф" * 200

    def test_missing_operator_is_none_in_command(self, api_env: ApiEnv) -> None:
        """Старый запрос без оператора: в команде None, всё работает как раньше."""
        link = self._attach(api_env, _make_record())
        _post(api_env)
        assert link.requests[0].operator is None

    def test_timeout_err_internal_within_deadline(self, apiv1_db_engine: Engine) -> None:
        """Агент молчит: с weigh_timeout_s=0.1 ответ ERR_INTERNAL приходит
        быстрее 2 с (тайм-аут конфигурируем, не захардкожен 120 с)."""
        env = _build_env(
            apiv1_db_engine,
            ApiV1Config(
                legacy_username=V1_USERNAME, legacy_password=V1_PASSWORD, weigh_timeout_s=0.1
            ),
        )
        env.hub.attach(env.scale_id, SilentLink())

        started = time.monotonic()
        response = _post(env)
        elapsed = time.monotonic() - started
        assert response.json()["code"] == "ERR_INTERNAL"
        assert elapsed < 2.0, f"тайм-аут из конфига не применился: ждали {elapsed:.1f} с"


# ---------------------------------------------------------------------------
# /api/v1/weigh: журналирование
# ---------------------------------------------------------------------------


class TestWeighAudit:
    def test_success_written_to_audit_without_password(self, api_env: ApiEnv) -> None:
        """Успешный запрос журналируется: есть code и record_uuid, пароля нет."""
        record = _make_record()
        api_env.hub.attach(
            api_env.scale_id, ScriptedAgentLink(api_env.hub, api_env.scale_id, record)
        )
        _post(api_env, vehicle_number="01KG777AAA")

        entries = _audit_details(api_env)
        assert len(entries) == 1
        actor, details = entries[0]
        assert actor == f"ais:{V1_USERNAME}"
        assert details["code"] == "OK"
        assert details["record_uuid"] == str(record.uuid)
        dumped = json.dumps(details, ensure_ascii=False)
        assert "password" not in dumped
        assert V1_PASSWORD not in dumped

    def test_agent_offline_written_to_audit(self, api_env: ApiEnv) -> None:
        """Отказ «агент офлайн» тоже попадает в журнал с кодом ошибки."""
        _post(api_env)
        entries = _audit_details(api_env)
        assert len(entries) == 1
        _, details = entries[0]
        assert details["code"] == "ERR_AGENT_OFFLINE"
        assert details["record_uuid"] is None


# ---------------------------------------------------------------------------
# create_app: сборка приложения и конфиг из окружения
# ---------------------------------------------------------------------------


class TestCreateApp:
    def test_env_config_applied_end_to_end(
        self, apiv1_db_engine: Engine, apiv1_db_url: URL, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """create_app читает DATABASE_URL и V1_* из окружения: учётка из env
        проходит авторизацию, команда идёт через app.state.hub до агента."""
        _truncate_all(apiv1_db_engine)
        factory = make_session_factory(apiv1_db_engine)
        scale_id, _ = _seed_scales(factory)

        monkeypatch.setenv("DATABASE_URL", apiv1_db_url.render_as_string(hide_password=False))
        monkeypatch.setenv("V1_USERNAME", "env-user")
        monkeypatch.setenv("V1_PASSWORD", "env-pass")
        monkeypatch.setenv("V1_WEIGH_TIMEOUT_S", "5")
        app = create_app()
        assert isinstance(app.state.hub, AgentHub)
        client = TestClient(app)

        # дефолтная учётка admin/admin отклоняется — конфиг реально из env
        response = client.post("/api/v1/weigh", json=_request_payload())
        assert response.status_code == 200
        assert response.json()["code"] == "ERR_INTERNAL"

        # учётка из env проходит; агент через state.hub отвечает успехом
        record = _make_record(vehicle_number=None)
        app.state.hub.attach(scale_id, ScriptedAgentLink(app.state.hub, scale_id, record))
        response = client.post(
            "/api/v1/weigh",
            json=_request_payload(username="env-user", password="env-pass"),
        )
        assert response.json()["code"] == "OK"

    def test_routes_include_ws_and_api_v1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Приложение центра публикует и WS агентов, и совместимый API v1."""
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:ves@localhost:5443/ves")
        app = create_app()

        def collect_paths(routes: list[Any]) -> set[str]:
            # FastAPI 0.141 оборачивает include_router в _IncludedRouter без
            # path — обходим вложенные роутеры (original_router) рекурсивно
            paths: set[str] = set()
            for route in routes:
                path = getattr(route, "path", None)
                if isinstance(path, str):
                    paths.add(path)
                inner = getattr(route, "original_router", route)
                if inner is not route:
                    paths |= collect_paths(list(inner.routes))
            return paths

        paths = collect_paths(list(app.routes))
        assert "/api/v1/weigh" in paths
        assert "/agents/ws" in paths

    def test_healthz(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        """Живость для healthcheck'ов compose/nginx — отвечает без похода в БД."""
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:ves@localhost:5443/ves")
        monkeypatch.setenv("PHOTOS_DIR", str(tmp_path))
        client = TestClient(create_app())
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_lifespan_runs_monitoring(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Lifespan поднимает фоновый мониторинг и гасит его на выходе.

        Ошибки детекторов (недоступная БД и т.п.) цикл ловит сам — контекст
        не падает; Telegram без токена не запускается вовсе.
        """
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:ves@localhost:5443/ves")
        monkeypatch.setenv("PHOTOS_DIR", str(tmp_path))
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        app = create_app()
        with TestClient(app) as client:  # with: запускает lifespan
            assert client.get("/healthz").status_code == 200
            assert app.state.monitor is not None
        # выход из with отменил фоновые задачи без исключений

    def test_panel_cookie_secure_env_switch(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """PANEL_COOKIE_SECURE=1 вешает на cookie панели флаг Secure.

        По умолчанию флага нет: на пилоте в панель ходят и по внутреннему
        http, dev-стенд тоже http (замечание ревью 13.08.2026).
        """
        from starlette.middleware.sessions import SessionMiddleware

        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:ves@localhost:5443/ves")
        monkeypatch.setenv("PHOTOS_DIR", str(tmp_path))

        def session_kwargs(app: Any) -> dict[str, Any]:
            middleware = next(m for m in app.user_middleware if m.cls is SessionMiddleware)
            return dict(middleware.kwargs)

        assert session_kwargs(create_app())["https_only"] is False
        monkeypatch.setenv("PANEL_COOKIE_SECURE", "1")
        assert session_kwargs(create_app())["https_only"] is True

    def test_production_refuses_missing_secrets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """CENTER_ENV=production без секретов — отказ старта с перечнем (правило №7)."""
        monkeypatch.setenv("CENTER_ENV", "production")
        for name in (
            "DATABASE_URL",
            "PANEL_SECRET",
            "AIS_PHOTO_TOKEN",
            "V1_USERNAME",
            "V1_PASSWORD",
        ):
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(RuntimeError) as err:
            create_app()
        for name in (
            "DATABASE_URL",
            "PANEL_SECRET",
            "AIS_PHOTO_TOKEN",
            "V1_USERNAME",
            "V1_PASSWORD",
        ):
            assert name in str(err.value)

    def test_production_refuses_dev_default_values(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Секрет, совпадающий с dev-дефолтом, в проде не принимается."""
        monkeypatch.setenv("CENTER_ENV", "production")
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:x@db:5432/ves")
        monkeypatch.setenv("PANEL_SECRET", "dev-only-panel-secret")
        monkeypatch.setenv("AIS_PHOTO_TOKEN", "настоящий-токен-из-env")
        monkeypatch.setenv("V1_PASSWORD", "admin")
        with pytest.raises(RuntimeError, match="PANEL_SECRET"):
            create_app()

    def test_production_starts_with_explicit_secrets(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """С явно заданными секретами прод-сборка проходит (admin в v1 — явный выбор)."""
        monkeypatch.setenv("CENTER_ENV", "production")
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://ves:x@db:5432/ves")
        monkeypatch.setenv("PANEL_SECRET", "s" * 32)
        monkeypatch.setenv("AIS_PHOTO_TOKEN", "t" * 32)
        monkeypatch.setenv("V1_USERNAME", "admin")
        monkeypatch.setenv("V1_PASSWORD", "admin")
        monkeypatch.setenv("PHOTOS_DIR", str(tmp_path))
        app = create_app()
        assert isinstance(app.state.hub, AgentHub)


# ---------------------------------------------------------------------------
# tools/ais_client: лёгкие проверки без сети
# ---------------------------------------------------------------------------


class TestAisClient:
    def test_module_imports(self) -> None:
        """Модуль импортируется, точка входа на месте."""
        import tools.ais_client as ais_client

        assert callable(ais_client.main)

    def test_help_exits_zero(self) -> None:
        """`python -m tools.ais_client --help` завершается кодом 0 (без сети)."""
        result = subprocess.run(
            [sys.executable, "-m", "tools.ais_client", "--help"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            timeout=30,
        )
        assert result.returncode == 0
        assert "--vehicle" in result.stdout


class TestTareHintToAgent:
    """17.08.2026: v1 тоже передаёт агенту действующую тару по реестру центра
    (агент 0.4.17 применяет её вместо реплики; старые агенты поле игнорируют)."""

    def test_weighing_carries_resolved_tare(self, api_env: ApiEnv) -> None:
        taring = _seed_taring(api_env)
        link = ScriptedAgentLink(api_env.hub, api_env.scale_id, _make_record())
        api_env.hub.attach(api_env.scale_id, link)
        _post(api_env, vehicle_number="01KG777AAA")
        sent = link.requests[0]
        assert sent.tare_resolved is True
        assert sent.tare is not None and sent.tare.weighing_uuid == taring.uuid
        assert sent.ais_ref is None  # у v1 номера документа АИС нет

    def test_taring_and_no_vehicle_have_no_hint(self, api_env: ApiEnv) -> None:
        link = ScriptedAgentLink(api_env.hub, api_env.scale_id, _make_record())
        api_env.hub.attach(api_env.scale_id, link)
        _post(api_env, operation="taring", vehicle_number="01KG777AAA")
        _post(api_env)  # без номера ТС
        assert [r.tare_resolved for r in link.requests] == [False, False]


# ---------------------------------------------------------------------------
# Лимит тары и правдоподобие тары на замороженном v1 (решение Игоря 04.09.2026)
# ---------------------------------------------------------------------------


def _monitoring_rows(env: ApiEnv, kind: str) -> list[str]:
    with env.factory() as session:
        return list(
            session.execute(
                select(MonitoringEvent.message)
                .where(MonitoringEvent.kind == kind)
                .order_by(MonitoringEvent.id)
            ).scalars()
        )


class TestTareLimitAndPlausibilityV1:
    """v1 заморожен, но новый отказ агента проходит через него как любой отказ
    ({code, message}), а событие tare_rejected уходит в «События»/Telegram;
    тара ≥ брутто не должна подставляться и фолбэком центра."""

    def _attach(self, env: ApiEnv, record: WeighingRecord) -> ScriptedAgentLink:
        link = ScriptedAgentLink(env.hub, env.scale_id, record)
        env.hub.attach(env.scale_id, link)
        return link

    def test_tare_too_heavy_code_message_and_alert(self, api_env: ApiEnv) -> None:
        """ERR_TARE_TOO_HEAVY: только {code, message}; событие tare_rejected с
        массой и нормализованной сцепкой; аудит с кодом; в журнале ничего."""
        message = (
            "Масса 37 120 кг больше допустимой тары 25 000 кг — это гружёная машина: "
            "проведите взвешивание, а не тарирование"
        )
        refusal = _make_record(
            operation=Operation.TARING,
            code=ErrorCode.ERR_TARE_TOO_HEAVY,
            massa=None,
            stable=False,
            weighed_at=None,
            message=message,
        )
        self._attach(api_env, refusal)

        data = _post(
            api_env, operation="taring", vehicle_number=" 01kg777aaa ", trailer_number="bd123ab"
        ).json()
        assert data == {"code": "ERR_TARE_TOO_HEAVY", "message": message}
        alerts = _monitoring_rows(api_env, "tare_rejected")
        assert len(alerts) == 1
        assert "37 120" in alerts[0] and "01KG777AAA/BD123AB" in alerts[0]
        assert _audit_details(api_env)[-1][1]["code"] == "ERR_TARE_TOO_HEAVY"
        with api_env.factory() as session:
            assert repo.find_active_tare(session, "01KG777AAA", "BD123AB") is None

    def test_other_refusal_gives_no_tare_rejected(self, api_env: ApiEnv) -> None:
        """Прочие отказы тарирования события tare_rejected не дают."""
        refusal = _make_record(
            operation=Operation.TARING,
            code=ErrorCode.ERR_VEHICLE_TIMEOUT,
            massa=None,
            stable=False,
            weighed_at=None,
            message="на весах нет АТС с зафиксированным весом",
        )
        self._attach(api_env, refusal)
        data = _post(api_env, operation="taring", vehicle_number="01KG777AAA").json()
        assert data["code"] == "ERR_VEHICLE_TIMEOUT"
        assert _monitoring_rows(api_env, "tare_rejected") == []

    def test_center_fallback_respects_tare_plausibility(self, api_env: ApiEnv) -> None:
        """Агент 0.4.29 не подставил тару ≥ брутто (tare/netto пусты, причина в
        message): ответ v1 обязан быть «без тары» (no_valid_tare), а не
        подставлять её заново с отрицательным нетто."""
        _seed_taring(api_env, massa=50000.0)
        record = _make_record(
            tare_value=None,
            netto=None,
            message=(
                "Тара 50 000 кг (тарирование от 05.08.2026) не меньше брутто 43 310 кг — "
                "в расчёт не подставлена, нетто не рассчитано: тарирование сцепки "
                "ошибочно (так тарируют гружёную машину)"
            ),
        )
        self._attach(api_env, record)

        data = _post(api_env, vehicle_number="01KG777AAA").json()
        assert data["code"] == "OK"
        assert data["tare"] is None
        assert data["netto"] is None
        assert data["no_valid_tare"] is True
