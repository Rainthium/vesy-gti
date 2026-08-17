"""Тесты нативного API v2 центра (center/api_v2) — контракт
docs/contracts/ais-api-v2.md (1.0, согласован с АИС 17.08.2026).

Закрепляют:
- схемы: WeighV2Request (обязательные поля, префикс ais_ref ↔ операция,
  нормализация номеров/ФИО), тело 422 с details;
- авторизацию: Bearer-токен интегратора (401) и allowlist IP (403) —
  тела ошибок {code, message};
- POST /api/v2/weighings: маршрут по паре ais_object + scale_no (404
  ERR_UNKNOWN_SCALE), исходы операции HTTP 200 + code, документ операции
  (id/номер карточки/объект с ais_object/весы с поверкой/тара во вложении со
  статусом applied|expired|not_applied|null/фото с available/checksum);
- идемпотентность по ais_ref: повтор по состоявшейся → тот же документ с
  repeated (агенту вторая команда не уходит), повтор во время выполнения
  ждёт исход первой, после отказа — новая попытка; номер АИС пишется в одной
  транзакции с записью (WS-путь через хаб: take_ais_ref → save);
- GET по id / по ais_ref / список за период с фильтрами и пагинацией;
- обратный вызов POST /api/v2/weighings/{id}/ais_ref (200/404/409/422) и
  появление номера во вложении tare.ais_ref у последующего взвешивания;
- audit_log: weigh_request_v2 с кодом и record_uuid;
- хаб: память номеров АИС по request_id с TTL.

Агент имитируется через AgentHub: фейковый линк ловит weigh_request и
делает то же, что WS-сервер центра, — забирает ais_ref у хаба, сохраняет
запись в журнал одной транзакцией с номером и будит команду.

Инфраструктура БД — по образцу tests/test_api_v1.py: одноразовая БД
ves_test_apiv2_<pid> + миграции alembic + TRUNCATE между тестами.
"""

import asyncio
import os
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx2 as httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center.agents_ws.hub import AgentHub
from center.api_v2.router import ApiV2Config, create_api_v2_router
from center.api_v2.schemas import WeighV2Request, validation_details
from center.db import repo
from center.db.models import AuditLog, Scale, ScaleKind, Site, WeighingAisRef
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

TOKEN = "v2-test-token-2026"
INTEGRATOR = "ais-svh"
AIS_OBJECT = "0014"  # Кызыл-Кыя в справочнике АИС
KANT_OBJECT = "0002"

WEIGHED_AT = datetime(2026, 8, 14, 9, 30, 12, tzinfo=UTC)  # 15:30:12 по Бишкеку
TARED_AT = datetime(2026, 6, 12, 4, 21, 0, tzinfo=UTC)  # 10:21:00 по Бишкеку

SHA_A = "a" * 64
SHA_B = "b" * 64


def _auth(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _command(**overrides: Any) -> dict[str, Any]:
    """Команда взвешивания по контракту 4.2; overrides — точечные замены."""
    payload: dict[str, Any] = {
        "ais_ref": "WEI000094176",
        "ais_object": AIS_OBJECT,
        "scale_no": 1,
        "operation": "weighing",
        "vehicle_number": "01KG777AAA",
        "trailer_number": "01KG500AB",
        "operator": "Акимов Нурлан Боронбаевич",
    }
    payload.update(overrides)
    for key in [k for k, v in payload.items() if v is ...]:
        del payload[key]
    return payload


def _make_record(**overrides: Any) -> WeighingRecord:
    """Успешное взвешивание от агента (без тары — реплика ничего не подставила)."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 43310.0,
        "stable": True,
        "weighed_at": WEIGHED_AT,
        "vehicle_number": "01KG777AAA",
        "trailer_number": "01KG500AB",
        "operator": "Акимов Нурлан Боронбаевич",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _make_taring(**overrides: Any) -> WeighingRecord:
    fields: dict[str, Any] = {
        "operation": Operation.TARING,
        "massa": 15300.0,
        "weighed_at": TARED_AT,
    }
    fields.update(overrides)
    return _make_record(**fields)


# ---------------------------------------------------------------------------
# Схемы (без БД)
# ---------------------------------------------------------------------------


class TestWeighV2RequestSchema:
    def test_valid_command_normalizes_vehicle_and_operator(self) -> None:
        request = WeighV2Request.model_validate(
            _command(vehicle_number=" 01kg777aaa ", trailer_number="", operator="  Акимов   Н. Б. ")
        )
        assert request.vehicle == "01KG777AAA"
        assert request.trailer is None
        assert request.operator == "Акимов Н. Б."
        assert request.scale_no == 1

    def test_prefix_must_match_operation(self) -> None:
        """WEI — только взвешивание, TAR — только тарирование."""
        with pytest.raises(ValidationError) as exc_info:
            WeighV2Request.model_validate(_command(ais_ref="TAR000012206"))
        details = validation_details(exc_info.value)
        assert any("префикс" in d["error"] for d in details)
        WeighV2Request.model_validate(_command(ais_ref="TAR000012206", operation="taring"))

    def test_bad_ais_ref_format_rejected(self) -> None:
        with pytest.raises(ValidationError):
            WeighV2Request.model_validate(_command(ais_ref="94176"))

    def test_required_fields(self) -> None:
        for field in ("ais_ref", "ais_object", "operation", "vehicle_number", "operator"):
            with pytest.raises(ValidationError):
                WeighV2Request.model_validate(_command(**{field: ...}))

    def test_ais_object_keeps_leading_zeros(self) -> None:
        request = WeighV2Request.model_validate(_command(ais_object="0014"))
        assert request.ais_object == "0014"

    def test_extra_fields_ignored(self) -> None:
        request = WeighV2Request.model_validate(_command(wait=False, comment="x"))
        assert not hasattr(request, "wait")


# ---------------------------------------------------------------------------
# Фейковый агент: делает то же, что WS-сервер (ais_ref → запись → resolve)
# ---------------------------------------------------------------------------


class SavingAgentLink:
    """На weigh_request сохраняет запись одной транзакцией с номером АИС и будит команду.

    ``records`` — сценарий ответов по порядку (последний повторяется);
    ``started``/``gate`` — события для проверки повторов во время выполнения:
    линк отмечает получение команды и ждёт разрешения ответить.
    """

    def __init__(
        self,
        hub: AgentHub,
        scale_id: int,
        factory: sessionmaker[Session],
        records: list[WeighingRecord],
        photos: list[PhotoMeta] | None = None,
    ) -> None:
        self.hub = hub
        self.scale_id = scale_id
        self.factory = factory
        self.records = list(records)
        self.photos = photos or []
        self.started: asyncio.Event | None = None
        self.gate: asyncio.Event | None = None
        self.requests: list[WeighRequest] = []
        self.saved: list[UUID] = []

    async def send_text(self, data: str) -> None:
        message = parse_center_message(data)
        if not isinstance(message, WeighRequest):
            return
        self.requests.append(message)
        if self.started is not None:
            self.started.set()
        if self.gate is not None:
            await self.gate.wait()
        record = self.records.pop(0) if len(self.records) > 1 else self.records[0]
        record = record.model_copy(update={"uuid": uuid4(), "photos": self.photos})
        ais_ref = self.hub.take_ais_ref(message.request_id)
        with self.factory() as session:
            saved = repo.save_weighing_record(
                session, self.scale_id, record, self.photos, ais_ref=ais_ref
            )
        if saved:
            self.saved.append(record.uuid)
        self.hub.resolve_result(
            WeighResult(request_id=message.request_id, record=record), scale_id=self.scale_id
        )


# ---------------------------------------------------------------------------
# Инфраструктура БД
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def apiv2_db_url() -> Iterator[URL]:
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip("PostgreSQL недоступен — тесты API v2 центра пропущены")

    db_name = f"ves_test_apiv2_{os.getpid()}"
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
def apiv2_db_engine(apiv2_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(apiv2_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


def _truncate_all(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))


def _seed_scales(factory: sessionmaker[Session]) -> tuple[int, int]:
    """Кызыл-Кыя (0014, весы 1, поверка) и Кант (0002, весы 2); вернуть их id."""
    with factory() as session:
        kk = Site(code="kyzyl-kyia", name="СВХ «Кызыл-Кыя»")
        kant = Site(code="kant", name="СВХ «Кант»")
        session.add_all([kk, kant])
        session.flush()
        scale_kk = Scale(
            site_id=kk.id,
            name="Весы SCS-80",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_object=AIS_OBJECT,
            ais_scale_no=1,
            verif_number="0123456",
            verif_date=datetime(2026, 3, 1).date(),
            verif_until=datetime(2027, 3, 1).date(),
        )
        scale_kant = Scale(
            site_id=kant.id,
            name="Весы КАНТ-2",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_object=KANT_OBJECT,
            ais_scale_no=2,
        )
        session.add_all([scale_kk, scale_kant])
        session.flush()
        ids = (scale_kk.id, scale_kant.id)
        session.commit()
    return ids


@dataclass
class ApiEnv:
    app: FastAPI
    client: TestClient
    hub: AgentHub
    factory: sessionmaker[Session]
    scale_id: int
    kant_scale_id: int
    photos_dir: Path
    config: ApiV2Config


def _build_env(
    engine: Engine, photos_dir: Path, *, allowed_ips: frozenset[str] | None = None
) -> ApiEnv:
    _truncate_all(engine)
    factory = make_session_factory(engine)
    scale_id, kant_scale_id = _seed_scales(factory)
    hub = AgentHub()
    config = ApiV2Config(
        service_tokens={TOKEN: INTEGRATOR},
        photos_dir=photos_dir,
        allowed_ips=allowed_ips,
        weigh_timeout_s=3.0,
    )
    app = FastAPI()
    app.include_router(create_api_v2_router(hub, factory, config))
    return ApiEnv(app, TestClient(app), hub, factory, scale_id, kant_scale_id, photos_dir, config)


@pytest.fixture
def api_env(apiv2_db_engine: Engine, tmp_path: Path) -> ApiEnv:
    return _build_env(apiv2_db_engine, tmp_path)


def _seed_taring(
    env: ApiEnv,
    *,
    ais_ref: str | None = "TAR000012206",
    weighed_at: datetime = TARED_AT,
    source: WeighingSource = WeighingSource.AIS,
    trailer_number: str | None = "01KG500AB",
    massa: float = 15300.0,
) -> WeighingRecord:
    """Тарирование в журнале — как его сохранил бы WS-сервер по команде v2."""
    taring = _make_taring(
        weighed_at=weighed_at, source=source, trailer_number=trailer_number, massa=massa
    )
    with env.factory() as session:
        repo.save_weighing_record(session, env.scale_id, taring, ais_ref=ais_ref)
    return taring


def _attach_agent(env: ApiEnv, *records: WeighingRecord, **kwargs: Any) -> SavingAgentLink:
    link = SavingAgentLink(env.hub, env.scale_id, env.factory, list(records), **kwargs)
    env.hub.attach(env.scale_id, link)
    return link


def _post(env: ApiEnv, **overrides: Any) -> httpx.Response:
    return env.client.post("/api/v2/weighings", json=_command(**overrides), headers=_auth())


def _audit_rows(env: ApiEnv, action: str = "weigh_request_v2") -> list[dict[str, Any]]:
    with env.factory() as session:
        rows = (
            session.execute(select(AuditLog).where(AuditLog.action == action).order_by(AuditLog.id))
            .scalars()
            .all()
        )
        return [{"actor": r.actor, **dict(r.details or {})} for r in rows]


# ---------------------------------------------------------------------------
# Авторизация и валидация
# ---------------------------------------------------------------------------


class TestAuthAndValidation:
    def test_missing_token_401(self, api_env: ApiEnv) -> None:
        response = api_env.client.post("/api/v2/weighings", json=_command())
        assert response.status_code == 401
        assert response.json() == {
            "code": "ERR_UNAUTHORIZED",
            "message": "нет или неверный сервисный токен",
        }

    def test_wrong_token_401(self, api_env: ApiEnv) -> None:
        response = api_env.client.post("/api/v2/weighings", json=_command(), headers=_auth("wrong"))
        assert response.status_code == 401

    def test_ip_not_allowed_403(self, apiv2_db_engine: Engine, tmp_path: Path) -> None:
        env = _build_env(apiv2_db_engine, tmp_path, allowed_ips=frozenset({"10.0.0.1"}))
        response = env.client.post("/api/v2/weighings", json=_command(), headers=_auth())
        assert response.status_code == 403
        assert response.json()["code"] == "ERR_FORBIDDEN"

    def test_ip_allowed_passes(self, apiv2_db_engine: Engine, tmp_path: Path) -> None:
        """TestClient приходит с адреса testclient — он в allowlist → доходим до 404 маршрута."""
        env = _build_env(apiv2_db_engine, tmp_path, allowed_ips=frozenset({"testclient"}))
        response = env.client.post(
            "/api/v2/weighings", json=_command(ais_object="9999"), headers=_auth()
        )
        assert response.status_code == 404

    def test_broken_json_422(self, api_env: ApiEnv) -> None:
        response = api_env.client.post(
            "/api/v2/weighings",
            content=b'{"ais_ref": ',
            headers={**_auth(), "Content-Type": "application/json"},
        )
        assert response.status_code == 422
        assert response.json()["code"] == "ERR_VALIDATION"

    def test_missing_operator_422_with_details(self, api_env: ApiEnv) -> None:
        response = _post(api_env, operator=...)
        assert response.status_code == 422
        body = response.json()
        assert body["code"] == "ERR_VALIDATION"
        assert any(d["field"] == "operator" for d in body["details"])

    def test_prefix_mismatch_422(self, api_env: ApiEnv) -> None:
        response = _post(api_env, ais_ref="TAR000012206")
        assert response.status_code == 422
        assert any("префикс" in d["error"] for d in response.json()["details"])

    def test_unknown_scale_404_and_audited(self, api_env: ApiEnv) -> None:
        response = _post(api_env, ais_object="0099")
        assert response.status_code == 404
        assert response.json()["code"] == "ERR_UNKNOWN_SCALE"
        rows = _audit_rows(api_env)
        assert rows[-1]["actor"] == f"ais:{INTEGRATOR}"
        assert rows[-1]["code"] == "ERR_UNKNOWN_SCALE"
        assert rows[-1]["request"]["ais_object"] == "0099"

    def test_scale_no_routes_to_second_scale(self, api_env: ApiEnv) -> None:
        """Кант, весы 2 — свой маршрут; агента нет → ERR_AGENT_OFFLINE, а не 404."""
        response = _post(api_env, ais_object=KANT_OBJECT, scale_no=2)
        assert response.status_code == 200
        assert response.json()["code"] == "ERR_AGENT_OFFLINE"
        response = _post(api_env, ais_object=KANT_OBJECT, scale_no=1)
        assert response.status_code == 404


# ---------------------------------------------------------------------------
# Команда: исходы и документ операции
# ---------------------------------------------------------------------------


class TestCommand:
    def test_agent_offline_is_200_with_code(self, api_env: ApiEnv) -> None:
        response = _post(api_env)
        assert response.status_code == 200
        body = response.json()
        assert body["code"] == "ERR_AGENT_OFFLINE"
        assert "message" in body and "weighing" not in body

    def test_success_document_with_applied_tare(self, api_env: ApiEnv) -> None:
        taring = _seed_taring(api_env)
        record = _make_record(
            tare_value=15300.0, tare_weighing_uuid=taring.uuid, netto=43310.0 - 15300.0
        )
        photos = [
            PhotoMeta(role=CameraRole.FRONT, filename="front.jpg", sha256=SHA_A, size_bytes=10),
            PhotoMeta(role=CameraRole.REAR, filename="rear.jpg", sha256=SHA_B, size_bytes=10),
        ]
        link = _attach_agent(api_env, record, photos=photos)

        response = _post(api_env)
        assert response.status_code == 200
        body = response.json()
        assert body["code"] == "OK"
        assert "repeated" not in body
        doc = body["weighing"]

        # команда агенту: нормализованные номера и ФИО
        assert link.requests[0].vehicle_number == "01KG777AAA"
        assert link.requests[0].trailer_number == "01KG500AB"
        assert link.requests[0].operator == "Акимов Нурлан Боронбаевич"

        assert doc["id"] == str(link.saved[0])
        assert doc["card_number"] == "ВЕС-20260814-153012"
        assert doc["operation"] == "weighing"
        assert doc["source"] == "ais"
        assert doc["ais_ref"] == "WEI000094176"
        assert doc["site"] == {
            "code": "kyzyl-kyia",
            "name": "СВХ «Кызыл-Кыя»",
            "ais_object": "0014",
        }
        assert doc["scale"]["id"] == api_env.scale_id
        assert doc["scale"]["no"] == 1
        assert doc["scale"]["verification"] == {
            "number": "0123456",
            "date": "2026-03-01",
            "valid_until": "2027-03-01",
        }
        assert doc["weighed_at"] == "2026-08-14T15:30:12+06:00"
        assert doc["recorded_at"] is not None
        assert doc["vehicle_number"] == "01KG777AAA"
        assert doc["trailer_number"] == "01KG500AB"
        assert doc["operator"] == "Акимов Нурлан Боронбаевич"
        assert doc["unit"] == "kg"
        assert doc["massa"] == 43310.0
        assert doc["tare"] == {
            "status": "applied",
            "id": str(taring.uuid),
            "card_number": "ТАР-20260612-102100",
            "ais_ref": "TAR000012206",
            "tared_at": "2026-06-12T10:21:00+06:00",
            "massa": 15300.0,
        }
        assert doc["netto"] == 28010.0
        # фото: канонические пути центра, файлы ещё не доехали
        front = doc["photos"]["front"]
        assert front["url"].endswith("_photo1.jpeg") and front["url"].startswith("/vesy/")
        assert front["sha256"] == SHA_A and front["available"] is False
        assert doc["photos"]["rear"]["url"].endswith("_photo2.jpeg")
        assert len(doc["checksum"]) == 64

        rows = _audit_rows(api_env)
        assert rows[-1]["code"] == "OK"
        assert rows[-1]["record_uuid"] == doc["id"]
        assert rows[-1]["request"]["ais_ref"] == "WEI000094176"

    def test_photo_available_when_file_delivered(self, api_env: ApiEnv) -> None:
        photos = [PhotoMeta(role=CameraRole.FRONT, filename="f.jpg", sha256=SHA_A, size_bytes=1)]
        _attach_agent(api_env, _make_record(), photos=photos)
        doc = _post(api_env).json()["weighing"]
        url = doc["photos"]["front"]["url"]
        target = api_env.photos_dir / url.lstrip("/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"jpeg")
        again = api_env.client.get(f"/api/v2/weighings/{doc['id']}", headers=_auth()).json()
        assert again["weighing"]["photos"]["front"]["available"] is True
        assert again["weighing"]["photos"]["rear"] is None

    def test_no_taring_gives_tare_null(self, api_env: ApiEnv) -> None:
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        assert doc["tare"] is None and doc["netto"] is None

    def test_expired_taring_reported_with_data(self, api_env: ApiEnv) -> None:
        """Тарирование старше 3 месяцев к моменту взвешивания: status expired, нетто null."""
        old = _seed_taring(
            api_env, ais_ref="TAR000011840", weighed_at=datetime(2026, 4, 12, 4, 21, tzinfo=UTC)
        )
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        assert doc["tare"]["status"] == "expired"
        assert doc["tare"]["id"] == str(old.uuid)
        assert doc["tare"]["ais_ref"] == "TAR000011840"
        assert doc["tare"]["tared_at"] == "2026-04-12T10:21:00+06:00"
        assert doc["tare"]["massa"] == 15300.0
        assert doc["netto"] is None

    def test_active_but_not_applied_taring(self, api_env: ApiEnv) -> None:
        """Тара действовала, но агент её не подставил (реплика отстала): not_applied."""
        _seed_taring(api_env, weighed_at=datetime(2026, 7, 1, 5, 0, tzinfo=UTC))
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        assert doc["tare"]["status"] == "not_applied"
        assert doc["netto"] is None

    def test_taring_after_weighing_not_shown(self, api_env: ApiEnv) -> None:
        """Тарирование позже момента взвешивания к нему не относится."""
        _seed_taring(api_env, weighed_at=WEIGHED_AT + timedelta(hours=1))
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        assert doc["tare"] is None

    def test_taring_command_document(self, api_env: ApiEnv) -> None:
        link = _attach_agent(api_env, _make_taring())
        response = _post(api_env, ais_ref="TAR000012206", operation="taring")
        body = response.json()
        assert body["code"] == "OK"
        doc = body["weighing"]
        assert doc["operation"] == "taring"
        assert doc["card_number"] == "ТАР-20260612-102100"
        assert doc["ais_ref"] == "TAR000012206"
        assert doc["massa"] == 15300.0
        assert doc["tare"] is None and doc["netto"] is None
        assert link.requests[0].operation is Operation.TARING
        # тарирование попало в реестр активных тар
        with api_env.factory() as session:
            active = repo.find_active_tare(session, "01KG777AAA", "01KG500AB")
        assert active is not None and active.tare_value == 15300.0

    def test_failure_outcome_not_recorded_and_retry_allowed(self, api_env: ApiEnv) -> None:
        """Отказ — {code, message}, ничего не записано; повтор с тем же номером — новая попытка."""
        refusal = _make_record(
            code=ErrorCode.ERR_VEHICLE_TIMEOUT,
            massa=None,
            weighed_at=None,
            message="на весах нет АТС с зафиксированным весом",
        )
        link = _attach_agent(api_env, refusal, _make_record())
        first = _post(api_env).json()
        assert first == {
            "code": "ERR_VEHICLE_TIMEOUT",
            "message": "на весах нет АТС с зафиксированным весом",
        }
        with api_env.factory() as session:
            assert repo.weighing_by_ais_ref(session, "WEI000094176") is None
        second = _post(api_env).json()
        assert second["code"] == "OK" and "repeated" not in second
        assert len(link.requests) == 2

    def test_timeout_is_err_internal(self, apiv2_db_engine: Engine, tmp_path: Path) -> None:
        env = _build_env(apiv2_db_engine, tmp_path)
        env.config = ApiV2Config(
            service_tokens={TOKEN: INTEGRATOR}, photos_dir=tmp_path, weigh_timeout_s=0.2
        )
        app = FastAPI()
        app.include_router(create_api_v2_router(env.hub, env.factory, env.config))

        class Silent:
            async def send_text(self, data: str) -> None:
                return None

        env.hub.attach(env.scale_id, Silent())
        response = TestClient(app).post("/api/v2/weighings", json=_command(), headers=_auth())
        assert response.json()["code"] == "ERR_INTERNAL"


# ---------------------------------------------------------------------------
# Идемпотентность по номеру документа АИС
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_repeat_returns_same_document_without_second_weighing(self, api_env: ApiEnv) -> None:
        link = _attach_agent(api_env, _make_record())
        first = _post(api_env).json()
        second = _post(api_env).json()
        assert second["code"] == "OK" and second["repeated"] is True
        assert second["weighing"]["id"] == first["weighing"]["id"]
        assert len(link.requests) == 1
        rows = _audit_rows(api_env)
        assert rows[-1]["repeated"] is True

    def test_repeat_ignores_changed_parameters(self, api_env: ApiEnv) -> None:
        """Документ уже зафиксирован — параметры повтора не сверяются (4.5)."""
        _attach_agent(api_env, _make_record())
        first = _post(api_env).json()
        second = _post(api_env, vehicle_number="01KG000ZZZ").json()
        assert second["repeated"] is True
        assert second["weighing"]["vehicle_number"] == first["weighing"]["vehicle_number"]

    def test_repeat_while_running_waits_for_same_outcome(self, api_env: ApiEnv) -> None:
        """Повтор во время выполнения не запускает вторую команду и получает тот же исход."""
        link = _attach_agent(api_env, _make_record())

        async def scenario() -> tuple[dict[str, Any], dict[str, Any]]:
            link.started, link.gate = asyncio.Event(), asyncio.Event()
            transport = httpx.ASGITransport(app=api_env.app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                first_task = asyncio.create_task(
                    client.post("/api/v2/weighings", json=_command(), headers=_auth())
                )
                await link.started.wait()  # первая команда уже у «агента» и ждёт
                second_task = asyncio.create_task(
                    client.post("/api/v2/weighings", json=_command(), headers=_auth())
                )
                # даём второму запросу дойти до проверки inflight, затем отпускаем агента
                await asyncio.sleep(0.05)
                link.gate.set()
                first, second = await asyncio.gather(first_task, second_task)
                return first.json(), second.json()

        first, second = asyncio.run(scenario())
        assert first["code"] == "OK" and "repeated" not in first
        assert second["code"] == "OK" and second["repeated"] is True
        assert second["weighing"]["id"] == first["weighing"]["id"]
        assert len(link.requests) == 1

    def test_ais_ref_saved_in_same_transaction_as_record(self, api_env: ApiEnv) -> None:
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        with api_env.factory() as session:
            link_row = session.execute(
                select(WeighingAisRef).where(WeighingAisRef.ais_ref == "WEI000094176")
            ).scalar_one()
            assert link_row.origin == "command"
            weighing = repo.weighing_by_ais_ref(session, "WEI000094176")
            assert weighing is not None and str(weighing.uuid) == doc["id"]

    def test_hub_forgets_ais_ref_after_ttl(self) -> None:
        """Номер без результата живёт TTL, а не вечно (поздние ответы после тайм-аута)."""
        from center.agents_ws.hub import AIS_REF_TTL_S

        clock = {"now": 1000.0}
        hub = AgentHub(clock=lambda: clock["now"])
        old, fresh = uuid4(), uuid4()
        hub.remember_ais_ref(old, "WEI000000001")
        clock["now"] += AIS_REF_TTL_S + 1
        hub.remember_ais_ref(fresh, "WEI000000002")  # чистка старых при регистрации
        assert hub.take_ais_ref(old) is None
        assert hub.take_ais_ref(fresh) == "WEI000000002"
        assert hub.take_ais_ref(fresh) is None


# ---------------------------------------------------------------------------
# GET: сверка
# ---------------------------------------------------------------------------


class TestQueries:
    def test_get_by_id_and_unknown(self, api_env: ApiEnv) -> None:
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        response = api_env.client.get(f"/api/v2/weighings/{doc['id']}", headers=_auth())
        assert response.status_code == 200
        assert response.json()["weighing"]["id"] == doc["id"]
        missing = api_env.client.get(f"/api/v2/weighings/{uuid4()}", headers=_auth())
        assert missing.status_code == 404 and missing.json()["code"] == "ERR_NOT_FOUND"
        bad = api_env.client.get("/api/v2/weighings/not-a-uuid", headers=_auth())
        assert bad.status_code == 404
        unauthorized = api_env.client.get(f"/api/v2/weighings/{doc['id']}")
        assert unauthorized.status_code == 401

    def test_get_by_ais_ref(self, api_env: ApiEnv) -> None:
        _attach_agent(api_env, _make_record())
        doc = _post(api_env).json()["weighing"]
        found = api_env.client.get(
            "/api/v2/weighings", params={"ais_ref": "WEI000094176"}, headers=_auth()
        ).json()
        assert [w["id"] for w in found["weighings"]] == [doc["id"]]
        empty = api_env.client.get(
            "/api/v2/weighings", params={"ais_ref": "WEI000000000"}, headers=_auth()
        ).json()
        assert empty["weighings"] == [] and empty["total"] == 0

    def test_list_period_filters_and_paging(self, api_env: ApiEnv) -> None:
        with api_env.factory() as session:
            for day, ref in ((13, "WEI000000013"), (14, "WEI000000014"), (15, "WEI000000015")):
                repo.save_weighing_record(
                    session,
                    api_env.scale_id,
                    _make_record(weighed_at=datetime(2026, 8, day, 6, 0, tzinfo=UTC)),
                    ais_ref=ref,
                )
            # офлайн-запись без номера АИС на других весах (Кант)
            repo.save_weighing_record(
                session,
                api_env.kant_scale_id,
                _make_record(
                    weighed_at=datetime(2026, 8, 14, 7, 0, tzinfo=UTC),
                    source=WeighingSource.LOCAL_OFFLINE,
                ),
            )
        base = {"from": "2026-08-14T00:00:00+06:00", "to": "2026-08-15T00:00:00+06:00"}
        day14 = api_env.client.get("/api/v2/weighings", params=base, headers=_auth()).json()
        assert day14["total"] == 2
        assert {w["ais_ref"] for w in day14["weighings"]} == {"WEI000000014", None}

        kk_only = api_env.client.get(
            "/api/v2/weighings", params={**base, "ais_object": AIS_OBJECT}, headers=_auth()
        ).json()
        assert [w["ais_ref"] for w in kk_only["weighings"]] == ["WEI000000014"]

        unlinked = api_env.client.get(
            "/api/v2/weighings",
            params={"source": "local_offline", "unlinked": "true"},
            headers=_auth(),
        ).json()
        assert unlinked["total"] == 1 and unlinked["weighings"][0]["ais_ref"] is None

        paged = api_env.client.get(
            "/api/v2/weighings", params={"per_page": 2, "page": 2}, headers=_auth()
        ).json()
        assert paged["total"] == 4 and paged["page"] == 2 and len(paged["weighings"]) == 2
        # порядок — по времени взвешивания по возрастанию
        all_rows = api_env.client.get("/api/v2/weighings", headers=_auth()).json()["weighings"]
        assert [w["weighed_at"] for w in all_rows] == sorted(w["weighed_at"] for w in all_rows)

    def test_list_bad_params_422(self, api_env: ApiEnv) -> None:
        response = api_env.client.get(
            "/api/v2/weighings", params={"from": "вчера"}, headers=_auth()
        )
        assert response.status_code == 422
        response = api_env.client.get(
            "/api/v2/weighings", params={"operation": "x"}, headers=_auth()
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Обратная связь: номер документа АИС для офлайн-операции (7.5)
# ---------------------------------------------------------------------------


class TestAisRefCallback:
    def _offline_taring(self, env: ApiEnv) -> WeighingRecord:
        return _seed_taring(env, ais_ref=None, source=WeighingSource.LOCAL_OFFLINE)

    def test_link_then_visible_in_document_and_tare_block(self, api_env: ApiEnv) -> None:
        taring = self._offline_taring(api_env)
        url = f"/api/v2/weighings/{taring.uuid}/ais_ref"
        response = api_env.client.post(url, json={"ais_ref": "TAR000030001"}, headers=_auth())
        assert response.status_code == 200
        assert response.json()["weighing"]["ais_ref"] == "TAR000030001"
        # повтор с тем же номером — 200 без изменений
        again = api_env.client.post(url, json={"ais_ref": "TAR000030001"}, headers=_auth())
        assert again.status_code == 200
        with api_env.factory() as session:
            row = session.execute(
                select(WeighingAisRef).where(WeighingAisRef.ais_ref == "TAR000030001")
            ).scalar_one()
            assert row.origin == "callback"
        # взвешивание по этой таре несёт её номер во вложении
        record = _make_record(tare_value=15300.0, tare_weighing_uuid=taring.uuid, netto=28010.0)
        _attach_agent(api_env, record)
        doc = _post(api_env).json()["weighing"]
        assert doc["tare"]["ais_ref"] == "TAR000030001"
        rows = _audit_rows(api_env, "ais_ref_link")
        assert rows[0]["ais_ref"] == "TAR000030001" and rows[0]["outcome"] == "linked"

    def test_conflicts_409(self, api_env: ApiEnv) -> None:
        taring = self._offline_taring(api_env)
        other = self._offline_taring(api_env)
        url = f"/api/v2/weighings/{taring.uuid}/ais_ref"
        api_env.client.post(url, json={"ais_ref": "TAR000030001"}, headers=_auth())
        # у операции уже другой номер
        response = api_env.client.post(url, json={"ais_ref": "TAR000030002"}, headers=_auth())
        assert response.status_code == 409
        assert response.json()["code"] == "ERR_ALREADY_LINKED"
        # номер занят другой операцией
        response = api_env.client.post(
            f"/api/v2/weighings/{other.uuid}/ais_ref",
            json={"ais_ref": "TAR000030001"},
            headers=_auth(),
        )
        assert response.status_code == 409

    def test_prefix_mismatch_422_and_unknown_404(self, api_env: ApiEnv) -> None:
        taring = self._offline_taring(api_env)
        response = api_env.client.post(
            f"/api/v2/weighings/{taring.uuid}/ais_ref",
            json={"ais_ref": "WEI000030001"},
            headers=_auth(),
        )
        assert response.status_code == 422 and response.json()["code"] == "ERR_VALIDATION"
        response = api_env.client.post(
            f"/api/v2/weighings/{uuid4()}/ais_ref",
            json={"ais_ref": "TAR000030001"},
            headers=_auth(),
        )
        assert response.status_code == 404
        response = api_env.client.post(
            f"/api/v2/weighings/{taring.uuid}/ais_ref", json={}, headers=_auth()
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Инструменты для разработчиков АИС (без сети)
# ---------------------------------------------------------------------------


class TestTools:
    @pytest.mark.parametrize("module", ["tools.ais_client_v2", "tools.ais_consumer"])
    def test_help_runs(self, module: str) -> None:
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", module, "--help"], capture_output=True, text=True, timeout=60
        )
        assert result.returncode == 0, result.stderr
        assert "АИС" in result.stdout

    def test_client_requires_token(self) -> None:
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-m", "tools.ais_client_v2", "get", "x"],
            capture_output=True,
            text=True,
            timeout=60,
            env={"PATH": "/usr/bin:/bin"},
        )
        assert result.returncode == 2
        assert "токен" in result.stderr

    def test_consumer_masks_password(self) -> None:
        from tools.ais_consumer import _masked, _summary

        assert _masked("amqp://ais-svh:secret@192.168.140.70:5672/vesy") == (
            "amqp://ais-svh:***@192.168.140.70:5672/vesy"
        )
        assert "secret" not in _masked("amqp://ais-svh:secret@host/vesy")
        line = _summary(
            {
                "type": "weighing.completed",
                "event_id": "e1",
                "ais_object": "0013",
                "weighing": {
                    "id": "w1",
                    "operation": "weighing",
                    "source": "local_offline",
                    "vehicle_number": "01KG777AAA",
                    "trailer_number": None,
                    "massa": 21850.0,
                    "tare": {"status": "expired", "massa": 15300.0},
                    "netto": None,
                    "ais_ref": None,
                },
            }
        )
        assert "СВХ 0013" in line and "[expired]" in line and "01KG777AAA/—" in line
