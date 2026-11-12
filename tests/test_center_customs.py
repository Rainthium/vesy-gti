"""Тесты таможенного потока (center/customs + outbox ``customs_events``) —
контракт customs-events-v1, версия 1.0 (11.11.2026).

Закрепляют:
- outbox: событие ``weighing.completed`` таможне ставится на КАЖДУЮ операцию
  любого источника и любых весов (в том числе не привязанных к АИС), в одной
  транзакции с записью; сторно → ``weighing.reversed`` по исходной записи;
  перенесённые тарирования и их сторно в таможню не идут; поток АИС «СВХ»
  при этом не меняется;
- документ: документ v2 для АИС остаётся прежним (нет таможенных полей), а
  таможенный отличается ровно тремя полями; ``weighed_at_utc`` — строка, по
  которой ``checksum`` пересчитывается формулой приложения Б; контрольный
  пример контракта;
- отказы: ``record_tare_rejection`` идемпотентна, берёт лимит весов и массу
  из текста; строка неизменяема; разбор массы — пара к формирователю текста;
- конверт: поля раздела 4, ``rejection`` вместо ``weighing`` у отказа, ключ
  ``<type>.<site.code>``, ``event_id`` детерминирован и не совпадает с АИС.
"""

import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center import events as ais_events
from center.api_v2.documents import build_document
from center.customs.documents import build_customs_document, build_rejection, weighed_at_utc
from center.customs.events import build_customs_event, event_id_for, routing_key_for
from center.db import repo
from center.db.models import (
    CustomsEvent,
    Scale,
    ScaleKind,
    Site,
    TareRejection,
    Weighing,
    WeighingEvent,
    weighing_checksum,
)
from center.db.session import database_url, make_session_factory
from center.tare_import import AisTaring
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import PhotoMeta, WeighingRecord
from shared.tare import parse_tare_too_heavy, parse_tare_too_heavy_mass, tare_too_heavy_message
from tests.test_center_db import ALL_TABLES, _upgrade_head

# момент с микросекундами — как в контрольном примере приложения Б
WEIGHED_AT = datetime(2026, 8, 14, 9, 30, 12, 345678, tzinfo=UTC)
NOW = datetime(2026, 11, 12, 4, 14, 8, tzinfo=UTC)  # 10:14:08 по Бишкеку
SHA_FRONT = "2d8d693177ac44895fc02c009ec3f6af32e51eb00783c17000d7051d1662b93a"
SHA_REAR = "32b36ae43a5ea4895ec88864321da8b5f4b8d6efb60c6c10183bb6def8cbf1ad"
CONTRACT_CHECKSUM = "74d9970b847cdb18990e7a397979767734b55f6916bccca7ee648f184bc45099"
REFUSAL_TEXT = tare_too_heavy_message(43310.0, 25000.0)


@pytest.fixture(scope="session")
def customs_db_url() -> Iterator[URL]:
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip("PostgreSQL недоступен — тесты таможенного потока пропущены")
    db_name = f"ves_test_customs_{os.getpid()}"
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
def customs_db_engine(customs_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(customs_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def factory(customs_db_engine: Engine) -> sessionmaker[Session]:
    with customs_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    return make_session_factory(customs_db_engine)


def _seed_scale(
    factory: sessionmaker[Session],
    *,
    ais_object: str | None = "0021",
    customs_code: str | None = "41714106",
    thresholds: dict[str, object] | None = None,
) -> int:
    with factory() as session:
        site = Site(code="kokcho-koz", name="СВХ «Кокчо-Коз»", customs_code=customs_code)
        session.add(site)
        session.flush()
        scale = Scale(
            site_id=site.id,
            name="Весы XK3190-DS3",
            kind=ScaleKind.STATIC,
            driver="xk3190",
            ais_object=ais_object,
            ais_scale_no=1 if ais_object else None,
            thresholds=thresholds,
        )
        session.add(scale)
        session.flush()
        scale_id = scale.id
        session.commit()
    return scale_id


def _record(source: WeighingSource = WeighingSource.AIS, **overrides: Any) -> WeighingRecord:
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
        "source": source,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _refusal(**overrides: Any) -> WeighingRecord:
    fields: dict[str, Any] = {
        "operation": Operation.TARING,
        "code": ErrorCode.ERR_TARE_TOO_HEAVY,
        "massa": None,
        "weighed_at": None,
        "message": REFUSAL_TEXT,
    }
    fields.update(overrides)
    return _record(**fields)


def _photos() -> list[PhotoMeta]:
    return [
        PhotoMeta(role=CameraRole.FRONT, filename="f.jpg", sha256=SHA_FRONT, size_bytes=1),
        PhotoMeta(role=CameraRole.REAR, filename="r.jpg", sha256=SHA_REAR, size_bytes=1),
    ]


def _customs_events(factory: sessionmaker[Session]) -> list[tuple[str, int | None, int | None]]:
    with factory() as session:
        rows = session.execute(select(CustomsEvent).order_by(CustomsEvent.id)).scalars()
        return [(row.event_type, row.weighing_id, row.rejection_id) for row in rows]


def _ais_events(factory: sessionmaker[Session]) -> int:
    with factory() as session:
        return len(session.execute(select(WeighingEvent)).scalars().all())


def _weighing(session: Session, record_uuid: UUID) -> Weighing:
    return session.execute(select(Weighing).where(Weighing.uuid == record_uuid)).scalar_one()


def _contract_checksum(doc: dict[str, Any]) -> str:
    """Функция проверки из приложения Б контракта — дословно."""
    photos = sorted(p["sha256"] for p in doc["photos"].values() if p)
    parts = [
        doc["id"],
        doc["operation"],
        "OK",
        f"{doc['massa']:.3f}",
        doc["weighed_at_utc"],
        doc["vehicle_number"],
        doc["source"],
        *photos,
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# текст отказа ↔ масса
# ---------------------------------------------------------------------------


class TestRefusalMass:
    @pytest.mark.parametrize("weight", [25010.0, 43310.0, 100000.0, 1234567.0, 999.6])
    def test_round_trip(self, weight: float) -> None:
        """Разбор — пара к формирователю: масса возвращается округлённой до кг."""
        assert parse_tare_too_heavy_mass(tare_too_heavy_message(weight, 25000.0)) == round(weight)
        assert parse_tare_too_heavy(tare_too_heavy_message(weight, 30000.0)) == (
            round(weight),
            30000.0,
        )

    def test_unrecognized_text_gives_none(self) -> None:
        assert parse_tare_too_heavy_mass(None) is None
        assert parse_tare_too_heavy_mass("") is None
        assert parse_tare_too_heavy_mass("ERR_TARE_TOO_HEAVY") is None
        assert parse_tare_too_heavy_mass("Масса кг больше допустимой тары 25 000 кг") is None


# ---------------------------------------------------------------------------
# outbox
# ---------------------------------------------------------------------------


class TestOutbox:
    def test_every_operation_any_source(self, factory: sessionmaker[Session]) -> None:
        """Команда АИС и ручная операция — обе в таможню; в АИС — только ручная."""
        scale_id = _seed_scale(factory)
        online, offline = _record(), _record(WeighingSource.LOCAL_OFFLINE)
        with factory() as session:
            assert repo.save_weighing_record(session, scale_id, online)
            assert repo.save_weighing_record(session, scale_id, offline)
        events = _customs_events(factory)
        assert [e[0] for e in events] == ["weighing.completed", "weighing.completed"]
        assert all(e[1] is not None and e[2] is None for e in events)
        assert _ais_events(factory) == 1  # поток АИС — как раньше: только офлайн

    def test_scale_without_ais_binding(self, factory: sessionmaker[Session]) -> None:
        """Весы без привязки АИС: в АИС событие не ставится, в таможню — да."""
        scale_id = _seed_scale(factory, ais_object=None)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        assert len(_customs_events(factory)) == 1
        assert _ais_events(factory) == 0

    def test_refusal_and_repeat_do_not_enqueue(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            assert not repo.save_weighing_record(session, scale_id, _refusal())
            assert repo.save_weighing_record(session, scale_id, record)
            assert not repo.save_weighing_record(session, scale_id, record)  # повтор досылки
        assert len(_customs_events(factory)) == 1

    def test_storno_enqueues_reversed_for_original(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        record = _record(operation=Operation.TARING, massa=15300.0)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
            original = _weighing(session, record.uuid)
            storno = repo.storno_weighing(
                session, original, actor="panel:igor", reason="гружёная машина"
            )
            original_id, storno_id = original.id, storno.id
        events = _customs_events(factory)
        assert events == [
            ("weighing.completed", original_id, None),
            ("weighing.reversed", original_id, None),
        ]
        assert storno_id != original_id  # сама запись-сторно событием не идёт

    def test_imported_taring_and_its_storno_stay_out(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        taring = AisTaring(
            tare_ref="TAR000099001",
            entry_ref="SVH000099001",
            vehicle_number="01KG777AAA",
            trailer_number="01KG500AB",
            massa=15620.0,
            object_name="Кокчо-Коз",
            operator="Акимов Нурлан Боронбаевич",
            tared_at=datetime(2026, 10, 1, 4, 0, tzinfo=UTC),
            completed=True,
            ignored=False,
        )
        with factory() as session:
            row = repo.save_imported_taring(session, scale_id, taring, imported_at=NOW)
            assert row is not None
            session.commit()
            repo.storno_weighing(session, row, actor="panel:igor", reason="ошибка переноса")
        assert _customs_events(factory) == []

    def test_subject_check_constraint(self, factory: sessionmaker[Session]) -> None:
        """Строка outbox ссылается ровно на одно: операцию либо отказ."""
        with factory() as session:
            session.add(CustomsEvent(event_type="weighing.completed"))
            with pytest.raises(IntegrityError):
                session.commit()


# ---------------------------------------------------------------------------
# документ
# ---------------------------------------------------------------------------


class TestDocument:
    def test_ais_document_unchanged_customs_adds_three_fields(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """Фиксатор: в документе АИС нет таможенных полей, состав ключей прежний;
        таможенный документ отличается от него ровно тремя полями."""
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record, photos=_photos())
            weighing = _weighing(session, record.uuid)
            ais_doc = build_document(session, weighing, photos_dir=tmp_path)
            customs_doc = build_customs_document(session, weighing, photos_dir=tmp_path)
        assert list(ais_doc) == [
            "id",
            "card_number",
            "operation",
            "source",
            "ais_ref",
            "site",
            "scale",
            "weighed_at",
            "recorded_at",
            "vehicle_number",
            "trailer_number",
            "operator",
            "unit",
            "massa",
            "tare",
            "netto",
            "photos",
            "storno",
            "checksum",
        ]
        assert list(ais_doc["site"]) == ["code", "name", "ais_object"]
        assert list(ais_doc["scale"]) == ["id", "no", "name", "verification"]

        assert list(customs_doc["site"]) == ["code", "name", "customs_code", "ais_object"]
        assert customs_doc["site"]["customs_code"] == "41714106"
        assert list(customs_doc["scale"]) == ["id", "no", "kind", "name", "verification"]
        assert customs_doc["scale"]["kind"] == "static"
        keys = list(customs_doc)
        assert keys[keys.index("weighed_at") + 1] == "weighed_at_utc"
        # остальное совпадает с документом АИС
        stripped = dict(customs_doc)
        del stripped["weighed_at_utc"]
        stripped["site"] = {k: v for k, v in customs_doc["site"].items() if k != "customs_code"}
        stripped["scale"] = {k: v for k, v in customs_doc["scale"].items() if k != "kind"}
        assert stripped == ais_doc

    def test_checksum_recomputed_by_contract_formula(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """По таможенному документу сумма пересчитывается функцией приложения Б;
        ``weighed_at`` при этом округлён до секунды, микросекунды — в ``_utc``."""
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record, photos=_photos())
            doc = build_customs_document(
                session, _weighing(session, record.uuid), photos_dir=tmp_path
            )
        assert doc["weighed_at"] == "2026-08-14T15:30:12+06:00"
        assert doc["weighed_at_utc"] == "2026-08-14T09:30:12.345678+00:00"
        assert _contract_checksum(doc) == doc["checksum"]

    def test_contract_control_example(self) -> None:
        """Контрольный пример приложения Б: его проверил Таргет-центр 10.11.2026."""
        assert (
            weighing_checksum(
                uuid=UUID("0d9b4d3e-8c0f-4b41-9f4c-2a6f1e3b7c55"),
                operation="weighing",
                code="OK",
                massa=43310.0,
                weighed_at=WEIGHED_AT,
                vehicle_number="01KG777AAA",
                source="ais",
                photo_sha256s=[SHA_REAR, SHA_FRONT],
            )
            == CONTRACT_CHECKSUM
        )

    def test_weighed_at_utc_forms(self) -> None:
        assert weighed_at_utc(None) is None
        # ноль микросекунд — дробной части нет (оговорено в приложении Б)
        assert (
            weighed_at_utc(datetime(2026, 8, 14, 9, 30, 12, tzinfo=UTC))
            == "2026-08-14T09:30:12+00:00"
        )


# ---------------------------------------------------------------------------
# отклонённые тарирования
# ---------------------------------------------------------------------------


class TestRejection:
    def test_record_and_event(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory, thresholds={"max_tare_kg": 30000})
        refusal = _refusal()
        with factory() as session:
            assert repo.record_tare_rejection(
                session, scale_id, refusal, operator="Иванов И.И.", ais_ref="TAR000004512", now=NOW
            )
            # повтор того же отказа — без второй строки и второго события
            assert not repo.record_tare_rejection(
                session, scale_id, refusal, operator="Иванов И.И.", ais_ref="TAR000004512"
            )
            row = session.execute(select(TareRejection)).scalar_one()
            assert row.uuid == refusal.uuid and row.code == "ERR_TARE_TOO_HEAVY"
            # лимит — тот, по которому отказал агент (из текста), а не настройка центра
            assert row.massa_kg == 43310.0 and row.tare_limit_kg == 25000.0
            assert row.vehicle_number == "01KG777AAA" and row.trailer_number == "01KG500AB"
            rejection_id = row.id
        assert _customs_events(factory) == [("weighing.rejected", None, rejection_id)]
        assert _ais_events(factory) == 0
        with factory() as session:
            assert session.execute(select(Weighing)).scalars().all() == []  # не операция

    def test_unparsed_text_falls_back_to_center_limit(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory, thresholds={"max_tare_kg": 30000})
        with factory() as session:
            repo.record_tare_rejection(
                session, scale_id, _refusal(message="отказ"), operator=None, ais_ref=None
            )
            row = session.execute(select(TareRejection)).scalar_one()
            assert row.tare_limit_kg == 30000.0 and row.massa_kg is None

    def test_default_limit_and_unparsed_mass(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.record_tare_rejection(
                session, scale_id, _refusal(message=None), operator=None, ais_ref=None
            )
            row = session.execute(select(TareRejection)).scalar_one()
            assert row.tare_limit_kg == 25000.0 and row.massa_kg is None
            assert row.message == "ERR_TARE_TOO_HEAVY"

    def test_rejection_immutable(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.record_tare_rejection(session, scale_id, _refusal(), operator=None, ais_ref=None)
        with factory() as session, pytest.raises(DBAPIError):
            session.execute(text("UPDATE tare_rejections SET massa_kg = 1"))
        with factory() as session, pytest.raises(DBAPIError):
            session.execute(text("DELETE FROM tare_rejections"))

    def test_rejection_body(self, factory: sessionmaker[Session]) -> None:
        """Тело ``rejection`` — поля и порядок раздела 5.5 контракта."""
        scale_id = _seed_scale(factory)
        refusal = _refusal()
        with factory() as session:
            repo.record_tare_rejection(
                session, scale_id, refusal, operator="Иванов И.И.", ais_ref="TAR000004512", now=NOW
            )
            body = build_rejection(session, session.execute(select(TareRejection)).scalar_one())
        assert list(body) == [
            "id",
            "operation",
            "code",
            "message",
            "rejected_at",
            "source",
            "ais_ref",
            "site",
            "scale",
            "vehicle_number",
            "trailer_number",
            "operator",
            "tare_limit",
            "massa",
        ]
        assert body["id"] == str(refusal.uuid)
        assert body["operation"] == "taring" and body["source"] == "ais"
        assert body["rejected_at"] == "2026-11-12T10:14:08+06:00"
        assert body["message"] == REFUSAL_TEXT
        assert body["tare_limit"] == 25000.0 and body["massa"] == 43310.0
        assert body["site"] == {
            "code": "kokcho-koz",
            "name": "СВХ «Кокчо-Коз»",
            "customs_code": "41714106",
            "ais_object": "0021",
        }
        assert body["scale"]["kind"] == "static" and body["scale"]["no"] == 1

    def test_scale_block_same_as_in_document(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """Блоки ``site`` и ``scale`` отказа — те же, что в документе операции;
        весы вида ``platform`` для таможни — ``static``."""
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            scale = session.get(Scale, scale_id)
            assert scale is not None
            scale.kind = ScaleKind.PLATFORM
            scale.verif_number = "857"
            session.commit()
            repo.save_weighing_record(session, scale_id, record)
            repo.record_tare_rejection(session, scale_id, _refusal(), operator=None, ais_ref=None)
            doc = build_customs_document(
                session, _weighing(session, record.uuid), photos_dir=tmp_path
            )
            body = build_rejection(session, session.execute(select(TareRejection)).scalar_one())
        assert body["scale"] == doc["scale"] and body["site"] == doc["site"]
        assert doc["scale"]["kind"] == "static"
        assert doc["scale"]["verification"]["number"] == "857"


# ---------------------------------------------------------------------------
# конверт события
# ---------------------------------------------------------------------------


class TestEnvelope:
    def test_ids_and_routing_key(self) -> None:
        record_uuid = UUID("0d9b4d3e-8c0f-4b41-9f4c-2a6f1e3b7c55")
        first = event_id_for(record_uuid, "weighing.completed")
        assert first == event_id_for(record_uuid, "weighing.completed")
        assert first != event_id_for(record_uuid, "weighing.reversed")
        # потоки независимы: идентификатор события АИС по той же операции другой
        assert first != ais_events.event_id_for(record_uuid, "weighing.completed")
        assert routing_key_for("weighing.rejected", "kokcho-koz") == "weighing.rejected.kokcho-koz"

    def test_weighing_envelope(self, factory: sessionmaker[Session], tmp_path: Path) -> None:
        scale_id = _seed_scale(factory, ais_object=None, customs_code=None)
        record = _record(WeighingSource.LOCAL_OFFLINE)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
            outbox = session.execute(select(CustomsEvent)).scalar_one()
            event = build_customs_event(session, outbox, photos_dir=tmp_path, now=NOW)
        assert event is not None
        assert event.routing_key == "weighing.completed.kokcho-koz"
        assert event.event_id == event_id_for(record.uuid, "weighing.completed")
        body = event.body
        assert list(body) == [
            "event_id",
            "type",
            "version",
            "published_at",
            "producer",
            "site",
            "scale_kind",
            "weighing",
        ]
        assert body["type"] == "weighing.completed" and body["version"] == 1
        assert body["published_at"] == "2026-11-12T10:14:08+06:00"
        assert body["producer"] == "vesy-center" and body["scale_kind"] == "static"
        assert body["site"] == {
            "code": "kokcho-koz",
            "name": "СВХ «Кокчо-Коз»",
            "customs_code": None,
        }
        assert body["weighing"]["id"] == str(record.uuid)
        assert body["weighing"]["site"]["ais_object"] is None

    def test_reversed_envelope_carries_storno(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory)
        record = _record(operation=Operation.TARING, massa=15300.0)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
            repo.storno_weighing(
                session, _weighing(session, record.uuid), actor="panel:igor", reason="ошибка"
            )
            outbox = session.execute(
                select(CustomsEvent).where(CustomsEvent.event_type == "weighing.reversed")
            ).scalar_one()
            event = build_customs_event(session, outbox, photos_dir=tmp_path, now=NOW)
        assert event is not None and event.routing_key == "weighing.reversed.kokcho-koz"
        assert event.body["weighing"]["id"] == str(record.uuid)
        assert event.body["weighing"]["storno"]["reason"] == "ошибка"

    def test_rejected_envelope_has_rejection_not_weighing(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory)
        refusal = _refusal()
        with factory() as session:
            repo.record_tare_rejection(
                session, scale_id, refusal, operator=None, ais_ref="TAR000004512", now=NOW
            )
            outbox = session.execute(select(CustomsEvent)).scalar_one()
            event = build_customs_event(session, outbox, photos_dir=tmp_path, now=NOW)
        assert event is not None
        assert event.routing_key == "weighing.rejected.kokcho-koz"
        assert event.event_id == event_id_for(refusal.uuid, "weighing.rejected")
        assert "weighing" not in event.body
        assert event.body["rejection"]["id"] == str(refusal.uuid)
        assert event.body["rejection"]["ais_ref"] == "TAR000004512"
