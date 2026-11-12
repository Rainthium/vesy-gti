# ruff: noqa: F811 — фикстуры БД импортированы из tests.test_center_customs
"""Тесты доступа таможенной системы и публикатора её событий —
контракт customs-events-v1, разделы 3, 7, 8 (версия 1.0).

Закрепляют:
- API v2 под токеном таможни: только чтение (команда и привязка номера АИС —
  403), таможенный вариант документа, все объекты (и без АИС), отбор по
  ``site``, справочник ``GET /api/v2/sites``; под токеном АИС — прежний
  документ и прежние права;
- публикатор: порция уходит с отметкой ``published_at``, сбой брокера —
  попытка и ошибка на строке, без брокера ничего не публикуется;
- брокер: свойства сообщения, существующий exchange без объявления
  топологии, предел размера, отказ брокера сбрасывает соединение;
- панель: код таможенного органа у объекта с аудитом, статистика очереди.
"""

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from center.agents_ws.hub import AgentHub
from center.api_v2.router import CUSTOMS_INTEGRATOR, ApiV2Config, create_api_v2_router
from center.customs.events import build_customs_event
from center.customs.publisher import (
    MAX_BUILD_ATTEMPTS,
    MAX_MESSAGE_BYTES,
    CustomsBroker,
    CustomsPublisher,
    UndeliverableEvent,
)
from center.db import repo
from center.db.models import AuditLog, CustomsEvent, Scale, ScaleKind, Site, Weighing
from center.events import OutgoingEvent
from center.web import refs_admin
from shared.enums import Operation, WeighingSource
from tests.test_center_customs import (  # noqa: F401 — фикстуры БД таможенных тестов
    NOW,
    _record,
    _refusal,
    _seed_scale,
    customs_db_engine,
    customs_db_url,
    factory,
)

AIS_TOKEN = "test-ais-token"
CUSTOMS_TOKEN = "test-customs-token"


def _client(factory: sessionmaker[Session], photos_dir: Path) -> TestClient:
    config = ApiV2Config(
        service_tokens={AIS_TOKEN: "ais-svh", CUSTOMS_TOKEN: CUSTOMS_INTEGRATOR},
        photos_dir=photos_dir,
    )
    app = FastAPI()
    app.include_router(create_api_v2_router(AgentHub(), factory, config))
    return TestClient(app)


def _by_uuid(session: Session, record_uuid: UUID) -> Weighing | None:
    return session.execute(
        select(Weighing).where(Weighing.uuid == record_uuid)
    ).scalar_one_or_none()


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _second_site(factory: sessionmaker[Session]) -> int:
    """Второй объект — без привязки к АИС и без кода органа."""
    with factory() as session:
        site = Site(code="belovodskoe", name="ПЗТК «Беловодское»")
        session.add(site)
        session.flush()
        scale = Scale(site_id=site.id, name="Весы SCS-80", kind=ScaleKind.STATIC, driver="xk3190")
        session.add(scale)
        session.flush()
        scale_id = scale.id
        session.commit()
    return scale_id


class TestCustomsApi:
    def test_read_only(self, factory: sessionmaker[Session], tmp_path: Path) -> None:
        """Токен таможни не проводит операций и не привязывает номера АИС."""
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
        client = _client(factory, tmp_path)
        command = client.post("/api/v2/weighings", json={}, headers=_auth(CUSTOMS_TOKEN))
        assert command.status_code == 403 and command.json()["code"] == "ERR_FORBIDDEN"
        link = client.post(
            f"/api/v2/weighings/{record.uuid}/ais_ref",
            json={"ais_ref": "WEI000000001"},
            headers=_auth(CUSTOMS_TOKEN),
        )
        assert link.status_code == 403
        with factory() as session:
            assert repo.weighing_by_ais_ref(session, "WEI000000001") is None
        # у АИС права прежние: пустая команда доходит до проверки тела
        assert (
            client.post("/api/v2/weighings", json={}, headers=_auth(AIS_TOKEN)).status_code == 422
        )
        assert client.get("/api/v2/sites").status_code == 401

    def test_document_variant_by_token(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
        client = _client(factory, tmp_path)
        url = f"/api/v2/weighings/{record.uuid}"
        ais_doc = client.get(url, headers=_auth(AIS_TOKEN)).json()["weighing"]
        customs_doc = client.get(url, headers=_auth(CUSTOMS_TOKEN)).json()["weighing"]
        assert "weighed_at_utc" not in ais_doc and "customs_code" not in ais_doc["site"]
        assert "kind" not in ais_doc["scale"]
        assert customs_doc["weighed_at_utc"] == "2026-08-14T09:30:12.345678+00:00"
        assert customs_doc["site"]["customs_code"] == "41714106"
        assert customs_doc["scale"]["kind"] == "static"
        assert customs_doc["checksum"] == ais_doc["checksum"]

    def test_list_all_sites_and_site_filter(
        self,
        factory: sessionmaker[Session],
        tmp_path: Path,
    ) -> None:
        """Таможня видит операции всех объектов (и без АИС), ``site`` отбирает
        объект; сторнированная запись видна с ``storno``; АИС ``site`` не знает."""
        first = _seed_scale(factory)
        second = _second_site(factory)
        a, b = _record(), _record(WeighingSource.LOCAL_OFFLINE, operation=Operation.TARING)
        with factory() as session:
            repo.save_weighing_record(session, first, a)
            repo.save_weighing_record(session, second, b)
            original = _by_uuid(session, b.uuid)
            assert original is not None
            repo.storno_weighing(session, original, actor="panel:igor", reason="ошибка")
        client = _client(factory, tmp_path)
        everything = client.get("/api/v2/weighings", headers=_auth(CUSTOMS_TOKEN)).json()
        assert everything["total"] == 2  # запись-сторно операцией не считается
        only = client.get(
            "/api/v2/weighings", params={"site": "belovodskoe"}, headers=_auth(CUSTOMS_TOKEN)
        ).json()
        assert [w["id"] for w in only["weighings"]] == [str(b.uuid)]
        assert only["weighings"][0]["storno"]["reason"] == "ошибка"
        assert only["weighings"][0]["site"] == {
            "code": "belovodskoe",
            "name": "ПЗТК «Беловодское»",
            "customs_code": None,
            "ais_object": None,
        }
        assert "weighed_at_utc" in only["weighings"][0]
        nothing = client.get(
            "/api/v2/weighings", params={"site": "нет-такого"}, headers=_auth(CUSTOMS_TOKEN)
        ).json()
        assert nothing["total"] == 0
        # для АИС параметр site не действует — её запросы и ответы прежние
        ais = client.get(
            "/api/v2/weighings", params={"site": "belovodskoe"}, headers=_auth(AIS_TOKEN)
        ).json()
        assert ais["total"] == 2 and "weighed_at_utc" not in ais["weighings"][0]

    def test_sites_directory(self, factory: sessionmaker[Session], tmp_path: Path) -> None:
        _seed_scale(factory)
        _second_site(factory)
        body = _client(factory, tmp_path).get("/api/v2/sites", headers=_auth(CUSTOMS_TOKEN)).json()
        assert [s["code"] for s in body["sites"]] == ["belovodskoe", "kokcho-koz"]
        bare, bound = body["sites"]
        assert bare["customs_code"] is None and bare["ais_object"] is None
        assert bound["customs_code"] == "41714106" and bound["ais_object"] == "0021"
        assert list(bound) == ["code", "name", "customs_code", "ais_object", "scales"]
        assert list(bound["scales"][0]) == ["id", "no", "kind", "name", "verification"]
        assert bound["scales"][0]["no"] == 1 and bound["scales"][0]["kind"] == "static"


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.published: list[OutgoingEvent] = []
        self.fail = fail

    async def ensure_topology(self) -> None:
        return None

    async def publish(self, event: OutgoingEvent) -> None:
        if self.fail:
            raise ConnectionError("нет связи с брокером таможни")
        self.published.append(event)

    async def close(self) -> None:
        return None


def _publisher(
    factory: sessionmaker[Session],
    broker: FakeBroker | None,
    photos_dir: Path,
) -> CustomsPublisher:
    return CustomsPublisher(factory, broker, photos_dir=photos_dir, now=lambda: NOW)


def _outbox(factory: sessionmaker[Session]) -> list[CustomsEvent]:
    with factory() as session:
        rows = list(session.execute(select(CustomsEvent).order_by(CustomsEvent.id)).scalars())
        for row in rows:
            session.expunge(row)
        return rows


class TestPublisher:
    def test_publishes_batch_in_order_and_marks(
        self,
        factory: sessionmaker[Session],
        tmp_path: Path,
    ) -> None:
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
            repo.record_tare_rejection(session, scale_id, _refusal(), operator=None, ais_ref=None)
        broker = FakeBroker()
        assert asyncio.run(_publisher(factory, broker, tmp_path).publish_pending()) == (2, 0)
        assert [e.routing_key for e in broker.published] == [
            "weighing.completed.kokcho-koz",
            "weighing.rejected.kokcho-koz",
        ]
        assert broker.published[0].body["weighing"]["id"] == str(record.uuid)
        rows = _outbox(factory)
        assert all(row.published_at == NOW and row.attempts == 1 for row in rows)
        assert all(row.last_error is None for row in rows)
        with factory() as session:
            assert repo.pending_customs_stats(session) == {}
        # второй проход — нечего отправлять
        assert asyncio.run(_publisher(factory, broker, tmp_path).publish_pending()) == (0, 0)

    def test_broker_failure_keeps_events_pending(
        self,
        factory: sessionmaker[Session],
        tmp_path: Path,
    ) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record())
            repo.save_weighing_record(session, scale_id, _record())
        publisher = _publisher(factory, FakeBroker(fail=True), tmp_path)
        assert asyncio.run(publisher.publish_pending()) == (0, 1)  # порция прервана
        first, second = _outbox(factory)
        assert first.published_at is None and first.attempts == 1
        assert first.last_error is not None and "нет связи" in first.last_error
        assert second.attempts == 0
        with factory() as session:
            stats = repo.pending_customs_stats(session)
        assert stats[scale_id][0] == 2

    def test_no_route_blocks_batch_with_clear_reason(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """У exchange нет очереди под ключ — порядок сохраняем, причина названа."""

        class NoRouteBroker(FakeBroker):
            async def publish(self, event: OutgoingEvent) -> None:
                raise RuntimeError("('NO_ROUTE', 'weighing.completed.kokcho-koz')")

        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record())
        publisher = _publisher(factory, NoRouteBroker(), tmp_path)
        assert asyncio.run(publisher.publish_pending()) == (0, 1)
        row = _outbox(factory)[0]
        assert row.published_at is None and row.last_error is not None
        assert "нет очереди под ключ weighing.completed.kokcho-koz" in row.last_error

    def test_undeliverable_event_skipped_then_closed(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """Событие, которое нельзя отправить в принципе, не держит очередь и
        паузу: остальные уходят, а оно закрывается после нескольких попыток."""

        class PickyBroker(FakeBroker):
            async def publish(self, event: OutgoingEvent) -> None:
                if event.event_type == "weighing.rejected":
                    raise UndeliverableEvent("сообщение больше предела")
                await super().publish(event)

        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.record_tare_rejection(session, scale_id, _refusal(), operator=None, ais_ref=None)
            repo.save_weighing_record(session, scale_id, _record())
        broker = PickyBroker()
        publisher = _publisher(factory, broker, tmp_path)
        assert asyncio.run(publisher.publish_pending()) == (1, 0)  # в паузу не засчитано
        rejected, completed = _outbox(factory)
        assert completed.published_at is not None and rejected.published_at is None
        assert rejected.attempts == 1
        for _ in range(MAX_BUILD_ATTEMPTS - 1):
            assert asyncio.run(publisher.publish_pending()) == (0, 0)
        closed = _outbox(factory)[0]
        assert closed.published_at is not None and closed.attempts == MAX_BUILD_ATTEMPTS
        assert closed.last_error is not None and "закрыто после" in closed.last_error

    def test_unbuildable_event_does_not_block(
        self, factory: sessionmaker[Session], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        real_build = build_customs_event

        def flaky(session: Session, outbox: CustomsEvent, **kwargs: Any) -> Any:
            if outbox.id == 1:
                raise RuntimeError("битые данные")
            return real_build(session, outbox, **kwargs)

        monkeypatch.setattr("center.customs.publisher.build_customs_event", flaky)
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record())
            repo.save_weighing_record(session, scale_id, _record())
        broker = FakeBroker()
        assert asyncio.run(_publisher(factory, broker, tmp_path).publish_pending()) == (1, 0)
        first, second = _outbox(factory)
        assert first.published_at is None and "сборка события" in (first.last_error or "")
        assert second.published_at is not None

    def test_disabled_without_broker(self, factory: sessionmaker[Session], tmp_path: Path) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record())
        publisher = _publisher(factory, None, tmp_path)
        assert not publisher.enabled
        assert asyncio.run(publisher.publish_pending()) == (0, 0)
        assert _outbox(factory)[0].published_at is None

    def test_stats_cover_rejections(self, factory: sessionmaker[Session]) -> None:
        """Статистика очереди относит событие отказа к весам отказа."""
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.record_tare_rejection(session, scale_id, _refusal(), operator=None, ais_ref=None)
            stats = repo.pending_customs_stats(session)
        assert list(stats) == [scale_id] and stats[scale_id][0] == 1


class TestBroker:
    @staticmethod
    def _event(body: dict[str, Any] | None = None) -> OutgoingEvent:
        return OutgoingEvent(
            outbox_id=1,
            event_id="9e4a6b1c-2d3f-5c7a-b8e9-4f0a1b2c3d4e",
            event_type="weighing.rejected",
            routing_key="weighing.rejected.kokcho-koz",
            body=body or {"event_id": "9e4a6b1c-2d3f-5c7a-b8e9-4f0a1b2c3d4e"},
        )

    def test_message_properties(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import aio_pika

        published: list[tuple[Any, str, bool]] = []

        class FakeExchange:
            async def publish(
                self, message: Any, routing_key: str, mandatory: bool, timeout: float
            ) -> None:
                published.append((message, routing_key, mandatory))

        broker = CustomsBroker("amqp://vesy-center:x@localhost/v", "vesy.events.test")
        fake = FakeExchange()

        async def ensure() -> Any:
            return fake

        monkeypatch.setattr(broker, "_ensure", ensure)
        event = self._event()
        asyncio.run(broker.publish(event))
        message, routing_key, mandatory = published[0]
        assert routing_key == "weighing.rejected.kokcho-koz" and mandatory is True
        assert message.message_id == event.event_id and message.type == "weighing.rejected"
        assert message.content_type == "application/json"
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert message.app_id == "vesy-center" and message.timestamp is not None
        assert json.loads(message.body) == event.body

    def test_uses_existing_exchange_without_declaring(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Центр не объявляет топологию у таможни: exchange берётся по имени
        без обращения к брокеру; на канале включён отказ для недоставленного."""
        import aio_pika

        calls: list[tuple[str, Any]] = []

        class FakeChannel:
            is_closed = False

            async def get_exchange(self, name: str, *, ensure: bool = True) -> str:
                calls.append(("get_exchange", (name, ensure)))
                return "exchange"

            def __getattr__(self, name: str) -> Any:
                raise AssertionError(f"объявление топологии запрещено: {name}")

        class FakeConnection:
            is_closed = False

            async def channel(self, **kwargs: Any) -> FakeChannel:
                calls.append(("channel", kwargs))
                return FakeChannel()

            async def close(self) -> None:
                calls.append(("close", None))

        async def connect(url: str, **kwargs: Any) -> FakeConnection:
            calls.append(("connect", (url, kwargs.get("ssl_context"))))
            return FakeConnection()

        monkeypatch.setattr(aio_pika, "connect_robust", connect)
        broker = CustomsBroker("amqp://vesy-center:x@192.168.0.115:5672/v", "vesy.events.test")
        asyncio.run(broker.ensure_topology())
        assert calls == [
            ("connect", ("amqp://vesy-center:x@192.168.0.115:5672/v", None)),
            ("channel", {"publisher_confirms": True, "on_return_raises": True}),
            ("get_exchange", ("vesy.events.test", False)),
        ]
        assert broker.exchange_name == "vesy.events.test"

    def test_oversized_message_rejected_before_connect(self) -> None:
        broker = CustomsBroker("amqp://x:y@localhost/v", "vesy.events.test")
        event = self._event({"blob": "x" * (MAX_MESSAGE_BYTES + 1)})
        with pytest.raises(ValueError, match="больше предела"):
            asyncio.run(broker.publish(event))

    def test_publish_failure_resets_connection(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class BrokenExchange:
            async def publish(self, *args: Any, **kwargs: Any) -> None:
                raise RuntimeError("NOT_FOUND - no exchange")

        broker = CustomsBroker("amqp://x:y@localhost/v", "нет-такого")
        closed: list[bool] = []

        async def ensure() -> Any:
            return BrokenExchange()

        async def close() -> None:
            closed.append(True)

        monkeypatch.setattr(broker, "_ensure", ensure)
        monkeypatch.setattr(broker, "close", close)
        with pytest.raises(RuntimeError):
            asyncio.run(broker.publish(self._event()))
        assert closed == [True]


class TestPanelSide:
    def test_site_customs_code_with_audit(self, factory: sessionmaker[Session]) -> None:
        _seed_scale(factory, customs_code=None)
        with factory() as session:
            site = session.execute(select(Site)).scalar_one()
            assert (
                refs_admin.update_site(
                    session, site.id, name=site.name, customs_code=" 41714106 ", actor="user:igor"
                )
                is None
            )
            assert site.customs_code == "41714106"
            # None — поле не трогаем; то же значение — без записи аудита
            assert refs_admin.update_site(session, site.id, name="СВХ «Кокчо-Коз»") is None
            assert site.customs_code == "41714106"
            error = refs_admin.update_site(session, site.id, name=site.name, customs_code="код №1")
            assert error is not None and "код таможенного органа" in error
            assert refs_admin.update_site(session, site.id, name=site.name, customs_code="") is None
            assert site.customs_code is None
            audit = (
                session.execute(
                    select(AuditLog)
                    .where(AuditLog.action == "site_customs_code")
                    .order_by(AuditLog.id)
                )
                .scalars()
                .all()
            )
            assert [(a.details["was"], a.details["now"]) for a in audit] == [
                (None, "41714106"),
                ("41714106", None),
            ]
            assert audit[0].actor == "user:igor"

    def test_resend_enqueues_same_event_id(
        self,
        factory: sessionmaker[Session],
        tmp_path: Path,
    ) -> None:
        scale_id = _seed_scale(factory)
        record = _record()
        with factory() as session:
            repo.save_weighing_record(session, scale_id, record)
            weighing = _by_uuid(session, record.uuid)
            assert weighing is not None
            repo.enqueue_customs_event(session, weighing)
            session.commit()
            latest = repo.latest_customs_event(session, weighing.id)
            assert latest is not None and latest.published_at is None
        broker = FakeBroker()
        assert asyncio.run(_publisher(factory, broker, tmp_path).publish_pending()) == (2, 0)
        assert broker.published[0].event_id == broker.published[1].event_id
