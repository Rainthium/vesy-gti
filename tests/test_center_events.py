"""Тесты событий в АИС «СВХ» (center/events + outbox в repo) — контракт v2,
раздел 7 (согласован 17.08.2026).

Закрепляют:
- outbox: офлайн-запись (source=local_offline) ставит событие
  weighing.completed в одной транзакции с записью, онлайн-запись — нет;
  повторная постановка (кнопка «переотправить») — новая строка;
- публикатор: конверт события (event_id детерминирован по операции,
  type/version/published_at/ais_object, документ операции внутри), routing
  key по ais_object, отметка published_at; ошибка брокера — попытка и
  last_error на строке, порция прерывается, backoff; без брокера — ничего
  не публикуется, но outbox копится; статистика неотправленного по весам;
- RabbitBroker: параметры сообщения (message_id = event_id, persistent,
  content_type, type, app_id) — через подмену aio_pika.

Брокер в тестах — фейк с протоколом EventBroker; реальный RabbitMQ
проверяется эталонным консьюмером tools/ais_consumer.py при выкате.
"""

import asyncio
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center.db import repo
from center.db.models import Scale, ScaleKind, Site, Weighing, WeighingEvent
from center.db.session import database_url, make_session_factory
from center.events import (
    EVENT_VERSION,
    EventPublisher,
    OutgoingEvent,
    RabbitBroker,
    event_id_for,
    routing_key_for,
)
from shared.enums import ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord
from tests.test_center_db import ALL_TABLES, _upgrade_head

WEIGHED_AT = datetime(2026, 8, 14, 3, 12, 40, tzinfo=UTC)  # 09:12:40 по Бишкеку


@pytest.fixture(scope="session")
def events_db_url() -> Iterator[URL]:
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip("PostgreSQL недоступен — тесты событий АИС пропущены")
    db_name = f"ves_test_events_{os.getpid()}"
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
def events_db_engine(events_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(events_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def factory(events_db_engine: Engine) -> sessionmaker[Session]:
    with events_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    return make_session_factory(events_db_engine)


def _seed_scale(factory: sessionmaker[Session], *, ais_object: str | None = "0013") -> int:
    with factory() as session:
        site = Site(code="jalal-abad", name="ПЗТК «Джалал-Абад»")
        session.add(site)
        session.flush()
        scale = Scale(
            site_id=site.id,
            name="Весы ПЕГАС-80",
            kind=ScaleKind.STATIC,
            driver="cas22",
            ais_object=ais_object,
            ais_scale_no=1 if ais_object else None,
        )
        session.add(scale)
        session.flush()
        scale_id = scale.id
        session.commit()
    return scale_id


def _record(source: WeighingSource, **overrides: Any) -> WeighingRecord:
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 21850.0,
        "stable": True,
        "weighed_at": WEIGHED_AT,
        "vehicle_number": "01KG777AAA",
        "trailer_number": "01KG500AB",
        "operator": "d.ivanov",
        "source": source,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def _pending(factory: sessionmaker[Session]) -> list[WeighingEvent]:
    with factory() as session:
        rows = repo.pending_weighing_events(session, 100)
        for row in rows:
            session.expunge(row)
        return rows


class FakeBroker:
    """Брокер-накопитель; ``fail`` — падать на publish (брокер недоступен)."""

    def __init__(self, fail: bool = False) -> None:
        self.published: list[OutgoingEvent] = []
        self.fail = fail
        self.closed = False

    async def publish(self, event: OutgoingEvent) -> None:
        if self.fail:
            raise ConnectionError("нет связи с брокером")
        self.published.append(event)

    async def close(self) -> None:
        self.closed = True


def _publisher(
    factory: sessionmaker[Session], broker: FakeBroker | None, photos_dir: Path
) -> EventPublisher:
    return EventPublisher(
        factory,
        broker,
        photos_dir=photos_dir,
        now=lambda: datetime(2026, 8, 14, 5, 5, 4, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# outbox в repo
# ---------------------------------------------------------------------------


class TestOutbox:
    def test_offline_record_enqueues_event_online_does_not(
        self, factory: sessionmaker[Session]
    ) -> None:
        scale_id = _seed_scale(factory)
        offline = _record(WeighingSource.LOCAL_OFFLINE)
        online = _record(WeighingSource.AIS)
        with factory() as session:
            assert repo.save_weighing_record(session, scale_id, offline)
            assert repo.save_weighing_record(session, scale_id, online)
        pending = _pending(factory)
        assert len(pending) == 1
        assert pending[0].event_type == "weighing.completed"
        assert pending[0].published_at is None and pending[0].attempts == 0
        with factory() as session:
            weighing = session.get(Weighing, pending[0].weighing_id)
            assert weighing is not None and weighing.uuid == offline.uuid
            # повтор досылки той же записи не плодит событий
            assert not repo.save_weighing_record(session, scale_id, offline)
        assert len(_pending(factory)) == 1

    def test_enqueue_again_and_stats(self, factory: sessionmaker[Session]) -> None:
        scale_id = _seed_scale(factory)
        offline = _record(WeighingSource.LOCAL_OFFLINE)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, offline)
            weighing = session.execute(
                select(Weighing).where(Weighing.uuid == offline.uuid)
            ).scalar_one()
            repo.enqueue_weighing_event(session, weighing)
            session.commit()
            stats = repo.pending_event_stats(session)
        assert stats[scale_id][0] == 2
        assert stats[scale_id][1] is not None
        with factory() as session:
            latest = repo.latest_weighing_event(session, weighing.id)
            assert latest is not None and latest.id == max(e.id for e in _pending(factory))


# ---------------------------------------------------------------------------
# конверт и публикатор
# ---------------------------------------------------------------------------


class TestPublisher:
    def test_event_id_deterministic_and_routing_key(self) -> None:
        record_uuid = UUID("0d9b4d3e-8c0f-4b41-9f4c-2a6f1e3b7c55")
        assert event_id_for(record_uuid, "weighing.completed") == event_id_for(
            record_uuid, "weighing.completed"
        )
        assert event_id_for(record_uuid, "weighing.completed") != event_id_for(
            uuid4(), "weighing.completed"
        )
        assert routing_key_for("weighing.completed", "0013") == "weighing.completed.0013"
        assert routing_key_for("weighing.completed", None) == "weighing.completed.unbound"

    def test_publish_pending_sends_envelope_and_marks(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory)
        offline = _record(WeighingSource.LOCAL_OFFLINE)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, offline)
        broker = FakeBroker()
        publisher = _publisher(factory, broker, tmp_path)
        assert publisher.enabled

        published, failed = asyncio.run(publisher.publish_pending())
        assert (published, failed) == (1, 0)
        assert len(broker.published) == 1
        event = broker.published[0]
        assert event.routing_key == "weighing.completed.0013"
        assert event.event_id == event_id_for(offline.uuid, "weighing.completed")
        body = json.loads(event.payload())
        assert body["event_id"] == event.event_id
        assert body["type"] == "weighing.completed"
        assert body["version"] == EVENT_VERSION
        assert body["published_at"] == "2026-08-14T11:05:04+06:00"
        assert body["ais_object"] == "0013"
        doc = body["weighing"]
        assert doc["id"] == str(offline.uuid)
        assert doc["source"] == "local_offline"
        assert doc["ais_ref"] is None
        assert doc["site"]["ais_object"] == "0013"
        assert doc["weighed_at"] == "2026-08-14T09:12:40+06:00"
        assert doc["operator"] == "d.ivanov"
        # кириллица в JSON как есть, а не \\u-последовательности
        assert "Джалал-Абад" in event.payload().decode("utf-8")

        assert _pending(factory) == []
        with factory() as session:
            row = session.execute(select(WeighingEvent)).scalar_one()
            assert row.published_at is not None
            assert row.attempts == 1 and row.last_error is None
        # второй проход — публиковать нечего
        assert asyncio.run(publisher.publish_pending()) == (0, 0)

    def test_broker_failure_marks_attempt_and_stops_batch(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            for _ in range(3):
                repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        broker = FakeBroker(fail=True)
        publisher = _publisher(factory, broker, tmp_path)
        assert asyncio.run(publisher.publish_pending()) == (0, 1)
        pending = _pending(factory)
        assert len(pending) == 3
        # помечена только первая строка порции — остальные не трогали
        assert [e.attempts for e in pending] == [1, 0, 0]
        assert pending[0].last_error is not None and "брокером" in pending[0].last_error
        # брокер ожил — всё уходит, порядок постановки сохраняется
        broker.fail = False
        assert asyncio.run(publisher.publish_pending()) == (3, 0)
        assert [e.outbox_id for e in broker.published] == [p.id for p in pending]
        assert _pending(factory) == []

    def test_disabled_without_broker(self, factory: sessionmaker[Session], tmp_path: Path) -> None:
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        publisher = _publisher(factory, None, tmp_path)
        assert not publisher.enabled
        assert asyncio.run(publisher.publish_pending()) == (0, 0)
        assert len(_pending(factory)) == 1  # копится до включения брокера
        with factory() as session:
            assert repo.pending_event_stats(session)[scale_id][0] == 1

    def test_unbound_scale_routing_key(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        scale_id = _seed_scale(factory, ais_object=None)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        broker = FakeBroker()
        asyncio.run(_publisher(factory, broker, tmp_path).publish_pending())
        assert broker.published[0].routing_key == "weighing.completed.unbound"
        assert json.loads(broker.published[0].payload())["ais_object"] is None

    def test_run_loop_publishes_and_backs_off(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        """Цикл run: после отправки идёт за следующей порцией без паузы, при
        ошибке пауза растёт, при пустом outbox — обычный интервал."""
        scale_id = _seed_scale(factory)
        with factory() as session:
            repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        broker = FakeBroker(fail=True)
        publisher = _publisher(factory, broker, tmp_path)
        publisher.interval_s = 1.0
        publisher.max_backoff_s = 8.0
        sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            sleeps.append(delay)
            if len(sleeps) == 2:
                broker.fail = False  # брокер ожил после второй паузы
            if len(sleeps) >= 4:
                raise asyncio.CancelledError

        publisher.sleep = fake_sleep
        with pytest.raises(asyncio.CancelledError):
            asyncio.run(publisher.run())
        # две ошибки → 2, 4; затем публикация → 0 (сразу за следующей); пусто → 1
        assert sleeps == [2.0, 4.0, 0.0, 1.0]
        assert len(broker.published) == 1


class TestRabbitBroker:
    def test_message_properties(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Сообщение: persistent, JSON, message_id = event_id, type события, app_id."""
        import aio_pika

        published: list[tuple[Any, str]] = []

        class FakeExchange:
            async def publish(self, message: Any, routing_key: str, timeout: float) -> None:
                published.append((message, routing_key))
                assert timeout > 0

        broker = RabbitBroker("amqp://x:y@localhost/vesy")
        fake_exchange = FakeExchange()

        async def ensure() -> Any:
            return fake_exchange

        monkeypatch.setattr(broker, "_ensure", ensure)
        event = OutgoingEvent(
            outbox_id=1,
            event_id="b3f6c2a1-9d0e-4c7b-8a5f-2e1d3c4b5a69",
            event_type="weighing.completed",
            routing_key="weighing.completed.0013",
            body={"event_id": "b3f6c2a1-9d0e-4c7b-8a5f-2e1d3c4b5a69"},
        )
        asyncio.run(broker.publish(event))
        message, routing_key = published[0]
        assert routing_key == "weighing.completed.0013"
        assert message.message_id == event.event_id
        assert message.content_type == "application/json"
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert message.type == "weighing.completed"
        assert message.app_id == "vesy-center"
        assert json.loads(message.body) == event.body

    def test_poison_event_does_not_block_batch(
        self, factory: sessionmaker[Session], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ошибка сборки одного события помечается на строке, остальные уходят."""
        scale_id = _seed_scale(factory)
        with factory() as session:
            for _ in range(2):
                repo.save_weighing_record(session, scale_id, _record(WeighingSource.LOCAL_OFFLINE))
        first_id = _pending(factory)[0].id
        broker = FakeBroker()
        publisher = _publisher(factory, broker, tmp_path)
        original = publisher._build

        def build(outbox: WeighingEvent) -> OutgoingEvent | None:
            if outbox.id == first_id:
                raise ValueError("битые данные")
            return original(outbox)

        monkeypatch.setattr(publisher, "_build", build)
        assert asyncio.run(publisher.publish_pending()) == (1, 1)
        pending = _pending(factory)
        assert [e.id for e in pending] == [first_id]
        assert pending[0].attempts == 1 and "битые данные" in (pending[0].last_error or "")
        assert len(broker.published) == 1
