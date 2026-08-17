"""Публикация событий в АИС «СВХ» через RabbitMQ (контракт v2, раздел 7).

Событие ``weighing.completed`` публикуется по офлайн-операциям (ручной режим
при потере связи) — единственный путь доставить их в АИС. Схема outbox:
строка ``weighing_events`` появляется в одной транзакции с записью
(``repo.save_weighing_record``), а фоновый ``EventPublisher`` отправляет её с
подтверждением брокера и ставит ``published_at``. Пока брокер недоступен,
события копятся; неотправленное видно на дашборде и мониторингу («события в
АИС не уходят»). Урок Telegram 14.08.2026: молчаливый сбой доставки недопустим.

Транспорт вынесен за протокол ``EventBroker``: ``RabbitBroker`` — aio-pika
(robust-соединение, publisher confirms), в тестах — фейк. Топология
(exchange ``vesy.events`` topic, очередь ``ais-svh.weighings`` с привязкой
``weighing.completed.#``) объявляется центром идемпотентно при подключении;
учётка АИС только читает очередь (deploy/README).
"""

import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy.orm import Session

from center.api_v1.schemas import bishkek_iso
from center.api_v2.documents import build_document
from center.db import repo
from center.db.models import Scale, Weighing, WeighingEvent

logger = logging.getLogger(__name__)

EXCHANGE_NAME = "vesy.events"
QUEUE_NAME = "ais-svh.weighings"
QUEUE_BINDING = "weighing.completed.#"
EVENT_VERSION = 1
APP_ID = "vesy-center"
CONNECT_TIMEOUT_S = 15.0
PUBLISH_TIMEOUT_S = 30.0  # ожидание подтверждения брокера одним сообщением
# event_id детерминирован по операции и типу: повторная публикация несёт тот
# же идентификатор (контракт 7.3), АИС схлопывает дубли
EVENT_NAMESPACE = uuid.UUID("5d0f9c8a-6b3e-4b0e-9a1c-2f4e7d8b9c01")


def event_id_for(weighing_uuid: uuid.UUID, event_type: str) -> str:
    return str(uuid.uuid5(EVENT_NAMESPACE, f"{weighing_uuid}:{event_type}"))


def routing_key_for(event_type: str, ais_object: str | None) -> str:
    """``weighing.completed.<ais_object>``; непривязанные весы — ``unbound``."""
    return f"{event_type}.{ais_object or 'unbound'}"


@dataclass(frozen=True)
class OutgoingEvent:
    """Сообщение, готовое к публикации (собрано из outbox и журнала)."""

    outbox_id: int
    event_id: str
    event_type: str
    routing_key: str
    body: dict[str, Any]

    def payload(self) -> bytes:
        return json.dumps(self.body, ensure_ascii=False).encode("utf-8")


class EventBroker(Protocol):
    """Минимум транспорта: опубликовать с подтверждением, закрыть."""

    async def publish(self, event: OutgoingEvent) -> None: ...

    async def close(self) -> None: ...


class RabbitBroker:
    """aio-pika: robust-соединение, топология объявляется при первом публикации.

    ``publish`` возвращается только после подтверждения брокера (publisher
    confirms включены у канала aio-pika по умолчанию); любая ошибка наружу —
    публикатор пометит попытку и повторит позже.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None

    async def _ensure(self) -> Any:
        import aio_pika

        if (
            self._exchange is not None
            and self._channel is not None
            and not self._channel.is_closed
            and not self._connection.is_closed
        ):
            return self._exchange
        if self._connection is None or self._connection.is_closed:
            self._connection = await aio_pika.connect_robust(self._url, timeout=CONNECT_TIMEOUT_S)
        try:
            channel = await self._connection.channel(publisher_confirms=True)
            exchange = await channel.declare_exchange(
                EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
            )
            queue = await channel.declare_queue(QUEUE_NAME, durable=True)
            await queue.bind(exchange, routing_key=QUEUE_BINDING)
        except Exception:
            # топология не объявилась (например, очередь уже существует с другими
            # параметрами — 406): закрываем соединение, чтобы каждая следующая
            # попытка не плодила robust-соединений с живыми задачами
            await self.close()
            raise
        self._channel = channel
        self._exchange = exchange
        logger.info(
            "RabbitMQ: подключено, топология объявлена (%s → %s)", EXCHANGE_NAME, QUEUE_NAME
        )
        return exchange

    async def publish(self, event: OutgoingEvent) -> None:
        import aio_pika

        exchange = await self._ensure()
        message = aio_pika.Message(
            event.payload(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=event.event_id,
            type=event.event_type,
            timestamp=datetime.now(UTC),
            app_id=APP_ID,
        )
        await exchange.publish(message, routing_key=event.routing_key, timeout=PUBLISH_TIMEOUT_S)

    async def close(self) -> None:
        connection, self._connection = self._connection, None
        self._channel = None
        self._exchange = None
        if connection is not None:
            await connection.close()


def build_event(
    session: Session, outbox: WeighingEvent, *, photos_dir: Path, now: datetime
) -> OutgoingEvent | None:
    """Собрать конверт события по строке outbox; None — записи уже нет."""
    weighing = session.get(Weighing, outbox.weighing_id)
    if weighing is None:
        return None
    scale = session.get(Scale, weighing.scale_id)
    ais_object = scale.ais_object if scale is not None else None
    document = build_document(session, weighing, photos_dir=photos_dir)
    return OutgoingEvent(
        outbox_id=outbox.id,
        event_id=event_id_for(weighing.uuid, outbox.event_type),
        event_type=outbox.event_type,
        routing_key=routing_key_for(outbox.event_type, ais_object),
        body={
            "event_id": event_id_for(weighing.uuid, outbox.event_type),
            "type": outbox.event_type,
            "version": EVENT_VERSION,
            "published_at": bishkek_iso(now),
            "ais_object": ais_object,
            "weighing": document,
        },
    )


@dataclass
class EventPublisher:
    """Фоновый публикатор outbox → брокер (задача lifespan центра).

    Без брокера (``RABBITMQ_URL`` пуст) события копятся в outbox — как
    Telegram-уведомления без токена: ничего не теряется, доставка начнётся
    после включения.
    """

    session_factory: Callable[[], Session]
    broker: EventBroker | None
    photos_dir: Path
    interval_s: float = 2.0
    batch_size: int = 50
    max_backoff_s: float = 60.0
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    sleep: Callable[[float], Awaitable[None]] = field(default=asyncio.sleep)

    @property
    def enabled(self) -> bool:
        return self.broker is not None

    async def run(self) -> None:
        """Цикл: публиковать хвост outbox; при ошибках — пауза с ростом до минуты."""
        backoff = self.interval_s
        logger.info("публикатор событий АИС запущен (интервал %.0f с)", self.interval_s)
        while True:
            try:
                published, failed = await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("публикатор событий АИС: проход не удался")
                published, failed = 0, 1
            backoff = min(backoff * 2, self.max_backoff_s) if failed else self.interval_s
            # после ошибки — пауза с ростом; пусто — обычный интервал; отправили
            # порцию — сразу за следующей
            await self.sleep(backoff if failed or not published else 0.0)

    async def publish_pending(self) -> tuple[int, int]:
        """Опубликовать порцию неотправленных; вернуть (отправлено, ошибок).

        Первая же ошибка ПУБЛИКАЦИИ завершает порцию: брокер, скорее всего,
        недоступен, и долбить остальные строки бессмысленно — повтор через
        backoff. Ошибка СБОРКИ события (битые данные одной операции) помечается
        на строке, но порцию не блокирует — иначе одно «ядовитое» событие
        остановило бы весь outbox. Ошибки — в attempts/last_error строки.
        """
        broker = self.broker
        if broker is None:
            return 0, 0
        pending = await asyncio.to_thread(self._load_pending)
        published = 0
        failed = 0
        for outbox in pending:
            try:
                event = await asyncio.to_thread(self._build, outbox)
            except Exception as exc:
                logger.exception(
                    "событие %s по операции id=%s не собрано", outbox.event_type, outbox.weighing_id
                )
                await asyncio.to_thread(self._mark_failed, outbox.id, f"сборка события: {exc}")
                failed += 1
                continue
            if event is None:
                # записи нет (не должно случаться — FK) — закрываем строку с пометкой
                await asyncio.to_thread(
                    self._mark_published, outbox.id, "запись операции не найдена"
                )
                continue
            try:
                await broker.publish(event)
            except Exception as exc:
                logger.warning(
                    "событие %s по операции id=%s не отправлено: %s",
                    outbox.event_type,
                    outbox.weighing_id,
                    exc,
                )
                await asyncio.to_thread(self._mark_failed, outbox.id, str(exc))
                return published, failed + 1
            await asyncio.to_thread(self._mark_published, outbox.id, None)
            published += 1
        return published, failed

    # --- синхронные помощники (потоки) ---

    def _load_pending(self) -> list[WeighingEvent]:
        with self.session_factory() as session:
            rows = repo.pending_weighing_events(session, self.batch_size)
            for row in rows:
                session.expunge(row)
            return rows

    def _build(self, outbox: WeighingEvent) -> OutgoingEvent | None:
        with self.session_factory() as session:
            return build_event(session, outbox, photos_dir=self.photos_dir, now=self.now())

    def _mark_published(self, outbox_id: int, note: str | None) -> None:
        with self.session_factory() as session:
            repo.mark_weighing_event_published(session, outbox_id, self.now(), note=note)
            if note:
                logger.warning("событие outbox id=%s закрыто без отправки: %s", outbox_id, note)

    def _mark_failed(self, outbox_id: int, error: str) -> None:
        with self.session_factory() as session:
            repo.mark_weighing_event_failed(session, outbox_id, error)
