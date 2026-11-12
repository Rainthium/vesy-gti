"""Публикация событий в брокер таможенной системы (Таргет-центр ГТС).

Отличия от публикатора АИС «СВХ» (``center.events``):

- брокер чужой, топологию ведёт Таргет-центр. У учётной записи центра там
  одно право — публиковать в exchange; объявлять exchange, очереди и
  привязки центр не может и не пытается (``get_exchange(..., ensure=False)``);
- имя exchange — настройка: тестовый контур ``vesy.events.test``, боевой —
  ``vesy.events`` (контракт customs-events-v1, раздел 3);
- outbox свой — ``customs_events``; сбой этого брокера не задевает поток АИС
  и наоборот.

Схема та же: строка outbox появляется в транзакции с операцией, сторно или
отказом, фоновая задача отправляет её с подтверждением брокера и ставит
``published_at``. Пока брокер недоступен, события копятся.
"""

import asyncio
import logging
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from center.customs.events import PRODUCER, build_customs_event
from center.db import repo
from center.db.models import CustomsEvent
from center.events import EventBroker, OutgoingEvent

logger = logging.getLogger(__name__)

# exchange по умолчанию — ТЕСТОВЫЙ: в боевой поток центр публикует только по
# явной настройке CUSTOMS_EXCHANGE=vesy.events
CUSTOMS_TEST_EXCHANGE = "vesy.events.test"
CONNECT_TIMEOUT_S = 15.0
PUBLISH_TIMEOUT_S = 30.0  # ожидание подтверждения брокера одним сообщением
# предел сообщения, согласованный с Таргет-центром; документ — 3–5 КБ
MAX_MESSAGE_BYTES = 1024 * 1024
# событие, которое не собирается (битые данные одной операции), закрывается с
# пометкой после стольких попыток — иначе оно вечно возвращалось бы в порцию
MAX_BUILD_ATTEMPTS = 5


class UndeliverableEvent(ValueError):
    """Сообщение нельзя отправить в принципе (не зависит от состояния брокера)."""


class CustomsBroker:
    """aio-pika: robust-соединение и публикация в СУЩЕСТВУЮЩИЙ exchange.

    ``publish`` возвращается только после подтверждения брокера; сообщение,
    которое брокер не смог положить ни в одну очередь (нет привязки), —
    ошибка, а не тихая потеря (``on_return_raises``). ``ca_file`` — сертификат
    удостоверяющего центра брокера для TLS (порт 5671); пусто — без TLS
    (тестовый прогон внутри сети по 5672).
    """

    def __init__(self, url: str, exchange_name: str, *, ca_file: str | None = None) -> None:
        self._url = url
        self._exchange_name = exchange_name
        self._ca_file = ca_file
        self._connection: Any = None
        self._channel: Any = None
        self._exchange: Any = None

    @property
    def exchange_name(self) -> str:
        return self._exchange_name

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
            context = ssl.create_default_context(cafile=self._ca_file) if self._ca_file else None
            self._connection = await aio_pika.connect_robust(
                self._url, timeout=CONNECT_TIMEOUT_S, ssl_context=context
            )
            # aio-pika включает TLS по схеме amqps; CA без amqps не действует
            tls = self._url.strip().lower().startswith("amqps")
            if self._ca_file and not tls:
                logger.warning(
                    "брокер таможни: задан CUSTOMS_TLS_CA, но адрес не amqps — соединение БЕЗ TLS"
                )
            logger.info("брокер таможни: соединение %s", "по TLS" if tls else "без TLS")
        try:
            channel = await self._connection.channel(publisher_confirms=True, on_return_raises=True)
            # ensure=False: без объявления и без проверки на брокере — у учётной
            # записи центра нет прав configure; отсутствие exchange проявится
            # ошибкой первой публикации
            exchange = await channel.get_exchange(self._exchange_name, ensure=False)
        except Exception:
            await self.close()
            raise
        self._channel = channel
        self._exchange = exchange
        logger.info("брокер таможни: подключено, exchange %s", self._exchange_name)
        return exchange

    async def ensure_topology(self) -> None:
        """Только подключиться: топологию у таможни центр не объявляет."""
        await self._ensure()

    async def publish(self, event: OutgoingEvent) -> None:
        import aio_pika

        payload = event.payload()
        if len(payload) > MAX_MESSAGE_BYTES:
            raise UndeliverableEvent(
                f"сообщение {len(payload)} байт больше предела {MAX_MESSAGE_BYTES} — не отправлено"
            )
        exchange = await self._ensure()
        message = aio_pika.Message(
            payload,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=event.event_id,
            type=event.event_type,
            timestamp=datetime.now(UTC),
            app_id=PRODUCER,
        )
        try:
            await exchange.publish(
                message, routing_key=event.routing_key, mandatory=True, timeout=PUBLISH_TIMEOUT_S
            )
        except Exception:
            # отказ брокера (нет exchange, нет прав) закрывает канал — следующая
            # попытка должна начать с чистого подключения
            await self.close()
            raise

    async def close(self) -> None:
        connection, self._connection = self._connection, None
        self._channel = None
        self._exchange = None
        if connection is not None:
            await connection.close()


@dataclass
class CustomsPublisher:
    """Фоновый публикатор outbox таможни → брокер (задача lifespan центра).

    Без брокера (``CUSTOMS_RABBITMQ_URL`` пуст) события копятся в
    ``customs_events``: ничего не теряется, доставка начнётся после включения.
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
        logger.info("публикатор событий таможни запущен (интервал %.0f с)", self.interval_s)
        while True:
            try:
                published, failed = await self.publish_pending()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("публикатор событий таможни: проход не удался")
                published, failed = 0, 1
            backoff = min(backoff * 2, self.max_backoff_s) if failed else self.interval_s
            await self.sleep(backoff if failed or not published else 0.0)

    async def publish_pending(self) -> tuple[int, int]:
        """Опубликовать порцию неотправленных; вернуть (отправлено, ошибок).

        Ошибка ПУБЛИКАЦИИ завершает порцию (брокер, скорее всего, недоступен),
        в том числе NO_ROUTE — у exchange нет очереди под ключ события: это
        ошибка топологии у Таргет-центра, порядок сохраняем и ждём её
        исправления. Событие, которое не собирается или не может быть
        отправлено в принципе, порцию не блокирует и в паузу не засчитывается:
        помечается на строке и после MAX_BUILD_ATTEMPTS попыток закрывается.
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
                    "событие таможни %s (outbox id=%s) не собрано", outbox.event_type, outbox.id
                )
                await asyncio.to_thread(self._mark_unbuildable, outbox, f"сборка события: {exc}")
                continue
            if event is None:
                await asyncio.to_thread(
                    self._mark_published, outbox.id, "предмет события не найден"
                )
                continue
            try:
                await broker.publish(event)
            except UndeliverableEvent as exc:
                logger.error("событие таможни outbox id=%s: %s", outbox.id, exc)
                await asyncio.to_thread(self._mark_unbuildable, outbox, str(exc))
                continue
            except Exception as exc:
                logger.warning(
                    "событие таможни %s (outbox id=%s) не отправлено: %s",
                    outbox.event_type,
                    outbox.id,
                    exc,
                )
                error = str(exc) or repr(exc)
                if "NO_ROUTE" in error:
                    error = (
                        f"брокер принял соединение, но у exchange нет очереди под ключ "
                        f"{event.routing_key} (NO_ROUTE) — привязку проверяет Таргет-центр"
                    )
                await asyncio.to_thread(self._mark_failed, outbox.id, error)
                return published, failed + 1
            await asyncio.to_thread(self._mark_published, outbox.id, None)
            published += 1
        return published, failed

    # --- синхронные помощники (потоки) ---

    def _load_pending(self) -> list[CustomsEvent]:
        with self.session_factory() as session:
            rows = repo.pending_customs_events(session, self.batch_size)
            for row in rows:
                session.expunge(row)
            return rows

    def _build(self, outbox: CustomsEvent) -> OutgoingEvent | None:
        with self.session_factory() as session:
            return build_customs_event(session, outbox, photos_dir=self.photos_dir, now=self.now())

    def _mark_published(self, outbox_id: int, note: str | None) -> None:
        with self.session_factory() as session:
            repo.mark_customs_event_published(session, outbox_id, self.now(), note=note)
            if note:
                logger.warning(
                    "событие таможни outbox id=%s закрыто без отправки: %s", outbox_id, note
                )

    def _mark_unbuildable(self, outbox: CustomsEvent, error: str) -> None:
        """Попытка по событию, которое не зависит от брокера; на последней — закрыть."""
        if outbox.attempts + 1 >= MAX_BUILD_ATTEMPTS:
            self._mark_published(outbox.id, f"закрыто после {MAX_BUILD_ATTEMPTS} попыток: {error}")
        else:
            self._mark_failed(outbox.id, error)

    def _mark_failed(self, outbox_id: int, error: str) -> None:
        with self.session_factory() as session:
            repo.mark_customs_event_failed(session, outbox_id, error)
