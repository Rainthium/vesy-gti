"""Эталонный консьюмер событий АИС «СВХ» (docs/contracts/ais-api-v2.md, раздел 7).

Читает очередь ``ais-svh.weighings`` брокера центра, печатает конверт
события и подтверждает его (``ack``) — образец для разработчиков АИС и
проверка боевого RabbitMQ при выкате. Правила из контракта 7.4 показаны в
коде: подтверждать только после обработки, дедуплицировать по
``weighing.id`` (at-least-once), не объявлять очередь (у учётки АИС нет права
configure — она только читает).

Примеры:
    uv run python -m tools.ais_consumer --url amqp://ais-svh:PASS@192.168.140.70:5672/vesy
    RABBITMQ_URL=amqp://… uv run python -m tools.ais_consumer --max 5 --timeout 30
    uv run python -m tools.ais_consumer --json   # печатать документ целиком

Пароль в URL в консоль не печатается.
"""

import argparse
import asyncio
import json
import os
import sys
import urllib.parse
from typing import Any

QUEUE_DEFAULT = "ais-svh.weighings"


def _masked(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.password:
        netloc = parsed.netloc.replace(f":{parsed.password}@", ":***@")
        return urllib.parse.urlunsplit(parsed._replace(netloc=netloc))
    return url


def _summary(body: dict[str, Any]) -> str:
    weighing = body.get("weighing") or {}
    tare = weighing.get("tare") or {}
    return (
        f"{body.get('type')} · event_id={body.get('event_id')} · СВХ {body.get('ais_object')} · "
        f"операция {weighing.get('id')} ({weighing.get('operation')}, {weighing.get('source')}) · "
        f"{weighing.get('vehicle_number')}/{weighing.get('trailer_number') or '—'} · "
        f"massa={weighing.get('massa')} tare={tare.get('massa')} [{tare.get('status')}] "
        f"netto={weighing.get('netto')} · ais_ref={weighing.get('ais_ref')}"
    )


async def consume(
    url: str, queue_name: str, *, max_messages: int, timeout_s: float, full: bool
) -> int:
    import aio_pika

    seen: set[str] = set()  # дедупликация по id операции (at-least-once)
    handled = 0
    connection = await aio_pika.connect_robust(url, timeout=15)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        # passive: очередь объявляет и сопровождает центр; консьюмер только читает
        queue = await channel.get_queue(queue_name, ensure=False)
        print(f"слушаю {queue_name} на {_masked(url)} (Ctrl+C — выход)")
        stop = asyncio.Event()

        async def on_message(message: Any) -> None:
            nonlocal handled
            body = json.loads(message.body.decode("utf-8"))
            weighing_id = str((body.get("weighing") or {}).get("id"))
            duplicate = weighing_id in seen
            seen.add(weighing_id)
            print(("ПОВТОР  " if duplicate else "событие ") + _summary(body))
            if full:
                print(json.dumps(body, ensure_ascii=False, indent=2))
            # здесь АИС сохраняет операцию upsert'ом по weighing.id — и только
            # потом подтверждает; при ошибке сохранения — не подтверждать
            await message.ack()
            handled += 1
            if max_messages and handled >= max_messages:
                stop.set()

        await queue.consume(on_message)
        try:
            await asyncio.wait_for(stop.wait(), timeout=timeout_s if timeout_s > 0 else None)
        except TimeoutError:
            print(f"тайм-аут {timeout_s:.0f} с — новых событий нет")
    finally:
        await connection.close()
    return handled


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Эталонный консьюмер событий АИС «СВХ»")
    parser.add_argument(
        "--url",
        default=os.environ.get("RABBITMQ_URL", ""),
        help="amqp://ais-svh:PASS@host:5672/vesy (или env RABBITMQ_URL)",
    )
    parser.add_argument("--queue", default=QUEUE_DEFAULT)
    parser.add_argument(
        "--max", type=int, default=0, help="выйти после N событий (0 — без предела)"
    )
    parser.add_argument(
        "--timeout", type=float, default=0.0, help="выйти, если N с нет событий (0 — ждать)"
    )
    parser.add_argument("--json", action="store_true", help="печатать документ операции целиком")
    args = parser.parse_args(argv)
    if not args.url:
        parser.error("нужен адрес брокера: --url или переменная окружения RABBITMQ_URL")
    try:
        handled = asyncio.run(
            consume(
                args.url, args.queue, max_messages=args.max, timeout_s=args.timeout, full=args.json
            )
        )
    except KeyboardInterrupt:
        handled = 0
    except OSError as exc:
        print(f"✗ брокер недоступен: {exc}", file=sys.stderr)
        sys.exit(2)
    print(f"обработано событий: {handled}")


if __name__ == "__main__":
    main()
