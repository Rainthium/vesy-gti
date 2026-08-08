"""WebSocket-эндпоинт центра для агентов (architecture §4.2).

Агент подключается на ``/agents/ws`` с токеном в ``Authorization``.
Цикл обслуживания:
- аутентификация по хешу токена → agent/scale, статус online;
- сразу после hello агенту уходит снимок реестра тарирований;
- heartbeat обновляет last_seen_at (и версию агента);
- weigh_result сохраняется в журнал и будит ожидающую команду API v1;
- offline_sync сохраняется идемпотентно, агенту уходит ack принятых
  uuid; новые тарирования обновляют реестр и рассылаются ВСЕМ агентам;
- разрыв → статус offline, зависшие команды этих весов завершаются
  ERR_AGENT_OFFLINE.

БД синхронная — вызовы через ``asyncio.to_thread`` (see center/db/repo).
Единственное исключение — запись offline-статуса в finally обработчика:
она намеренно синхронная, чтобы пережить отмену задачи (см. комментарий там).
"""

import asyncio
import logging
from collections.abc import Callable

from fastapi import APIRouter, WebSocket
from pydantic import ValidationError
from sqlalchemy.orm import Session
from starlette.websockets import WebSocketDisconnect

from center.agents_ws.hub import AgentHub, AgentLink
from center.db import repo
from center.db.models import AgentStatus
from shared.messages import (
    Heartbeat,
    Hello,
    OfflineSync,
    OfflineSyncAck,
    TareRegistryUpdate,
    WeighResult,
    parse_agent_message,
)

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]


class _WebSocketLink:
    """Адаптер WebSocket → AgentLink (см. hub)."""

    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket

    async def send_text(self, data: str) -> None:
        await self._websocket.send_text(data)


def create_agents_router(hub: AgentHub, session_factory: SessionFactory) -> APIRouter:
    """Собрать маршрут ``/agents/ws`` поверх хаба и фабрики сессий БД."""
    router = APIRouter()

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    async def send_tare_registry(link: AgentLink) -> None:
        records = await asyncio.to_thread(_db, repo.load_tare_registry)
        await link.send_text(TareRegistryUpdate(records=records).model_dump_json())

    async def broadcast_tare_registry() -> None:
        records = await asyncio.to_thread(_db, repo.load_tare_registry)
        await hub.broadcast_tare_registry(TareRegistryUpdate(records=records))

    @router.websocket("/agents/ws")
    async def agents_ws(websocket: WebSocket) -> None:
        authorization = websocket.headers.get("Authorization", "")
        token = authorization.removeprefix("Bearer ").strip()
        agent = None
        if token:
            agent = await asyncio.to_thread(_db, lambda s: repo.authenticate_agent(s, token))
        if agent is None:
            # токен неизвестен: соединение не принимаем
            await websocket.close(code=4401)
            logger.warning("агент отклонён: неизвестный токен")
            return
        agent_id, scale_id = agent.id, agent.scale_id

        await websocket.accept()
        link = _WebSocketLink(websocket)
        old_link = hub.attach(scale_id, link)
        if old_link is not None:
            logger.info("весы %d: новое соединение вытеснило старое", scale_id)
        await asyncio.to_thread(
            _db, lambda s: repo.set_agent_status(s, agent_id, AgentStatus.ONLINE)
        )
        logger.info("агент весов %d подключился", scale_id)

        try:
            async for raw in websocket.iter_text():
                try:
                    message = parse_agent_message(raw)
                except ValidationError:
                    logger.warning("весы %d: непонятное сообщение: %.200s", scale_id, raw)
                    continue

                if isinstance(message, Hello):
                    hub.update_equipment(scale_id, message.equipment)
                    await asyncio.to_thread(
                        _db,
                        lambda s, m=message: repo.set_agent_status(
                            s, agent_id, AgentStatus.ONLINE, version=m.version
                        ),
                    )
                    # раздача реестра тарирований при каждом подключении
                    await send_tare_registry(link)

                elif isinstance(message, Heartbeat):
                    hub.update_equipment(scale_id, message.equipment)
                    await asyncio.to_thread(
                        _db, lambda s: repo.set_agent_status(s, agent_id, AgentStatus.ONLINE)
                    )

                elif isinstance(message, WeighResult):
                    saved = await asyncio.to_thread(
                        _db,
                        lambda s, m=message: repo.save_weighing_record(
                            s, scale_id, m.record, m.record.photos
                        ),
                    )
                    hub.resolve_result(message, scale_id=scale_id)
                    if saved and _is_taring(message):
                        await broadcast_tare_registry()

                elif isinstance(message, OfflineSync):
                    accepted = []
                    any_taring = False
                    for record in message.records:
                        await asyncio.to_thread(
                            _db,
                            lambda s, r=record: repo.save_weighing_record(s, scale_id, r, r.photos),
                        )
                        # ack и за новые, и за повторные записи: агент должен
                        # пометить их synced в любом случае (идемпотентность)
                        accepted.append(record.uuid)
                        any_taring = any_taring or record.operation.value == "taring"
                    await link.send_text(OfflineSyncAck(accepted_uuids=accepted).model_dump_json())
                    logger.info("весы %d: досылка %d записей принята", scale_id, len(accepted))
                    if any_taring:
                        await broadcast_tare_registry()

        except WebSocketDisconnect:
            pass
        finally:
            was_current = hub.detach(scale_id, link)
            if was_current:
                # это было действующее соединение — агент действительно офлайн.
                # Запись статуса — СИНХРОННО, без await: при отмене задачи
                # (остановка сервера, тестовый клиент) await в finally
                # прерывается CancelledError и статус остался бы online.
                # Блокировка цикла на один UPDATE при редком разрыве допустима.
                hub.fail_pending_for_scale(scale_id, "связь с агентом потеряна во время операции")
                try:
                    _db(lambda s: repo.set_agent_status(s, agent_id, AgentStatus.OFFLINE))
                except Exception:
                    logger.exception("весы %d: не удалось записать статус offline", scale_id)
                logger.info("агент весов %d отключился", scale_id)
            else:
                # умерло вытесненное соединение — агент жив через новое
                logger.info("весы %d: закрылось вытесненное соединение", scale_id)

    return router


def _is_taring(message: WeighResult) -> bool:
    return message.record.operation.value == "taring"
