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
from datetime import UTC, datetime

from fastapi import APIRouter, WebSocket
from pydantic import ValidationError
from sqlalchemy.orm import Session
from starlette.websockets import WebSocketDisconnect

from center.agents_ws.hub import AgentHub, AgentLink
from center.db import repo
from center.db.models import AgentStatus
from shared.messages import (
    ConfigStatus,
    Heartbeat,
    HeartbeatAck,
    Hello,
    LogTailResponse,
    OfflineSync,
    OfflineSyncAck,
    OperatorsRegistryUpdate,
    OperatorsReport,
    ScaleConfigUpdate,
    TareRegistryUpdate,
    UpdateStatus,
    WeighResult,
    parse_agent_message,
    supports_secure_sync,
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

    async def send_operators(link: AgentLink, scale_id: int) -> None:
        records = await asyncio.to_thread(_db, lambda s: repo.load_operators_for_scale(s, scale_id))
        await link.send_text(OperatorsRegistryUpdate(records=records).model_dump_json())

    async def send_scale_config(link: AgentLink, scale_id: int) -> None:
        """Настройки весов из центра — только если там что-то задано."""
        settings = await asyncio.to_thread(_db, lambda s: repo.load_scale_settings(s, scale_id))
        if settings is not None:
            await link.send_text(ScaleConfigUpdate(settings=settings).model_dump_json())

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
        # версия из последнего hello: гейт снимков с секретами и heartbeat_ack
        agent_version: str | None = None

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
                    agent_version = message.version
                    hub.update_equipment(scale_id, message.equipment)
                    await asyncio.to_thread(
                        _db,
                        lambda s, m=message: repo.set_agent_status(
                            s, agent_id, AgentStatus.ONLINE, version=m.version
                        ),
                    )
                    # раздача при каждом подключении: реестр тар, затем
                    # операторы и настройки весов (если заданы в центре).
                    # Снимки с секретами — только агентам, понимающим их:
                    # старый ws_client логирует незнакомые сообщения (№7)
                    if supports_secure_sync(message.version):
                        # время центра — первым (фиксированная позиция),
                        # затем реестры и настройки
                        await link.send_text(
                            HeartbeatAck(server_time=datetime.now(UTC)).model_dump_json()
                        )
                    await send_tare_registry(link)
                    if supports_secure_sync(message.version):
                        await send_operators(link, scale_id)
                        await send_scale_config(link, scale_id)
                    else:
                        logger.info(
                            "весы %d: агент v%s не получает операторов/настройки — "
                            "обновите его из панели",
                            scale_id,
                            message.version,
                        )

                elif isinstance(message, Heartbeat):
                    hub.update_equipment(scale_id, message.equipment)
                    await asyncio.to_thread(
                        _db, lambda s: repo.set_agent_status(s, agent_id, AgentStatus.ONLINE)
                    )
                    if supports_secure_sync(agent_version):
                        # старый агент логировал бы незнакомый ack каждые 5 с
                        await link.send_text(
                            HeartbeatAck(server_time=datetime.now(UTC)).model_dump_json()
                        )

                elif isinstance(message, WeighResult):
                    # номер документа АИС команды v2 — в ту же транзакцию, что
                    # и запись (идемпотентность контракта v2); у v1 его нет
                    ais_ref = hub.take_ais_ref(message.request_id)
                    saved = await asyncio.to_thread(
                        _db,
                        lambda s, m=message, ref=ais_ref: repo.save_weighing_record(
                            s, scale_id, m.record, m.record.photos, ais_ref=ref
                        ),
                    )
                    hub.resolve_result(message, scale_id=scale_id)
                    if saved and _is_taring(message):
                        await broadcast_tare_registry()

                elif isinstance(message, ConfigStatus):
                    if message.ok:
                        logger.info("весы %d: настройки применены агентом", scale_id)
                    else:
                        logger.error(
                            "весы %d: настройки НЕ применены%s: %s",
                            scale_id,
                            " (откат COM-порта)" if message.rolled_back else "",
                            message.error,
                        )

                elif isinstance(message, LogTailResponse):
                    if not hub.resolve_log_tail(message, scale_id=scale_id):
                        # никто не ждёт: запрос уже отвалился по тайм-ауту
                        logger.info("весы %d: журнал пришёл поздно, отброшен", scale_id)

                elif isinstance(message, OperatorsReport):
                    # снимок учёток весового ПК (агент 0.4.14): полная
                    # замена; отчёт плановый — лог не засоряем
                    await asyncio.to_thread(
                        _db,
                        lambda s, m=message: repo.replace_agent_operators(s, scale_id, m.records),
                    )
                    logger.debug(
                        "весы %d: снимок учёток агента, %d записей",
                        scale_id,
                        len(message.records),
                    )

                elif isinstance(message, UpdateStatus):
                    if message.ok:
                        logger.info(
                            "весы %d: автообновление до %s запущено агентом",
                            scale_id,
                            message.version,
                        )
                    else:
                        logger.error(
                            "весы %d: автообновление до %s НЕ выполнено: %s",
                            scale_id,
                            message.version,
                            message.error,
                        )

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
