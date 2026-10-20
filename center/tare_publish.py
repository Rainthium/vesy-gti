"""Рассылка изменений реестра тар агентам (инкрементальная репликация, 20.10.2026).

Один вход для всех мест, где реестр меняется (тарирование по команде,
досылка офлайн-тарирований, сторно в панели): агентам без поддержки дельт
(< 0.4.37) — полный снимок под предел их версии, агентам с дельтами —
только изменения после номера, до которого хаб уже рассылал
(``hub.last_tare_seq``). Если дельтой не догнать (журнал подчищен, номер
неизвестен) — полный снимок частями.

Догонялка одного агента (hello, heartbeat с отставшим номером, запрос
``tare_registry_sync``) — ``sync_agent``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Protocol, TypeVar

from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentLink
from center.db import repo
from center.tare_snapshot import TareSnapshot
from shared.messages import TareChange, TareRecord, TareRegistryDelta, TareRegistryUpdate

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RunInDb(Protocol):
    """Выполнить функцию в сессии БД (замыкание ``_db`` маршрутов)."""

    def __call__(self, fn: Callable[[Session], T]) -> T: ...


Loaded = tuple[int, list[TareRecord] | None, list[TareChange] | None]


async def publish_tare_registry(hub: AgentHub, run_in_db: RunInDb) -> int:
    """Разослать актуальное состояние реестра всем подключённым агентам.

    Возвращает число агентов, которым что-то отправлено. Номер журнала
    фиксируется в ``hub.last_tare_seq`` даже если отправлять было некому.
    """
    legacy_ids = hub.legacy_tare_scale_ids()
    delta_ids = hub.delta_tare_scale_ids()
    since = hub.last_tare_seq

    def load(session: Session) -> Loaded:
        current = repo.tare_registry_seq(session)
        need_snapshot = bool(legacy_ids) or (bool(delta_ids) and since is None)
        records = repo.load_tare_registry(session) if need_snapshot else None
        changes = (
            repo.tare_changes_since(session, since) if delta_ids and since is not None else None
        )
        if delta_ids and changes is None and records is None:
            records = repo.load_tare_registry(session)
        return current, records, changes

    current, records, changes = await asyncio.to_thread(lambda: run_in_db(load))
    sent = 0
    if legacy_ids and records is not None:
        sent += await hub.broadcast_tare_registry(TareRegistryUpdate(records=records))
    if delta_ids:
        if changes is None:
            assert records is not None
            payloads = TareSnapshot(records).chunk_payloads(current)
            for scale_id in delta_ids:
                if await _send_all(hub, scale_id, payloads):
                    sent += 1
        elif changes and since is not None:
            payload = TareRegistryDelta(since_seq=since, changes=changes).model_dump_json(
                exclude_none=True
            )
            for scale_id in delta_ids:
                if await hub.send_text(scale_id, payload):
                    sent += 1
    hub.last_tare_seq = current
    return sent


async def sync_agent(
    hub: AgentHub,
    link: AgentLink,
    scale_id: int,
    agent_seq: int | None,
    run_in_db: RunInDb,
) -> None:
    """Догнать реплику одного агента (≥0.4.37) до текущего состояния реестра:
    ничего, дельта после ``agent_seq`` или полный снимок частями."""

    def load(session: Session) -> Loaded:
        current = repo.tare_registry_seq(session)
        changes = repo.tare_changes_since(session, agent_seq) if agent_seq is not None else None
        records = repo.load_tare_registry(session) if changes is None else None
        return current, records, changes

    current, records, changes = await asyncio.to_thread(lambda: run_in_db(load))
    if hub.last_tare_seq is None or hub.last_tare_seq < current:
        # изменения могли пройти мимо publish (перенос из АИС инструментом):
        # не гонять каждый heartbeat в БД из-за «агент впереди хаба»
        hub.last_tare_seq = current
    if changes is None:
        assert records is not None
        for payload in TareSnapshot(records).chunk_payloads(current):
            await link.send_text(payload)
        logger.info(
            "весы %d: реестр тар — полный снимок %d записей частями (агент на №%s, центр на №%d)",
            scale_id,
            len(records),
            agent_seq,
            current,
        )
    elif changes and agent_seq is not None:
        await link.send_text(
            TareRegistryDelta(since_seq=agent_seq, changes=changes).model_dump_json(
                exclude_none=True
            )
        )
        logger.info(
            "весы %d: реестр тар — дельта %d изменений (№%d → №%d)",
            scale_id,
            len(changes),
            agent_seq,
            current,
        )


async def _send_all(hub: AgentHub, scale_id: int, payloads: list[str]) -> bool:
    for payload in payloads:
        if not await hub.send_text(scale_id, payload):
            return False
    return True
