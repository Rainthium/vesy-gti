# ruff: noqa: F811 — фикстуры pytest импортируются и переиспользуются как параметры
"""Инкрементальная репликация реестра тар, сторона центра (решение 20.10.2026).

Журнал ``tare_registry_changes`` заполняется из обоих мест, меняющих реестр
(тарирование/перенос из АИС через _upsert_tare, сторно через
rebuild_tare_registry_entry); ``tare_changes_since`` отдаёт дельту или None
(нужен снимок). По WebSocket агент ≥0.4.37 получает снимок частями,
дельту или ничего — по номеру из hello/heartbeat/запроса; старый агент —
снимок одним сообщением.

Инфраструктура БД — из tests/test_center_ws.py (postgres, миграции alembic).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from center.db import repo
from center.db.models import TareRegistryChange
from center.tare_publish import publish_tare_registry
from shared.enums import ScaleStatus
from shared.messages import (
    EquipmentStatus,
    Heartbeat,
    Hello,
    TareRegistrySyncRequest,
    WeighResult,
)
from tests.test_center_ws import (  # noqa: F401 — фикстуры БД и окружения WS
    AUTH_HEADERS,
    WsEnv,
    _make_taring,
    _seed_tare,
    _tare_row,
    _weighing_by_uuid,
    repo_env,
    ws_db_engine,
    ws_db_url,
    ws_env,
)

NEW_VERSION = "0.4.37"
OLD_VERSION = "0.4.36"


def _hello(version: str, seq: int | None) -> str:
    return Hello(
        agent_id="agent-1",
        version=version,
        driver="cas22",
        equipment=EquipmentStatus(scale_status=ScaleStatus.OK),
        tare_registry_seq=seq,
    ).model_dump_json()


def _heartbeat(seq: int | None) -> str:
    return Heartbeat(
        agent_id="agent-1",
        sent_at=datetime.now(UTC),
        equipment=EquipmentStatus(scale_status=ScaleStatus.OK),
        tare_registry_seq=seq,
    ).model_dump_json()


def _recv(ws: Any) -> dict[str, Any]:
    message: dict[str, Any] = json.loads(ws.receive_text())
    return message


def _changes(session: Session) -> list[TareRegistryChange]:
    session.expire_all()
    return list(session.query(TareRegistryChange).order_by(TareRegistryChange.seq).all())


class TestChangeLog:
    def test_taring_appends_change_with_state(self, repo_env: tuple[Session, int, int]) -> None:
        session, scale_id, _ = repo_env
        assert repo.tare_registry_seq(session) == 0
        taring = _make_taring(vehicle_number="01KG100AA")
        repo.save_weighing_record(session, scale_id, taring)
        rows = _changes(session)
        assert len(rows) == 1
        assert rows[0].vehicle_number == "01KG100AA" and rows[0].tare_value == 7500.0
        assert rows[0].weighing_id == _weighing_by_uuid(session, taring.uuid).id
        assert repo.tare_registry_seq(session) == rows[0].seq

    def test_older_taring_logs_current_state(self, repo_env: tuple[Session, int, int]) -> None:
        """Более раннее тарирование не затирает тару — в журнал всё равно
        ложится актуальное состояние сцепки (безвредный повтор)."""
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        repo.save_weighing_record(session, scale_id, _make_taring(massa=8000.0, weighed_at=now))
        repo.save_weighing_record(
            session, scale_id, _make_taring(massa=7000.0, weighed_at=now - timedelta(days=5))
        )
        rows = _changes(session)
        assert [r.tare_value for r in rows] == [8000.0, 8000.0]

    def test_storno_logs_rollback_or_removal(self, repo_env: tuple[Session, int, int]) -> None:
        session, scale_id, _ = repo_env
        now = datetime.now(UTC)
        first = _make_taring(massa=7000.0, weighed_at=now - timedelta(days=5))
        second = _make_taring(massa=8000.0, weighed_at=now - timedelta(days=1))
        repo.save_weighing_record(session, scale_id, first)
        repo.save_weighing_record(session, scale_id, second)
        repo.storno_weighing(
            session, _weighing_by_uuid(session, second.uuid), actor="igor", reason="ошибка"
        )
        rows = _changes(session)
        # откат к первому тарированию
        assert rows[-1].tare_value == 7000.0
        repo.storno_weighing(
            session, _weighing_by_uuid(session, first.uuid), actor="igor", reason="ошибка"
        )
        rows = _changes(session)
        # действующей тары не осталось — запись-удаление
        assert rows[-1].weighing_id is None and rows[-1].tare_value is None
        assert _tare_row(session, "01KG123ABC") is None

    def test_changes_since_semantics(self, repo_env: tuple[Session, int, int]) -> None:
        session, scale_id, _ = repo_env
        for i in range(3):
            repo.save_weighing_record(
                session, scale_id, _make_taring(vehicle_number=f"01KG00{i}AA")
            )
        current = repo.tare_registry_seq(session)
        assert repo.tare_changes_since(session, current) == []
        delta = repo.tare_changes_since(session, current - 2)
        assert delta is not None and [c.vehicle_number for c in delta] == ["01KG001AA", "01KG002AA"]
        assert delta[-1].record is not None and delta[-1].record.weighing_uuid is not None
        # номер впереди центра (БД восстановлена из бэкапа) — снимок
        assert repo.tare_changes_since(session, current + 5) is None
        # журнал подчищен ниже номера агента — снимок
        session.query(TareRegistryChange).filter(TareRegistryChange.seq == current - 2).delete()
        session.flush()
        assert repo.tare_changes_since(session, current - 3) is None
        assert repo.tare_changes_since(session, current - 2) is not None

    def test_empty_log_zero_seq_means_nothing_to_send(
        self, repo_env: tuple[Session, int, int]
    ) -> None:
        session, _, _ = repo_env
        assert repo.tare_changes_since(session, 0) == []

    def test_retention_prunes_old_rows(self, repo_env: tuple[Session, int, int]) -> None:
        session, scale_id, _ = repo_env
        repo.save_weighing_record(session, scale_id, _make_taring(vehicle_number="01KG001AA"))
        old = _changes(session)[0]
        old.changed_at = datetime.now(UTC) - repo.TARE_CHANGES_RETENTION - timedelta(days=1)
        session.flush()
        repo.save_weighing_record(session, scale_id, _make_taring(vehicle_number="01KG002AA"))
        assert [r.vehicle_number for r in _changes(session)] == ["01KG002AA"]


class TestWsDeltas:
    def test_new_agent_without_replica_gets_chunked_snapshot(self, ws_env: WsEnv) -> None:
        taring = _seed_tare(ws_env)
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(NEW_VERSION, None))
            assert _recv(ws)["type"] == "heartbeat_ack"
            snapshot = _recv(ws)
            assert snapshot["type"] == "tare_registry"
            assert snapshot["reset"] is True and snapshot["final"] is True
            assert snapshot["seq"] == 1
            assert [r["weighing_uuid"] for r in snapshot["records"]] == [str(taring.uuid)]
            assert _recv(ws)["type"] == "operators_registry"

    def test_agent_in_sync_gets_no_registry_message(self, ws_env: WsEnv) -> None:
        _seed_tare(ws_env)
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(NEW_VERSION, 1))
            assert _recv(ws)["type"] == "heartbeat_ack"
            assert _recv(ws)["type"] == "operators_registry"

    def test_lagging_agent_gets_delta_only(self, ws_env: WsEnv) -> None:
        _seed_tare(ws_env, "01KG111AA")
        second = _seed_tare(ws_env, "01KG222BB")
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(NEW_VERSION, 1))
            assert _recv(ws)["type"] == "heartbeat_ack"
            delta = _recv(ws)
            assert delta["type"] == "tare_registry_delta"
            assert delta["since_seq"] == 1
            assert [c["seq"] for c in delta["changes"]] == [2]
            assert delta["changes"][0]["record"]["weighing_uuid"] == str(second.uuid)
            assert _recv(ws)["type"] == "operators_registry"

    def test_old_agent_gets_full_snapshot_single_message(self, ws_env: WsEnv) -> None:
        _seed_tare(ws_env)
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(OLD_VERSION, None))
            assert _recv(ws)["type"] == "heartbeat_ack"
            snapshot = _recv(ws)
            assert snapshot["type"] == "tare_registry"
            assert "seq" not in snapshot  # старый формат без номеров
            assert len(snapshot["records"]) == 1

    def test_taring_result_broadcasts_delta_to_new_agent(self, ws_env: WsEnv) -> None:
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(NEW_VERSION, None))
            assert _recv(ws)["type"] == "heartbeat_ack"
            assert _recv(ws)["type"] == "tare_registry"  # пустой снимок №0
            assert _recv(ws)["type"] == "operators_registry"
            taring = _make_taring(vehicle_number="01KG777CC")
            ws.send_text(WeighResult(request_id=uuid4(), record=taring).model_dump_json())
            delta = _recv(ws)
            assert delta["type"] == "tare_registry_delta"
            assert delta["since_seq"] == 0
            assert [c["vehicle_number"] for c in delta["changes"]] == ["01KG777CC"]
            assert ws_env.hub.last_tare_seq == 1

    def test_sync_request_and_stale_heartbeat_catch_up(self, ws_env: WsEnv) -> None:
        _seed_tare(ws_env, "01KG111AA")
        with ws_env.client.websocket_connect("/agents/ws", headers=AUTH_HEADERS) as ws:
            ws.send_text(_hello(NEW_VERSION, 1))
            assert _recv(ws)["type"] == "heartbeat_ack"
            assert _recv(ws)["type"] == "operators_registry"
            # агент просит досылку с номера 0 — получает дельту №1
            ws.send_text(TareRegistrySyncRequest(since_seq=0).model_dump_json())
            delta = _recv(ws)
            assert delta["type"] == "tare_registry_delta" and delta["since_seq"] == 0
            # реестр изменился мимо этого соединения (другой объект) — heartbeat
            # с отставшим номером догоняет без переподключения
            _seed_tare(ws_env, "01KG222BB")
            with ws_env.factory() as session:
                asyncio.run(publish_tare_registry(ws_env.hub, lambda fn: fn(session)))
            # publish разослал дельту №2 этому агенту
            delta2 = _recv(ws)
            assert delta2["type"] == "tare_registry_delta" and [
                c["seq"] for c in delta2["changes"]
            ] == [2]
            ws.send_text(_heartbeat(1))
            ack_or_delta = _recv(ws)
            # heartbeat_ack и дельта — в любом порядке
            kinds = {ack_or_delta["type"], _recv(ws)["type"]}
            assert kinds == {"heartbeat_ack", "tare_registry_delta"}
