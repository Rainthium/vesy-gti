"""Инкрементальная репликация реестра тар, сторона агента (0.4.37).

Хранилище: номер реплики, снимок частями через буфер (реплика подменяется
только по последней части), дельты — вставка/замена/удаление по сцепке.
Клиент: hello/heartbeat несут номер, дельта применяется, разрыв номеров —
запрос досылки, дельта во время снимка — досылка после него.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from agent.sync.storage import AgentStorage
from shared.messages import (
    Heartbeat,
    Hello,
    TareChange,
    TareRecord,
    TareRegistryDelta,
    TareRegistrySyncRequest,
    TareRegistryUpdate,
)
from tests.test_ws_client import Scene, run_scenario, wait_until

NOW = datetime.now(UTC)


def _tare(vehicle: str, value: float = 7000.0, trailer: str | None = None) -> TareRecord:
    return TareRecord(
        vehicle_number=vehicle,
        trailer_number=trailer,
        tare_value=value,
        tared_at=NOW - timedelta(days=1),
        weighing_uuid=uuid4(),
    )


class TestStorage:
    def test_seq_unknown_until_snapshot_with_seq(self) -> None:
        storage = AgentStorage(":memory:")
        assert storage.tare_registry_seq() is None
        storage.replace_tare_registry([_tare("A")])
        assert storage.tare_registry_seq() is None  # старый способ — номера нет
        storage.replace_tare_registry([_tare("A")], seq=7)
        assert storage.tare_registry_seq() == 7

    def test_chunked_snapshot_swaps_replica_only_at_finish(self) -> None:
        storage = AgentStorage(":memory:")
        storage.replace_tare_registry([_tare("OLD")], seq=1)
        storage.begin_tare_snapshot()
        storage.append_tare_snapshot([_tare("A"), _tare("B")])
        # посреди приёма рабочая реплика прежняя
        assert storage.latest_tare("OLD") is not None and storage.latest_tare("A") is None
        storage.append_tare_snapshot([_tare("C")])
        assert storage.finish_tare_snapshot(9) == 3
        assert storage.latest_tare("OLD") is None
        assert {v for v in ("A", "B", "C") if storage.latest_tare(v) is not None} == {"A", "B", "C"}
        assert storage.tare_registry_seq() == 9

    def test_apply_changes_upsert_and_delete(self) -> None:
        storage = AgentStorage(":memory:")
        storage.replace_tare_registry([_tare("A", 7000.0), _tare("B")], seq=3)
        applied = storage.apply_tare_changes(
            [
                TareChange(seq=4, vehicle_number="A", record=_tare("A", 7500.0)),
                TareChange(seq=5, vehicle_number="B", record=None),
                TareChange(
                    seq=6, vehicle_number="C", trailer_number="T1", record=_tare("C", trailer="T1")
                ),
            ]
        )
        assert applied == 6 and storage.tare_registry_seq() == 6
        a = storage.latest_tare("A")
        assert a is not None and a.tare_value == 7500.0
        assert storage.latest_tare("B") is None
        assert storage.latest_tare("C", "T1") is not None and storage.latest_tare("C") is None
        assert storage.apply_tare_changes([]) is None


class TestClient:
    def test_hello_and_heartbeat_carry_seq(self) -> None:
        async def scenario() -> None:
            storage = AgentStorage(":memory:")
            storage.replace_tare_registry([_tare("A")], seq=5)
            async with Scene(storage=storage, heartbeat_interval_s=0.05) as scene:
                hello = await scene.center.expect(Hello)
                assert hello.tare_registry_seq == 5
                heartbeat = await scene.center.expect(Heartbeat)
                assert heartbeat.tare_registry_seq == 5

        run_scenario(scenario())

    def test_delta_applied_when_continuous(self) -> None:
        async def scenario() -> None:
            storage = AgentStorage(":memory:")
            storage.replace_tare_registry([_tare("A")], seq=5)
            async with Scene(storage=storage) as scene:
                await scene.center.expect(Hello)
                delta = TareRegistryDelta(
                    since_seq=5,
                    changes=[TareChange(seq=6, vehicle_number="B", record=_tare("B"))],
                )
                await scene.center.connection.send(delta.model_dump_json())
                await wait_until(lambda: storage.latest_tare("B") is not None)
                assert storage.tare_registry_seq() == 6
                await scene.center.assert_no_message(TareRegistrySyncRequest, 0.1)

        run_scenario(scenario())

    def test_gap_requests_sync_from_own_seq(self) -> None:
        async def scenario() -> None:
            storage = AgentStorage(":memory:")
            storage.replace_tare_registry([_tare("A")], seq=5)
            async with Scene(storage=storage) as scene:
                await scene.center.expect(Hello)
                delta = TareRegistryDelta(
                    since_seq=7,
                    changes=[TareChange(seq=8, vehicle_number="B", record=_tare("B"))],
                )
                await scene.center.connection.send(delta.model_dump_json())
                request = await scene.center.expect(TareRegistrySyncRequest)
                assert request.since_seq == 5
                assert storage.latest_tare("B") is None and storage.tare_registry_seq() == 5

        run_scenario(scenario())

    def test_chunked_snapshot_and_delta_during_it(self) -> None:
        async def scenario() -> None:
            storage = AgentStorage(":memory:")
            async with Scene(storage=storage) as scene:
                await scene.center.expect(Hello)
                conn = scene.center.connection
                await conn.send(
                    TareRegistryUpdate(
                        records=[_tare("A")], seq=10, reset=True, final=False
                    ).model_dump_json()
                )
                # дельта пришла посреди снимка — применять нельзя, запомнить
                await conn.send(
                    TareRegistryDelta(
                        since_seq=10,
                        changes=[TareChange(seq=11, vehicle_number="C", record=_tare("C"))],
                    ).model_dump_json()
                )
                await conn.send(
                    TareRegistryUpdate(
                        records=[_tare("B")], seq=10, reset=False, final=True
                    ).model_dump_json()
                )
                request = await scene.center.expect(TareRegistrySyncRequest)
                assert request.since_seq == 10
                assert storage.tare_registry_size() == 2
                assert storage.tare_registry_seq() == 10

        run_scenario(scenario())
