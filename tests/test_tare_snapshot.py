"""Снимок реестра тар под предел WS-сообщения агента (инцидент 20.10.2026).

Старые агенты (до 0.4.36) принимают не больше 1 МиБ распакованного текста:
снимок режется, новые записи остаются. Новым — полный снимок.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from center.agents_ws.hub import AgentHub
from center.tare_snapshot import SAFETY_RATIO, TareSnapshot
from shared.messages import (
    AGENT_WS_LEGACY_MAX_MESSAGE_BYTES,
    TareRecord,
    TareRegistryUpdate,
    supports_large_registry,
)

NOW = datetime(2026, 10, 20, 10, 43, tzinfo=UTC)


def _records(count: int) -> list[TareRecord]:
    """count записей, самая новая — tared_at=NOW, далее по минуте назад."""
    return [
        TareRecord(
            vehicle_number=f"01KG{i:05d}AA",
            trailer_number=f"01KG{i:05d}PP" if i % 7 else None,
            tare_value=14_000.0 + i,
            tared_at=NOW - timedelta(minutes=i),
            weighing_uuid=uuid4(),
        )
        for i in range(count)
    ]


def _parse(payload: str) -> list[dict[str, object]]:
    message = json.loads(payload)
    assert message["type"] == "tare_registry"
    records: list[dict[str, object]] = list(message["records"])
    return records


class TestVersionGate:
    def test_large_registry_from_0_4_36(self) -> None:
        assert not supports_large_registry("0.4.35")
        assert not supports_large_registry(None)
        assert supports_large_registry("0.4.36")
        assert supports_large_registry("0.5.0")


class TestSnapshotSmall:
    def test_small_registry_sent_whole_to_everyone(self) -> None:
        snapshot = TareSnapshot(_records(50))
        for version in ("0.4.35", "0.4.36", None):
            assert len(_parse(snapshot.payload_for(version))) == 50

    def test_records_newest_first_and_null_trailer_omitted(self) -> None:
        records = _records(10)
        shuffled = list(reversed(records))
        parsed = _parse(TareSnapshot(shuffled).payload_for("0.4.35"))
        assert [r["vehicle_number"] for r in parsed] == [r.vehicle_number for r in records]
        no_trailer = next(r for r in parsed if r["vehicle_number"] == "01KG00000AA")
        assert "trailer_number" not in no_trailer  # exclude_none
        # агент читает отсутствие поля как None
        parsed_update = TareRegistryUpdate.model_validate_json(
            TareSnapshot(shuffled).payload_for("0.4.35")
        )
        assert parsed_update.records[0].trailer_number is None


class TestSnapshotOverLimit:
    """Реестр, не влезающий в 1 МиБ: боевой размер 20.10.2026 — 6 054 записи."""

    def setup_method(self) -> None:
        self.records = _records(7000)
        self.snapshot = TareSnapshot(self.records)
        assert len(self.snapshot.full_payload.encode()) > AGENT_WS_LEGACY_MAX_MESSAGE_BYTES

    def test_legacy_agent_gets_trimmed_snapshot_under_limit(self) -> None:
        payload = self.snapshot.payload_for("0.4.35")
        assert len(payload.encode("utf-8")) <= AGENT_WS_LEGACY_MAX_MESSAGE_BYTES * SAFETY_RATIO
        parsed = _parse(payload)
        # отброшены самые старые, новые остались и идут первыми
        assert 0 < len(parsed) < 7000
        assert parsed[0]["vehicle_number"] == "01KG00000AA"
        assert [r["vehicle_number"] for r in parsed] == [
            r.vehicle_number for r in self.records[: len(parsed)]
        ]
        # не режем лишнего: добавить ещё одну запись уже нельзя
        one_more = TareRegistryUpdate(records=self.records[: len(parsed) + 1]).model_dump_json(
            exclude_none=True
        )
        tight = AGENT_WS_LEGACY_MAX_MESSAGE_BYTES * SAFETY_RATIO * 0.97
        assert len(one_more.encode("utf-8")) > tight

    def test_new_agent_gets_full_snapshot(self) -> None:
        assert len(_parse(self.snapshot.payload_for("0.4.36"))) == 7000


class _FakeLink:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send_text(self, data: str) -> None:
        self.sent.append(data)


class TestHubBroadcastPerVersion:
    def test_each_agent_gets_payload_for_its_version(self) -> None:
        hub = AgentHub()
        old_link, new_link, unknown_link = _FakeLink(), _FakeLink(), _FakeLink()
        hub.attach(1, old_link)
        hub.update_version(1, "0.4.35")
        hub.attach(2, new_link)
        hub.update_version(2, "0.4.36")
        hub.attach(3, unknown_link)  # hello ещё не было — считаем старым
        update = TareRegistryUpdate(records=_records(7000))
        sent = asyncio.run(hub.broadcast_tare_registry(update))
        assert sent == 3
        assert len(_parse(new_link.sent[0])) == 7000
        assert len(_parse(old_link.sent[0])) < 7000
        assert len(_parse(unknown_link.sent[0])) < 7000
        assert len(old_link.sent[0].encode()) <= AGENT_WS_LEGACY_MAX_MESSAGE_BYTES * SAFETY_RATIO

    def test_detach_forgets_version(self) -> None:
        hub = AgentHub()
        link = _FakeLink()
        hub.attach(1, link)
        hub.update_version(1, "0.4.36")
        assert hub.agent_version(1) == "0.4.36"
        hub.detach(1, link)
        assert hub.agent_version(1) is None
