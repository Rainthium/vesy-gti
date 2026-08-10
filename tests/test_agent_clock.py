"""Тесты скорректированных часов агента (agent/clock.py).

«Время от центра» (вопрос Игоря 10.08.2026): агент по heartbeat_ack
считает смещение своих часов относительно центра и ставит в записи
скорректированное время. Покрытие CenterClock:

- без storage и без синка: now() — локальные часы, synced False, offset 0;
- set_server_time со сдвигом → now() сдвинуто на смещение (допуск на
  время исполнения), synced True, offset_s соответствует;
- повторный set_server_time обновляет смещение (в т.ч. на отрицательное);
- с реальным AgentStorage: смещение персистится и переживает пересоздание
  CenterClock (офлайн-режим работает от последнего известного смещения);
- кривое значение в БД ('abc' руками) → load None → часы локальные,
  а свежая синхронизация перезаписывает мусор валидным числом.
"""

import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from agent.clock import CenterClock
from agent.sync.storage import AgentStorage

# Допуск на время исполнения между datetime.now() внутри и снаружи часов.
TOLERANCE_S = 2.0


def offset_now_s(clock: CenterClock) -> float:
    """Фактический сдвиг clock.now() относительно локальных часов, секунды."""
    return (clock.now() - datetime.now(UTC)).total_seconds()


class TestWithoutStorage:
    """CenterClock без хранилища: чистая арифметика смещения."""

    def test_unsynced_now_equals_local_clock(self) -> None:
        """Синхронизации не было: now() — локальные часы, synced False, offset 0."""
        clock = CenterClock()
        assert clock.synced is False
        assert clock.offset_s == 0.0
        before = datetime.now(UTC)
        now = clock.now()
        after = datetime.now(UTC)
        assert before <= now <= after

    def test_now_is_aware_utc(self) -> None:
        """now() возвращает aware-дату в UTC (в БД — UTC, правило №4а)."""
        clock = CenterClock()
        assert clock.now().utcoffset() == timedelta(0)
        clock.set_server_time(datetime.now(UTC) + timedelta(seconds=120))
        assert clock.now().utcoffset() == timedelta(0)

    def test_set_server_time_shifts_now_forward(self) -> None:
        """Часы центра на +120 с впереди: now() сдвигается на ~120 с."""
        clock = CenterClock()
        clock.set_server_time(datetime.now(UTC) + timedelta(seconds=120))
        assert clock.synced is True
        assert clock.offset_s == pytest.approx(120.0, abs=TOLERANCE_S)
        assert offset_now_s(clock) == pytest.approx(120.0, abs=TOLERANCE_S)

    def test_repeated_set_updates_offset(self) -> None:
        """Повторный heartbeat_ack обновляет смещение (часы ПК продолжают уходить)."""
        clock = CenterClock()
        clock.set_server_time(datetime.now(UTC) + timedelta(seconds=120))
        # центр «уехал» в другую сторону: смещение стало отрицательным
        clock.set_server_time(datetime.now(UTC) - timedelta(seconds=60))
        assert clock.synced is True
        assert clock.offset_s == pytest.approx(-60.0, abs=TOLERANCE_S)
        assert offset_now_s(clock) == pytest.approx(-60.0, abs=TOLERANCE_S)

    def test_zero_offset_still_synced(self) -> None:
        """Часы совпали (смещение ~0): synced всё равно True — синк был."""
        clock = CenterClock()
        clock.set_server_time(datetime.now(UTC))
        assert clock.synced is True
        assert clock.offset_s == pytest.approx(0.0, abs=TOLERANCE_S)


class TestWithStorage:
    """CenterClock поверх AgentStorage: смещение переживает рестарт агента."""

    @pytest.fixture
    def db_path(self, tmp_path: Path) -> Path:
        return tmp_path / "agent.db"

    @pytest.fixture
    def storage(self, db_path: Path) -> Iterator[AgentStorage]:
        store = AgentStorage(db_path)
        yield store
        store.close()

    def test_fresh_db_not_synced(self, storage: AgentStorage) -> None:
        """Свежая БД без сохранённого смещения: часы локальные."""
        clock = CenterClock(storage)
        assert clock.synced is False
        assert clock.offset_s == 0.0

    def test_set_server_time_persists_offset(self, storage: AgentStorage) -> None:
        """set_server_time тут же кладёт смещение в БД (переживёт рестарт)."""
        clock = CenterClock(storage)
        clock.set_server_time(datetime.now(UTC) + timedelta(seconds=120))
        stored = storage.load_clock_offset_s()
        assert stored is not None
        assert stored == pytest.approx(120.0, abs=TOLERANCE_S)

    def test_offset_survives_clock_recreation(self, db_path: Path) -> None:
        """Пересоздание CenterClock (рестарт агента): новый экземпляр сразу
        synced с тем же смещением — офлайн работает от последнего синка."""
        first_storage = AgentStorage(db_path)
        clock = CenterClock(first_storage)
        clock.set_server_time(datetime.now(UTC) + timedelta(seconds=120))
        saved_offset = clock.offset_s
        first_storage.close()

        second_storage = AgentStorage(db_path)
        try:
            reborn = CenterClock(second_storage)
            assert reborn.synced is True
            assert reborn.offset_s == saved_offset
            assert offset_now_s(reborn) == pytest.approx(120.0, abs=TOLERANCE_S)
        finally:
            second_storage.close()

    def test_negative_offset_survives_recreation(self, db_path: Path) -> None:
        """Отрицательное смещение (часы ПК убежали вперёд) тоже персистится."""
        first_storage = AgentStorage(db_path)
        CenterClock(first_storage).set_server_time(datetime.now(UTC) - timedelta(seconds=90))
        first_storage.close()

        second_storage = AgentStorage(db_path)
        try:
            reborn = CenterClock(second_storage)
            assert reborn.synced is True
            assert reborn.offset_s == pytest.approx(-90.0, abs=TOLERANCE_S)
        finally:
            second_storage.close()

    def _write_corrupt_offset(self, db_path: Path) -> None:
        """Руками вписать мусор в agent_settings (повреждение файла БД)."""
        AgentStorage(db_path).close()  # создать схему
        conn = sqlite3.connect(db_path)
        with conn:
            conn.execute(
                "INSERT INTO agent_settings (key, value) VALUES ('clock_offset_s', 'abc')"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value"
            )
        conn.close()

    def test_corrupt_stored_value_falls_back_to_local(self, db_path: Path) -> None:
        """Кривое значение в БД ('abc') → load None → часы локальные, без падения."""
        self._write_corrupt_offset(db_path)
        storage = AgentStorage(db_path)
        try:
            assert storage.load_clock_offset_s() is None
            clock = CenterClock(storage)
            assert clock.synced is False
            assert clock.offset_s == 0.0
            assert offset_now_s(clock) == pytest.approx(0.0, abs=TOLERANCE_S)
        finally:
            storage.close()

    def test_fresh_sync_overwrites_corrupt_value(self, db_path: Path) -> None:
        """Свежий heartbeat_ack перезаписывает мусор валидным числом."""
        self._write_corrupt_offset(db_path)
        storage = AgentStorage(db_path)
        try:
            clock = CenterClock(storage)
            clock.set_server_time(datetime.now(UTC) + timedelta(seconds=45))
            stored = storage.load_clock_offset_s()
            assert stored is not None
            assert stored == pytest.approx(45.0, abs=TOLERANCE_S)
        finally:
            storage.close()
