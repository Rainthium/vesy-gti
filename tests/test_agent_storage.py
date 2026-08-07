"""Тесты локальной БД агента (agent/sync/storage.py).

Покрытие:
- журнал операций: round-trip всех полей, однократность вставки, очередь
  досылки (pending/mark_synced), журнал интерфейса, фото операций;
- неизменяемость (правило проекта №2): триггеры БД блокируют UPDATE/DELETE
  даже при прямом SQL-доступе; разрешён только переход synced 0 → 1;
- реплика реестра тарирований и правило «тара не старше 3 месяцев» (№4);
- календарная арифметика three_months_before с поджатием дня месяца;
- персистентность файла БД и потокобезопасность при параллельной записи.
"""

import sqlite3
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from agent.sync.storage import AgentStorage, StoredPhoto, three_months_before
from shared import (
    CameraRole,
    ErrorCode,
    Operation,
    TareRecord,
    WeighingRecord,
    WeighingSource,
)


def make_record(**overrides: Any) -> WeighingRecord:
    """Типичная успешная запись взвешивания; overrides — точечные замены полей."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 12340.0,
        "unit": "kg",
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 10, 30, 15, 123456, tzinfo=UTC),
        "vehicle_number": "01KG123ABC",
        "trailer_number": None,
        "tare_value": None,
        "tare_weighing_uuid": None,
        "netto": None,
        "source": WeighingSource.AIS,
        "operator": None,
        "message": None,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def make_tare(vehicle_number: str, tared_at: datetime, tare_value: float = 7500.0) -> TareRecord:
    """Строка реестра тарирований."""
    return TareRecord(
        vehicle_number=vehicle_number,
        tare_value=tare_value,
        tared_at=tared_at,
        weighing_uuid=uuid4(),
    )


def save_spaced(storage: AgentStorage, *records: WeighingRecord) -> None:
    """Сохранить записи с гарантированно различным created_at (для проверок порядка)."""
    for record in records:
        storage.save_weighing(record)
        time.sleep(0.002)


@pytest.fixture
def storage() -> Iterator[AgentStorage]:
    """БД в памяти — для всех тестов, кроме персистентности и потоков."""
    st = AgentStorage(":memory:")
    yield st
    st.close()


class TestWeighingRoundTrip:
    """Сохранение и чтение записи: все поля восстанавливаются точно."""

    def test_full_record_round_trip(self, storage: AgentStorage) -> None:
        # Заполнены все поля, включая ссылку на тарирование и нетто
        record = make_record(
            operation=Operation.WEIGHING,
            massa=24680.5,
            trailer_number="01KG456ПП",
            tare_value=7500.0,
            tare_weighing_uuid=uuid4(),
            netto=17180.5,
            source=WeighingSource.LOCAL_OFFLINE,
            operator="operator1",
            message="взвешено в ручном режиме",
        )
        storage.save_weighing(record)
        assert storage.get_weighing(record.uuid) == record

    def test_none_fields_round_trip(self, storage: AgentStorage) -> None:
        # Операция не дошла до фиксации: масса и дата отсутствуют
        record = make_record(
            code=ErrorCode.ERR_SCALE_OFFLINE,
            massa=None,
            stable=False,
            weighed_at=None,
            vehicle_number=None,
            message="нет данных от индикатора",
        )
        storage.save_weighing(record)
        restored = storage.get_weighing(record.uuid)
        assert restored == record
        assert restored is not None
        assert restored.massa is None
        assert restored.weighed_at is None
        assert restored.vehicle_number is None

    def test_negative_mass_round_trip(self, storage: AgentStorage) -> None:
        # Индикатор может показывать отрицательный вес (сдвиг нуля)
        record = make_record(massa=-40.0, code=ErrorCode.OK)
        storage.save_weighing(record)
        restored = storage.get_weighing(record.uuid)
        assert restored is not None
        assert restored.massa == -40.0

    def test_taring_operation_round_trip(self, storage: AgentStorage) -> None:
        record = make_record(operation=Operation.TARING, massa=7500.0)
        storage.save_weighing(record)
        restored = storage.get_weighing(record.uuid)
        assert restored is not None
        assert restored.operation is Operation.TARING

    def test_get_unknown_uuid_returns_none(self, storage: AgentStorage) -> None:
        assert storage.get_weighing(uuid4()) is None

    def test_duplicate_uuid_rejected(self, storage: AgentStorage) -> None:
        # Журнал не перезаписывается: повторная вставка того же uuid — ошибка
        record = make_record()
        storage.save_weighing(record)
        with pytest.raises(sqlite3.IntegrityError):
            storage.save_weighing(record)
        # Исходная запись не пострадала
        assert storage.get_weighing(record.uuid) == record


class TestPendingQueue:
    """Очередь досылки: pending_records / pending_count / mark_synced."""

    def test_pending_only_unsynced_old_first(self, storage: AgentStorage) -> None:
        first, second, third = make_record(), make_record(), make_record()
        save_spaced(storage, first, second, third)
        # Помечаем среднюю запись — в очереди остаются две, старые первыми
        assert storage.mark_synced([second.uuid]) == 1
        pending = storage.pending_records()
        assert [r.uuid for r in pending] == [first.uuid, third.uuid]
        assert storage.pending_count() == 2

    def test_pending_limit(self, storage: AgentStorage) -> None:
        records = [make_record() for _ in range(5)]
        save_spaced(storage, *records)
        limited = storage.pending_records(limit=3)
        # limit срезает хвост, порядок — от старых к новым
        assert [r.uuid for r in limited] == [r.uuid for r in records[:3]]

    def test_pending_count_matches_records(self, storage: AgentStorage) -> None:
        records = [make_record() for _ in range(4)]
        save_spaced(storage, *records)
        assert storage.pending_count() == len(storage.pending_records())

    def test_mark_synced_returns_actual_count(self, storage: AgentStorage) -> None:
        first, second = make_record(), make_record()
        save_spaced(storage, first, second)
        assert storage.mark_synced([first.uuid, second.uuid]) == 2

    def test_mark_synced_repeated_returns_zero(self, storage: AgentStorage) -> None:
        record = make_record()
        storage.save_weighing(record)
        assert storage.mark_synced([record.uuid]) == 1
        # Повторная пометка уже досланной записи не считается
        assert storage.mark_synced([record.uuid]) == 0

    def test_mark_synced_unknown_uuid_returns_zero(self, storage: AgentStorage) -> None:
        assert storage.mark_synced([uuid4()]) == 0

    def test_mark_synced_mixed_counts_only_new(self, storage: AgentStorage) -> None:
        first, second = make_record(), make_record()
        save_spaced(storage, first, second)
        storage.mark_synced([first.uuid])
        # Из трёх uuid реально помечается только вторая запись
        assert storage.mark_synced([first.uuid, second.uuid, uuid4()]) == 1

    def test_synced_record_leaves_pending_but_stays_readable(self, storage: AgentStorage) -> None:
        record = make_record()
        storage.save_weighing(record)
        storage.mark_synced([record.uuid])
        assert storage.pending_records() == []
        assert storage.pending_count() == 0
        # Досланная запись остаётся в журнале навсегда (правило №2)
        assert storage.get_weighing(record.uuid) == record
        assert record.uuid in {r.uuid for r in storage.recent_weighings()}

    def test_mark_synced_empty_list_no_db_access(self, storage: AgentStorage) -> None:
        # После close() любое обращение к БД упало бы — значит, пустой список
        # обрабатывается без обращения к соединению
        storage.close()
        assert storage.mark_synced([]) == 0


class TestRecentWeighings:
    """Журнал локального интерфейса: новые записи первыми."""

    def test_newest_first(self, storage: AgentStorage) -> None:
        first, second, third = make_record(), make_record(), make_record()
        save_spaced(storage, first, second, third)
        recent = storage.recent_weighings()
        assert [r.uuid for r in recent] == [third.uuid, second.uuid, first.uuid]

    def test_limit(self, storage: AgentStorage) -> None:
        records = [make_record() for _ in range(5)]
        save_spaced(storage, *records)
        recent = storage.recent_weighings(limit=2)
        assert [r.uuid for r in recent] == [records[4].uuid, records[3].uuid]


class TestPhotos:
    """Снимки операции: привязка к записи, роли, однократность роли."""

    def test_photos_bound_to_record(self, storage: AgentStorage) -> None:
        record = make_record()
        front = StoredPhoto(
            role=CameraRole.FRONT, path="photos/f.jpg", sha256="a" * 64, size_bytes=123456
        )
        rear = StoredPhoto(
            role=CameraRole.REAR, path="photos/r.jpg", sha256="b" * 64, size_bytes=654321
        )
        storage.save_weighing(record, photos=[front, rear])
        # photos_for сортирует по роли: front < rear
        assert storage.photos_for(record.uuid) == [front, rear]

    def test_other_record_does_not_see_photos(self, storage: AgentStorage) -> None:
        with_photos = make_record()
        without_photos = make_record()
        photo = StoredPhoto(
            role=CameraRole.FRONT, path="photos/f.jpg", sha256="c" * 64, size_bytes=1000
        )
        storage.save_weighing(with_photos, photos=[photo])
        storage.save_weighing(without_photos)
        assert storage.photos_for(without_photos.uuid) == []
        assert storage.photos_for(with_photos.uuid) == [photo]

    def test_no_photos_gives_empty_list(self, storage: AgentStorage) -> None:
        record = make_record()
        storage.save_weighing(record)
        assert storage.photos_for(record.uuid) == []

    def test_duplicate_role_rejected(self, storage: AgentStorage) -> None:
        # PRIMARY KEY (weighing_uuid, role): две фотографии одной роли невозможны
        record = make_record()
        photos = [
            StoredPhoto(role=CameraRole.FRONT, path="photos/1.jpg", sha256="d" * 64, size_bytes=1),
            StoredPhoto(role=CameraRole.FRONT, path="photos/2.jpg", sha256="e" * 64, size_bytes=2),
        ]
        with pytest.raises(sqlite3.IntegrityError):
            storage.save_weighing(record, photos=photos)
        # Транзакция атомарна: при ошибке фото запись взвешивания тоже не сохраняется
        assert storage.get_weighing(record.uuid) is None

    def test_photo_requires_existing_weighing(self, storage: AgentStorage) -> None:
        # Внешний ключ включён: фото без записи взвешивания не вставляется
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "INSERT INTO weighing_photos_local"
                " (weighing_uuid, role, path, sha256, size_bytes)"
                " VALUES (?, 'front', 'x.jpg', 'f', 1)",
                (str(uuid4()),),
            )


class TestImmutability:
    """Правило №2: триггеры БД блокируют изменение и удаление даже прямым SQL."""

    @pytest.fixture
    def saved(self, storage: AgentStorage) -> WeighingRecord:
        record = make_record(netto=1000.0, tare_value=500.0)
        photo = StoredPhoto(
            role=CameraRole.FRONT, path="photos/f.jpg", sha256="0" * 64, size_bytes=10
        )
        storage.save_weighing(record, photos=[photo])
        return record

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE weighings_local SET massa = 99999 WHERE uuid = ?",
            "UPDATE weighings_local SET code = 'ERR_INTERNAL' WHERE uuid = ?",
            "UPDATE weighings_local SET netto = 0 WHERE uuid = ?",
            "UPDATE weighings_local SET vehicle_number = '02KG000XXX' WHERE uuid = ?",
            "UPDATE weighings_local SET created_at = '1970-01-01T00:00:00+00:00' WHERE uuid = ?",
        ],
        ids=["massa", "code", "netto", "vehicle_number", "created_at"],
    )
    def test_update_content_field_blocked(
        self, storage: AgentStorage, saved: WeighingRecord, statement: str
    ) -> None:
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(statement, (str(saved.uuid),))
        # Запись не изменилась
        assert storage.get_weighing(saved.uuid) == saved

    def test_delete_blocked(self, storage: AgentStorage, saved: WeighingRecord) -> None:
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute("DELETE FROM weighings_local WHERE uuid = ?", (str(saved.uuid),))
        assert storage.get_weighing(saved.uuid) == saved

    def test_synced_forward_allowed_backward_blocked(
        self, storage: AgentStorage, saved: WeighingRecord
    ) -> None:
        # Переход 0 → 1 прямым SQL разрешён (это и делает mark_synced)
        with storage._conn:
            storage._conn.execute(
                "UPDATE weighings_local SET synced = 1 WHERE uuid = ?", (str(saved.uuid),)
            )
        assert storage.pending_count() == 0
        # Обратный переход 1 → 0 запрещён: дослали — значит дослали
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighings_local SET synced = 0 WHERE uuid = ?", (str(saved.uuid),)
            )

    def test_photo_update_blocked(self, storage: AgentStorage, saved: WeighingRecord) -> None:
        # Фото после сохранения не подменяется: sha256 связан с записью
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighing_photos_local SET sha256 = 'x' WHERE weighing_uuid = ?",
                (str(saved.uuid),),
            )

    def test_photo_delete_blocked(self, storage: AgentStorage, saved: WeighingRecord) -> None:
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "DELETE FROM weighing_photos_local WHERE weighing_uuid = ?",
                (str(saved.uuid),),
            )
        assert len(storage.photos_for(saved.uuid)) == 1


class TestTareRegistry:
    """Реплика реестра тарирований и правило «не старше 3 месяцев» (№4)."""

    NOW = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)

    def test_replace_swaps_snapshot_completely(self, storage: AgentStorage) -> None:
        old_snapshot = [
            make_tare("01KG111AAA", self.NOW - timedelta(days=10)),
            make_tare("01KG222BBB", self.NOW - timedelta(days=20)),
        ]
        assert storage.replace_tare_registry(old_snapshot) == 2
        new_snapshot = [make_tare("01KG333CCC", self.NOW - timedelta(days=5))]
        assert storage.replace_tare_registry(new_snapshot) == 1
        # Старые записи исчезли, осталась только новая реплика
        assert storage.tare_registry_size() == 1
        assert storage.find_active_tare("01KG111AAA", self.NOW) is None
        assert storage.find_active_tare("01KG222BBB", self.NOW) is None
        found = storage.find_active_tare("01KG333CCC", self.NOW)
        assert found == new_snapshot[0]

    def test_replace_with_empty_clears_registry(self, storage: AgentStorage) -> None:
        storage.replace_tare_registry([make_tare("01KG111AAA", self.NOW - timedelta(days=1))])
        assert storage.replace_tare_registry([]) == 0
        assert storage.tare_registry_size() == 0
        assert storage.find_active_tare("01KG111AAA", self.NOW) is None

    def test_recent_tare_found(self, storage: AgentStorage) -> None:
        # Тара месячной давности действует
        tare = make_tare("01KG777DDD", self.NOW - timedelta(days=30))
        storage.replace_tare_registry([tare])
        found = storage.find_active_tare("01KG777DDD", self.NOW)
        assert found == tare

    def test_expired_tare_gives_none(self, storage: AgentStorage) -> None:
        # Тарирование старше 3 календарных месяцев: нетто считать нельзя
        expired = make_tare("01KG777DDD", datetime(2026, 5, 7, 11, 59, 59, tzinfo=UTC))
        storage.replace_tare_registry([expired])
        assert storage.find_active_tare("01KG777DDD", self.NOW) is None

    def test_unknown_vehicle_gives_none(self, storage: AgentStorage) -> None:
        storage.replace_tare_registry([make_tare("01KG111AAA", self.NOW - timedelta(days=1))])
        assert storage.find_active_tare("09KG999ZZZ", self.NOW) is None

    def test_boundary_exactly_three_months_still_active(self, storage: AgentStorage) -> None:
        # Граница нестрогая: tared_at == three_months_before(at) → тара ещё действует
        boundary = three_months_before(self.NOW)
        tare = make_tare("01KG555EEE", boundary)
        storage.replace_tare_registry([tare])
        assert storage.find_active_tare("01KG555EEE", self.NOW) == tare
        # А на микросекунду раньше границы — уже нет
        stale = make_tare("01KG555EEE", boundary - timedelta(microseconds=1))
        storage.replace_tare_registry([stale])
        assert storage.find_active_tare("01KG555EEE", self.NOW) is None

    def test_utc_round_trip(self, storage: AgentStorage) -> None:
        # tared_at хранится в UTC и восстанавливается aware-датой без искажений
        tared_at = datetime(2026, 6, 15, 23, 59, 59, 999999, tzinfo=UTC)
        tare = make_tare("01KG888FFF", tared_at)
        storage.replace_tare_registry([tare])
        found = storage.find_active_tare("01KG888FFF", self.NOW)
        assert found is not None
        assert found.tared_at == tared_at
        assert found.tared_at.tzinfo is not None


class TestThreeMonthsBefore:
    """Календарная арифметика: 3 месяца назад с поджатием дня месяца."""

    def test_plain_date(self) -> None:
        assert three_months_before(datetime(2026, 8, 7, tzinfo=UTC)) == datetime(
            2026, 5, 7, tzinfo=UTC
        )

    @pytest.mark.parametrize(
        ("moment", "expected"),
        [
            # Переход через границу года: январь/февраль/март → прошлый год
            (datetime(2026, 1, 15, tzinfo=UTC), datetime(2025, 10, 15, tzinfo=UTC)),
            (datetime(2026, 2, 28, tzinfo=UTC), datetime(2025, 11, 28, tzinfo=UTC)),
            (datetime(2026, 3, 31, tzinfo=UTC), datetime(2025, 12, 31, tzinfo=UTC)),
        ],
        ids=["january", "february", "march"],
    )
    def test_year_rollover(self, moment: datetime, expected: datetime) -> None:
        assert three_months_before(moment) == expected

    @pytest.mark.parametrize(
        ("moment", "expected"),
        [
            # 31 мая → 28 февраля (невисокосный год)
            (datetime(2026, 5, 31, tzinfo=UTC), datetime(2026, 2, 28, tzinfo=UTC)),
            # 31 мая 2024 → 29 февраля (високосный год)
            (datetime(2024, 5, 31, tzinfo=UTC), datetime(2024, 2, 29, tzinfo=UTC)),
            # 31 декабря → 30 сентября (в сентябре 30 дней)
            (datetime(2026, 12, 31, tzinfo=UTC), datetime(2026, 9, 30, tzinfo=UTC)),
            # 31 июля → 30 апреля
            (datetime(2026, 7, 31, tzinfo=UTC), datetime(2026, 4, 30, tzinfo=UTC)),
        ],
        ids=["may31", "may31-leap", "dec31", "jul31"],
    )
    def test_day_clamped_to_month_end(self, moment: datetime, expected: datetime) -> None:
        assert three_months_before(moment) == expected

    def test_preserves_tzinfo_and_time_of_day(self) -> None:
        # Время суток и часовой пояс (Бишкек, UTC+6) не теряются
        bishkek = timezone(timedelta(hours=6))
        moment = datetime(2026, 5, 31, 13, 45, 59, 123456, tzinfo=bishkek)
        result = three_months_before(moment)
        assert result == datetime(2026, 2, 28, 13, 45, 59, 123456, tzinfo=bishkek)
        assert result.tzinfo is bishkek

    def test_naive_datetime_stays_naive(self) -> None:
        result = three_months_before(datetime(2026, 8, 7, 12, 0))
        assert result == datetime(2026, 5, 7, 12, 0)
        assert result.tzinfo is None


class TestPersistence:
    """Буфер переживает перезапуск агента: файл БД хранит всё."""

    def test_data_survives_reopen(self, tmp_path: Path) -> None:
        db_path = tmp_path / "agent.db"
        synced_rec = make_record()
        pending_rec = make_record()
        photo = StoredPhoto(
            role=CameraRole.REAR, path="photos/r.jpg", sha256="9" * 64, size_bytes=777
        )
        tare = make_tare("01KG444GGG", datetime(2026, 7, 1, tzinfo=UTC))

        first = AgentStorage(db_path)
        try:
            save_spaced(first, synced_rec, pending_rec)
            photo_owner_uuid = uuid4()
            first.save_weighing(make_record(uuid=photo_owner_uuid), photos=[photo])
            first.mark_synced([synced_rec.uuid])
            first.replace_tare_registry([tare])
        finally:
            first.close()

        # Открываем файл заново: журнал, очередь, фото и реестр на месте
        second = AgentStorage(db_path)
        try:
            assert second.get_weighing(synced_rec.uuid) == synced_rec
            assert second.get_weighing(pending_rec.uuid) == pending_rec
            pending_uuids = {r.uuid for r in second.pending_records()}
            assert synced_rec.uuid not in pending_uuids  # пометка synced сохранилась
            assert pending_rec.uuid in pending_uuids
            assert second.photos_for(photo_owner_uuid) == [photo]
            assert second.tare_registry_size() == 1
            found = second.find_active_tare("01KG444GGG", datetime(2026, 8, 7, tzinfo=UTC))
            assert found == tare
        finally:
            second.close()


class TestThreadSafety:
    """Параллельная запись из нескольких потоков в одно хранилище."""

    THREADS = 8
    RECORDS_PER_THREAD = 25

    def test_concurrent_writes_lose_nothing(self, tmp_path: Path) -> None:
        # Модель нагрузки агента: поток цикла взвешивания и поток синхронизации
        # пишут одновременно; здесь — 8 потоков по 25 записей
        storage = AgentStorage(tmp_path / "agent.db")
        errors: list[Exception] = []
        written: list[UUID] = []
        lock = threading.Lock()

        def writer() -> None:
            try:
                for _ in range(self.RECORDS_PER_THREAD):
                    record = make_record()
                    storage.save_weighing(record)
                    with lock:
                        written.append(record.uuid)
            except Exception as exc:
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=writer) for _ in range(self.THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        try:
            total = self.THREADS * self.RECORDS_PER_THREAD
            assert errors == []
            assert len(written) == total
            assert storage.pending_count() == total
            stored = {r.uuid for r in storage.pending_records(limit=total + 1)}
            assert stored == set(written)
        finally:
            storage.close()


def test_find_active_tare_with_naive_tared_at_treated_as_utc(tmp_path: Path) -> None:
    """Naive-дата в реплике (без пояса) трактуется как UTC, а не роняет поиск."""
    storage = AgentStorage(":memory:")
    naive = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=10)
    storage.replace_tare_registry(
        [
            TareRecord(
                vehicle_number="01KG777XYZ",
                tare_value=7500.0,
                tared_at=naive,  # pydantic пропускает naive — защищаемся при чтении
                weighing_uuid=uuid4(),
            )
        ]
    )
    tare = storage.find_active_tare("01KG777XYZ", datetime.now(UTC))
    assert tare is not None
    assert tare.tare_value == 7500.0
    storage.close()


def test_synced_beyond_one_is_rejected() -> None:
    """synced строго 0 или 1: значение 2 или текст отвергаются CHECK/триггером."""
    storage = AgentStorage(":memory:")
    record = make_record()
    storage.save_weighing(record)
    for bad_value in (2, "abc"):
        # прямой SQL мимо API — проверяем защиту самой БД
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighings_local SET synced = ? WHERE uuid = ?",
                (bad_value, str(record.uuid)),
            )
    storage.close()


def test_find_active_tare_with_naive_at_treated_as_utc() -> None:
    """Naive-аргумент at тоже трактуется как UTC, а не роняет метод."""
    storage = AgentStorage(":memory:")
    storage.replace_tare_registry(
        [
            TareRecord(
                vehicle_number="01KG888ZZZ",
                tare_value=6900.0,
                tared_at=datetime.now(UTC) - timedelta(days=5),
                weighing_uuid=uuid4(),
            )
        ]
    )
    naive_now = datetime.now(UTC).replace(tzinfo=None)
    tare = storage.find_active_tare("01KG888ZZZ", naive_now)
    assert tare is not None and tare.tare_value == 6900.0
    storage.close()
