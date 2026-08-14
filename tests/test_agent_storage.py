"""Тесты локальной БД агента (agent/sync/storage.py).

Покрытие:
- журнал операций: round-trip всех полей, однократность вставки, очередь
  досылки (pending/mark_synced), журнал интерфейса, фото операций;
- неизменяемость (правило проекта №2): триггеры БД блокируют UPDATE/DELETE
  даже при прямом SQL-доступе; разрешён только переход synced 0 → 1;
- реплика реестра тарирований и правило «тара не старше 3 месяцев» (№4);
- календарная арифметика three_months_before с поджатием дня месяца;
- персистентность файла БД и потокобезопасность при параллельной записи;
- реплика операторов центра (operators_registry): миграция схемы local_users,
  полный снимок replace_center_operators, сохранение локальных учёток,
  блокировка is_active=0, CLI upsert_operator поверх реплики;
- служебные поля снимков (attempts/next_attempt_at/uploaded_at/file_removed):
  триггер пропускает их изменение, но по-прежнему держит доказательные поля,
  а метки uploaded и file_removed не откатываются;
- миграция БД агента, поставленного до 11.08.2026 (боевой Кызыл-Кыя):
  таблица фото без новых колонок и со СТАРЫМ триггером дополняется, очередь
  с повторами и ретеншн работают, доказательства не теряются;
- снимок настроек центра (scale_config): save/load_center_settings —
  нет снимка → None, перезапись последним, персистентность файла БД;
- смещение часов до центра (save/load_clock_offset_s): round-trip,
  перезапись, отсутствие → None, мусор в БД → None, персистентность.
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

from agent.sync.storage import AgentStorage, StoredPhoto
from shared import (
    CameraRole,
    ErrorCode,
    Operation,
    TareRecord,
    WeighingRecord,
    WeighingSource,
)
from shared.messages import OperatorRecord
from shared.passwords import hash_password
from shared.tare import three_months_before


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


def make_tare(
    vehicle_number: str,
    tared_at: datetime,
    tare_value: float = 7500.0,
    trailer_number: str | None = None,
) -> TareRecord:
    """Строка реестра тарирований (trailer_number=None — тарирование без прицепа)."""
    return TareRecord(
        vehicle_number=vehicle_number,
        trailer_number=trailer_number,
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

    def test_pending_photos_count(self, storage: AgentStorage) -> None:
        """Метрика heartbeat (0.4.13): незагруженные снимки, включая фото
        несинхронизированных записей — «сколько снимков ещё не в центре»."""
        first, second = make_record(), make_record()
        storage.save_weighing(
            first,
            photos=[
                StoredPhoto(
                    role=CameraRole.FRONT, path="photos/f.jpg", sha256="a" * 64, size_bytes=1
                ),
                StoredPhoto(
                    role=CameraRole.REAR, path="photos/r.jpg", sha256="b" * 64, size_bytes=1
                ),
            ],
        )
        storage.save_weighing(
            second,
            photos=[
                StoredPhoto(
                    role=CameraRole.FRONT, path="photos/g.jpg", sha256="c" * 64, size_bytes=1
                ),
            ],
        )
        # запись second НЕ синхронизирована, но её снимок тоже в счётчике
        storage.mark_synced([first.uuid])
        assert storage.pending_photos_count() == 3
        storage.mark_photo_uploaded(first.uuid, CameraRole.FRONT)
        assert storage.pending_photos_count() == 2


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


class TestPhotoServiceFields:
    """Служебные поля очереди и ретеншна (11.08.2026): двигаться можно, но
    только «вперёд», а доказательные поля остаются неприкосновенными."""

    @pytest.fixture
    def photo_key(self, storage: AgentStorage) -> tuple[str, str]:
        record = make_record()
        storage.save_weighing(
            record,
            photos=[
                StoredPhoto(
                    role=CameraRole.FRONT, path="photos/f.jpg", sha256="0" * 64, size_bytes=10
                )
            ],
        )
        storage.mark_synced([record.uuid])
        return str(record.uuid), CameraRole.FRONT.value

    def test_queue_fields_are_updatable(
        self, storage: AgentStorage, photo_key: tuple[str, str]
    ) -> None:
        # На этом держится очередь с повторами: attempts и пауза меняются
        with storage._conn:
            storage._conn.execute(
                "UPDATE weighing_photos_local SET attempts = 3, next_attempt_at = ?"
                " WHERE weighing_uuid = ? AND role = ?",
                ("2026-08-11T12:00:00+00:00", *photo_key),
            )
        row = storage._conn.execute(
            "SELECT attempts, next_attempt_at FROM weighing_photos_local"
            " WHERE weighing_uuid = ? AND role = ?",
            photo_key,
        ).fetchone()
        assert (row["attempts"], row["next_attempt_at"]) == (3, "2026-08-11T12:00:00+00:00")

    @pytest.mark.parametrize(
        "statement",
        [
            "UPDATE weighing_photos_local SET path = 'other.jpg'"
            " WHERE weighing_uuid = ? AND role = ?",
            "UPDATE weighing_photos_local SET sha256 = 'x' WHERE weighing_uuid = ? AND role = ?",
            "UPDATE weighing_photos_local SET size_bytes = 1 WHERE weighing_uuid = ? AND role = ?",
        ],
        ids=["path", "sha256", "size_bytes"],
    )
    def test_evidence_fields_still_locked(
        self, storage: AgentStorage, photo_key: tuple[str, str], statement: str
    ) -> None:
        # Правило №2: путь, хеш и размер снимка неизменны и после 11.08.2026
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(statement, photo_key)

    def test_file_removed_requires_uploaded(
        self, storage: AgentStorage, photo_key: tuple[str, str]
    ) -> None:
        # Незагруженный снимок нельзя объявить убранным: он единственный
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighing_photos_local SET file_removed = 1"
                " WHERE weighing_uuid = ? AND role = ?",
                photo_key,
            )

    def test_file_removed_forward_only(
        self, storage: AgentStorage, photo_key: tuple[str, str]
    ) -> None:
        uuid, role = photo_key
        storage.mark_photo_uploaded(UUID(uuid), CameraRole(role))
        storage.mark_photo_file_removed(UUID(uuid), CameraRole(role))
        # откат 1 → 0 запрещён: удалённый файл не воскресает
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighing_photos_local SET file_removed = 0"
                " WHERE weighing_uuid = ? AND role = ?",
                photo_key,
            )
        # и значение вне {0, 1} отбивает CHECK
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "UPDATE weighing_photos_local SET file_removed = 2"
                " WHERE weighing_uuid = ? AND role = ?",
                photo_key,
            )

    def test_row_survives_file_removal(
        self, storage: AgentStorage, photo_key: tuple[str, str]
    ) -> None:
        """Ретеншн убирает файл, но не строку: метаданные — часть записи."""
        uuid, role = photo_key
        storage.mark_photo_uploaded(UUID(uuid), CameraRole(role))
        storage.mark_photo_file_removed(UUID(uuid), CameraRole(role))
        with pytest.raises(sqlite3.IntegrityError), storage._conn:
            storage._conn.execute(
                "DELETE FROM weighing_photos_local WHERE weighing_uuid = ? AND role = ?",
                photo_key,
            )
        photos = storage.photos_for(UUID(uuid))
        assert [(p.path, p.sha256, p.size_bytes) for p in photos] == [
            ("photos/f.jpg", "0" * 64, 10)
        ]


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

    def test_pair_tare_only_for_matching_pair(self, storage: AgentStorage) -> None:
        # Решение 09.08.2026: тара привязана к СЦЕПКЕ — нужны оба номера;
        # соло-тарирование действует только для машины без прицепа
        pair = make_tare("01KG111AAA", self.NOW - timedelta(days=10), trailer_number="BD123AB")
        solo = make_tare("01KG222BBB", self.NOW - timedelta(days=10), tare_value=6800.0)
        storage.replace_tare_registry([pair, solo])
        # совпавшая пара — тара найдена вместе с номером прицепа
        assert storage.find_active_tare("01KG111AAA", self.NOW, trailer_number="BD123AB") == pair
        # чужой прицеп и запрос без прицепа — действующей тары нет
        assert storage.find_active_tare("01KG111AAA", self.NOW, trailer_number="XX999YY") is None
        assert storage.find_active_tare("01KG111AAA", self.NOW) is None
        # соло-тара: без прицепа находится, с прицепом — нет
        assert storage.find_active_tare("01KG222BBB", self.NOW) == solo
        assert storage.find_active_tare("01KG222BBB", self.NOW, trailer_number="BD123AB") is None

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


def test_old_replica_schema_recreated_with_trailer_column(tmp_path: Path) -> None:
    """Реплика СТАРОЙ схемы (тара по одной голове, до 09.08.2026) пересоздаётся
    при старте: данные расходные, центр пришлёт снимок заново."""
    db_path = tmp_path / "agent.db"
    conn = sqlite3.connect(db_path)
    with conn:
        # старая схема: ключ — только голова, колонки trailer_number нет
        conn.execute(
            "CREATE TABLE tare_registry_replica ("
            " vehicle_number TEXT PRIMARY KEY, tare_value REAL,"
            " tared_at TEXT, weighing_uuid TEXT)"
        )
        conn.execute(
            "INSERT INTO tare_registry_replica VALUES (?, ?, ?, ?)",
            ("01KG111AAA", 7500.0, datetime(2026, 8, 1, tzinfo=UTC).isoformat(), str(uuid4())),
        )
    conn.close()

    storage = AgentStorage(db_path)
    try:
        # таблица пересоздана по новой схеме: колонка trailer_number есть,
        # старых строк нет (тара по одной голове больше не действует)
        columns = {
            row["name"] for row in storage._conn.execute("PRAGMA table_info(tare_registry_replica)")
        }
        assert "trailer_number" in columns
        assert storage.tare_registry_size() == 0
        assert storage.find_active_tare("01KG111AAA", datetime(2026, 8, 7, tzinfo=UTC)) is None
        # реплика работоспособна: снимок центра встаёт и ищется по паре
        tare = make_tare("01KG222BBB", datetime(2026, 8, 1, tzinfo=UTC), trailer_number="BD123AB")
        assert storage.replace_tare_registry([tare]) == 1
        found = storage.find_active_tare(
            "01KG222BBB", datetime(2026, 8, 7, tzinfo=UTC), trailer_number="BD123AB"
        )
        assert found == tare
    finally:
        storage.close()


# Схема фото ДО 11.08.2026 — дословно из git show HEAD:agent/sync/storage.py.
# Именно такая база стоит на боевом весовом ПК Кызыл-Кыи: колонок очереди
# и ретеншна нет, а триггер запрещает ЛЮБОЕ изменение строки снимка, кроме
# uploaded 0 → 1. Если миграция не обновит и колонки, и текст триггера,
# mark_photo_failed/mark_photo_uploaded упрутся в «фото не редактируются».
_OLD_PHOTO_SCHEMA = """
CREATE TABLE weighings_local (
    uuid TEXT PRIMARY KEY,
    operation TEXT NOT NULL,
    code TEXT NOT NULL,
    massa REAL,
    unit TEXT NOT NULL,
    stable INTEGER NOT NULL,
    weighed_at TEXT,
    vehicle_number TEXT,
    trailer_number TEXT,
    tare_value REAL,
    tare_weighing_uuid TEXT,
    netto REAL,
    source TEXT NOT NULL,
    operator TEXT,
    message TEXT,
    synced INTEGER NOT NULL DEFAULT 0 CHECK (synced IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE TABLE weighing_photos_local (
    weighing_uuid TEXT NOT NULL REFERENCES weighings_local (uuid),
    role TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    uploaded INTEGER NOT NULL DEFAULT 0 CHECK (uploaded IN (0, 1)),
    PRIMARY KEY (weighing_uuid, role)
);

CREATE INDEX idx_photos_local_upload ON weighing_photos_local (uploaded);

CREATE TRIGGER weighing_photos_local_no_delete
    BEFORE DELETE ON weighing_photos_local
BEGIN
    SELECT RAISE(ABORT, 'фото не удаляются (правило неизменяемости)');
END;

CREATE TRIGGER weighing_photos_local_no_update
    BEFORE UPDATE ON weighing_photos_local
    WHEN NOT (
        NEW.weighing_uuid = OLD.weighing_uuid AND NEW.role = OLD.role
        AND NEW.path = OLD.path AND NEW.sha256 = OLD.sha256
        AND NEW.size_bytes = OLD.size_bytes
        AND (NEW.uploaded = OLD.uploaded OR (OLD.uploaded = 0 AND NEW.uploaded = 1))
    )
BEGIN
    SELECT RAISE(ABORT, 'фото не редактируются (правило неизменяемости)');
END;
"""


class TestPhotoSchemaMigration:
    """БД агента, поставленного до 11.08.2026: очередь с повторами и ретеншн
    должны заработать после первого же запуска новой версии."""

    OLD_CREATED_AT = datetime(2026, 6, 1, 10, 0, tzinfo=UTC)  # запись 60+ дней назад
    PENDING_PATH = "C:/vesy-agent/photos/2026/06/01/pending_photo1.jpeg"
    UPLOADED_PATH = "C:/vesy-agent/photos/2026/06/01/uploaded_photo2.jpeg"

    def _create_old_db(self, db_path: Path) -> UUID:
        """Файл БД старой схемы: досланная запись, снимок в очереди (front)
        и снимок, уже принятый центром (rear, без времени подтверждения)."""
        record_uuid = uuid4()
        conn = sqlite3.connect(db_path)
        with conn:
            conn.executescript(_OLD_PHOTO_SCHEMA)
            conn.execute(
                "INSERT INTO weighings_local (uuid, operation, code, massa, unit, stable,"
                " weighed_at, source, synced, created_at)"
                " VALUES (?, 'weighing', 'OK', 15000.0, 'kg', 1, ?, 'ais', 1, ?)",
                (
                    str(record_uuid),
                    self.OLD_CREATED_AT.isoformat(),
                    self.OLD_CREATED_AT.isoformat(),
                ),
            )
            conn.executemany(
                "INSERT INTO weighing_photos_local"
                " (weighing_uuid, role, path, sha256, size_bytes, uploaded)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (str(record_uuid), "front", self.PENDING_PATH, "a" * 64, 111, 0),
                    (str(record_uuid), "rear", self.UPLOADED_PATH, "b" * 64, 222, 1),
                ],
            )
        conn.close()
        return record_uuid

    def test_columns_added_without_touching_evidence(self, tmp_path: Path) -> None:
        """Колонки очереди/ретеншна появляются с безопасными значениями,
        а пути, хеши и размеры снимков остаются как были."""
        db_path = tmp_path / "agent.sqlite3"
        record_uuid = self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            columns = {
                row["name"]
                for row in storage._conn.execute("PRAGMA table_info(weighing_photos_local)")
            }
            assert {"attempts", "next_attempt_at", "uploaded_at", "file_removed"} <= columns
            rows = storage._conn.execute(
                "SELECT * FROM weighing_photos_local ORDER BY role"
            ).fetchall()
            assert [(r["attempts"], r["next_attempt_at"], r["file_removed"]) for r in rows] == [
                (0, None, 0),
                (0, None, 0),
            ]
            assert all(r["uploaded_at"] is None for r in rows)
            assert [(r["path"], r["sha256"], r["size_bytes"]) for r in rows] == [
                (self.PENDING_PATH, "a" * 64, 111),
                (self.UPLOADED_PATH, "b" * 64, 222),
            ]
            assert len(storage.photos_for(record_uuid)) == 2
        finally:
            storage.close()

    def test_retry_queue_works_on_migrated_db(self, tmp_path: Path) -> None:
        """Главное: mark_photo_failed/mark_photo_uploaded не упираются в
        старый триггер «фото не редактируются» — иначе на боевом объекте
        загрузка фото легла бы с ошибкой при первой же неудаче."""
        db_path = tmp_path / "agent.sqlite3"
        record_uuid = self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            queued = storage.photos_to_upload()
            assert [(u, p.path) for u, p in queued] == [(record_uuid, self.PENDING_PATH)]

            now = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)
            attempts = storage.mark_photo_failed(
                record_uuid, CameraRole.FRONT, base_delay_s=15.0, max_delay_s=1800.0, now=now
            )
            assert attempts == 1
            assert storage.photos_to_upload(now=now) == []
            assert len(storage.photos_to_upload(now=now + timedelta(seconds=15))) == 1
            assert storage.photo_queue_stats() == (1, 0)

            storage.mark_photo_uploaded(record_uuid, CameraRole.FRONT, now=now)
            row = storage._conn.execute(
                "SELECT * FROM weighing_photos_local WHERE role = 'front'"
            ).fetchone()
            assert (row["uploaded"], row["uploaded_at"], row["next_attempt_at"]) == (
                1,
                now.isoformat(),
                None,
            )
        finally:
            storage.close()

    def test_migrated_trigger_still_guards_evidence(self, tmp_path: Path) -> None:
        """Триггер пересоздан, но правило №2 в силе: доказательные поля
        неизменны, строка не удаляется, uploaded не откатывается."""
        db_path = tmp_path / "agent.sqlite3"
        self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            for statement in (
                "UPDATE weighing_photos_local SET sha256 = 'x' WHERE role = 'front'",
                "UPDATE weighing_photos_local SET path = 'D:/evil.jpeg' WHERE role = 'front'",
                "UPDATE weighing_photos_local SET size_bytes = 0 WHERE role = 'front'",
                "UPDATE weighing_photos_local SET uploaded = 0 WHERE role = 'rear'",
                "UPDATE weighing_photos_local SET file_removed = 1 WHERE role = 'front'",
                "DELETE FROM weighing_photos_local WHERE role = 'front'",
            ):
                with pytest.raises(sqlite3.IntegrityError), storage._conn:
                    storage._conn.execute(statement)
        finally:
            storage.close()

    def test_retention_uses_record_date_for_old_uploads(self, tmp_path: Path) -> None:
        """У снимка, принятого центром до появления uploaded_at, срок
        ретеншна считается от даты записи: старые файлы всё-таки убираются,
        а неотправленный снимок не трогается никогда."""
        db_path = tmp_path / "agent.sqlite3"
        record_uuid = self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            threshold = self.OLD_CREATED_AT + timedelta(days=30)
            assert storage.photos_to_purge(threshold) == [
                (record_uuid, CameraRole.REAR, self.UPLOADED_PATH)
            ]
            # запись ещё не «состарилась» — уборки нет
            assert storage.photos_to_purge(self.OLD_CREATED_AT - timedelta(days=1)) == []

            storage.mark_photo_file_removed(record_uuid, CameraRole.REAR)
            assert storage.photos_to_purge(threshold) == []
            # строка со всеми доказательствами на месте
            assert len(storage.photos_for(record_uuid)) == 2
        finally:
            storage.close()

    def test_reopen_after_migration_is_noop(self, tmp_path: Path) -> None:
        """Повторный старт службы не ломает мигрированную БД (нет duplicate
        column) и не теряет накопленные попытки."""
        db_path = tmp_path / "agent.sqlite3"
        record_uuid = self._create_old_db(db_path)
        first = AgentStorage(db_path)
        first.mark_photo_failed(
            record_uuid, CameraRole.FRONT, base_delay_s=15.0, max_delay_s=1800.0
        )
        first.close()

        second = AgentStorage(db_path)
        try:
            row = second._conn.execute(
                "SELECT attempts FROM weighing_photos_local WHERE role = 'front'"
            ).fetchone()
            assert row["attempts"] == 1
            assert (
                second.mark_photo_failed(
                    record_uuid, CameraRole.FRONT, base_delay_s=15.0, max_delay_s=1800.0
                )
                == 2
            )
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


# ---------------------------------------------------------------------------
# Реплика операторов центра (operators_registry) и миграция local_users
# ---------------------------------------------------------------------------

# PBKDF2 дорогой (200k итераций) — считаем хеши один раз на модуль
CENTER_PASSWORD = "center-pass-111"
CENTER_PW_HASH = hash_password(CENTER_PASSWORD)
CLI_PASSWORD = "cli-pass-222"


def make_operator(
    login: str,
    *,
    pw_hash: str = CENTER_PW_HASH,
    full_name: str = "",
    is_active: bool = True,
) -> OperatorRecord:
    """Учётка оператора из снимка центра (пароль по умолчанию общий,
    чтобы не пересчитывать PBKDF2 в каждом тесте)."""
    return OperatorRecord(login=login, pw_hash=pw_hash, full_name=full_name, is_active=is_active)


class TestLocalUsersMigration:
    """Старая БД без колонок is_active/from_center мигрирует при открытии,
    учётки НЕ теряются (в отличие от расходной реплики тар)."""

    OLD_LOGIN = "old.operator"

    def _create_old_db(self, db_path: Path) -> None:
        """Файл со СТАРОЙ схемой local_users (до репликации из центра,
        10.08.2026) и одной учёткой оператора."""
        conn = sqlite3.connect(db_path)
        with conn:
            conn.execute(
                "CREATE TABLE local_users ("
                " login TEXT PRIMARY KEY,"
                " pw_hash TEXT NOT NULL,"
                " full_name TEXT NOT NULL DEFAULT '')"
            )
            conn.execute(
                "INSERT INTO local_users VALUES (?, ?, ?)",
                (self.OLD_LOGIN, hash_password(CLI_PASSWORD), "Старый Оператор"),
            )
        conn.close()

    def test_columns_added_and_old_operator_alive(self, tmp_path: Path) -> None:
        """Открытие старой БД добавляет колонки; старый оператор жив и входит."""
        db_path = tmp_path / "agent.db"
        self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            columns = {
                row["name"] for row in storage._conn.execute("PRAGMA table_info(local_users)")
            }
            assert {"is_active", "from_center"} <= columns
            # DEFAULT 1 / DEFAULT 0: учётка активна и считается локальной
            row = storage._conn.execute(
                "SELECT is_active, from_center FROM local_users WHERE login = ?",
                (self.OLD_LOGIN,),
            ).fetchone()
            assert (row["is_active"], row["from_center"]) == (1, 0)
            # вход работает как до миграции
            assert storage.verify_operator(self.OLD_LOGIN, CLI_PASSWORD) == "Старый Оператор"
            assert storage.verify_operator(self.OLD_LOGIN, "wrong-pass") is None
        finally:
            storage.close()

    def test_migrated_operator_survives_center_snapshot(self, tmp_path: Path) -> None:
        """Мигрированная учётка — локальная (from_center=0): снимок центра
        без этого логина её не удаляет (аварийный доступ сохраняется)."""
        db_path = tmp_path / "agent.db"
        self._create_old_db(db_path)
        storage = AgentStorage(db_path)
        try:
            storage.replace_center_operators([make_operator("center.only")])
            assert storage.verify_operator(self.OLD_LOGIN, CLI_PASSWORD) == "Старый Оператор"
            assert storage.verify_operator("center.only", CENTER_PASSWORD) == "center.only"
        finally:
            storage.close()

    def test_reopen_after_migration_is_noop(self, tmp_path: Path) -> None:
        """Повторное открытие мигрированной БД идемпотентно (без ошибок
        duplicate column и без потери данных)."""
        db_path = tmp_path / "agent.db"
        self._create_old_db(db_path)
        AgentStorage(db_path).close()
        storage = AgentStorage(db_path)
        try:
            assert storage.verify_operator(self.OLD_LOGIN, CLI_PASSWORD) == "Старый Оператор"
        finally:
            storage.close()


class TestReplaceCenterOperators:
    """Полный снимок операторов из центра: замена, удаление снятых,
    сохранение локальных, перекрытие совпадающего логина."""

    def test_snapshot_replaces_previous_completely(self, storage: AgentStorage) -> None:
        """Новый снимок вытесняет прошлый: снятые с объекта удаляются."""
        assert storage.replace_center_operators([make_operator("op.a"), make_operator("op.b")]) == 2
        assert storage.replace_center_operators([make_operator("op.b"), make_operator("op.c")]) == 2
        assert storage.operators_size() == 2
        assert storage.verify_operator("op.a", CENTER_PASSWORD) is None, "снятый оператор остался"
        assert storage.verify_operator("op.b", CENTER_PASSWORD) == "op.b"
        assert storage.verify_operator("op.c", CENTER_PASSWORD) == "op.c"

    def test_repeat_snapshot_idempotent(self, storage: AgentStorage) -> None:
        """Один и тот же снимок дважды — без ошибок и без дублей."""
        snapshot = [make_operator("op.a", full_name="Оператор А"), make_operator("op.b")]
        assert storage.replace_center_operators(snapshot) == 2
        assert storage.replace_center_operators(snapshot) == 2
        assert storage.operators_size() == 2
        assert storage.verify_operator("op.a", CENTER_PASSWORD) == "Оператор А"

    def test_empty_snapshot_removes_center_keeps_local(self, storage: AgentStorage) -> None:
        """Пустой снимок удаляет всех центровых, но не локальных (CLI)."""
        storage.upsert_operator("local.op", CLI_PASSWORD, "Локальный")
        storage.replace_center_operators([make_operator("center.op")])
        assert storage.replace_center_operators([]) == 0
        assert storage.verify_operator("center.op", CENTER_PASSWORD) is None
        assert storage.verify_operator("local.op", CLI_PASSWORD) == "Локальный"
        assert storage.operators_size() == 1

    def test_matching_login_overwritten_by_center(self, storage: AgentStorage) -> None:
        """Совпадающий логин перезаписывается центром: центр главнее,
        локальный пароль перестаёт подходить, from_center становится 1."""
        storage.upsert_operator("shared.login", CLI_PASSWORD, "Локальное имя")
        storage.replace_center_operators([make_operator("shared.login", full_name="Имя из центра")])
        assert storage.verify_operator("shared.login", CLI_PASSWORD) is None
        assert storage.verify_operator("shared.login", CENTER_PASSWORD) == "Имя из центра"
        # from_center=1: следующий снимок без этого логина его удаляет
        storage.replace_center_operators([])
        assert storage.verify_operator("shared.login", CENTER_PASSWORD) is None
        assert storage.operators_size() == 0

    def test_inactive_operator_cannot_login(self, storage: AgentStorage) -> None:
        """Заблокированная в центре учётка (is_active=False) не входит даже
        с верным паролем; разблокировка новым снимком возвращает вход."""
        storage.replace_center_operators([make_operator("blocked.op", is_active=False)])
        assert storage.verify_operator("blocked.op", CENTER_PASSWORD) is None
        # учётка при этом хранится (снимок полный), а не отброшена
        assert storage.operators_size() == 1
        storage.replace_center_operators([make_operator("blocked.op", is_active=True)])
        assert storage.verify_operator("blocked.op", CENTER_PASSWORD) == "blocked.op"


class TestUpsertOperatorAfterReplica:
    """CLI add-operator поверх реплики центра: локальная учётка перекрывает
    центровую, но следующий снимок возвращает центрового."""

    def test_cli_upsert_overrides_center_and_resets_from_center(
        self, storage: AgentStorage
    ) -> None:
        storage.replace_center_operators([make_operator("op.x", full_name="Центровой")])
        storage.upsert_operator("op.x", CLI_PASSWORD, "Аварийный")
        # входит по CLI-паролю, центровой пароль перестал подходить
        assert storage.verify_operator("op.x", CENTER_PASSWORD) is None
        assert storage.verify_operator("op.x", CLI_PASSWORD) == "Аварийный"
        # from_center сброшен: пустой снимок центра учётку НЕ удаляет
        storage.replace_center_operators([])
        assert storage.verify_operator("op.x", CLI_PASSWORD) == "Аварийный"

    def test_next_snapshot_returns_center_operator(self, storage: AgentStorage) -> None:
        """Следующий снимок с этим логином возвращает центровые учётные данные."""
        storage.replace_center_operators([make_operator("op.x", full_name="Центровой")])
        storage.upsert_operator("op.x", CLI_PASSWORD, "Аварийный")
        storage.replace_center_operators([make_operator("op.x", full_name="Центровой")])
        assert storage.verify_operator("op.x", CLI_PASSWORD) is None
        assert storage.verify_operator("op.x", CENTER_PASSWORD) == "Центровой"
        # и учётка снова центровая: пустой снимок её удаляет
        storage.replace_center_operators([])
        assert storage.verify_operator("op.x", CENTER_PASSWORD) is None

    def test_cli_upsert_reactivates_blocked_center_operator(self, storage: AgentStorage) -> None:
        """CLI-upsert ставит is_active=1: заблокированный центром логин
        после аварийного пересоздания входит (локальная учётка)."""
        storage.replace_center_operators([make_operator("op.x", is_active=False)])
        assert storage.verify_operator("op.x", CENTER_PASSWORD) is None
        storage.upsert_operator("op.x", CLI_PASSWORD)
        assert storage.verify_operator("op.x", CLI_PASSWORD) == "op.x"


class TestListOperators:
    """Снимок учёток для отчёта центру (operators_report, 14.08.2026)."""

    def test_lists_both_sources_without_hashes(self, storage: AgentStorage) -> None:
        """В снимке обе разновидности учёток с верными флагами; хешей
        паролей в записях нет по построению модели (правило №7)."""
        storage.upsert_operator("local.op", CLI_PASSWORD, "Местный Оператор")
        storage.replace_center_operators(
            [make_operator("center.op", full_name="Из Центра", is_active=False)]
        )
        records = storage.list_operators()
        assert [r.login for r in records] == ["center.op", "local.op"]  # порядок по логину
        by_login = {r.login: r for r in records}
        assert by_login["local.op"].from_center is False
        assert by_login["local.op"].full_name == "Местный Оператор"
        assert by_login["local.op"].is_active is True
        assert by_login["center.op"].from_center is True
        assert by_login["center.op"].is_active is False
        assert "pw_hash" not in type(records[0]).model_fields

    def test_empty_db_gives_empty_list(self, storage: AgentStorage) -> None:
        assert storage.list_operators() == []


class TestCenterSettingsStore:
    """Снимок настроек центра (scale_config): save/load_center_settings."""

    def test_load_without_save_returns_none(self, storage: AgentStorage) -> None:
        """Свежая БД: снимка нет — None, без исключений."""
        assert storage.load_center_settings() is None

    def test_save_then_load_round_trip(self, storage: AgentStorage) -> None:
        """Сохранённый JSON возвращается байт-в-байт (разбор — дело settings)."""
        payload = '{"cycle": null, "scale_port": "COM11", "baudrate": 19200}'
        storage.save_center_settings(payload)
        assert storage.load_center_settings() == payload

    def test_second_save_overwrites_first(self, storage: AgentStorage) -> None:
        """Повторный снимок замещает предыдущий: хранится только последний."""
        storage.save_center_settings('{"scale_port": "COM7"}')
        storage.save_center_settings('{"scale_port": "COM11"}')
        assert storage.load_center_settings() == '{"scale_port": "COM11"}'

    def test_settings_survive_reopen(self, tmp_path: Path) -> None:
        """Снимок переживает рестарт агента (файл БД перечитывается)."""
        db_path = tmp_path / "agent.db"
        first = AgentStorage(db_path)
        first.save_center_settings('{"scale_port": "COM11"}')
        first.close()
        second = AgentStorage(db_path)
        try:
            assert second.load_center_settings() == '{"scale_port": "COM11"}'
        finally:
            second.close()


class TestClockOffsetStore:
    """Смещение часов до центра (agent/clock.py): save/load_clock_offset_s."""

    def test_load_without_save_returns_none(self, storage: AgentStorage) -> None:
        """Свежая БД: смещения нет — None, без исключений."""
        assert storage.load_clock_offset_s() is None

    @pytest.mark.parametrize("offset", [120.5, -87.25, 0.0], ids=["positive", "negative", "zero"])
    def test_round_trip(self, storage: AgentStorage, offset: float) -> None:
        """Сохранённое смещение возвращается тем же числом (включая знак и ноль)."""
        storage.save_clock_offset_s(offset)
        assert storage.load_clock_offset_s() == offset

    def test_second_save_overwrites_first(self, storage: AgentStorage) -> None:
        """Каждый heartbeat_ack перезаписывает смещение: хранится последнее."""
        storage.save_clock_offset_s(10.0)
        storage.save_clock_offset_s(-3.25)
        assert storage.load_clock_offset_s() == -3.25

    def test_corrupt_value_returns_none(self, storage: AgentStorage) -> None:
        """Мусор в agent_settings (прямой SQL мимо API) → None, а не падение."""
        with storage._conn:
            storage._conn.execute(
                "INSERT INTO agent_settings (key, value) VALUES ('clock_offset_s', 'abc')"
            )
        assert storage.load_clock_offset_s() is None

    def test_offset_does_not_collide_with_settings(self, storage: AgentStorage) -> None:
        """Смещение и снимок настроек живут под разными ключами таблицы."""
        storage.save_center_settings('{"scale_port": "COM11"}')
        storage.save_clock_offset_s(42.0)
        assert storage.load_clock_offset_s() == 42.0
        assert storage.load_center_settings() == '{"scale_port": "COM11"}'

    def test_offset_survives_reopen(self, tmp_path: Path) -> None:
        """Смещение переживает рестарт агента (офлайн-режим от последнего синка)."""
        db_path = tmp_path / "agent.db"
        first = AgentStorage(db_path)
        first.save_clock_offset_s(120.5)
        first.close()
        second = AgentStorage(db_path)
        try:
            assert second.load_clock_offset_s() == 120.5
        finally:
            second.close()
