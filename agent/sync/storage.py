"""Локальная БД агента (SQLite): журнал операций и реплика реестра тарирований.

Назначение (architecture §2 п.2, §3.3а):
- **Журнал операций** — каждая завершённая операция пишется сюда до отправки
  в центр; флаг ``synced`` отмечает досланные записи. Взвешивания не теряются
  никогда: буфер переживает перезапуск агента и офлайн любой длительности.
- **Реплика реестра тарирований** — полный снимок единого реестра центра,
  заменяется целиком при каждой синхронизации. Благодаря ей агент в офлайне
  сам подставляет действующую тару и считает нетто (правило проекта №4).

Неизменяемость (правило №2): записанная операция не редактируется и не
удаляется — API даёт только вставку и пометку synced, а триггеры БД
блокируют UPDATE/DELETE даже при прямом доступе к файлу базы.

Потокобезопасность: одно соединение под локом (у агента к БД обращаются
поток цикла взвешивания и поток синхронизации).
"""

import sqlite3
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import PhotoMeta, TareRecord, WeighingRecord
from shared.passwords import hash_password, verify_password
from shared.tare import TARE_VALIDITY_MONTHS, three_months_before  # noqa: F401 — реэкспорт

_SCHEMA = """
CREATE TABLE IF NOT EXISTS weighings_local (
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

CREATE INDEX IF NOT EXISTS idx_weighings_local_pending
    ON weighings_local (synced, created_at);

CREATE TABLE IF NOT EXISTS weighing_photos_local (
    weighing_uuid TEXT NOT NULL REFERENCES weighings_local (uuid),
    role TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    uploaded INTEGER NOT NULL DEFAULT 0 CHECK (uploaded IN (0, 1)),
    PRIMARY KEY (weighing_uuid, role)
);

CREATE INDEX IF NOT EXISTS idx_photos_local_upload
    ON weighing_photos_local (uploaded);

-- Тара по ПАРЕ голова+прицеп (решение 09.08.2026); '' = без прицепа
-- (NULL в первичном ключе SQLite невозможен)
CREATE TABLE IF NOT EXISTS tare_registry_replica (
    vehicle_number TEXT NOT NULL,
    trailer_number TEXT NOT NULL DEFAULT '',
    tare_value REAL NOT NULL,
    tared_at TEXT NOT NULL,
    weighing_uuid TEXT NOT NULL,
    PRIMARY KEY (vehicle_number, trailer_number)
);

-- Локальные операторы (architecture §3.4: список синхронизируется с центром;
-- пока центр не готов — заводятся из конфига агента при старте)
CREATE TABLE IF NOT EXISTS local_users (
    login TEXT PRIMARY KEY,
    pw_hash TEXT NOT NULL,
    full_name TEXT NOT NULL DEFAULT ''
);

-- Правило №2: запись неизменяема. Разрешён только переход synced 0 -> 1;
-- любое другое изменение и любое удаление блокируются на уровне БД.
CREATE TRIGGER IF NOT EXISTS weighings_local_no_delete
    BEFORE DELETE ON weighings_local
BEGIN
    SELECT RAISE(ABORT, 'взвешивания не удаляются (правило неизменяемости)');
END;

CREATE TRIGGER IF NOT EXISTS weighings_local_no_update
    BEFORE UPDATE ON weighings_local
    WHEN NOT (
        NEW.uuid = OLD.uuid AND NEW.operation = OLD.operation
        AND NEW.code = OLD.code AND NEW.massa IS OLD.massa
        AND NEW.unit = OLD.unit AND NEW.stable = OLD.stable
        AND NEW.weighed_at IS OLD.weighed_at
        AND NEW.vehicle_number IS OLD.vehicle_number
        AND NEW.trailer_number IS OLD.trailer_number
        AND NEW.tare_value IS OLD.tare_value
        AND NEW.tare_weighing_uuid IS OLD.tare_weighing_uuid
        AND NEW.netto IS OLD.netto AND NEW.source = OLD.source
        AND NEW.operator IS OLD.operator AND NEW.message IS OLD.message
        AND NEW.created_at = OLD.created_at
        AND (NEW.synced = OLD.synced OR (OLD.synced = 0 AND NEW.synced = 1))
    )
BEGIN
    SELECT RAISE(ABORT, 'взвешивания не редактируются (правило неизменяемости)');
END;

CREATE TRIGGER IF NOT EXISTS weighing_photos_local_no_delete
    BEFORE DELETE ON weighing_photos_local
BEGIN
    SELECT RAISE(ABORT, 'фото не удаляются (правило неизменяемости)');
END;

CREATE TRIGGER IF NOT EXISTS weighing_photos_local_no_update
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


@dataclass(frozen=True)
class StoredPhoto:
    """Снимок, лежащий файлом на диске агента до досылки в центр."""

    role: CameraRole
    path: str
    sha256: str
    size_bytes: int


def photo_meta(photo: StoredPhoto) -> PhotoMeta:
    """Метаданные фото для протокола (имя файла — без локального пути)."""
    return PhotoMeta(
        role=photo.role,
        filename=Path(photo.path).name,
        sha256=photo.sha256,
        size_bytes=photo.size_bytes,
    )


def _iso(moment: datetime | None) -> str | None:
    return moment.isoformat() if moment is not None else None


class AgentStorage:
    """Локальное хранилище агента поверх одного файла SQLite.

    ``db_path=":memory:"`` — для тестов. Все методы потокобезопасны.
    """

    def __init__(self, db_path: str | Path) -> None:
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock, self._conn:
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA foreign_keys = ON")
            # реплика старой схемы (тара по одной голове, до 09.08.2026)
            # пересоздаётся: это расходные данные, центр пришлёт снимок заново
            columns = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(tare_registry_replica)").fetchall()
            }
            if columns and "trailer_number" not in columns:
                self._conn.execute("DROP TABLE tare_registry_replica")
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --- журнал операций ---

    def save_weighing(self, record: WeighingRecord, photos: Iterable[StoredPhoto] = ()) -> None:
        """Записать завершённую операцию (однократно; запись неизменяема).

        Повторная вставка того же uuid → sqlite3.IntegrityError: журнал
        не перезаписывается.
        """
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO weighings_local (
                    uuid, operation, code, massa, unit, stable, weighed_at,
                    vehicle_number, trailer_number, tare_value,
                    tare_weighing_uuid, netto, source, operator, message,
                    synced, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    str(record.uuid),
                    record.operation.value,
                    record.code.value,
                    record.massa,
                    record.unit,
                    int(record.stable),
                    _iso(record.weighed_at),
                    record.vehicle_number,
                    record.trailer_number,
                    record.tare_value,
                    str(record.tare_weighing_uuid) if record.tare_weighing_uuid else None,
                    record.netto,
                    record.source.value,
                    record.operator,
                    record.message,
                    datetime.now(UTC).isoformat(),
                ),
            )
            self._conn.executemany(
                """
                INSERT INTO weighing_photos_local
                    (weighing_uuid, role, path, sha256, size_bytes)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(str(record.uuid), p.role.value, p.path, p.sha256, p.size_bytes) for p in photos],
            )

    def mark_synced(self, uuids: Iterable[UUID]) -> int:
        """Пометить записи как досланные в центр; вернуть число помеченных."""
        keys = [(str(u),) for u in uuids]
        if not keys:
            return 0
        with self._lock, self._conn:
            cursor = self._conn.executemany(
                "UPDATE weighings_local SET synced = 1 WHERE uuid = ? AND synced = 0",
                keys,
            )
            return cursor.rowcount

    def pending_records(self, limit: int = 100) -> list[WeighingRecord]:
        """Недосланные записи, старые первыми (для offline_sync).

        Метаданные фото включаются в записи (record.photos) — центр
        зафиксирует их в контрольной сумме; файлы уедут отдельно.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM weighings_local WHERE synced = 0 ORDER BY created_at, uuid LIMIT ?",
                (limit,),
            ).fetchall()
        records = []
        for row in rows:
            record = self._row_to_record(row)
            photos = self.photos_for(record.uuid)
            if photos:
                record = record.model_copy(update={"photos": [photo_meta(p) for p in photos]})
            records.append(record)
        return records

    def photos_to_upload(self, limit: int = 8) -> list[tuple[UUID, StoredPhoto]]:
        """Фото досланных записей, ещё не загруженные в центр (файлами)."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT p.* FROM weighing_photos_local p
                JOIN weighings_local w ON w.uuid = p.weighing_uuid
                WHERE p.uploaded = 0 AND w.synced = 1
                ORDER BY w.created_at, p.role LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            (
                UUID(row["weighing_uuid"]),
                StoredPhoto(
                    role=CameraRole(row["role"]),
                    path=row["path"],
                    sha256=row["sha256"],
                    size_bytes=row["size_bytes"],
                ),
            )
            for row in rows
        ]

    def mark_photo_uploaded(self, weighing_uuid: UUID, role: CameraRole) -> None:
        """Пометить файл фото как принятый центром."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE weighing_photos_local SET uploaded = 1"
                " WHERE weighing_uuid = ? AND role = ?",
                (str(weighing_uuid), role.value),
            )

    def pending_count(self) -> int:
        """Размер очереди досылки (для heartbeat и локального интерфейса)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM weighings_local WHERE synced = 0"
            ).fetchone()
        return int(row["n"])

    def get_weighing(self, uuid: UUID) -> WeighingRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM weighings_local WHERE uuid = ?", (str(uuid),)
            ).fetchone()
        return self._row_to_record(row) if row is not None else None

    def recent_weighings(self, limit: int = 50) -> list[WeighingRecord]:
        """Последние операции для журнала локального интерфейса."""
        return [record for record, _ in self.recent_weighings_synced(limit)]

    def recent_weighings_synced(self, limit: int = 50) -> list[tuple[WeighingRecord, bool]]:
        """Последние операции с флагом досылки (для колонки «Синхр.» журнала)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM weighings_local ORDER BY created_at DESC, uuid LIMIT ?",
                (limit,),
            ).fetchall()
        return [(self._row_to_record(row), bool(row["synced"])) for row in rows]

    def photos_for(self, uuid: UUID) -> list[StoredPhoto]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT role, path, sha256, size_bytes FROM weighing_photos_local"
                " WHERE weighing_uuid = ? ORDER BY role",
                (str(uuid),),
            ).fetchall()
        return [
            StoredPhoto(
                role=CameraRole(row["role"]),
                path=row["path"],
                sha256=row["sha256"],
                size_bytes=row["size_bytes"],
            )
            for row in rows
        ]

    # --- реплика реестра тарирований ---

    def replace_tare_registry(self, records: Iterable[TareRecord]) -> int:
        """Заменить реплику целиком (реестр реплицируется полным снимком).

        Возвращает размер новой реплики.
        """
        rows = [
            (
                r.vehicle_number,
                r.trailer_number or "",
                r.tare_value,
                r.tared_at.isoformat(),
                str(r.weighing_uuid),
            )
            for r in records
        ]
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM tare_registry_replica")
            self._conn.executemany(
                "INSERT INTO tare_registry_replica"
                " (vehicle_number, trailer_number, tare_value, tared_at, weighing_uuid)"
                " VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        return len(rows)

    def find_active_tare(
        self, vehicle_number: str, at: datetime, trailer_number: str | None = None
    ) -> TareRecord | None:
        """Действующая тара СЦЕПКИ: пара голова+прицеп, не старше 3 месяцев.

        Тара подставляется только при совпадении ОБОИХ номеров (решение
        09.08.2026); None — действующей тары нет (нетто не считается,
        правило №4). Naive-даты (без пояса) трактуются как UTC.
        """
        if at.tzinfo is None:
            at = at.replace(tzinfo=UTC)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tare_registry_replica"
                " WHERE vehicle_number = ? AND trailer_number = ?",
                (vehicle_number, trailer_number or ""),
            ).fetchone()
        if row is None:
            return None
        tared_at = datetime.fromisoformat(row["tared_at"])
        if tared_at.tzinfo is None:
            # дата без пояса (не должна приходить, но протокол её не запрещает) —
            # трактуем как UTC, чтобы сравнение не падало TypeError
            tared_at = tared_at.replace(tzinfo=UTC)
        if tared_at < three_months_before(at):
            return None
        return TareRecord(
            vehicle_number=row["vehicle_number"],
            trailer_number=row["trailer_number"] or None,
            tare_value=row["tare_value"],
            tared_at=tared_at,
            weighing_uuid=UUID(row["weighing_uuid"]),
        )

    def photo_paths(self) -> set[str]:
        """Пути всех снимков, привязанных к записям журнала.

        Для уборки снимков-сирот при старте агента: файл в photos_dir,
        которого здесь нет, не принадлежит ни одной записи (погибшее
        превью ручного режима) и подлежит удалению.
        """
        with self._lock:
            rows = self._conn.execute("SELECT path FROM weighing_photos_local").fetchall()
        return {row["path"] for row in rows}

    def tare_registry_size(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM tare_registry_replica").fetchone()
        return int(row["n"])

    # --- локальные операторы ---

    def upsert_operator(self, login: str, password: str, full_name: str = "") -> None:
        """Создать или обновить оператора (пароль хранится хешем PBKDF2)."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO local_users (login, pw_hash, full_name) VALUES (?, ?, ?)"
                " ON CONFLICT (login) DO UPDATE SET pw_hash = excluded.pw_hash,"
                " full_name = excluded.full_name",
                (login, hash_password(password), full_name),
            )

    def verify_operator(self, login: str, password: str) -> str | None:
        """Проверить пароль; вернуть отображаемое имя оператора или None.

        Возвращаемое имя — full_name, а при его отсутствии сам login.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT pw_hash, full_name FROM local_users WHERE login = ?", (login,)
            ).fetchone()
        if row is None or not verify_password(password, row["pw_hash"]):
            return None
        return str(row["full_name"]) or login

    # --- внутреннее ---

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> WeighingRecord:
        return WeighingRecord(
            uuid=UUID(row["uuid"]),
            operation=Operation(row["operation"]),
            code=ErrorCode(row["code"]),
            massa=row["massa"],
            unit=row["unit"],
            stable=bool(row["stable"]),
            weighed_at=(datetime.fromisoformat(row["weighed_at"]) if row["weighed_at"] else None),
            vehicle_number=row["vehicle_number"],
            trailer_number=row["trailer_number"],
            tare_value=row["tare_value"],
            tare_weighing_uuid=(
                UUID(row["tare_weighing_uuid"]) if row["tare_weighing_uuid"] else None
            ),
            netto=row["netto"],
            source=WeighingSource(row["source"]),
            operator=row["operator"],
            message=row["message"],
        )
