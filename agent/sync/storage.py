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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import (
    AgentOperatorInfo,
    OperatorRecord,
    PhotoMeta,
    TareRecord,
    WeighingRecord,
)
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

-- attempts/next_attempt_at: очередь загрузки с повторами (11.08.2026).
-- Вечно падающее фото (битый файл, 409 от центра) не должно держать
-- голову очереди: после каждой неудачи растёт пауза, а порядок выборки
-- сначала берёт снимки с меньшим числом неудач — свежие уезжают первыми.
-- uploaded_at/file_removed: ретеншн локальных файлов после подтверждения
-- центром; сами метаданные (sha256) остаются навсегда — правило №2.
CREATE TABLE IF NOT EXISTS weighing_photos_local (
    weighing_uuid TEXT NOT NULL REFERENCES weighings_local (uuid),
    role TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    uploaded INTEGER NOT NULL DEFAULT 0 CHECK (uploaded IN (0, 1)),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    uploaded_at TEXT,
    file_removed INTEGER NOT NULL DEFAULT 0 CHECK (file_removed IN (0, 1)),
    PRIMARY KEY (weighing_uuid, role)
);

CREATE INDEX IF NOT EXISTS idx_photos_local_upload
    ON weighing_photos_local (uploaded, attempts, next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_photos_local_retention
    ON weighing_photos_local (uploaded, file_removed, uploaded_at);

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

-- Операторы весов (architecture §3.4). from_center=1 — реплика учёток
-- из центра (operators_registry, полный снимок); from_center=0 — заведённые
-- локально CLI add-operator (аварийный доступ). is_active=0 — заблокирован
-- в центре: вход невозможен и офлайн.
CREATE TABLE IF NOT EXISTS local_users (
    login TEXT PRIMARY KEY,
    pw_hash TEXT NOT NULL,
    full_name TEXT NOT NULL DEFAULT '',
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    from_center INTEGER NOT NULL DEFAULT 0 CHECK (from_center IN (0, 1))
);

-- Настройки, присланные центром (scale_config): последний применённый
-- снимок накатывается на конфиг при старте — переживает рестарт и офлайн
CREATE TABLE IF NOT EXISTS agent_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
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

-- Доказательные поля (путь, хеш, размер) неизменны; служебные поля
-- очереди и ретеншна меняться могут, но только «вперёд»: загруженное
-- фото не разгружается, удалённый файл не воскресает.
CREATE TRIGGER IF NOT EXISTS weighing_photos_local_no_update
    BEFORE UPDATE ON weighing_photos_local
    WHEN NOT (
        NEW.weighing_uuid = OLD.weighing_uuid AND NEW.role = OLD.role
        AND NEW.path = OLD.path AND NEW.sha256 = OLD.sha256
        AND NEW.size_bytes = OLD.size_bytes
        AND (NEW.uploaded = OLD.uploaded OR (OLD.uploaded = 0 AND NEW.uploaded = 1))
        AND (NEW.file_removed = OLD.file_removed
             OR (OLD.file_removed = 0 AND NEW.file_removed = 1 AND OLD.uploaded = 1))
    )
BEGIN
    SELECT RAISE(ABORT, 'фото не редактируются (правило неизменяемости)');
END;
"""

# Триггер и индекс фото сделаны через CREATE ... IF NOT EXISTS, поэтому
# на агентах, поставленных до 11.08.2026, они остались бы в старом виде
# (старое тело триггера блокировало бы служебные поля очереди). Сносим их
# ТОЛЬКО если тело устарело — так окно «таблица без триггера» бывает
# единственный раз, при обновлении, а не на каждом старте.
_DROP_PHOTO_TRIGGER = "DROP TRIGGER IF EXISTS weighing_photos_local_no_update"
_DROP_PHOTO_INDEX = "DROP INDEX IF EXISTS idx_photos_local_upload"
_PHOTO_TRIGGER_MARK = "file_removed"  # признак нового тела триггера
_PHOTO_INDEX_MARK = "attempts"  # признак нового состава индекса

# Колонки очереди/ретеншна, добавляемые к старой таблице фото
_PHOTO_COLUMNS = {
    "attempts": "INTEGER NOT NULL DEFAULT 0",
    "next_attempt_at": "TEXT",
    "uploaded_at": "TEXT",
    "file_removed": "INTEGER NOT NULL DEFAULT 0 CHECK (file_removed IN (0, 1))",
}


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


def _utc_iso(moment: datetime) -> str:
    """Момент строкой в UTC — служебные времена очереди и ретеншна.

    SQLite сравнивает их как текст, поэтому пояс обязан быть один: тот же
    момент, записанный как «12:00+00:00» и «18:00+06:00», иначе давал бы
    разный результат сравнения (находка qa-tester 11.08.2026). Наивное
    время трактуем как UTC — так его пишет весь агент.
    """
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC).isoformat()
    return moment.astimezone(UTC).isoformat()


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
            # операторы старой схемы (до репликации из центра, 10.08.2026):
            # учётки НЕ расходные — дополняем колонками, не пересоздаём
            user_columns = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(local_users)").fetchall()
            }
            if user_columns and "is_active" not in user_columns:
                self._conn.execute(
                    "ALTER TABLE local_users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1"
                    " CHECK (is_active IN (0, 1))"
                )
                self._conn.execute(
                    "ALTER TABLE local_users ADD COLUMN from_center INTEGER NOT NULL DEFAULT 0"
                    " CHECK (from_center IN (0, 1))"
                )
            # фото старой схемы (до очереди с повторами и ретеншна,
            # 11.08.2026): дополняем служебными колонками, доказательные
            # поля не трогаем
            photo_columns = {
                row["name"]
                for row in self._conn.execute("PRAGMA table_info(weighing_photos_local)").fetchall()
            }
            if photo_columns:
                for column, definition in _PHOTO_COLUMNS.items():
                    if column not in photo_columns:
                        self._conn.execute(
                            f"ALTER TABLE weighing_photos_local ADD COLUMN {column} {definition}"
                        )
                self._drop_if_outdated(
                    "trigger",
                    "weighing_photos_local_no_update",
                    _PHOTO_TRIGGER_MARK,
                    _DROP_PHOTO_TRIGGER,
                )
                self._drop_if_outdated(
                    "index", "idx_photos_local_upload", _PHOTO_INDEX_MARK, _DROP_PHOTO_INDEX
                )
            self._conn.executescript(_SCHEMA)

    def _drop_if_outdated(self, kind: str, name: str, mark: str, drop_sql: str) -> None:
        """Снести объект схемы, если его тело осталось от старой версии.

        CREATE ... IF NOT EXISTS не обновляет уже существующие триггеры и
        индексы, поэтому устаревшие сносим — пересоздаст их _SCHEMA.
        """
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?", (kind, name)
        ).fetchone()
        if row is not None and mark not in (row["sql"] or ""):
            self._conn.execute(drop_sql)

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

    def photos_to_upload(
        self, limit: int = 8, *, now: datetime | None = None, max_pause_s: float | None = None
    ) -> list[tuple[UUID, StoredPhoto]]:
        """Фото досланных записей, ещё не загруженные в центр (файлами).

        Снимки, ждущие паузы после неудачи, пропускаются; порядок —
        сначала с наименьшим числом неудач, внутри — старые записи
        первыми. Так вечно падающее фото уходит в хвост очереди и не
        задерживает свежие (находка ревью, 11.08.2026).

        ``max_pause_s`` — предельная разумная пауза. Отметка дальше неё
        могла быть записана только при уехавших вперёд часах весового ПК
        (их никто не обслуживает, см. agent/clock.py), и снимок с такой
        отметкой иначе не уехал бы никогда — поэтому он снова берётся
        в работу (находка ревью 11.08.2026).
        """
        moment = now or datetime.now(UTC)
        horizon = (
            _utc_iso(moment + timedelta(seconds=max_pause_s)) if max_pause_s is not None else None
        )
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT p.* FROM weighing_photos_local p
                JOIN weighings_local w ON w.uuid = p.weighing_uuid
                WHERE p.uploaded = 0 AND w.synced = 1
                  AND (p.next_attempt_at IS NULL OR p.next_attempt_at <= ?
                       OR (? IS NOT NULL AND p.next_attempt_at > ?))
                ORDER BY p.attempts, w.created_at, p.role LIMIT ?
                """,
                (_utc_iso(moment), horizon, horizon, limit),
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

    def mark_photo_uploaded(
        self, weighing_uuid: UUID, role: CameraRole, *, now: datetime | None = None
    ) -> None:
        """Пометить файл фото как принятый центром (время — для ретеншна)."""
        moment = _utc_iso(now or datetime.now(UTC))
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE weighing_photos_local SET uploaded = 1, uploaded_at = ?,"
                " next_attempt_at = NULL WHERE weighing_uuid = ? AND role = ? AND uploaded = 0",
                (moment, str(weighing_uuid), role.value),
            )

    def mark_photo_failed(
        self,
        weighing_uuid: UUID,
        role: CameraRole,
        *,
        base_delay_s: float,
        max_delay_s: float,
        now: datetime | None = None,
    ) -> int:
        """Учесть неудачную попытку загрузки; вернуть новое число попыток.

        Пауза до следующей попытки удваивается с каждой неудачей, но не
        дольше ``max_delay_s``: недоступный центр не превращается в поток
        запросов, а битый файл не крутится в цикле каждые 5 секунд.
        """
        moment = now or datetime.now(UTC)
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT attempts FROM weighing_photos_local WHERE weighing_uuid = ? AND role = ?",
                (str(weighing_uuid), role.value),
            ).fetchone()
            if row is None:
                return 0
            attempts = int(row["attempts"]) + 1
            # показатель ограничен: у снимка, падающего неделями, attempts
            # доходит до тысяч, и 2 ** attempts перестаёт помещаться во
            # float — пауза переставала продлеваться (находка qa-tester)
            delay = min(base_delay_s * 2 ** min(attempts - 1, 32), max_delay_s)
            self._conn.execute(
                "UPDATE weighing_photos_local SET attempts = ?, next_attempt_at = ?"
                " WHERE weighing_uuid = ? AND role = ?",
                (
                    attempts,
                    _utc_iso(moment + timedelta(seconds=delay)),
                    str(weighing_uuid),
                    role.value,
                ),
            )
        return attempts

    def photo_queue_stats(self, *, stuck_after: int = 5) -> tuple[int, int]:
        """(всего в очереди, из них застрявших) — для лога и диагностики."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN p.attempts >= ? THEN 1 ELSE 0 END) AS stuck
                FROM weighing_photos_local p
                JOIN weighings_local w ON w.uuid = p.weighing_uuid
                WHERE p.uploaded = 0 AND w.synced = 1
                """,
                (stuck_after,),
            ).fetchone()
        return int(row["total"]), int(row["stuck"] or 0)

    def photos_to_purge(
        self, older_than: datetime, limit: int = 200
    ) -> list[tuple[UUID, CameraRole, str]]:
        """Файлы, загруженные в центр раньше ``older_than`` (для ретеншна).

        Время подтверждения у снимков, сделанных до 11.08.2026, неизвестно —
        для них берётся дата самой записи.
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT p.weighing_uuid, p.role, p.path FROM weighing_photos_local p
                JOIN weighings_local w ON w.uuid = p.weighing_uuid
                WHERE p.uploaded = 1 AND p.file_removed = 0
                  AND COALESCE(p.uploaded_at, w.created_at) <= ?
                ORDER BY COALESCE(p.uploaded_at, w.created_at) LIMIT ?
                """,
                (_utc_iso(older_than), limit),
            ).fetchall()
        return [(UUID(row["weighing_uuid"]), CameraRole(row["role"]), row["path"]) for row in rows]

    def mark_photo_file_removed(self, weighing_uuid: UUID, role: CameraRole) -> None:
        """Отметить, что локальный файл убран ретеншном (метаданные живут)."""
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE weighing_photos_local SET file_removed = 1"
                " WHERE weighing_uuid = ? AND role = ? AND uploaded = 1",
                (str(weighing_uuid), role.value),
            )

    def pending_count(self) -> int:
        """Размер очереди досылки (для heartbeat и локального интерфейса)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM weighings_local WHERE synced = 0"
            ).fetchone()
        return int(row["n"])

    def pending_photos_count(self) -> int:
        """Снимки, ещё не загруженные на центр (метрика heartbeat, 0.4.13).

        Считаются все незагруженные, включая фото несинхронизированных
        записей: для мониторинга важно «сколько снимков ещё не в центре»,
        а не размер очереди загрузчика.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM weighing_photos_local WHERE uploaded = 0"
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

    def latest_tare(
        self, vehicle_number: str, trailer_number: str | None = None
    ) -> TareRecord | None:
        """Последнее тарирование СЦЕПКИ без проверки срока действия.

        Реестр хранит одну строку на сцепку, и с 14.08.2026 центр реплицирует
        его целиком, включая просроченные строки, — они нужны примечаниям
        «почему нет нетто» (просьба Игоря; от центра старого выката реплика
        приезжает без просроченных — примечание честно откатывается к
        «тарирования не было»). В расчёт нетто устаревшую тару подставлять
        нельзя — для расчёта только find_active_tare (правило №4).
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tare_registry_replica"
                " WHERE vehicle_number = ? AND trailer_number = ?",
                (vehicle_number, trailer_number or ""),
            ).fetchone()
        if row is None:
            return None
        return self._tare_from_row(row)

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
        record = self.latest_tare(vehicle_number, trailer_number)
        if record is None or record.tared_at < three_months_before(at):
            return None
        return record

    def tare_by_weighing_uuid(self, weighing_uuid: UUID) -> TareRecord | None:
        """Строка реплики реестра по uuid исходного тарирования.

        Для печатной карточки: дата тарирования, когда сама запись
        тарирования прошла на других весах и в локальном журнале её нет.
        Срок действия здесь не проверяется — дата нужна и у тары,
        успевшей устареть к моменту печати.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tare_registry_replica WHERE weighing_uuid = ?",
                (str(weighing_uuid),),
            ).fetchone()
        if row is None:
            return None
        return self._tare_from_row(row)

    @staticmethod
    def _tare_from_row(row: sqlite3.Row) -> TareRecord:
        tared_at = datetime.fromisoformat(row["tared_at"])
        if tared_at.tzinfo is None:
            # дата без пояса (не должна приходить, но протокол её не запрещает) —
            # трактуем как UTC, чтобы сравнение не падало TypeError
            tared_at = tared_at.replace(tzinfo=UTC)
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
        """Создать или обновить ЛОКАЛЬНОГО оператора (CLI add-operator,
        аварийный доступ; пароль хранится хешем PBKDF2). Основной путь —
        реплика из центра, см. replace_center_operators."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO local_users (login, pw_hash, full_name, is_active, from_center)"
                " VALUES (?, ?, ?, 1, 0)"
                " ON CONFLICT (login) DO UPDATE SET pw_hash = excluded.pw_hash,"
                " full_name = excluded.full_name, is_active = 1, from_center = 0",
                (login, hash_password(password), full_name),
            )

    def replace_center_operators(self, records: Iterable[OperatorRecord]) -> int:
        """Заменить реплику операторов центра целиком (полный снимок).

        Строки from_center=1, отсутствующие в снимке, удаляются (оператора
        сняли с объекта). Локальные учётки (from_center=0) не трогаются,
        кроме совпадающего логина — центр главнее. Возвращает размер реплики.
        """
        rows = [(r.login, r.pw_hash, r.full_name, int(r.is_active)) for r in records]
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM local_users WHERE from_center = 1")
            self._conn.executemany(
                "INSERT INTO local_users (login, pw_hash, full_name, is_active, from_center)"
                " VALUES (?, ?, ?, ?, 1)"
                " ON CONFLICT (login) DO UPDATE SET pw_hash = excluded.pw_hash,"
                " full_name = excluded.full_name, is_active = excluded.is_active,"
                " from_center = 1",
                rows,
            )
        return len(rows)

    # --- настройки из центра (scale_config) ---

    def save_center_settings(self, payload_json: str) -> None:
        """Сохранить последний применённый снимок настроек (JSON)."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO agent_settings (key, value) VALUES ('scale_settings', ?)"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (payload_json,),
            )

    def load_center_settings(self) -> str | None:
        """Последний применённый снимок настроек центра (JSON) или None."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM agent_settings WHERE key = 'scale_settings'"
            ).fetchone()
        return str(row["value"]) if row is not None else None

    def save_clock_offset_s(self, offset_s: float) -> None:
        """Смещение часов до центра (переживает рестарт, см. agent/clock.py)."""
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO agent_settings (key, value) VALUES ('clock_offset_s', ?)"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (str(offset_s),),
            )

    def load_clock_offset_s(self) -> float | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM agent_settings WHERE key = 'clock_offset_s'"
            ).fetchone()
        if row is None:
            return None
        try:
            return float(row["value"])
        except ValueError:
            return None

    def operators_size(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM local_users").fetchone()
        return int(row["n"])

    def list_operators(self) -> list[AgentOperatorInfo]:
        """Все учётки весового ПК для отчёта центру (operators_report).

        Хеши паролей в отчёт не попадают (правило №7): центру достаточно
        знать, ЧТО за учётки существуют и откуда они взялись.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT login, full_name, is_active, from_center FROM local_users ORDER BY login"
            ).fetchall()
        return [
            AgentOperatorInfo(
                login=str(row["login"]),
                full_name=str(row["full_name"] or ""),
                is_active=bool(row["is_active"]),
                from_center=bool(row["from_center"]),
            )
            for row in rows
        ]

    def verify_operator(self, login: str, password: str) -> str | None:
        """Проверить пароль; вернуть отображаемое имя оператора или None.

        Заблокированная в центре учётка (is_active=0) не входит и офлайн.
        Возвращаемое имя — full_name, а при его отсутствии сам login.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT pw_hash, full_name FROM local_users WHERE login = ? AND is_active = 1",
                (login,),
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
