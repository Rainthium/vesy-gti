"""Тесты ретеншна локальных фото агента (agent/sync/retention.py) и опор
в хранилище (photos_to_purge / mark_photo_file_removed).

Снимок — доказательство операции: удалять с весового ПК можно только тот
файл, который центр уже принял (uploaded=1), и только сам файл — строка
журнала с путём, sha256 и размером остаётся навсегда (правило №2).

Покрытие:
- purge_once убирает ТОЛЬКО подтверждённые снимки старше N дней; граница
  ровно N дней включительно; порция batch, самые старые первыми;
- незагруженные снимки не трогаются никогда, даже пятилетней давности;
- метаданные (строка в БД, путь, sha256, размер) переживают уборку;
- retention_days = 0 — уборка выключена целиком (и run() выходит сразу);
- снимки, загруженные ДО появления uploaded_at (NULL у агентов старее
  11.08.2026): берётся дата записи — старые файлы чистятся, свежие нет;
- ошибка удаления (OSError) не ставит file_removed — повторим в следующий
  раз; исчезнувший файл, наоборот, помечается без исключения;
- часовые пояса: бишкекский и наивный момент считаются так же, как UTC;
- повторный прогон не делает лишней работы (file_removed уже стоит);
- цикл run(): убирает файлы, переживает сбой хранилища, снимается отменой.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from agent.sync.retention import PhotoRetention, run_forever
from agent.sync.storage import AgentStorage, StoredPhoto
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord

JPEG = b"\xff\xd8\xff\xe0photo\xff\xd9"
SHA = "a" * 64
BISHKEK = timezone(timedelta(hours=6))  # Asia/Bishkek, UTC+6


def make_record(**overrides: Any) -> WeighingRecord:
    """Типичная успешная запись взвешивания; overrides — точечные замены."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 15000.0,
        "stable": True,
        "weighed_at": datetime.now(UTC),
        "vehicle_number": "01KG123ABC",
        "source": WeighingSource.AIS,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


@dataclass
class RetentionEnv:
    """Хранилище агента и каталог со снимками на диске."""

    storage: AgentStorage
    photos_dir: Path

    def add_photo(
        self,
        name: str,
        *,
        uploaded_at: datetime | None = None,
        uploaded: bool = True,
        role: CameraRole = CameraRole.FRONT,
    ) -> tuple[UUID, Path]:
        """Записать операцию со снимком-файлом; вернуть (uuid, путь файла).

        ``uploaded=True, uploaded_at=None`` — снимок, подтверждённый СТАРЫМ
        агентом (колонки uploaded_at тогда не было): помечаем uploaded
        прямым SQL, как это делал прежний mark_photo_uploaded.
        """
        path = self.photos_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(JPEG)
        record = make_record()
        self.storage.save_weighing(
            record,
            [StoredPhoto(role=role, path=str(path), sha256=SHA, size_bytes=len(JPEG))],
        )
        self.storage.mark_synced([record.uuid])
        if uploaded and uploaded_at is not None:
            self.storage.mark_photo_uploaded(record.uuid, role, now=uploaded_at)
        elif uploaded:
            with self.storage._conn:
                self.storage._conn.execute(
                    "UPDATE weighing_photos_local SET uploaded = 1"
                    " WHERE weighing_uuid = ? AND role = ?",
                    (str(record.uuid), role.value),
                )
        time.sleep(0.002)  # created_at записей должен различаться (порядок уборки)
        return record.uuid, path

    def photo_row(self, weighing_uuid: UUID, role: CameraRole = CameraRole.FRONT) -> Any:
        row = self.storage._conn.execute(
            "SELECT * FROM weighing_photos_local WHERE weighing_uuid = ? AND role = ?",
            (str(weighing_uuid), role.value),
        ).fetchone()
        assert row is not None, "строка снимка исчезла из журнала"
        return row


@pytest.fixture
def env(tmp_path: Path) -> Iterator[RetentionEnv]:
    storage = AgentStorage(":memory:")
    yield RetentionEnv(storage, tmp_path / "photos")
    storage.close()


NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)  # опорный «сейчас» уборки


class TestPurgeOnce:
    """Одна уборка: что удаляется, что остаётся неприкосновенным."""

    def test_uploaded_old_file_removed_metadata_kept(self, env: RetentionEnv) -> None:
        """Файл старше срока удалён, а запись журнала — цела (правило №2)."""
        uuid, path = env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1

        assert not path.exists(), "файл не убран"
        row = env.photo_row(uuid)
        assert (row["path"], row["sha256"], row["size_bytes"]) == (str(path), SHA, len(JPEG))
        assert (row["uploaded"], row["file_removed"]) == (1, 1)
        # доказательные метаданные по-прежнему отдаются наружу
        assert env.storage.photos_for(uuid)[0].sha256 == SHA
        assert str(path) in env.storage.photo_paths()

    def test_recent_uploaded_file_kept(self, env: RetentionEnv) -> None:
        """Загруженный, но свежий снимок не трогаем."""
        uuid, path = env.add_photo("fresh.jpeg", uploaded_at=NOW - timedelta(days=5))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 0
        assert path.exists()
        assert env.photo_row(uuid)["file_removed"] == 0

    def test_boundary_exactly_n_days_removed(self, env: RetentionEnv) -> None:
        """Ровно N дней — уже подлежит уборке (граница включительно),
        на секунду позже — ещё нет."""
        _, on_edge = env.add_photo("edge.jpeg", uploaded_at=NOW - timedelta(days=30))
        _, inside = env.add_photo(
            "inside.jpeg", uploaded_at=NOW - timedelta(days=30) + timedelta(seconds=1)
        )
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert not on_edge.exists()
        assert inside.exists()

    def test_never_uploaded_is_never_purged(self, env: RetentionEnv) -> None:
        """Неподтверждённый снимок не удаляется НИКОГДА: он в единственном
        экземпляре и лежит только здесь (даже если ему пять лет)."""
        uuid, path = env.add_photo("not-uploaded.jpeg", uploaded=False)
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW + timedelta(days=5 * 365)) == 0
        assert path.exists(), "потеряно доказательство операции, не уехавшее в центр"
        assert env.photo_row(uuid)["file_removed"] == 0

    def test_only_uploaded_role_of_same_record(self, env: RetentionEnv) -> None:
        """У одной записи убирается только подтверждённый снимок; второй
        (ещё не уехавший в центр) остаётся."""
        record = make_record()
        front = env.photos_dir / "pair_front.jpeg"
        rear = env.photos_dir / "pair_rear.jpeg"
        front.parent.mkdir(parents=True, exist_ok=True)
        front.write_bytes(JPEG)
        rear.write_bytes(JPEG)
        env.storage.save_weighing(
            record,
            [
                StoredPhoto(
                    role=CameraRole.FRONT, path=str(front), sha256=SHA, size_bytes=len(JPEG)
                ),
                StoredPhoto(role=CameraRole.REAR, path=str(rear), sha256=SHA, size_bytes=len(JPEG)),
            ],
        )
        env.storage.mark_synced([record.uuid])
        env.storage.mark_photo_uploaded(record.uuid, CameraRole.FRONT, now=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert not front.exists()
        assert rear.exists()

    def test_zero_days_disables_purge(self, env: RetentionEnv) -> None:
        """photo_retention_days = 0 — не убираем ничего и никогда."""
        _, path = env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=400))
        retention = PhotoRetention(env.storage, retention_days=0)

        assert retention.enabled is False
        assert retention.purge_once(now=NOW) == 0
        assert retention.purge_once(now=NOW + timedelta(days=365)) == 0
        assert path.exists()

    def test_batch_limits_portion_oldest_first(self, env: RetentionEnv) -> None:
        """Порция ограничена batch; первыми уходят самые старые снимки."""
        _, oldest = env.add_photo("a.jpeg", uploaded_at=NOW - timedelta(days=90))
        _, middle = env.add_photo("b.jpeg", uploaded_at=NOW - timedelta(days=60))
        _, newest = env.add_photo("c.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30, batch=2, max_batches=1)

        assert retention.purge_once(now=NOW) == 2
        assert not oldest.exists() and not middle.exists()
        assert newest.exists()
        # следующий прогон добирает остаток и на этом успокаивается
        assert retention.purge_once(now=NOW) == 1
        assert not newest.exists()
        assert retention.purge_once(now=NOW) == 0

    def test_portions_rotate_within_one_pass(self, env: RetentionEnv) -> None:
        """За один заход прокручиваются порции, пока есть что убирать.

        Одной порции в 6 часов не хватало бы на объекте с сотнями
        взвешиваний в день, а накопленный до включения уборки хвост
        рассасывался бы месяцами (замечание ревью 11.08.2026).
        """
        files = [
            env.add_photo(f"{i}.jpeg", uploaded_at=NOW - timedelta(days=40 + i))[1]
            for i in range(5)
        ]
        retention = PhotoRetention(env.storage, retention_days=30, batch=2, max_batches=25)

        assert retention.purge_once(now=NOW) == 5
        assert not any(file.exists() for file in files)

    def test_pass_limited_by_max_batches(self, env: RetentionEnv) -> None:
        """Но не бесконечно: за заход не больше batch × max_batches."""
        for i in range(5):
            env.add_photo(f"{i}.jpeg", uploaded_at=NOW - timedelta(days=40 + i))
        retention = PhotoRetention(env.storage, retention_days=30, batch=2, max_batches=2)

        assert retention.purge_once(now=NOW) == 4

    def test_thumbnail_removed_with_original(self, env: RetentionEnv) -> None:
        """Миниатюра журнала уходит вместе со своим кадром: иначе кэш рос бы
        вечно там, где уборка как раз освобождает диск (замечание ревью)."""
        _, old = env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=40))
        thumb = old.with_name(old.stem + "_thumb" + old.suffix)
        thumb.write_bytes(b"\xff\xd8thumb\xff\xd9")
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert not old.exists() and not thumb.exists()

    def test_missing_thumbnail_is_fine(self, env: RetentionEnv) -> None:
        """Миниатюры может не быть (запись не открывали) — не падаем."""
        _, old = env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)
        assert retention.purge_once(now=NOW) == 1
        assert not old.exists()

    def test_missing_file_marked_without_error(self, env: RetentionEnv) -> None:
        """Файл уже исчез (уборка сирот, ручное удаление) — не падаем и
        отмечаем строку, чтобы не возвращаться к ней каждые 6 часов."""
        uuid, path = env.add_photo("gone.jpeg", uploaded_at=NOW - timedelta(days=40))
        path.unlink()
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert env.photo_row(uuid)["file_removed"] == 1

    def test_oserror_keeps_flag_for_next_run(
        self, env: RetentionEnv, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Файл занят/нет прав → метка file_removed НЕ ставится: иначе агент
        потерял бы след файла на диске и тот остался бы навсегда."""
        uuid, path = env.add_photo("busy.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        def refuse(self: Path, missing_ok: bool = False) -> None:
            raise PermissionError(13, "файл занят другим процессом")

        monkeypatch.setattr(Path, "unlink", refuse)
        assert retention.purge_once(now=NOW) == 0
        assert env.photo_row(uuid)["file_removed"] == 0
        assert path.exists()

        # причина отпала — следующий прогон убирает файл
        monkeypatch.undo()
        assert retention.purge_once(now=NOW) == 1
        assert not path.exists()
        assert env.photo_row(uuid)["file_removed"] == 1

    def test_purged_photo_does_not_return_to_upload_queue(self, env: RetentionEnv) -> None:
        """Убранный файл не возвращается в очередь загрузки: центр его уже
        принял, повторно слать нечего (и нечем)."""
        env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert env.storage.photos_to_upload(now=NOW + timedelta(days=1)) == []
        assert env.storage.photo_queue_stats() == (0, 0)

    def test_second_run_skips_already_purged(self, env: RetentionEnv) -> None:
        """Повторная уборка не берёт уже помеченные строки (нет лишней работы)."""
        env.add_photo("old.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW) == 1
        assert retention.purge_once(now=NOW) == 0
        assert env.storage.photos_to_purge(NOW) == []


class TestPurgeTimezones:
    """Часовые пояса: срок считается по моментам, а не по виду ISO-строки
    (в БД они сравниваются посимвольно, разнобой поясов давал бы ошибку)."""

    def test_bishkek_moments_behave_like_utc(self, env: RetentionEnv) -> None:
        """Подтверждение с бишкекским поясом и уборка «по-бишкекски» дают тот
        же результат, что и чистый UTC."""
        _, path = env.add_photo(
            "tz.jpeg", uploaded_at=(NOW - timedelta(days=40)).astimezone(BISHKEK)
        )
        retention = PhotoRetention(env.storage, retention_days=30)

        # 11 дней назад: снимку ещё не исполнилось 30 дней с подтверждения
        assert retention.purge_once(now=(NOW - timedelta(days=11)).astimezone(BISHKEK)) == 0
        assert path.exists()
        assert retention.purge_once(now=NOW.astimezone(BISHKEK)) == 1
        assert not path.exists()

    def test_naive_now_treated_as_utc(self, env: RetentionEnv) -> None:
        """Наивный момент (без пояса) трактуется как UTC — как во всём агенте."""
        _, path = env.add_photo("naive.jpeg", uploaded_at=NOW - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=NOW.replace(tzinfo=None)) == 1
        assert not path.exists()


class TestLegacyUploadedAt:
    """Снимки агентов старее 11.08.2026: uploaded=1, uploaded_at IS NULL.

    Дата подтверждения неизвестна — берётся дата самой записи. Проверяем оба
    исхода: старьё всё-таки убирается (а не живёт вечно) и свежие снимки не
    исчезают разом сразу после обновления агента.
    """

    def test_null_uploaded_at_old_record_purged(self, env: RetentionEnv) -> None:
        """Запись старше срока: файл убирается, хотя uploaded_at пуст."""
        uuid, path = env.add_photo("legacy-old.jpeg")  # uploaded=1, uploaded_at NULL
        assert env.photo_row(uuid)["uploaded_at"] is None
        retention = PhotoRetention(env.storage, retention_days=30)

        # запись создана «сейчас», поэтому уборку двигаем на 40 дней вперёд
        assert retention.purge_once(now=datetime.now(UTC) + timedelta(days=40)) == 1
        assert not path.exists()

    def test_null_uploaded_at_fresh_record_kept(self, env: RetentionEnv) -> None:
        """Свежая запись: обновление агента не сносит локальные снимки разом."""
        _, path = env.add_photo("legacy-fresh.jpeg")
        retention = PhotoRetention(env.storage, retention_days=30)

        assert retention.purge_once(now=datetime.now(UTC) + timedelta(days=1)) == 0
        assert path.exists()


class TestPhotosToPurgeQuery:
    """Выборка хранилища: что именно отдаётся ретеншну."""

    def test_returns_uuid_role_and_path(self, env: RetentionEnv) -> None:
        uuid, path = env.add_photo(
            "r.jpeg", uploaded_at=NOW - timedelta(days=40), role=CameraRole.REAR
        )
        assert env.storage.photos_to_purge(NOW - timedelta(days=30)) == [
            (uuid, CameraRole.REAR, str(path))
        ]

    def test_limit_applied(self, env: RetentionEnv) -> None:
        env.add_photo("a.jpeg", uploaded_at=NOW - timedelta(days=90))
        env.add_photo("b.jpeg", uploaded_at=NOW - timedelta(days=80))
        assert len(env.storage.photos_to_purge(NOW, limit=1)) == 1

    def test_mark_file_removed_ignores_not_uploaded(self, env: RetentionEnv) -> None:
        """Пометка file_removed не ставится снимку, который ещё не в центре
        (страховка от вызова мимо ретеншна)."""
        uuid, _ = env.add_photo("pending.jpeg", uploaded=False)
        env.storage.mark_photo_file_removed(uuid, CameraRole.FRONT)
        assert env.photo_row(uuid)["file_removed"] == 0


class TestRetentionLoop:
    """Фоновая задача агента: run() / run_forever()."""

    def test_disabled_loop_waits_instead_of_exiting(self, env: RetentionEnv) -> None:
        """При retention_days = 0 цикл ждёт отмены, а НЕ завершается.

        Завершение любой фоновой задачи останавливает агента целиком
        (agent/main.py ждёт FIRST_COMPLETED), поэтому «выключено» не
        должно означать «вышли» — иначе служба с выключенной уборкой
        умирала бы сразу после старта (баг найден qa-tester 11.08.2026).
        """
        retention = PhotoRetention(env.storage, retention_days=0, interval_s=0.01)

        async def scenario() -> None:
            task = asyncio.create_task(retention.run())
            await asyncio.sleep(0.1)
            assert not task.done(), "выключенный ретеншн завершил задачу — агент бы остановился"
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())

    def test_disabled_loop_does_not_purge(self, env: RetentionEnv) -> None:
        """И, разумеется, ничего не убирает: файл на месте."""
        _, old = env.add_photo("old.jpeg", uploaded_at=datetime.now(UTC) - timedelta(days=400))
        retention = PhotoRetention(env.storage, retention_days=0, interval_s=0.01)

        async def scenario() -> None:
            task = asyncio.create_task(retention.run())
            await asyncio.sleep(0.1)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        asyncio.run(scenario())
        assert old.exists(), "выключенный ретеншн удалил файл"

    def test_loop_purges_old_files(self, env: RetentionEnv) -> None:
        """Включённый цикл убирает старый файл и снимается отменой."""
        _, old = env.add_photo("old.jpeg", uploaded_at=datetime.now(UTC) - timedelta(days=40))
        _, fresh = env.add_photo("fresh.jpeg", uploaded_at=datetime.now(UTC) - timedelta(days=1))
        retention = PhotoRetention(env.storage, retention_days=30, interval_s=0.01)

        async def scenario() -> None:
            task = asyncio.create_task(run_forever(retention))
            deadline = time.monotonic() + 5
            while old.exists():
                assert time.monotonic() < deadline, "цикл не убрал старый файл"
                await asyncio.sleep(0.01)
            task.cancel()
            await task  # run_forever гасит CancelledError сам

        asyncio.run(asyncio.wait_for(scenario(), timeout=10))
        assert fresh.exists(), "цикл убрал свежий снимок"

    def test_loop_survives_storage_failure(
        self, env: RetentionEnv, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Сбой хранилища не убивает задачу: ошибка в лог, следующий заход
        доделывает работу."""
        logging.getLogger("agent.sync.retention").disabled = False
        _, old = env.add_photo("old.jpeg", uploaded_at=datetime.now(UTC) - timedelta(days=40))
        retention = PhotoRetention(env.storage, retention_days=30, interval_s=0.01)
        original = env.storage.photos_to_purge
        calls = {"n": 0}

        def flaky(*args: Any, **kwargs: Any) -> Any:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("база занята")
            return original(*args, **kwargs)

        monkeypatch.setattr(env.storage, "photos_to_purge", flaky)

        async def scenario() -> None:
            task = asyncio.create_task(retention.run())
            deadline = time.monotonic() + 5
            while old.exists():
                assert time.monotonic() < deadline, "цикл не оправился после сбоя"
                await asyncio.sleep(0.01)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        with caplog.at_level(logging.ERROR, logger="agent.sync.retention"):
            asyncio.run(asyncio.wait_for(scenario(), timeout=10))
        assert any("сбой уборки" in r.getMessage() for r in caplog.records)
