"""Ретеншн локальных фото агента (задача бэклога, сделано 11.08.2026).

Снимки — доказательство операции, и в центре они хранятся 5 лет
(правило №2). На весовом ПК держать их вечно не нужно: диск не резиновый,
а за месяц работы объекта набегают гигабайты. Поэтому файл удаляется
с агента через ``retention_days`` дней ПОСЛЕ подтверждённой загрузки
в центр (``uploaded = 1``), и только его — метаданные (путь, sha256,
размер) остаются в журнале агента навсегда.

Что важно:
- незагруженные снимки не трогаются никогда, сколько бы им ни было лет:
  пока центр их не принял, единственный экземпляр лежит здесь;
- ``retention_days = 0`` полностью отключает уборку;
- запись в журнале агента остаётся; когда локального файла уже нет,
  интерфейс оператора возьмёт снимок из центра (отдельная задача
  бэклога — фолбэк на центр);
- с 0.4.25 срок задаётся и из центра (снимок настроек весов, на лету), а
  кнопка «Освободить место» на карточке весов убирает ВСЕ подтверждённые
  снимки сразу — вместо удаления записей за период, которое запрещает
  правило №2 (решение Игоря 02.09.2026).
"""

import asyncio
import contextlib
import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from agent.photos import thumb_path
from agent.sync.storage import AgentStorage
from shared.enums import CameraRole

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 6 * 3600.0  # проверять четыре раза в сутки — не гонка
DEFAULT_BATCH = 200
# за один заход прокручиваем до 25 порций (5000 файлов): иначе хвост,
# накопленный до включения уборки, рассасывался бы месяцами
DEFAULT_MAX_BATCHES = 25


@dataclass(frozen=True)
class CleanupResult:
    """Итог принудительной уборки: сколько кадров убрано и сколько байт
    освобождено (кадры + миниатюры)."""

    removed_files: int
    freed_bytes: int


class PhotoRetention:
    """Периодическая уборка локальных файлов уже загруженных снимков."""

    def __init__(
        self,
        storage: AgentStorage,
        *,
        retention_days: int,
        interval_s: float = DEFAULT_INTERVAL_S,
        batch: int = DEFAULT_BATCH,
        max_batches: int = DEFAULT_MAX_BATCHES,
    ) -> None:
        self._storage = storage
        self._retention_days = retention_days
        self._interval_s = interval_s
        self._batch = batch
        self._max_batches = max_batches
        # плановая и принудительная уборки идут в потоках: одна за раз,
        # иначе обе брали бы одну порцию и спорили за файлы
        self._lock = threading.Lock()
        # будильник цикла: смена срока из центра не ждёт следующего захода
        self._wake: asyncio.Event | None = None

    @property
    def enabled(self) -> bool:
        return self._retention_days > 0

    @property
    def retention_days(self) -> int:
        return self._retention_days

    def set_retention_days(self, days: int) -> None:
        """Сменить срок на лету (снимок настроек центра, 0.4.25); 0 — выключить.

        Цикл будится сразу: уменьшенный срок должен подействовать сейчас,
        а не через шесть часов.
        """
        if days < 0:
            raise ValueError("срок хранения не может быть отрицательным")
        if days == self._retention_days:
            return
        logger.info("ретеншн локальных фото: срок %d → %d дней", self._retention_days, days)
        self._retention_days = days
        if self._wake is not None:
            self._wake.set()

    async def run(self) -> None:
        """Цикл уборки; останавливается отменой задачи.

        Выключенная уборка не возвращает управление: завершение любой
        фоновой задачи останавливает агента целиком. Срок может измениться
        на лету (настройки центра), поэтому условие проверяется на каждом
        заходе, а не один раз при старте.
        """
        self._wake = asyncio.Event()
        while True:
            # сигнал снимается ДО уборки: смена срока, пришедшая пока
            # purge_once крутится в потоке, не должна стираться перед
            # ожиданием — иначе новый срок ждал бы весь интервал (ревью 02.09)
            self._wake.clear()
            if self.enabled:
                try:
                    await asyncio.to_thread(self.purge_once)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("сбой уборки локальных фото")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval_s)

    def purge_once(self, *, now: datetime | None = None) -> int:
        """Удалить файлы старше срока; вернуть число убранных.

        Порции прокручиваются, пока есть что убирать (не больше
        ``max_batches`` за заход): при 300 взвешиваниях в день одной
        порции в 6 часов не хватило бы, а хвост, накопленный до включения
        уборки, рассасывался бы месяцами (замечание ревью 11.08.2026).
        """
        if not self.enabled:
            return 0
        moment = now or datetime.now(UTC)
        threshold = moment - timedelta(days=self._retention_days)
        removed = 0
        with self._lock:
            for _ in range(self._max_batches):
                portion = self._storage.photos_to_purge(threshold, self._batch)
                if not portion:
                    break
                removed_in_portion, _ = self._purge_portion(portion)
                removed += removed_in_portion
                if removed_in_portion == 0:
                    # ни один файл не поддался (заняты, нет прав) — следующая
                    # порция будет той же самой, крутить бессмысленно
                    break
        if removed:
            logger.info(
                "ретеншн локальных фото: убрано %d файлов старше %d дней (в центре они есть)",
                removed,
                self._retention_days,
            )
        return removed

    def cleanup_now(self, *, now: datetime | None = None) -> CleanupResult:
        """Убрать ВСЕ уже принятые центром снимки, не дожидаясь срока.

        Кнопка «Освободить место» на карточке весов (02.09.2026): работает
        и при выключенной плановой уборке — это осознанное действие
        диспетчера, когда диск весового ПК забит. Неподтверждённые снимки
        не трогаются, как и всегда; порций не ограничиваем — команда ждёт
        полного результата, а не «сколько успели».
        """
        moment = now or datetime.now(UTC)
        removed = 0
        freed = 0
        with self._lock:
            while True:
                portion = self._storage.photos_to_purge(moment, self._batch)
                if not portion:
                    break
                removed_in_portion, freed_in_portion = self._purge_portion(portion)
                removed += removed_in_portion
                freed += freed_in_portion
                if removed_in_portion == 0:
                    break  # та же порция вернётся снова — файлы не поддаются
        logger.info(
            "уборка по команде центра: убрано %d файлов, освобождено %d МБ",
            removed,
            freed // (1024 * 1024),
        )
        return CleanupResult(removed_files=removed, freed_bytes=freed)

    def _purge_portion(self, portion: list[tuple[UUID, CameraRole, str]]) -> tuple[int, int]:
        """Убрать файлы одной порции; вернуть (сколько удалено, сколько байт)."""
        removed = 0
        freed = 0
        for weighing_uuid, role, path in portion:
            frame = Path(path)
            thumb = thumb_path(frame)
            size = _file_size(frame) + _file_size(thumb)
            try:
                frame.unlink(missing_ok=True)
                # миниатюра журнала живёт при своём кадре — уходит вместе с
                # ним, иначе кэш рос бы вечно ровно там, где чистим диск
                thumb.unlink(missing_ok=True)
            except OSError as exc:
                # файл занят или нет прав — метку не ставим, попробуем в
                # следующий раз (иначе потеряли бы след файла на диске)
                logger.warning("не удалось убрать локальное фото %s: %s", path, exc)
                continue
            self._storage.mark_photo_file_removed(weighing_uuid, role)
            removed += 1
            freed += size
        return removed, freed


def _file_size(path: Path) -> int:
    """Размер файла до удаления; нет файла или нет доступа — 0."""
    try:
        return path.stat().st_size
    except OSError:
        return 0


async def run_forever(retention: PhotoRetention) -> None:
    """Запуск до отмены с подавлением CancelledError снаружи."""
    with contextlib.suppress(asyncio.CancelledError):
        await retention.run()
