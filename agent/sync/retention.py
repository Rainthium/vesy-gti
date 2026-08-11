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
  бэклога — фолбэк на центр).
"""

import asyncio
import contextlib
import logging
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

    @property
    def enabled(self) -> bool:
        return self._retention_days > 0

    async def run(self) -> None:
        """Цикл уборки; останавливается отменой задачи.

        Выключенная уборка не возвращает управление, а спит до отмены:
        завершение любой фоновой задачи останавливает агента целиком
        (agent/main.py задачу выключенного ретеншна и не создаёт, но
        поведение самого цикла должно быть безопасным).
        """
        if not self.enabled:
            while True:
                await asyncio.sleep(self._interval_s)
        while True:
            try:
                await asyncio.to_thread(self.purge_once)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("сбой уборки локальных фото")
            await asyncio.sleep(self._interval_s)

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
        for _ in range(self._max_batches):
            portion = self._storage.photos_to_purge(threshold, self._batch)
            if not portion:
                break
            removed_in_portion = self._purge_portion(portion)
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

    def _purge_portion(self, portion: list[tuple[UUID, CameraRole, str]]) -> int:
        """Убрать файлы одной порции; вернуть число реально удалённых."""
        removed = 0
        for weighing_uuid, role, path in portion:
            try:
                Path(path).unlink(missing_ok=True)
                # миниатюра журнала живёт при своём кадре — уходит вместе с
                # ним, иначе кэш рос бы вечно ровно там, где чистим диск
                thumb_path(Path(path)).unlink(missing_ok=True)
            except OSError as exc:
                # файл занят или нет прав — метку не ставим, попробуем в
                # следующий раз (иначе потеряли бы след файла на диске)
                logger.warning("не удалось убрать локальное фото %s: %s", path, exc)
                continue
            self._storage.mark_photo_file_removed(weighing_uuid, role)
            removed += 1
        return removed


async def run_forever(retention: PhotoRetention) -> None:
    """Запуск до отмены с подавлением CancelledError снаружи."""
    with contextlib.suppress(asyncio.CancelledError):
        await retention.run()
