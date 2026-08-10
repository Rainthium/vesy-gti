"""Время взвешивания от центра (вопрос Игоря 10.08.2026).

Часы весовых ПК никто не обслуживает и они уходят; часы ВМ центра
синхронизированы NTP. Агент по каждому heartbeat_ack считает смещение
своих часов относительно центра и ставит в записи скорректированное
время — единое по всем 13 объектам.

Оффлайн — работаем от последнего известного смещения (оно сохраняется
в SQLite и переживает рестарт); если связи не было вообще — локальные
часы. Задержкой канала пренебрегаем: внутренняя сеть, heartbeat раз
в 5 с, точность в доли секунды для журнала достаточна.
"""

import logging
import threading
from datetime import UTC, datetime, timedelta

from agent.sync.storage import AgentStorage

logger = logging.getLogger(__name__)

# лог о заметном расхождении часов — не чаще, чем при изменении на столько
DRIFT_LOG_THRESHOLD_S = 5.0


class CenterClock:
    """Скорректированные часы: локальное время + смещение до центра."""

    def __init__(self, storage: AgentStorage | None = None) -> None:
        self._lock = threading.Lock()
        self._storage = storage
        self._offset = timedelta(0)
        self._synced = False
        if storage is not None:
            stored = storage.load_clock_offset_s()
            if stored is not None:
                self._offset = timedelta(seconds=stored)
                self._synced = True
                logger.info("смещение часов из прошлой сессии: %+.1f с", stored)

    @property
    def synced(self) -> bool:
        with self._lock:
            return self._synced

    @property
    def offset_s(self) -> float:
        with self._lock:
            return self._offset.total_seconds()

    def set_server_time(self, server_time: datetime) -> None:
        """Обновить смещение по heartbeat_ack центра."""
        offset = server_time - datetime.now(UTC)
        with self._lock:
            drift = abs((offset - self._offset).total_seconds())
            first = not self._synced
            self._offset = offset
            self._synced = True
        if first or drift >= DRIFT_LOG_THRESHOLD_S:
            logger.info("смещение часов относительно центра: %+.1f с", offset.total_seconds())
        if self._storage is not None:
            self._storage.save_clock_offset_s(offset.total_seconds())

    def now(self) -> datetime:
        """Текущее время по часам центра (UTC)."""
        with self._lock:
            return datetime.now(UTC) + self._offset
