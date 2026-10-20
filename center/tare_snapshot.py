"""Снимок реестра тарирований для агентов с пределом размера сообщения.

Инцидент 20.10.2026: центр шлёт реестр ЦЕЛИКОМ (hello и широковещательно
после каждого тарирования), снимок перерос 1 МиБ — предел ``websockets``
у агентов до 0.4.36 — и все девять агентов рвали соединение сразу после
hello, переподключаясь каждые 5 с. Здесь снимок сериализуется компактно
и, если агент старый, режется под его предел: самые НОВЫЕ тарирования
остаются, самые старые отбрасываются. Режется по байтам, а не по сроку:
среди отброшенных могут быть и тары моложе 3 месяцев — старый агент не
подставит их только в РУЧНОМ режиме (при связи тару присылает центр в
команде). Переходное состояние до обновления агентов; каждое урезание —
предупреждение в лог с датой самой свежей отброшенной записи.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from functools import cached_property

from shared.messages import (
    AGENT_WS_LEGACY_MAX_MESSAGE_BYTES,
    AGENT_WS_MAX_MESSAGE_BYTES,
    TareRecord,
    TareRegistryUpdate,
    supports_large_registry,
)

logger = logging.getLogger(__name__)

# Запас от предела: websockets сравнивает длину РАСПАКОВАННОГО сообщения
# в байтах UTF-8, а номера машин могут содержать не-ASCII — считаем байты,
# но оставляем 10 % на всякий случай
SAFETY_RATIO = 0.9


def _payload(records: Sequence[TareRecord]) -> str:
    # exclude_none: у тарирований без прицепа не шлём "trailer_number": null —
    # агент читает отсутствие поля как None (значение по умолчанию модели)
    return TareRegistryUpdate(records=list(records)).model_dump_json(exclude_none=True)


class TareSnapshot:
    """Один снимок реестра, сериализуемый под предел конкретного агента."""

    def __init__(self, records: Sequence[TareRecord]) -> None:
        # новые первыми: при урезании отбрасывается хвост — самые старые
        self._records = sorted(records, key=lambda r: r.tared_at, reverse=True)

    @property
    def total(self) -> int:
        return len(self._records)

    @cached_property
    def full_payload(self) -> str:
        return _payload(self._records)

    @cached_property
    def legacy_payload(self) -> str:
        return self._fit(int(AGENT_WS_LEGACY_MAX_MESSAGE_BYTES * SAFETY_RATIO))

    def payload_for(self, agent_version: str | None) -> str:
        """Текст сообщения ``tare_registry`` под предел агента этой версии."""
        if supports_large_registry(agent_version):
            payload = self.full_payload
            if len(payload.encode("utf-8")) <= AGENT_WS_MAX_MESSAGE_BYTES * SAFETY_RATIO:
                return payload
            return self._fit(int(AGENT_WS_MAX_MESSAGE_BYTES * SAFETY_RATIO))
        return self.legacy_payload

    def _fit(self, max_bytes: int) -> str:
        payload = self.full_payload
        size = len(payload.encode("utf-8"))
        if size <= max_bytes:
            return payload
        records = self._records
        kept = len(records)
        while kept > 0 and size > max_bytes:
            # пропорциональная оценка числа записей, что влезают, с запасом
            per_record = size / kept
            target = int(max_bytes / per_record * 0.98)
            kept = min(kept - 1, max(target, 0))
            payload = _payload(records[:kept])
            size = len(payload.encode("utf-8"))
        newest_dropped = records[kept].tared_at if kept < len(records) else None
        logger.warning(
            "реестр тар: снимок урезан под предел %d байт — отправлено %d из %d записей, "
            "отброшены тарирования до %s включительно; обновите агентов до 0.4.36",
            max_bytes,
            kept,
            self.total,
            newest_dropped.isoformat(timespec="minutes") if newest_dropped else "—",
        )
        return payload
