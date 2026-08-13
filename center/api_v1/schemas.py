"""Схемы совместимого API v1 (docs/contracts/ais-api-v1.md).

Формат дат — ISO 8601 с явным поясом +06:00 (Бишкек, без сезонных
переводов): решение от 07.08.2026, правило проекта №4а.
"""

from datetime import UTC, datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict

from shared.enums import Operation

# Бишкек: фиксированное смещение UTC+6, переходов на летнее время нет
BISHKEK_TZ = timezone(timedelta(hours=6), name="+06:00")


def bishkek_iso(moment: datetime | None) -> str | None:
    """Дата-время для ответов v1: локальное бишкекское с поясом +06:00."""
    if moment is None:
        return None
    if moment.tzinfo is None:
        # naive считаем UTC (в БД время хранится в UTC)
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(BISHKEK_TZ).isoformat(timespec="seconds")


class WeighV1Request(BaseModel):
    """Запрос АИС «СВХ» (текущий контракт + согласованные новые поля).

    Неизвестные поля игнорируются — старый клиент может слать что угодно.
    """

    model_config = ConfigDict(extra="ignore")

    ip_address: str
    port: int
    username: str
    password: str
    autoscale: int
    # НОВЫЕ поля (АИС реализует запрос по нашему контракту);
    # отсутствие operation = weighing
    operation: Operation = Operation.WEIGHING
    vehicle_number: str | None = None
    trailer_number: str | None = None
    # ФИО оператора весового контроля: пишется в запись и печатается
    # на весовой карточке; без поля запись остаётся без оператора
    operator: str | None = None
