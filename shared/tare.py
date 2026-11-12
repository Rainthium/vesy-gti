"""Правило действия тары (правило проекта №4) — общее для агента и центра.

Тара действует 3 календарных месяца с момента тарирования; единственный
источник тары — тарирования, проведённые через систему (docs/decisions.md,
07.08.2026).
"""

import re
from datetime import UTC, datetime, timedelta, timezone

TARE_VALIDITY_MONTHS = 3
# лимит массы тарирования по умолчанию, кг (решение Игоря 04.09.2026): на
# Кокчо-Козе 23 гружёные машины (31–43 т) прошли как «тарирование», а настоящие
# тары пилотных объектов — 13–18 т; 0 — лимит выключен
DEFAULT_MAX_TARE_KG = 25_000.0

# смещение, а не zoneinfo (как в shared/card.py): на весовом ПК и в замороженной
# сборке базы tzdata может не оказаться, а модуль грузится при старте агента;
# у Бишкека переводов времени нет
_BISHKEK = timezone(timedelta(hours=6))


def three_months_before(moment: datetime) -> datetime:
    """Момент ровно на 3 календарных месяца раньше (с поджатием дня месяца).

    Правило №4 сформулировано в месяцах, а не в днях: «последняя тара этого
    номера ТС не старше 3 месяцев». 31 мая → 28/29 февраля.
    """
    month = moment.month - TARE_VALIDITY_MONTHS
    year = moment.year
    if month < 1:
        month += 12
        year -= 1
    # последний день целевого месяца (переход к 1-му числу следующего минус день)
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1, tzinfo=moment.tzinfo)
    else:
        next_month_first = datetime(year, month + 1, 1, tzinfo=moment.tzinfo)
    last_day = (next_month_first - next_month_first.resolution).day
    return moment.replace(year=year, month=month, day=min(moment.day, last_day))


def _kg(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def _date(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(_BISHKEK).strftime("%d.%m.%Y")


def tare_too_heavy(weight_kg: float, max_tare_kg: float) -> bool:
    """Лимит тары: масса при тарировании больше лимита — это гружёная машина.

    Решение Игоря 04.09.2026 после Кокчо-Коза: такое тарирование не
    проводится (агент отвечает ERR_TARE_TOO_HEAVY, записи нет).
    ``max_tare_kg`` ≤ 0 — лимит выключен.
    """
    return max_tare_kg > 0 and weight_kg > max_tare_kg


def tare_too_heavy_message(weight_kg: float, max_tare_kg: float) -> str:
    """Текст отказа — оператору АИС (поле message) и оператору весового ПК."""
    return (
        f"Масса {_kg(weight_kg)} кг больше допустимой тары {_kg(max_tare_kg)} кг — "
        "это гружёная машина: проведите взвешивание, а не тарирование"
    )


# разбор текста отказа обратно в число — пара к tare_too_heavy_message: менять
# формулировку можно только вместе с этим шаблоном (тест «туда-обратно»)
_TOO_HEAVY_MASS_RE = re.compile(r"Масса (\d[\d ]*) кг больше допустимой тары (\d[\d ]*) кг")


def parse_tare_too_heavy_mass(message: str | None) -> float | None:
    """Измеренная масса из текста отказа ERR_TARE_TOO_HEAVY, кг (целые).

    Агент при отказе вес числом не возвращает — только в тексте; центр
    достаёт его для события ``weighing.rejected`` таможенной системе
    (решение Игоря 11.11.2026). Текст не распознан — None.
    """
    parsed = parse_tare_too_heavy(message)
    return parsed[0] if parsed is not None else None


def parse_tare_too_heavy(message: str | None) -> tuple[float, float] | None:
    """(измеренная масса, применённый лимит тары) из текста отказа, кг.

    Лимит в тексте — тот, по которому отказал агент; он может отличаться от
    настройки центра (локальный конфиг весового ПК, ещё не применённый
    снимок настроек). Текст не распознан — None.
    """
    match = _TOO_HEAVY_MASS_RE.search(message or "")
    if match is None:
        return None
    mass, limit = (float(group.replace(" ", "")) for group in match.groups())
    return mass, limit


def tare_below_gross(tare_kg: float, gross_kg: float) -> bool:
    """Правдоподобие тары: подставлять можно только тару МЕНЬШЕ брутто.

    Тара не меньше брутто означает, что тарирование сцепки ошибочно
    (гружёная машина прошла как тарирование) либо пустую машину взвесили
    как брутто; нетто по такой паре — отрицательное или нулевое — не
    считается (решение Игоря 04.09.2026).
    """
    return tare_kg < gross_kg


def implausible_tare_message(tare_kg: float, tared_at: datetime, gross_kg: float) -> str:
    """Примечание в записи взвешивания, когда тара не подставлена как неправдоподобная."""
    return (
        f"Тара {_kg(tare_kg)} кг (тарирование от {_date(tared_at)}) не меньше брутто "
        f"{_kg(gross_kg)} кг — в расчёт не подставлена, нетто не рассчитано: "
        "тарирование сцепки ошибочно (так тарируют гружёную машину)"
    )
