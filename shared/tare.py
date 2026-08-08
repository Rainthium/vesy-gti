"""Правило действия тары (правило проекта №4) — общее для агента и центра.

Тара действует 3 календарных месяца с момента тарирования; единственный
источник тары — тарирования, проведённые через систему (docs/decisions.md,
07.08.2026).
"""

from datetime import datetime

TARE_VALIDITY_MONTHS = 3


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
