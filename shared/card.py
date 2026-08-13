"""Весовая карточка: общая печатная форма агента и центра.

Печатный документ по образцу акта АИС «СВХ» (без банковских реквизитов
в шапке — решение Игоря 13.08.2026). Шаблон один на обе стороны —
``shared/templates/card.html``; агент и центр рендерят его каждый своим
Jinja-окружением, но контекст собирают функцией :func:`build_card`,
поэтому номер, даты и строка поверки одной и той же записи совпадают,
из какой бы точки её ни печатали.

Номер карточки — производный от времени операции (``ВЕС-20260813-102301``).
Сквозной счётчик, как у АИС (WEI000092948), не годится: агент печатает
и без связи с центром, а номер обязан совпадать с обеих сторон.
"""

import base64
from datetime import UTC, date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

from shared.enums import Operation
from shared.messages import VerificationInfo

TEMPLATES_DIR = Path(__file__).parent / "templates"
_LOGO_PATH = Path(__file__).parent / "assets" / "gti-logo.jpg"

# Бишкек: фиксированное UTC+6 без сезонных переводов (правило №4а).
# Смещение, а не zoneinfo: на весовом ПК базы tzdata может не оказаться.
BISHKEK_TZ = timezone(timedelta(hours=6))

_NUMBER_PREFIX = {Operation.WEIGHING: "ВЕС", Operation.TARING: "ТАР"}


def _to_bishkek(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        # naive-время в системе означает UTC (как в БД)
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(BISHKEK_TZ)


def card_number(operation: Operation, weighed_at: datetime) -> str:
    """Номер карточки: «ВЕС-20260813-102301» (дата-время операции, Бишкек)."""
    stamp = _to_bishkek(weighed_at).strftime("%Y%m%d-%H%M%S")
    return f"{_NUMBER_PREFIX[operation]}-{stamp}"


def fmt_dt(moment: datetime) -> str:
    """ДД.ММ.ГГГГ ЧЧ:ММ:СС по Бишкеку — как в журнале панели."""
    return _to_bishkek(moment).strftime("%d.%m.%Y %H:%M:%S")


def fmt_kg(value: float | None) -> str:
    """Вес целыми килограммами с разделителем тысяч; None — прочерк.

    Разделитель — обычный пробел (не U+202F, как на экранах): у шрифтов
    печати на старых весовых ПК узкого пробела может не быть в глифах.
    """
    if value is None:
        return "—"
    return f"{value:,.0f}".replace(",", " ")


def _fmt_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def verification_text(verification: VerificationInfo | None) -> str | None:
    """Строка поверки как в акте АИС: «№3961 от 26.02.2026 (срок до 26.02.2027)»."""
    if verification is None or not verification.number:
        return None
    text = verification.number
    if verification.verified_on is not None:
        text += f" от {_fmt_date(verification.verified_on)}"
    if verification.valid_until is not None:
        text += f" (срок до {_fmt_date(verification.valid_until)})"
    return text


@lru_cache(maxsize=1)
def logo_data_uri() -> str:
    """Герб ГТИ из акта АИС, встроенный data:URI — печатной странице
    не нужна статика ни агента, ни центра."""
    return "data:image/jpeg;base64," + base64.b64encode(_LOGO_PATH.read_bytes()).decode()


def build_card(
    *,
    operation: Operation,
    weighed_at: datetime,
    site_name: str,
    scale_name: str,
    vehicle_number: str | None,
    trailer_number: str | None,
    massa: float,
    tare_value: float | None,
    netto: float | None,
    tared_at: datetime | None,
    operator: str | None,
    verification: VerificationInfo | None,
    photos: list[dict[str, str]],
    photos_note: str | None,
    record_uuid: str,
) -> dict[str, object]:
    """Контекст печатной формы card.html (вся логика прочерков — здесь).

    Для взвешивания ``massa`` — брутто, ``tared_at`` — дата использованного
    тарирования; для тарирования ``massa`` — масса тары, брутто и нетто
    печатаются прочерками. ``photos`` — [{"label": ..., "url": ...}];
    ``photos_note`` — предупреждение вместо недоступных снимков.
    """
    is_weighing = operation is Operation.WEIGHING
    return {
        "number": card_number(operation, weighed_at),
        "is_weighing": is_weighing,
        "operation_label": "Взвешивание" if is_weighing else "Тарирование",
        "site_name": site_name,
        "scale_name": scale_name,
        "weighed_at_text": fmt_dt(weighed_at),
        "tared_at_text": fmt_dt(tared_at) if tared_at is not None else None,
        "vehicle_number": vehicle_number,
        "trailer_number": trailer_number,
        "verification_text": verification_text(verification),
        "massa_text": fmt_kg(massa),
        "gross_text": fmt_kg(massa) if is_weighing else "—",
        "tare_text": fmt_kg(tare_value) if is_weighing else fmt_kg(massa),
        "netto_text": fmt_kg(netto) if is_weighing else "—",
        "operator": operator,
        "photos": photos,
        "photos_note": photos_note,
        "record_uuid": record_uuid,
    }
