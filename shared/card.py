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
from shared.tare import TARE_VALIDITY_MONTHS, tare_below_gross, three_months_before

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


def _as_utc(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


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


def netto_note(
    *,
    operation: Operation,
    code_ok: bool,
    weighed_at: datetime,
    vehicle_number: str | None,
    netto: float | None,
    latest_tared_at: datetime | None,
    latest_tare_value: float | None,
    massa: float | None = None,
) -> str | None:
    """Примечание «почему нет нетто» для карты и экранов (просьба Игоря 14.08.2026).

    Только для успешных взвешиваний без нетто; расчёт и записи не трогаются —
    примечание вычисляется при показе из реестра тарирований (``latest_*`` —
    его строка по сцепке, там лежит и устаревшая тара). «Устарело» пишется,
    лишь когда тарирование действительно истекло К МОМЕНТУ ВЗВЕШИВАНИЯ
    (правило №4 той же границей, что и подстановка): это заодно отсекает
    тарирования ПОЗЖЕ записи — сцепку перетарировали после, и чужая свежая
    дата на старой карте появиться не должна: карта печатается одинаково
    в любой момент и с обеих сторон.
    """
    if operation is not Operation.WEIGHING or netto is not None or not code_ok:
        return None
    if not vehicle_number:
        return "Нетто не рассчитано: номер транспортного средства не передан."
    # граница в UTC — ровно как у подстановки (agent storage / center repo);
    # от бишкекского времени поджатие дня (31-е число) даёт иной результат
    if latest_tared_at is not None and _as_utc(latest_tared_at) < three_months_before(
        _as_utc(weighed_at)
    ):
        return (
            f"Нетто не рассчитано: тарирование сцепки от {fmt_dt(latest_tared_at)}, "
            f"тара {fmt_kg(latest_tare_value)} кг — устарело "
            f"(тара действует {TARE_VALIDITY_MONTHS} календарных месяца)."
        )
    if (
        latest_tared_at is not None
        and latest_tare_value is not None
        and massa is not None
        and _as_utc(latest_tared_at) <= _as_utc(weighed_at)
        and not tare_below_gross(latest_tare_value, massa)
    ):
        # тара не меньше брутто — тарирование сцепки ошибочно (гружёная
        # машина); агент 0.4.29 такую тару не подставляет (решение Игоря
        # 04.09.2026), карта объясняет прочерк в нетто
        return (
            f"Нетто не рассчитано: тара сцепки {fmt_kg(latest_tare_value)} кг "
            f"(тарирование от {fmt_dt(latest_tared_at)}) не меньше брутто "
            f"{fmt_kg(massa)} кг — тарирование ошибочно (гружёная машина), "
            "в расчёт не подставлено."
        )
    return "Нетто не рассчитано: действующего тарирования сцепки не было."


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
    photo_front_url: str | None,
    photo_rear_url: str | None,
    photos_note: str | None,
    record_uuid: str,
    code_ok: bool = True,
    latest_tared_at: datetime | None = None,
    latest_tare_value: float | None = None,
    annulled_note: str | None = None,
) -> dict[str, object]:
    """Контекст печатной формы card.html (вся логика прочерков — здесь).

    Для взвешивания ``massa`` — брутто, ``tared_at`` — дата использованного
    тарирования; для тарирования ``massa`` — масса тары, брутто и нетто
    печатаются прочерками. Фото ПЕРЕД/ЗАД печатаются всегда (просьба Игоря
    13.08.2026): недоступный снимок — пустая рамка, ``photos_note`` —
    предупреждение, почему снимка нет и где его взять. ``latest_*`` —
    строка реестра тарирований по сцепке (включая устаревшую тару) для
    примечания «почему нет нетто»; в таблице масс тара остаётся прочерком —
    печатная таблица совпадает с записью и ответом АИС.
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
        "netto_note": netto_note(
            operation=operation,
            code_ok=code_ok,
            weighed_at=weighed_at,
            vehicle_number=vehicle_number,
            netto=netto,
            latest_tared_at=latest_tared_at,
            latest_tare_value=latest_tare_value,
            massa=massa,
        ),
        "gross_text": fmt_kg(massa) if is_weighing else "—",
        "tare_text": fmt_kg(tare_value) if is_weighing else fmt_kg(massa),
        "netto_text": fmt_kg(netto) if is_weighing else "—",
        "operator": operator,
        "photos": [
            {"label": "Фото 1 (перед)", "url": photo_front_url},
            {"label": "Фото 2 (зад)", "url": photo_rear_url},
        ],
        "photos_note": photos_note,
        "record_uuid": record_uuid,
        # запись аннулирована сторно (04.09.2026): печатается крупно — бумага
        # на операцию, которой «как не было», не должна выглядеть действующей
        "annulled_note": annulled_note,
    }
