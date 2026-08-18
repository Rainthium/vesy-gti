"""Сводная аналитика за период — экран «Отчёты» панели (этап 4, макет 5.7).

Только чтение журнала и служебных таблиц центра; ничего не пишется.
Все функции синхронные (маршруты панели зовут их через ``asyncio.to_thread``),
берут открытую сессию и уже РАЗРЕШЁННЫЙ фильтр по объекту (маршрут сводит
выбор пользователя с PanelScope до вызова, как в журнале).

Что считается и по каким правилам:

* Период задаётся бишкекскими сутками включительно (как фильтр журнала):
  ``[date_from 00:00; date_to 24:00)`` по Asia/Bishkek, в БД сравнивается
  в UTC. Момент операции — ``coalesce(weighed_at, created_at)`` (тот же,
  что в журнале и его выгрузке).
* В расчёт идут только СОСТОЯВШИЕСЯ операции (``code = OK``; отказы АИС в
  журнал не пишутся с 10.08.2026, исторические ERR-строки скрыты и здесь);
  сторно — если появится — вычитается парой (и запись-сторно, и та, что
  она отменяет).
* «Взвешивания» — операции weighing, «тарирования» — taring; «офлайн» —
  операции любого вида с ``source = local_offline`` (ручной режим агента).
* Массы: нетто есть только у взвешиваний, куда подставилась ДЕЙСТВУЮЩАЯ
  тара; по построению Σнетто = Σбрутто − Σтары по этим же записям. Для
  остальных взвешиваний «чистого» веса нет — они показываются отдельно
  (число и брутто), их брутто в «нетто» НЕ складывается. Отчёт не считает
  нетто по устаревшей таре задним числом (решение 14.08.2026: показ ≠
  подстановка) — иначе цифра разошлась бы с записями и с ответом АИС.
* Причины «без нетто» — той же логикой, что примечание на весовой карте
  (``shared.card.netto_note``): номер ТС не передан / действующего
  тарирования не было / тарирование устарело к моменту взвешивания
  (граница правила №4 в UTC, по журналу тарирований сцепки — последнее
  не позже момента взвешивания).
* Доступность объекта — по переходам детектора «офлайн» мониторинга
  (``monitoring_events``, kind=offline: danger — пропал, ok — вернулся):
  доля времени периода, когда агент был на связи. Мониторинг работает с
  13.08.2026 — раньше событий нет и доступность считается полной; для
  весов без агента — не считается.
* Отказы команд АИС — из ``audit_log`` (weigh_request_v1/v2, code ≠ OK):
  сама запись при отказе не создаётся, но команда журналируется.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo

from sqlalchemy import and_, case, func, literal_column, select, tuple_
from sqlalchemy.orm import Session, aliased

from center.db.models import (
    Agent,
    AuditLog,
    MonitoringEvent,
    MonitoringSeverity,
    Scale,
    Site,
    TareRegistry,
    Weighing,
    WeighingAisRef,
)
from shared.enums import ErrorCode, Operation, WeighingSource
from shared.tare import three_months_before

BISHKEK = ZoneInfo("Asia/Bishkek")

Step = Literal["day", "week", "month"]

# офлайн-операция без номера АИС старше этого срока — «зависшее»
# сопоставление (АИС обычно сообщает номер в течение суток-двух)
UNLINKED_STALE_DAYS = 3
# сколько сцепок показывать в списке «к перетарированию»
RETARE_LIMIT = 20
# отказы АИС-команд: сколько кодов перечислять по объекту
REFUSAL_CODES_LIMIT = 4

# --- период ---------------------------------------------------------------


@dataclass(frozen=True)
class Period:
    """Отчётный период: бишкекские сутки включительно, в БД — UTC."""

    date_from: date
    date_to: date

    @property
    def start(self) -> datetime:
        return datetime.combine(self.date_from, datetime.min.time(), BISHKEK).astimezone(UTC)

    @property
    def end(self) -> datetime:
        """Исключающая граница: 00:00 следующих суток после date_to."""
        return datetime.combine(
            self.date_to + timedelta(days=1), datetime.min.time(), BISHKEK
        ).astimezone(UTC)

    @property
    def days(self) -> int:
        return (self.date_to - self.date_from).days + 1

    @property
    def label(self) -> str:
        if self.date_from == self.date_to:
            return self.date_from.strftime("%d.%m.%Y")
        return f"{self.date_from:%d.%m.%Y} — {self.date_to:%d.%m.%Y}"

    def step(self) -> Step:
        """Шаг графика динамики по длине периода: до 31 дня — дни,
        до полугода — недели, дальше — месяцы."""
        if self.days <= 31:
            return "day"
        if self.days <= 190:
            return "week"
        return "month"


PRESETS: tuple[tuple[str, str], ...] = (
    ("today", "Сегодня"),
    ("yesterday", "Вчера"),
    ("7d", "7 дней"),
    ("30d", "30 дней"),
    ("month", "Этот месяц"),
    ("prev_month", "Прошлый месяц"),
    ("quarter", "Этот квартал"),
    ("year", "Этот год"),
)
PRESET_KEYS = frozenset(key for key, _ in PRESETS)


def preset_period(key: str, *, today: date) -> Period | None:
    """Период по имени пресета относительно ``today`` (бишкекская дата)."""
    if key == "today":
        return Period(today, today)
    if key == "yesterday":
        day = today - timedelta(days=1)
        return Period(day, day)
    if key == "7d":
        return Period(today - timedelta(days=6), today)
    if key == "30d":
        return Period(today - timedelta(days=29), today)
    if key == "month":
        return Period(today.replace(day=1), today)
    if key == "prev_month":
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        return Period(last_prev.replace(day=1), last_prev)
    if key == "quarter":
        first_month = 3 * ((today.month - 1) // 3) + 1
        return Period(today.replace(month=first_month, day=1), today)
    if key == "year":
        return Period(today.replace(month=1, day=1), today)
    return None


def _shift_months(day: date, months: int) -> date:
    """Та же дата на ``months`` месяцев раньше (день поджимается к концу месяца)."""
    month = day.month - months
    year = day.year
    while month < 1:
        month += 12
        year -= 1
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, last))


PRESET_UNIT_MONTHS: dict[str, int] = {"month": 1, "prev_month": 1, "quarter": 3, "year": 12}


def previous_period(period: Period, *, unit_months: int | None = None) -> Period:
    """Период для сравнения «к прошлому периоду».

    ``unit_months`` — календарная единица пресета (месяц 1, квартал 3, год
    12): период сравнивается с теми же числами единицей раньше («с 1 по 18
    августа» ↔ «с 1 по 18 июля», квартал ↔ прошлый квартал). Без единицы:
    отрезок, начатый с 1-го числа, сдвигается на столько месяцев, сколько
    затрагивает (целые месяцы — тем же составом, «с 1-го по сегодня» — те же
    числа месяцем раньше); произвольный — на свою длину в днях (смежный
    отрезок той же длины).
    """
    if unit_months is not None:
        return Period(
            _shift_months(period.date_from, unit_months),
            _shift_months(period.date_to, unit_months),
        )
    if period.date_from.day == 1:
        end_next = period.date_to + timedelta(days=1)
        if end_next.day == 1:
            # целые календарные месяцы: N месяцев назад тем же составом
            months = (
                (end_next.year - period.date_from.year) * 12
                + end_next.month
                - period.date_from.month
            )
            start = _shift_months(period.date_from, months)
            return Period(start, _shift_months(end_next, months) - timedelta(days=1))
        months = (
            (period.date_to.year - period.date_from.year) * 12
            + period.date_to.month
            - period.date_from.month
            + 1
        )
        return Period(
            _shift_months(period.date_from, months), _shift_months(period.date_to, months)
        )
    length = timedelta(days=period.days)
    return Period(period.date_from - length, period.date_to - length)


# --- общие куски запросов ------------------------------------------------

moment_col = func.coalesce(Weighing.weighed_at, Weighing.created_at)
local_moment_col = func.timezone("Asia/Bishkek", moment_col)  # naive timestamp по Бишкеку


def _base_filter(period: Period, site_id: int | None) -> list[Any]:
    """Условия «состоявшаяся операция в периоде на объекте» (без сторно)."""
    storno_alias = aliased(Weighing)
    cancelled = select(storno_alias.storno_of).where(storno_alias.storno_of.is_not(None))
    conditions: list[Any] = [
        Weighing.code == ErrorCode.OK,
        Weighing.storno_of.is_(None),
        Weighing.id.not_in(cancelled),
        moment_col >= period.start,
        moment_col < period.end,
    ]
    if site_id is not None:
        conditions.append(Scale.site_id == site_id)
    return conditions


_IS_WEIGHING = Weighing.operation == Operation.WEIGHING
_IS_TARING = Weighing.operation == Operation.TARING
_IS_OFFLINE = Weighing.source == WeighingSource.LOCAL_OFFLINE
_HAS_NETTO = Weighing.netto.is_not(None)


def _agg_columns() -> list[Any]:
    """Набор агрегатов, общий для итогов, строк по объектам и точек динамики."""
    return [
        func.count(Weighing.id).filter(_IS_WEIGHING).label("weighings"),
        func.count(Weighing.id).filter(_IS_TARING).label("tarings"),
        func.count(Weighing.id).filter(_IS_OFFLINE).label("offline"),
        func.coalesce(func.sum(Weighing.netto).filter(_IS_WEIGHING), 0.0).label("netto"),
        func.coalesce(func.sum(Weighing.massa).filter(_IS_WEIGHING), 0.0).label("gross"),
        func.count(Weighing.id).filter(_IS_WEIGHING, _HAS_NETTO).label("with_tare"),
        func.coalesce(func.sum(Weighing.massa).filter(_IS_WEIGHING, _HAS_NETTO), 0.0).label(
            "gross_with_tare"
        ),
        func.coalesce(func.sum(Weighing.tare_value).filter(_IS_WEIGHING, _HAS_NETTO), 0.0).label(
            "tare_sum"
        ),
    ]


def _unlinked_condition() -> Any:
    """Офлайн-операция, по которой АИС ещё не сообщила номер документа."""
    return and_(_IS_OFFLINE, Weighing.id.not_in(select(WeighingAisRef.weighing_id)))


# --- итоги ----------------------------------------------------------------


@dataclass(frozen=True)
class Totals:
    """Счётчики периода (верхний ряд экрана + сравнение с прошлым периодом)."""

    weighings: int = 0
    tarings: int = 0
    offline: int = 0
    netto_kg: float = 0.0
    gross_kg: float = 0.0
    with_tare: int = 0
    gross_with_tare_kg: float = 0.0
    tare_sum_kg: float = 0.0
    refusals: int = 0  # отказы АИС-команд (audit_log)
    unlinked: int = 0  # офлайн-операций без номера АИС

    @property
    def operations(self) -> int:
        return self.weighings + self.tarings

    @property
    def offline_share(self) -> float | None:
        return self.offline / self.operations if self.operations else None

    @property
    def without_tare(self) -> int:
        return self.weighings - self.with_tare

    @property
    def gross_without_tare_kg(self) -> float:
        return self.gross_kg - self.gross_with_tare_kg

    @property
    def avg_gross_kg(self) -> float | None:
        return self.gross_kg / self.weighings if self.weighings else None


def _totals_from_row(row: Any, *, refusals: int = 0, unlinked: int = 0) -> Totals:
    return Totals(
        weighings=int(row.weighings),
        tarings=int(row.tarings),
        offline=int(row.offline),
        netto_kg=float(row.netto),
        gross_kg=float(row.gross),
        with_tare=int(row.with_tare),
        gross_with_tare_kg=float(row.gross_with_tare),
        tare_sum_kg=float(row.tare_sum),
        refusals=refusals,
        unlinked=unlinked,
    )


def totals(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    refusals: Refusals | None = None,
) -> Totals:
    """Итоги периода одним запросом + отказы АИС и офлайн без номера.

    ``refusals`` — уже посчитанные отказы периода (build_report считает их
    один раз на все блоки); None — посчитать здесь.
    """
    query = (
        select(*_agg_columns())
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id))
    )
    row = session.execute(query).one()
    unlinked = session.execute(
        select(func.count(Weighing.id))
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id), _unlinked_condition())
    ).scalar_one()
    if refusals is None:
        refusals = refusals_by_scale(session, period, site_id)
    return _totals_from_row(row, refusals=refusals_total(refusals), unlinked=int(unlinked))


# --- массы и причины «без нетто» -------------------------------------------

Reason = Literal["expired", "none", "no_vehicle", "not_applied"]
REASON_LABELS: dict[Reason, str] = {
    "expired": "тарирование устарело",
    "none": "действующего тарирования не было",
    "no_vehicle": "номер ТС не передан",
    "not_applied": "тара была, но не подставилась",
}


@dataclass(frozen=True)
class RetareRow:
    """Сцепка, которая в периоде взвешивалась без нетто и до сих пор без
    действующей тары — кандидат на перетарирование."""

    vehicle_number: str
    trailer_number: str | None
    weighings: int  # взвешиваний без нетто в периоде
    last_weighed_at: datetime
    last_tared_at: datetime | None  # последнее тарирование сцепки (устаревшее) или None
    last_tare_value: float | None


@dataclass(frozen=True)
class MassReport:
    """Блок «Массы» экрана: три числа + причины без нетто + список сцепок."""

    reasons: dict[Reason, int]
    retare: list[RetareRow]
    retare_total: int  # всего сцепок без действующей тары (список обрезан RETARE_LIMIT)


def _reason_rows_query(period: Period, site_id: int | None) -> Any:
    """Подзапрос: взвешивания без нетто с причиной (SQL-агрегат).

    Причина считается в БД той же границей, что у подстановки: последнее
    тарирование сцепки не позже момента взвешивания (коррелированный
    подзапрос по журналу) сравнивается с «моментом минус 3 календарных
    месяца» в UTC — месячная арифметика PostgreSQL по timestamp без пояса
    поджимает день месяца так же, как ``shared.tare.three_months_before``.
    """
    tare = aliased(Weighing)
    latest_tared = (
        select(func.max(tare.weighed_at))
        .where(
            tare.operation == Operation.TARING,
            tare.code == ErrorCode.OK,
            tare.weighed_at.is_not(None),
            tare.vehicle_number == Weighing.vehicle_number,
            func.coalesce(tare.trailer_number, "") == func.coalesce(Weighing.trailer_number, ""),
            tare.weighed_at <= moment_col,
        )
        .correlate(Weighing)
        .scalar_subquery()
    )
    vehicle_key = func.upper(func.trim(func.coalesce(Weighing.vehicle_number, "")))
    trailer_key = func.upper(func.trim(func.coalesce(Weighing.trailer_number, "")))
    inner = (
        select(
            vehicle_key.label("vehicle"),
            trailer_key.label("trailer"),
            moment_col.label("moment"),
            latest_tared.label("latest_tared_at"),
        )
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id), _IS_WEIGHING, Weighing.netto.is_(None))
        .subquery("no_netto")
    )
    # timestamp без пояса в UTC: month-арифметика не зависит от TimeZone сессии
    moment_utc = func.timezone("UTC", inner.c.moment)
    latest_utc = func.timezone("UTC", inner.c.latest_tared_at)
    reason = case(
        (inner.c.vehicle == "", "no_vehicle"),
        (inner.c.latest_tared_at.is_(None), "none"),
        (latest_utc < moment_utc - literal_column("interval '3 months'"), "expired"),
        else_="not_applied",
    ).label("reason")
    return select(inner.c.vehicle, inner.c.trailer, inner.c.moment, reason).subquery("reasons")


def mass_report(
    session: Session, period: Period, site_id: int | None = None, *, now: datetime | None = None
) -> MassReport:
    """Причины «без нетто» и сцепки к перетарированию.

    Для каждого взвешивания без нетто берётся последнее тарирование сцепки
    не позже момента взвешивания (по журналу — реестр после перетарирования
    уже не помнит прежнюю тару), и решается той же границей, что у
    подстановки: не было / устарело / было действующее, но не подставилось.
    Считается агрегатами в БД: строки в Python не поднимаются.
    """
    now = now or datetime.now(UTC)
    rows = _reason_rows_query(period, site_id)
    reasons: dict[Reason, int] = {"expired": 0, "none": 0, "no_vehicle": 0, "not_applied": 0}
    for key, count in session.execute(select(rows.c.reason, func.count()).group_by(rows.c.reason)):
        for reason in reasons:
            if reason == key:
                reasons[reason] += int(count)

    # сцепки без действующей тары: сколько раз и когда последний раз взвешивались
    candidates_query = (
        select(
            rows.c.vehicle,
            rows.c.trailer,
            func.count().label("weighings"),
            func.max(rows.c.moment).label("last_moment"),
        )
        .where(rows.c.reason.in_(("none", "expired")))
        .group_by(rows.c.vehicle, rows.c.trailer)
        .order_by(func.count().desc(), rows.c.vehicle, rows.c.trailer)
    )
    candidates = [
        (str(vehicle), str(trailer), int(count), _as_utc(last_moment))
        for vehicle, trailer, count, last_moment in session.execute(candidates_query)
    ]
    retare_rows: list[RetareRow] = []
    if candidates:
        # действующая тара СЕЙЧАС снимает сцепку с повестки (уже перетарировали)
        threshold = three_months_before(now)
        registry: dict[tuple[str, str], TareRegistry] = {}
        pairs = [(vehicle, trailer) for vehicle, trailer, _, _ in candidates]
        for chunk_start in range(0, len(pairs), 500):
            chunk = pairs[chunk_start : chunk_start + 500]
            for row in session.execute(
                select(TareRegistry).where(
                    tuple_(TareRegistry.vehicle_number, TareRegistry.trailer_number).in_(chunk)
                )
            ).scalars():
                registry[(row.vehicle_number, row.trailer_number)] = row
        for vehicle, trailer, count, last_moment in candidates:
            reg = registry.get((vehicle, trailer))
            if reg is not None and _as_utc(reg.tared_at) >= threshold:
                continue
            retare_rows.append(
                RetareRow(
                    vehicle_number=vehicle,
                    trailer_number=trailer or None,
                    weighings=count,
                    last_weighed_at=last_moment,
                    last_tared_at=reg.tared_at if reg is not None else None,
                    last_tare_value=reg.tare_value if reg is not None else None,
                )
            )
    return MassReport(
        reasons=reasons,
        retare=retare_rows[:RETARE_LIMIT],
        retare_total=len(retare_rows),
    )


# --- по объектам -----------------------------------------------------------


@dataclass(frozen=True)
class SiteRow:
    """Строка таблицы «По объектам»."""

    site_id: int
    site_name: str
    totals: Totals
    availability: float | None  # 0..1; None — нет агента/данных
    incidents: int  # событий мониторинга danger/warning в периоде


def _site_totals(
    session: Session,
    period: Period,
    site_id: int | None,
    *,
    refusals: Refusals,
    index: ScaleIndex,
) -> dict[int, Totals]:
    query = (
        select(Scale.site_id, *_agg_columns())
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id))
        .group_by(Scale.site_id)
    )
    unlinked_query = (
        select(Scale.site_id, func.count(Weighing.id))
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id), _unlinked_condition())
        .group_by(Scale.site_id)
    )
    unlinked = {int(sid): int(n) for sid, n in session.execute(unlinked_query)}
    per_site_refusals = refusals_per_site(refusals, index)
    result: dict[int, Totals] = {}
    for row in session.execute(query):
        sid = int(row.site_id)
        result[sid] = _totals_from_row(
            row, refusals=per_site_refusals.get(sid, 0), unlinked=unlinked.get(sid, 0)
        )
    return result


def _sites(session: Session, site_id: int | None) -> list[Site]:
    query = select(Site).order_by(Site.name, Site.id)
    if site_id is not None:
        query = query.where(Site.id == site_id)
    return list(session.execute(query).scalars())


def by_site(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    now: datetime | None = None,
    reliability: list[ReliabilityRow] | None = None,
    refusals: Refusals | None = None,
    index: ScaleIndex | None = None,
) -> list[SiteRow]:
    """Таблица по объектам: все объекты (и без операций тоже — нули видны).

    ``reliability``/``refusals``/``index`` — уже посчитанные блоки (build_report
    считает их один раз); None — посчитать здесь.
    """
    index = index or scale_index(session)
    if refusals is None:
        refusals = refusals_by_scale(session, period, site_id, index=index)
    if reliability is None:
        reliability = reliability_by_site(
            session, period, site_id, now=now, refusals=refusals, index=index
        )
    per_site = _site_totals(session, period, site_id, refusals=refusals, index=index)
    reliability_map = {r.site_id: r for r in reliability}
    rows: list[SiteRow] = []
    for site in _sites(session, site_id):
        rel = reliability_map.get(site.id)
        rows.append(
            SiteRow(
                site_id=site.id,
                site_name=site.name,
                totals=per_site.get(site.id, Totals()),
                availability=rel.availability if rel is not None else None,
                incidents=rel.incidents if rel is not None else 0,
            )
        )
    return rows


# --- динамика --------------------------------------------------------------


@dataclass(frozen=True)
class SeriesPoint:
    """Точка динамики: начало отрезка (бишкекская дата) и агрегаты."""

    bucket: date
    totals: Totals


@dataclass(frozen=True)
class Series:
    step: Step
    points: list[SeriesPoint]  # все отрезки периода подряд, включая пустые
    by_site: dict[int, list[SeriesPoint]] = field(default_factory=dict)


def _bucket_start(day: date, step: Step) -> date:
    if step == "day":
        return day
    if step == "week":
        return day - timedelta(days=day.weekday())
    return day.replace(day=1)


def _next_bucket(day: date, step: Step) -> date:
    if step == "day":
        return day + timedelta(days=1)
    if step == "week":
        return day + timedelta(days=7)
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def buckets(period: Period, step: Step) -> list[date]:
    """Все отрезки периода по шагу (первый может начинаться раньше периода)."""
    result: list[date] = []
    cursor = _bucket_start(period.date_from, step)
    while cursor <= period.date_to:
        result.append(cursor)
        cursor = _next_bucket(cursor, step)
    return result


def series(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    step: Step | None = None,
    split_sites: bool = False,
) -> Series:
    """Динамика по дням/неделям/месяцам; ``split_sites`` — ещё и по объектам."""
    step = step or period.step()
    bucket_col = func.date_trunc(step, local_moment_col).label("bucket")
    group_cols: list[Any] = [bucket_col]
    if split_sites:
        group_cols.append(Scale.site_id)
    query = (
        select(*group_cols, *_agg_columns())
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id))
        .group_by(*group_cols)
    )
    total_map: dict[date, Totals] = {}
    site_map: dict[int, dict[date, Totals]] = defaultdict(dict)
    for row in session.execute(query):
        bucket = row.bucket.date() if isinstance(row.bucket, datetime) else row.bucket
        item = _totals_from_row(row)
        if split_sites:
            site_map[int(row.site_id)][bucket] = item
            prev = total_map.get(bucket, Totals())
            total_map[bucket] = _sum_totals(prev, item)
        else:
            total_map[bucket] = item
    all_buckets = buckets(period, step)
    points = [SeriesPoint(b, total_map.get(b, Totals())) for b in all_buckets]
    by_site_points = {
        sid: [SeriesPoint(b, values.get(b, Totals())) for b in all_buckets]
        for sid, values in site_map.items()
    }
    return Series(step=step, points=points, by_site=by_site_points)


def _sum_totals(a: Totals, b: Totals) -> Totals:
    return Totals(
        weighings=a.weighings + b.weighings,
        tarings=a.tarings + b.tarings,
        offline=a.offline + b.offline,
        netto_kg=a.netto_kg + b.netto_kg,
        gross_kg=a.gross_kg + b.gross_kg,
        with_tare=a.with_tare + b.with_tare,
        gross_with_tare_kg=a.gross_with_tare_kg + b.gross_with_tare_kg,
        tare_sum_kg=a.tare_sum_kg + b.tare_sum_kg,
        refusals=a.refusals + b.refusals,
        unlinked=a.unlinked + b.unlinked,
    )


# --- ручные (офлайн) операции ----------------------------------------------


@dataclass(frozen=True)
class ManualRow:
    """Офлайн-операции одного оператора на одном объекте."""

    site_id: int
    site_name: str
    operator: str  # как записано агентом; пусто → «(не указан)»
    weighings: int
    tarings: int
    unlinked: int  # без номера АИС
    unlinked_stale: int  # без номера АИС старше UNLINKED_STALE_DAYS суток
    site_operations: int  # всех операций объекта в периоде — для доли

    @property
    def operations(self) -> int:
        return self.weighings + self.tarings

    @property
    def share(self) -> float | None:
        return self.operations / self.site_operations if self.site_operations else None


def manual_operations(
    session: Session, period: Period, site_id: int | None = None, *, now: datetime | None = None
) -> list[ManualRow]:
    """Контроль ручного режима: кто и сколько провёл офлайн, что без номера АИС."""
    now = now or datetime.now(UTC)
    stale_before = now - timedelta(days=UNLINKED_STALE_DAYS)
    unlinked = _unlinked_condition()
    operator_col = func.coalesce(func.nullif(func.trim(Weighing.operator), ""), "").label("op")
    query = (
        select(
            Scale.site_id,
            Site.name.label("site_name"),
            operator_col,
            func.count(Weighing.id).filter(_IS_WEIGHING).label("weighings"),
            func.count(Weighing.id).filter(_IS_TARING).label("tarings"),
            func.count(Weighing.id).filter(unlinked).label("unlinked"),
            func.count(Weighing.id).filter(unlinked, moment_col < stale_before).label("stale"),
        )
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .where(*_base_filter(period, site_id), _IS_OFFLINE)
        .group_by(Scale.site_id, Site.name, operator_col)
        .order_by(Site.name, operator_col)
    )
    site_ops_query = (
        select(Scale.site_id, func.count(Weighing.id))
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(period, site_id))
        .group_by(Scale.site_id)
    )
    site_ops = {int(sid): int(n) for sid, n in session.execute(site_ops_query)}
    rows = [
        ManualRow(
            site_id=int(r.site_id),
            site_name=r.site_name,
            operator=r.op or "",
            weighings=int(r.weighings),
            tarings=int(r.tarings),
            unlinked=int(r.unlinked),
            unlinked_stale=int(r.stale),
            site_operations=site_ops.get(int(r.site_id), 0),
        )
        for r in session.execute(query)
    ]
    rows.sort(key=lambda r: (-r.operations, r.site_name, r.operator))
    return rows


# --- надёжность: доступность, инциденты, отказы АИС -------------------------


@dataclass(frozen=True)
class ReliabilityRow:
    site_id: int
    site_name: str
    has_agent: bool
    availability: float | None  # доля времени на связи, 0..1
    offline_count: int  # сколько раз уходил в офлайн (переходов в периоде)
    offline_seconds: float
    indicator_incidents: int  # kind=no_data
    camera_incidents: int  # kind=camera_*
    other_incidents: int  # остальные danger/warning (очереди, диск, АИС, обновление)
    refusals: dict[str, int]  # код отказа → сколько раз

    @property
    def incidents(self) -> int:
        return (
            self.offline_count
            + self.indicator_incidents
            + self.camera_incidents
            + (self.other_incidents)
        )

    @property
    def refusals_total(self) -> int:
        return sum(self.refusals.values())


@dataclass(frozen=True)
class ScaleIndex:
    """Весы → объект и legacy-адрес UniServer → весы (для сопоставления
    отказов v1); читается один раз на отчёт."""

    site_of: dict[int, int]
    legacy: dict[tuple[str, int], int]


def scale_index(session: Session) -> ScaleIndex:
    rows = session.execute(
        select(Scale.id, Scale.site_id, Scale.legacy_ip, Scale.legacy_autoscale)
    ).all()
    return ScaleIndex(
        site_of={int(sid): int(site) for sid, site, _, _ in rows},
        legacy={
            (str(ip), int(autoscale)): int(sid)
            for sid, _, ip, autoscale in rows
            if ip and autoscale is not None
        },
    )


def _refusal_scale(
    scale_id: str | None, ip: str | None, autoscale: str | None, index: ScaleIndex
) -> int | None:
    """Весы, к которым относился отказ: v2 пишет scale_id, v1 — только
    legacy-адрес UniServer (ip + autoscale) из самого запроса."""
    if scale_id and scale_id.isdigit():
        return int(scale_id)
    if ip and autoscale and autoscale.isdigit():
        return index.legacy.get((ip, int(autoscale)))
    return None


Refusals = dict[int, dict[str, int]]  # scale_id → {код отказа: сколько}


def refusals_by_scale(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    index: ScaleIndex | None = None,
) -> Refusals:
    """Отказы АИС-команд по весам из audit_log (weigh_request_v1/v2, code ≠ OK).

    Фильтр по коду и выборка нужных ключей — в SQL: строки с OK (каждая
    команда АИС) в Python не поднимаются.
    """
    index = index or scale_index(session)
    code_col = AuditLog.details["code"].astext
    query = (
        select(
            code_col,
            AuditLog.details["scale_id"].astext,
            AuditLog.details["request"]["ip_address"].astext,
            AuditLog.details["request"]["autoscale"].astext,
        )
        .where(
            AuditLog.action.in_(("weigh_request_v1", "weigh_request_v2")),
            AuditLog.at >= period.start,
            AuditLog.at < period.end,
            code_col.is_not(None),
            code_col != ErrorCode.OK.value,
        )
        .order_by(AuditLog.id)
    )
    result: Refusals = defaultdict(lambda: defaultdict(int))
    for code, scale_raw, ip, autoscale in session.execute(query):
        scale_id = _refusal_scale(scale_raw, ip, autoscale, index)
        if scale_id is None or scale_id not in index.site_of:
            continue
        if site_id is not None and index.site_of[scale_id] != site_id:
            continue
        result[scale_id][str(code)] += 1
    return {k: dict(v) for k, v in result.items()}


def refusals_total(refusals: Refusals) -> int:
    return sum(sum(codes.values()) for codes in refusals.values())


def refusals_per_site(refusals: Refusals, index: ScaleIndex) -> dict[int, int]:
    """Отказы по объектам: site_id → сколько."""
    result: dict[int, int] = defaultdict(int)
    for scale_id, codes in refusals.items():
        result[index.site_of[scale_id]] += sum(codes.values())
    return dict(result)


def _offline_seconds(
    events: list[tuple[datetime, MonitoringSeverity]],
    initial_offline: bool,
    start: datetime,
    end: datetime,
) -> tuple[float, int]:
    """Секунды офлайна внутри [start; end) по переходам и число уходов в офлайн."""
    offline_since: datetime | None = start if initial_offline else None
    total = 0.0
    count = 0
    for at, severity in events:
        at = max(start, min(at, end))
        if severity is MonitoringSeverity.OK:
            if offline_since is not None:
                total += (at - offline_since).total_seconds()
                offline_since = None
        elif offline_since is None:
            offline_since = at
            count += 1
    if offline_since is not None:
        total += (end - offline_since).total_seconds()
    return total, count


def reliability_by_site(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    now: datetime | None = None,
    refusals: Refusals | None = None,
    index: ScaleIndex | None = None,
) -> list[ReliabilityRow]:
    """Доступность и инциденты по объектам за период.

    Офлайн считается по каждым весам отдельно (переходы детектора offline
    внутри периода плюс состояние на его начало); доступность объекта с
    несколькими весами — среднее по его весам с агентами. Период
    обрезается «сейчас»: будущее в знаменатель не идёт. Оценка: детектор
    пишет переходы с шагом 30 с, а «возвращение» в первые минуты после
    рестарта центра может остаться без события ok — тогда офлайн считается
    до конца периода (об оценочности сказано на экране и в выгрузке).
    """
    now = now or datetime.now(UTC)
    start = period.start
    end = min(period.end, now)
    scales_query = (
        select(Scale, Site, Agent.id)
        .join(Site, Site.id == Scale.site_id)
        .outerjoin(Agent, Agent.scale_id == Scale.id)
        .order_by(Site.name, Site.id, Scale.name, Scale.id)
    )
    if site_id is not None:
        scales_query = scales_query.where(Scale.site_id == site_id)
    scale_rows = session.execute(scales_query).all()
    if not scale_rows:
        return []
    scale_ids = [scale.id for scale, _, _ in scale_rows]

    # состояние «офлайн» на начало периода: последнее событие offline до start
    last_before = (
        select(MonitoringEvent.scale_id, MonitoringEvent.severity)
        .distinct(MonitoringEvent.scale_id)
        .where(
            MonitoringEvent.kind == "offline",
            MonitoringEvent.scale_id.in_(scale_ids),
            MonitoringEvent.created_at < start,
        )
        .order_by(
            MonitoringEvent.scale_id, MonitoringEvent.created_at.desc(), MonitoringEvent.id.desc()
        )
    )
    initial_offline = {
        int(sid): severity is not MonitoringSeverity.OK
        for sid, severity in session.execute(last_before)
    }
    in_period = (
        select(
            MonitoringEvent.scale_id,
            MonitoringEvent.kind,
            MonitoringEvent.severity,
            MonitoringEvent.created_at,
        )
        .where(
            MonitoringEvent.scale_id.in_(scale_ids),
            MonitoringEvent.created_at >= start,
            MonitoringEvent.created_at < period.end,
        )
        .order_by(MonitoringEvent.created_at, MonitoringEvent.id)
    )
    offline_events: dict[int, list[tuple[datetime, MonitoringSeverity]]] = defaultdict(list)
    incidents: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for sid, kind, severity, created_at in session.execute(in_period):
        sid = int(sid)
        if kind == "offline":
            offline_events[sid].append((_as_utc(created_at), severity))
        elif severity is not MonitoringSeverity.OK:
            incidents[sid][str(kind)] += 1
    if refusals is None:
        refusals = refusals_by_scale(session, period, site_id, index=index)

    length = (end - start).total_seconds()
    per_site: dict[int, dict[str, Any]] = {}
    for scale, site, agent_id in scale_rows:
        bucket = per_site.setdefault(
            site.id,
            {
                "name": site.name,
                "agents": 0,
                "avail": [],
                "offline_count": 0,
                "offline_seconds": 0.0,
                "indicator": 0,
                "camera": 0,
                "other": 0,
                "refusals": defaultdict(int),
            },
        )
        for code, n in refusals.get(scale.id, {}).items():
            bucket["refusals"][code] += n
        for kind, n in incidents.get(scale.id, {}).items():
            if kind == "no_data":
                bucket["indicator"] += n
            elif kind.startswith("camera"):
                bucket["camera"] += n
            else:
                bucket["other"] += n
        if agent_id is None:
            continue
        bucket["agents"] += 1
        if length <= 0:
            continue
        seconds, count = _offline_seconds(
            offline_events.get(scale.id, []),
            initial_offline.get(scale.id, False),
            start,
            end,
        )
        bucket["offline_seconds"] += seconds
        bucket["offline_count"] += count
        bucket["avail"].append(max(0.0, 1.0 - seconds / length))

    result: list[ReliabilityRow] = []
    for sid, data in per_site.items():
        avail_list = data["avail"]
        availability = sum(avail_list) / len(avail_list) if avail_list else None
        result.append(
            ReliabilityRow(
                site_id=sid,
                site_name=data["name"],
                has_agent=data["agents"] > 0,
                availability=availability,
                offline_count=int(data["offline_count"]),
                offline_seconds=float(data["offline_seconds"]),
                indicator_incidents=int(data["indicator"]),
                camera_incidents=int(data["camera"]),
                other_incidents=int(data["other"]),
                refusals=dict(sorted(data["refusals"].items(), key=lambda kv: (-kv[1], kv[0]))),
            )
        )
    return result


# --- объёмы для дашборда -----------------------------------------------------


@dataclass(frozen=True)
class VolumeCard:
    """Карточка объёмов на дашборде: период, счётчики и прошлый период для Δ."""

    key: str  # today | week | month
    label: str
    period: Period
    weighings: int
    tarings: int
    offline: int
    netto_kg: float
    previous: Period | None = None
    prev_weighings: int | None = None
    prev_netto_kg: float | None = None

    @property
    def weighings_delta(self) -> Delta | None:
        if self.prev_weighings is None:
            return None
        return Delta(self.weighings, self.prev_weighings)


def volume_summary(session: Session, site_id: int | None, *, today: date) -> list[VolumeCard]:
    """Объёмы «сегодня / 7 дней / этот месяц» одним запросом (дашборд, этап 4).

    Дашборд опрашивается каждые 10 с, поэтому все пять окон (три текущих
    и два прошлых для Δ) считаются одним проходом по журналу с FILTER по
    окну; правила те же, что у отчёта (состоявшиеся операции, момент по
    Бишкеку, сторно парой).
    """
    week = Period(today - timedelta(days=6), today)
    month = Period(today.replace(day=1), today)
    windows: dict[str, Period] = {
        "today": Period(today, today),
        "week": week,
        "prev_week": Period(today - timedelta(days=13), today - timedelta(days=7)),
        "month": month,
        "prev_month": previous_period(month, unit_months=1),
    }
    span = Period(
        min(p.date_from for p in windows.values()), max(p.date_to for p in windows.values())
    )
    columns: list[Any] = []
    for key, period in windows.items():
        in_window = and_(moment_col >= period.start, moment_col < period.end)
        columns.extend(
            [
                func.count(Weighing.id).filter(in_window, _IS_WEIGHING).label(f"{key}_w"),
                func.count(Weighing.id).filter(in_window, _IS_TARING).label(f"{key}_t"),
                func.count(Weighing.id).filter(in_window, _IS_OFFLINE).label(f"{key}_o"),
                func.coalesce(func.sum(Weighing.netto).filter(in_window, _IS_WEIGHING), 0.0).label(
                    f"{key}_n"
                ),
            ]
        )
    row = session.execute(
        select(*columns)
        .select_from(Weighing)
        .join(Scale, Scale.id == Weighing.scale_id)
        .where(*_base_filter(span, site_id))
    ).one()
    values = row._mapping

    def card(key: str, label: str, prev_key: str | None) -> VolumeCard:
        return VolumeCard(
            key=key,
            label=label,
            period=windows[key],
            weighings=int(values[f"{key}_w"]),
            tarings=int(values[f"{key}_t"]),
            offline=int(values[f"{key}_o"]),
            netto_kg=float(values[f"{key}_n"]),
            previous=windows[prev_key] if prev_key else None,
            prev_weighings=int(values[f"{prev_key}_w"]) if prev_key else None,
            prev_netto_kg=float(values[f"{prev_key}_n"]) if prev_key else None,
        )

    return [
        card("today", "Сегодня", None),
        card("week", "7 дней", "prev_week"),
        card("month", "Этот месяц", "prev_month"),
    ]


# --- сравнение с прошлым периодом --------------------------------------------


@dataclass(frozen=True)
class Delta:
    """Изменение показателя к прошлому периоду."""

    current: float
    previous: float

    @property
    def change(self) -> float | None:
        """Доля изменения (0.12 = +12 %); None — не с чем сравнивать."""
        if not self.previous:
            return None
        return (self.current - self.previous) / self.previous

    @property
    def diff(self) -> float:
        return self.current - self.previous


@dataclass(frozen=True)
class Comparison:
    period: Period  # прошлый период
    weighings: Delta
    tarings: Delta
    offline: Delta
    netto_kg: Delta


def comparison(
    session: Session,
    period: Period,
    current: Totals,
    site_id: int | None = None,
    *,
    previous: Period | None = None,
) -> Comparison:
    prev_period = previous or previous_period(period)
    prev = totals(session, prev_period, site_id)
    return Comparison(
        period=prev_period,
        weighings=Delta(current.weighings, prev.weighings),
        tarings=Delta(current.tarings, prev.tarings),
        offline=Delta(current.offline, prev.offline),
        netto_kg=Delta(current.netto_kg, prev.netto_kg),
    )


# --- всё разом --------------------------------------------------------------


@dataclass(frozen=True)
class Report:
    period: Period
    site_id: int | None
    generated_at: datetime
    totals: Totals
    masses: MassReport
    sites: list[SiteRow]
    dynamics: Series
    manual: list[ManualRow]
    reliability: list[ReliabilityRow]
    comparison: Comparison


def build_report(
    session: Session,
    period: Period,
    site_id: int | None = None,
    *,
    split_sites: bool = False,
    previous: Period | None = None,
    now: datetime | None = None,
) -> Report:
    """Собрать все блоки экрана одним вызовом (маршрут и экспорт).

    Общие для блоков вещи — справочник весов, отказы АИС и надёжность —
    считаются один раз и передаются вниз (иначе audit_log и весы читались
    бы по нескольку раз на страницу).
    """
    now = now or datetime.now(UTC)
    index = scale_index(session)
    refusals = refusals_by_scale(session, period, site_id, index=index)
    current = totals(session, period, site_id, refusals=refusals)
    reliability = reliability_by_site(
        session, period, site_id, now=now, refusals=refusals, index=index
    )
    return Report(
        period=period,
        site_id=site_id,
        generated_at=now,
        totals=current,
        masses=mass_report(session, period, site_id, now=now),
        sites=by_site(
            session,
            period,
            site_id,
            now=now,
            reliability=reliability,
            refusals=refusals,
            index=index,
        ),
        dynamics=series(session, period, site_id, split_sites=split_sites),
        manual=manual_operations(session, period, site_id, now=now),
        reliability=reliability,
        comparison=comparison(session, period, current, site_id, previous=previous),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
