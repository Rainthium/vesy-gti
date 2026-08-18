"""Экран «Отчёты»: разбор параметров запроса и подготовка данных к показу.

Маршрут панели тонкий: здесь — период из пресета/дат, подписи отрезков
динамики, SVG-графики (``center.web.charts``) и строка запроса для ссылок
«Печать / Экспорт», чтобы те открывались с теми же фильтрами.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

from center import reports
from center.web import charts

# произвольный период длиннее — режем (запрос по журналу за годы не нужен
# ни экрану, ни Excel; таблица растёт с тиражом)
MAX_PERIOD_DAYS = 3 * 366
# нижний предел дат из адреса: раньше системы данных нет, а «0001-01-01»
# роняет арифметику периодов (OverflowError/ValueError вместо страницы)
MIN_DATE = date(2000, 1, 1)
DEFAULT_PRESET = "month"

MONTHS_SHORT = ("янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек")


@dataclass(frozen=True)
class ReportQuery:
    """Разобранные параметры экрана (уже сведённые с PanelScope)."""

    period: reports.Period
    preset: str  # ключ пресета или "custom"
    site_id: int | None
    split_sites: bool
    unit_months: int | None  # календарная единица пресета для сравнения

    @property
    def previous(self) -> reports.Period:
        return reports.previous_period(self.period, unit_months=self.unit_months)

    def query_string(self, **extra: str) -> str:
        """Параметры для ссылок печати/экспорта: те же фильтры."""
        params: dict[str, str] = {}
        if self.preset != "custom":
            params["preset"] = self.preset
        else:
            params["date_from"] = self.period.date_from.isoformat()
            params["date_to"] = self.period.date_to.isoformat()
        if self.site_id is not None:
            params["site_id"] = str(self.site_id)
        if self.split_sites:
            params["split"] = "sites"
        params.update(extra)
        return urlencode(params)


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw.strip())
    except ValueError:
        return None


def resolve_query(
    *,
    preset: str | None,
    date_from: str | None,
    date_to: str | None,
    site_id: int | None,
    split: str | None,
    today: date,
) -> ReportQuery:
    """Период экрана: пресет сильнее дат; даты — включительно, кривые
    значения → пресет по умолчанию; будущее и слишком длинное — обрезается."""
    preset_key = (preset or "").strip()
    period: reports.Period | None = None
    unit_months: int | None = None
    if preset_key in reports.PRESET_KEYS:
        period = reports.preset_period(preset_key, today=today)
        unit_months = reports.PRESET_UNIT_MONTHS.get(preset_key)
    else:
        start = _parse_date(date_from)
        end = _parse_date(date_to)
        if start or end:
            start = start or end
            end = end or start
            assert start is not None and end is not None
            if start > end:
                start, end = end, start
            end = min(max(end, MIN_DATE), max(today, MIN_DATE))
            start = min(max(start, MIN_DATE), end)
            if (end - start).days + 1 > MAX_PERIOD_DAYS:
                start = end - timedelta(days=MAX_PERIOD_DAYS - 1)
            period = reports.Period(start, end)
            preset_key = "custom"
    if period is None:
        preset_key = DEFAULT_PRESET
        period = reports.preset_period(DEFAULT_PRESET, today=today)
        unit_months = reports.PRESET_UNIT_MONTHS.get(DEFAULT_PRESET)
    assert period is not None
    return ReportQuery(
        period=period,
        preset=preset_key,
        site_id=site_id,
        split_sites=(split or "") == "sites",
        unit_months=unit_months,
    )


def bucket_label(bucket: date, step: reports.Step) -> str:
    """Подпись отрезка на оси: день/неделя — «18.08», месяц — «авг 2026»."""
    if step == "month":
        return f"{MONTHS_SHORT[bucket.month - 1]} {bucket.year}"
    return bucket.strftime("%d.%m")


def bucket_title(bucket: date, step: reports.Step) -> str:
    """Подпись отрезка в таблицах и экспорте (полная)."""
    if step == "day":
        return bucket.strftime("%d.%m.%Y")
    if step == "week":
        end = bucket + timedelta(days=6)
        return f"{bucket:%d.%m.%Y} — {end:%d.%m.%Y}"
    return f"{MONTHS_SHORT[bucket.month - 1]} {bucket.year}"


STEP_LABELS: dict[reports.Step, str] = {
    "day": "по дням",
    "week": "по неделям",
    "month": "по месяцам",
}


def build_charts(report: reports.Report) -> dict[str, str]:
    """SVG-графики экрана: по объектам (взвешивания и нетто) и динамика."""
    ordered = sorted(report.sites, key=lambda r: (-r.totals.weighings, r.site_name))
    sites_weighings = charts.bar_chart_horizontal(
        [(row.site_name, float(row.totals.weighings)) for row in ordered],
        title="Взвешиваний по объектам",
    )
    sites_netto = charts.bar_chart_horizontal(
        [(row.site_name, row.totals.netto_kg / 1000) for row in ordered],
        unit="т",
        decimals=1,
        color=charts.PALETTE[1],
        title="Нетто по объектам, т",
    )
    step = report.dynamics.step
    labels = [bucket_label(point.bucket, step) for point in report.dynamics.points]
    result = {"sites_weighings": sites_weighings, "sites_netto": sites_netto}
    if report.dynamics.by_site:
        names = {row.site_id: row.site_name for row in report.sites}
        series = sorted(
            (
                (
                    names.get(site_id, f"объект {site_id}"),
                    [float(p.totals.weighings) for p in points],
                )
                for site_id, points in report.dynamics.by_site.items()
            ),
            key=lambda item: (-sum(item[1]), item[0]),
        )
        result["dynamics_weighings"] = charts.line_chart(
            labels, series, title="Взвешиваний по объектам"
        )
        netto_series = sorted(
            (
                (
                    names.get(site_id, f"объект {site_id}"),
                    [p.totals.netto_kg / 1000 for p in points],
                )
                for site_id, points in report.dynamics.by_site.items()
            ),
            key=lambda item: (-sum(item[1]), item[0]),
        )
        result["dynamics_netto"] = charts.line_chart(
            labels, netto_series, unit="т", decimals=1, title="Нетто по объектам, т"
        )
    else:
        result["dynamics_weighings"] = charts.column_chart(
            labels,
            [float(p.totals.weighings) for p in report.dynamics.points],
            title="Взвешиваний",
        )
        result["dynamics_netto"] = charts.column_chart(
            labels,
            [p.totals.netto_kg / 1000 for p in report.dynamics.points],
            unit="т",
            decimals=1,
            color=charts.PALETTE[1],
            title="Нетто, т",
        )
    return result


def fmt_hours(seconds: float) -> str:
    """Длительность офлайна: «3 ч 05 мин», «12 мин», «—» при нуле."""
    total = round(seconds / 60)
    if total <= 0:
        return "—"
    hours, minutes = divmod(total, 60)
    if hours:
        return f"{hours} ч {minutes:02d} мин"
    return f"{minutes} мин"


def fmt_share(value: float | None, *, decimals: int = 1) -> str:
    """Доля 0..1 → «12,5 %»; None → «—»."""
    if value is None:
        return "—"
    return f"{value * 100:.{decimals}f} %".replace(".", ",")


def fmt_tonnes(kg: float | None, *, decimals: int = 1) -> str:
    """Килограммы → тонны с запятой: 128 640,5; None → «—»."""
    if kg is None:
        return "—"
    return f"{kg / 1000:,.{decimals}f}".replace(",", " ").replace(".", ",")


def fmt_pct_change(delta: reports.Delta) -> str:
    """Короткая пилюля изменения: «+8,3 %», «−12,0 %», «—» если раньше был ноль."""
    change = delta.change
    if change is None:
        return "—"
    return f"{change * 100:+.1f} %".replace(".", ",").replace("-", "−")


def fmt_delta(delta: reports.Delta, *, tonnes: bool = False) -> str:
    """Изменение к прошлому периоду: «+12 (+8,3 %)», «−3», «— (было 0)»."""
    diff = delta.diff / 1000 if tonnes else delta.diff
    if tonnes:
        text = f"{diff:+,.1f}".replace(",", " ").replace(".", ",")
    else:
        text = f"{diff:+,.0f}".replace(",", " ")
    text = text.replace("-", "−")
    change = delta.change
    if change is None:
        return f"{text} (прошлый период: 0)"
    pct = f"{change * 100:+.1f} %".replace(".", ",").replace("-", "−")
    return f"{text} ({pct})"


def report_title(report: reports.Report, site_name: str | None) -> str:
    where = site_name or "все объекты"
    return f"Отчёт по взвешиваниям за {report.period.label} — {where}"


def generated_stamp(now: datetime) -> str:
    return now.astimezone(reports.BISHKEK).strftime("%d.%m.%Y %H:%M")
