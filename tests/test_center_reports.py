"""Тесты сводной аналитики центра (center/reports.py, этап 4) на живом PostgreSQL.

Стенд — тестовая БД панели (фикстуры test_center_panel). Посев подобран
так, чтобы каждое правило модуля имело свою строку:
- границы периода по бишкекским суткам (31.07 23:30 и 11.08 00:00 не входят,
  10.08 23:59 входит);
- отказы (code≠OK) и сторно-пара не считаются;
- нетто только у взвешиваний с подставленной тарой; причины «без нетто»
  (не было / устарело / номер не передан / была, но не подставилась);
- сцепки к перетарированию (с учётом реестра «сейчас»);
- по объектам, динамика по дням/неделям/месяцам с пустыми отрезками,
  разбивка по объектам;
- ручные операции по операторам, без номера АИС и «зависшие»;
- доступность и инциденты по monitoring_events, отказы АИС из audit_log
  (v2 по scale_id, v1 по legacy-адресу);
- сравнение с прошлым периодом и чистые функции периодов/пресетов.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.orm import Session, sessionmaker

from center import reports
from center.db import repo
from center.db.models import (
    Agent,
    AuditLog,
    MonitoringEvent,
    MonitoringSeverity,
    Scale,
    ScaleKind,
    Site,
    TareRegistry,
    Weighing,
)
from shared.enums import ErrorCode, Operation, WeighingSource
from tests.test_center_panel import (
    db,  # noqa: F401 — фикстуры БД панели
    panel_db_engine,  # noqa: F401
    panel_db_url,  # noqa: F401
)

BISHKEK = ZoneInfo("Asia/Bishkek")
PERIOD = reports.Period(date(2026, 8, 1), date(2026, 8, 10))
NOW = datetime(2026, 8, 12, 9, 0, tzinfo=UTC)  # «сейчас» — после периода


def _bishkek(y: int, m: int, d: int, hh: int = 12, mm: int = 0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=BISHKEK).astimezone(UTC)


@dataclass
class Seed:
    site_a: int
    site_b: int
    site_c: int
    scale_a: int
    scale_b: int
    scale_c: int


def _weighing(
    session: Session,
    scale_id: int,
    at: datetime,
    *,
    massa: float,
    vehicle: str | None = "01KG000AAA",
    trailer: str | None = None,
    operation: Operation = Operation.WEIGHING,
    code: ErrorCode = ErrorCode.OK,
    source: WeighingSource = WeighingSource.AIS,
    tare_value: float | None = None,
    netto: float | None = None,
    operator: str | None = None,
    storno_of: int | None = None,
) -> Weighing:
    from uuid import uuid4

    row = Weighing(
        uuid=uuid4(),
        scale_id=scale_id,
        operation=operation,
        code=code,
        massa=massa,
        stable=True,
        weighed_at=at,
        created_at=at,
        vehicle_number=vehicle,
        trailer_number=trailer,
        tare_value=tare_value,
        netto=netto,
        source=source,
        operator=operator,
        storno_of=storno_of,
        checksum="0" * 64,
    )
    session.add(row)
    session.commit()
    return row


def _event(
    session: Session, scale_id: int, kind: str, severity: MonitoringSeverity, at: datetime
) -> None:
    session.add(
        MonitoringEvent(
            scale_id=scale_id,
            kind=kind,
            severity=severity,
            message=f"{kind}:{severity}",
            created_at=at,
        )
    )
    session.commit()


def _audit(session: Session, action: str, at: datetime, details: dict[str, object]) -> None:
    session.add(AuditLog(actor="ais:test", action=action, at=at, details=details))
    session.commit()


@pytest.fixture
def seed(db: sessionmaker[Session]) -> Seed:  # noqa: F811
    with db() as session:
        ids: dict[str, int] = {}
        for code, name, with_agent, legacy in (
            ("alpha", "СВХ «Альфа»", True, ("192.168.1.10", 2)),
            ("beta", "ПЗТК «Бета»", True, ("192.168.1.20", 3)),
            ("gamma", "СВХ «Гамма»", False, None),
        ):
            site = Site(code=code, name=name)
            session.add(site)
            session.flush()
            scale = Scale(
                site_id=site.id,
                name="Весы",
                kind=ScaleKind.STATIC,
                driver="cas22",
                legacy_ip=legacy[0] if legacy else None,
                legacy_port=8087 if legacy else None,
                legacy_autoscale=legacy[1] if legacy else None,
            )
            session.add(scale)
            session.flush()
            if with_agent:
                session.add(
                    Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(f"tok-{code}"))
                )
            ids[f"site_{code}"] = site.id
            ids[f"scale_{code}"] = scale.id
        session.commit()
        s = Seed(
            site_a=ids["site_alpha"],
            site_b=ids["site_beta"],
            site_c=ids["site_gamma"],
            scale_a=ids["scale_alpha"],
            scale_b=ids["scale_beta"],
            scale_c=ids["scale_gamma"],
        )
        a, b = s.scale_a, s.scale_b

        # --- объект A ---
        # тарирование V1 (действует), взвешивание V1 с нетто 02.08
        _weighing(
            session, a, _bishkek(2026, 7, 20), massa=7000, vehicle="V1", operation=Operation.TARING
        )
        _weighing(
            session,
            a,
            _bishkek(2026, 8, 2),
            massa=20000,
            vehicle="V1",
            tare_value=7000,
            netto=13000,
        )
        # V2: тарирования не было → «не было»
        _weighing(session, a, _bishkek(2026, 8, 3), massa=15000, vehicle="V2")
        # V3: тарирование 01.03 (устарело к 04.08) → «устарело»; реестр помнит его
        v3_tare = _weighing(
            session, a, _bishkek(2026, 3, 1), massa=6000, vehicle="V3", operation=Operation.TARING
        )
        session.add(
            TareRegistry(
                vehicle_number="V3",
                trailer_number="",
                weighing_id=v3_tare.id,
                tare_value=6000,
                tared_at=v3_tare.weighed_at,
            )
        )
        session.commit()
        _weighing(session, a, _bishkek(2026, 8, 4), massa=18000, vehicle="V3")
        # без номера ТС → «номер не передан»
        _weighing(session, a, _bishkek(2026, 8, 5), massa=12000, vehicle=None)
        # офлайн V1 с тарой (оператор), без номера АИС, старше 3 суток к NOW
        _weighing(
            session,
            a,
            _bishkek(2026, 8, 6),
            massa=21000,
            vehicle="V1",
            tare_value=7000,
            netto=14000,
            source=WeighingSource.LOCAL_OFFLINE,
            operator="Оператор А",
        )
        # V1 без нетто при действующей таре → «была, но не подставилась»
        _weighing(session, a, _bishkek(2026, 8, 8), massa=19000, vehicle="V1")
        # отказ (исторический ERR) — не считается
        _weighing(
            session, a, _bishkek(2026, 8, 7), massa=0, vehicle="V9", code=ErrorCode.ERR_UNSTABLE
        )
        # границы суток по Бишкеку
        _weighing(session, a, _bishkek(2026, 7, 31, 23, 30), massa=10000, vehicle="V8")  # вне
        _weighing(session, a, _bishkek(2026, 8, 10, 23, 59), massa=11000, vehicle="V7")  # внутри
        _weighing(session, a, _bishkek(2026, 8, 11, 0, 0), massa=10500, vehicle="V6")  # вне

        # --- объект B ---
        _weighing(
            session,
            b,
            _bishkek(2026, 8, 2),
            massa=30000,
            vehicle="B1",
            tare_value=10000,
            netto=20000,
        )
        _weighing(
            session,
            b,
            _bishkek(2026, 8, 9),
            massa=32000,
            vehicle="B1",
            tare_value=10000,
            netto=22000,
        )
        _weighing(
            session, b, _bishkek(2026, 8, 3), massa=10000, vehicle="B2", operation=Operation.TARING
        )
        # сторно-пара: обе строки вне расчёта
        original = _weighing(session, b, _bishkek(2026, 8, 4), massa=50000, vehicle="B3")
        _weighing(
            session, b, _bishkek(2026, 8, 4, 13), massa=50000, vehicle="B3", storno_of=original.id
        )

        # --- мониторинг: A офлайн 2 часа 03.08, у B индикатор и камера ---
        _event(
            session,
            a,
            "offline",
            MonitoringSeverity.DANGER,
            datetime(2026, 8, 3, 10, 0, tzinfo=UTC),
        )
        _event(
            session, a, "offline", MonitoringSeverity.OK, datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
        )
        _event(
            session,
            b,
            "no_data",
            MonitoringSeverity.DANGER,
            datetime(2026, 8, 4, 10, 0, tzinfo=UTC),
        )
        _event(
            session, b, "no_data", MonitoringSeverity.OK, datetime(2026, 8, 4, 10, 30, tzinfo=UTC)
        )
        _event(
            session,
            b,
            "camera_front",
            MonitoringSeverity.WARNING,
            datetime(2026, 8, 5, 10, 0, tzinfo=UTC),
        )
        # событие вне периода — не считается
        _event(
            session,
            b,
            "camera_rear",
            MonitoringSeverity.WARNING,
            datetime(2026, 8, 20, 10, 0, tzinfo=UTC),
        )

        # --- отказы АИС в audit_log ---
        _audit(
            session,
            "weigh_request_v2",
            datetime(2026, 8, 4, 9, 0, tzinfo=UTC),
            {"code": "ERR_UNSTABLE", "scale_id": a, "request": {}},
        )
        _audit(
            session,
            "weigh_request_v2",
            datetime(2026, 8, 4, 9, 5, tzinfo=UTC),
            {"code": "OK", "scale_id": a, "request": {}},
        )
        _audit(
            session,
            "weigh_request_v1",
            datetime(2026, 8, 5, 9, 0, tzinfo=UTC),
            {
                "code": "ERR_VEHICLE_TIMEOUT",
                "request": {"ip_address": "192.168.1.20", "autoscale": 3},
            },
        )
        _audit(
            session,
            "weigh_request_v2",
            datetime(2026, 8, 20, 9, 0, tzinfo=UTC),
            {"code": "ERR_BUSY", "scale_id": a},
        )  # вне периода
        return s


# ---------------------------------------------------------------------------
# чистые функции: период, пресеты, отрезки
# ---------------------------------------------------------------------------


class TestPeriod:
    def test_bounds_are_bishkek_days_in_utc(self) -> None:
        period = reports.Period(date(2026, 8, 1), date(2026, 8, 10))
        assert period.start == datetime(2026, 7, 31, 18, 0, tzinfo=UTC)
        assert period.end == datetime(2026, 8, 10, 18, 0, tzinfo=UTC)
        assert period.days == 10
        assert period.label == "01.08.2026 — 10.08.2026"
        assert reports.Period(date(2026, 8, 1), date(2026, 8, 1)).label == "01.08.2026"

    def test_step_by_length(self) -> None:
        assert reports.Period(date(2026, 8, 1), date(2026, 8, 31)).step() == "day"
        assert reports.Period(date(2026, 3, 1), date(2026, 8, 31)).step() == "week"
        assert reports.Period(date(2026, 1, 1), date(2026, 8, 31)).step() == "month"

    def test_presets(self) -> None:
        today = date(2026, 8, 18)
        assert reports.preset_period("today", today=today) == reports.Period(today, today)
        assert reports.preset_period("yesterday", today=today) == reports.Period(
            date(2026, 8, 17), date(2026, 8, 17)
        )
        assert reports.preset_period("7d", today=today) == reports.Period(date(2026, 8, 12), today)
        assert reports.preset_period("30d", today=today) == reports.Period(date(2026, 7, 20), today)
        assert reports.preset_period("month", today=today) == reports.Period(
            date(2026, 8, 1), today
        )
        assert reports.preset_period("prev_month", today=today) == reports.Period(
            date(2026, 7, 1), date(2026, 7, 31)
        )
        assert reports.preset_period("quarter", today=today) == reports.Period(
            date(2026, 7, 1), today
        )
        assert reports.preset_period("year", today=today) == reports.Period(date(2026, 1, 1), today)
        assert reports.preset_period("nope", today=today) is None
        # прошлый месяц в январе — декабрь прошлого года
        assert reports.preset_period("prev_month", today=date(2026, 1, 5)) == reports.Period(
            date(2025, 12, 1), date(2025, 12, 31)
        )

    def test_previous_period(self) -> None:
        # единица пресета: те же числа месяцем/кварталом раньше
        month_to_date = reports.Period(date(2026, 8, 1), date(2026, 8, 18))
        assert reports.previous_period(month_to_date, unit_months=1) == reports.Period(
            date(2026, 7, 1), date(2026, 7, 18)
        )
        quarter = reports.Period(date(2026, 7, 1), date(2026, 8, 18))
        assert reports.previous_period(quarter, unit_months=3) == reports.Period(
            date(2026, 4, 1), date(2026, 5, 18)
        )
        # 31-е число поджимается
        assert reports.previous_period(
            reports.Period(date(2026, 5, 1), date(2026, 5, 31)), unit_months=1
        ) == reports.Period(date(2026, 4, 1), date(2026, 4, 30))
        # без единицы: целые месяцы — тем же составом
        assert reports.previous_period(reports.Period(date(2026, 7, 1), date(2026, 8, 31))) == (
            reports.Period(date(2026, 5, 1), date(2026, 6, 30))
        )
        # с 1-го по сегодня без единицы — те же числа месяцем раньше
        assert reports.previous_period(month_to_date) == reports.Period(
            date(2026, 7, 1), date(2026, 7, 18)
        )
        # произвольный отрезок — смежный той же длины
        assert reports.previous_period(reports.Period(date(2026, 8, 5), date(2026, 8, 14))) == (
            reports.Period(date(2026, 7, 26), date(2026, 8, 4))
        )

    def test_buckets(self) -> None:
        period = reports.Period(date(2026, 8, 5), date(2026, 8, 20))
        assert reports.buckets(period, "day")[0] == date(2026, 8, 5)
        assert len(reports.buckets(period, "day")) == 16
        weeks = reports.buckets(period, "week")
        assert weeks[0] == date(2026, 8, 3)  # понедельник недели, куда попало 5.08
        assert weeks[-1] == date(2026, 8, 17)
        months = reports.buckets(reports.Period(date(2025, 11, 15), date(2026, 2, 3)), "month")
        assert months == [date(2025, 11, 1), date(2025, 12, 1), date(2026, 1, 1), date(2026, 2, 1)]


# ---------------------------------------------------------------------------
# агрегаты на БД
# ---------------------------------------------------------------------------


class TestTotals:
    def test_totals_all_sites(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            t = reports.totals(session, PERIOD)
        # A: V1(02), V2, V3, без номера, офлайн V1, V1(08), 10.08 23:59 = 7; B: 2 → 9
        assert t.weighings == 9
        assert t.tarings == 1  # только тарирование B 03.08 (A: 20.07 и 01.03 вне периода)
        assert t.offline == 1
        assert t.operations == 10
        assert t.offline_share == pytest.approx(0.1)
        # нетто: 13000 + 14000 (A) + 20000 + 22000 (B)
        assert t.netto_kg == pytest.approx(69000)
        assert t.with_tare == 4
        assert t.tare_sum_kg == pytest.approx(7000 + 7000 + 10000 + 10000)
        assert t.gross_with_tare_kg == pytest.approx(20000 + 21000 + 30000 + 32000)
        # брутто всего: A 20000+15000+18000+12000+21000+19000+11000 = 116000; B 62000
        assert t.gross_kg == pytest.approx(178000)
        assert t.without_tare == 5
        assert t.gross_without_tare_kg == pytest.approx(178000 - 103000)
        assert t.avg_gross_kg == pytest.approx(178000 / 9)
        # нетто = Σбрутто − Σтары по тем же записям
        assert t.netto_kg == pytest.approx(t.gross_with_tare_kg - t.tare_sum_kg)
        assert (
            t.refusals == 2
        )  # v2 ERR_UNSTABLE (A) + v1 ERR_VEHICLE_TIMEOUT (B); OK и вне периода нет
        assert t.unlinked == 1

    def test_totals_single_site_and_empty(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            b = reports.totals(session, PERIOD, seed.site_b)
            c = reports.totals(session, PERIOD, seed.site_c)
        assert (b.weighings, b.tarings, b.offline, b.refusals) == (2, 1, 0, 1)
        assert b.netto_kg == pytest.approx(42000)
        assert c == reports.Totals()
        assert c.offline_share is None and c.avg_gross_kg is None


class TestMassReport:
    def test_reasons_and_retare(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            m = reports.mass_report(session, PERIOD)
        assert m.reasons == {"expired": 1, "none": 2, "no_vehicle": 1, "not_applied": 1}
        # к перетарированию: V2 (не было), V3 (устарело), V7 (10.08 без тары — тарирований не было);
        # V1 «не подставилась» — тара действует, в список не идёт
        vehicles = {(r.vehicle_number, r.trailer_number): r for r in m.retare}
        assert set(vehicles) == {("V2", None), ("V3", None), ("V7", None)}
        v3 = vehicles[("V3", None)]
        assert v3.last_tared_at is not None and v3.last_tare_value == 6000
        assert vehicles[("V2", None)].last_tared_at is None
        assert m.retare_total == 3

    def test_expiry_boundary_matches_python_rule(
        self,
        db: sessionmaker[Session],  # noqa: F811
        seed: Seed,
    ) -> None:
        """Граница правила №4 в SQL совпадает с shared.tare.three_months_before:
        31 мая ↔ 28 февраля (поджатие дня), «ровно на границе» — ещё действует."""
        moment = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
        with db() as session:
            for vehicle, tared_at in (
                ("VB1", datetime(2026, 2, 28, 12, 0, tzinfo=UTC)),  # ровно граница → действует
                ("VB2", datetime(2026, 2, 28, 11, 59, tzinfo=UTC)),  # на минуту раньше → устарело
            ):
                _weighing(
                    session,
                    seed.scale_a,
                    tared_at,
                    massa=5000,
                    vehicle=vehicle,
                    operation=Operation.TARING,
                )
                _weighing(session, seed.scale_a, moment, massa=15000, vehicle=vehicle)
            period = reports.Period(date(2026, 5, 31), date(2026, 5, 31))
            m = reports.mass_report(session, period)
        assert m.reasons == {"expired": 1, "none": 0, "no_vehicle": 0, "not_applied": 1}
        assert [r.vehicle_number for r in m.retare] == ["VB2"]

    def test_retare_skips_recently_retared(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        """Сцепку перетарировали после периода — из списка уходит."""
        with db() as session:
            fresh = _weighing(
                session,
                seed.scale_a,
                datetime(2026, 8, 11, 6, 0, tzinfo=UTC),
                massa=6500,
                vehicle="V2",
                operation=Operation.TARING,
            )
            session.add(
                TareRegistry(
                    vehicle_number="V2",
                    trailer_number="",
                    weighing_id=fresh.id,
                    tare_value=6500,
                    tared_at=fresh.weighed_at,
                )
            )
            session.commit()
            m = reports.mass_report(session, PERIOD)
        assert {r.vehicle_number for r in m.retare} == {"V3", "V7"}
        # причина у взвешивания V2 03.08 не меняется — тогда тары не было
        assert m.reasons["none"] == 2


class TestBySiteAndSeries:
    def test_by_site_rows(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            rows = {r.site_id: r for r in reports.by_site(session, PERIOD, now=NOW)}
        assert set(rows) == {seed.site_a, seed.site_b, seed.site_c}
        a, b, c = rows[seed.site_a], rows[seed.site_b], rows[seed.site_c]
        assert a.totals.weighings == 7 and b.totals.weighings == 2 and c.totals.weighings == 0
        # доступность A: 2 часа офлайна из 240 → 99,17 %
        assert a.availability == pytest.approx(1 - 2 / 240, abs=1e-6)
        assert b.availability == pytest.approx(1.0)
        assert c.availability is None  # агента нет
        assert a.incidents == 1  # уход в офлайн
        assert b.incidents == 2  # индикатор + камера (событие 20.08 вне периода)
        assert b.totals.refusals == 1 and a.totals.refusals == 1

    def test_series_days_with_gaps(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            s = reports.series(session, PERIOD)
        assert s.step == "day"
        assert [p.bucket for p in s.points] == [date(2026, 8, d) for d in range(1, 11)]
        by_day = {p.bucket.day: p.totals for p in s.points}
        assert by_day[1].operations == 0
        assert by_day[2].weighings == 2 and by_day[2].netto_kg == pytest.approx(33000)
        assert by_day[3].weighings == 1 and by_day[3].tarings == 1
        assert by_day[10].weighings == 1  # 23:59 по Бишкеку — свои сутки
        assert s.by_site == {}

    def test_series_split_and_steps(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            split = reports.series(session, PERIOD, split_sites=True)
            weeks = reports.series(session, reports.Period(date(2026, 7, 1), date(2026, 8, 31)))
            months = reports.series(session, reports.Period(date(2026, 1, 1), date(2026, 8, 31)))
        assert set(split.by_site) == {seed.site_a, seed.site_b}
        assert sum(p.totals.weighings for p in split.by_site[seed.site_a]) == 7
        assert sum(p.totals.weighings for p in split.points) == 9  # сумма по объектам = итог
        assert weeks.step == "week" and weeks.points[0].bucket == date(2026, 6, 29)
        assert months.step == "month" and len(months.points) == 8
        march = next(p for p in months.points if p.bucket == date(2026, 3, 1))
        assert march.totals.tarings == 1  # тарирование V3 01.03
        july = next(p for p in months.points if p.bucket == date(2026, 7, 1))
        assert july.totals.weighings == 1 and july.totals.tarings == 1  # 31.07 23:30 и тара 20.07


class TestManualAndReliability:
    def test_manual_operations(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            rows = reports.manual_operations(session, PERIOD, now=NOW)
        assert len(rows) == 1
        row = rows[0]
        assert (row.site_id, row.operator, row.weighings, row.tarings) == (
            seed.site_a,
            "Оператор А",
            1,
            0,
        )
        assert row.unlinked == 1 and row.unlinked_stale == 1
        assert row.site_operations == 7 and row.share == pytest.approx(1 / 7)
        # «сейчас» сразу после операции — ещё не зависшая
        with db() as session:
            fresh = reports.manual_operations(session, PERIOD, now=_bishkek(2026, 8, 7))
        assert fresh[0].unlinked_stale == 0

    def test_reliability(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            rows = {r.site_id: r for r in reports.reliability_by_site(session, PERIOD, now=NOW)}
        a, b, c = rows[seed.site_a], rows[seed.site_b], rows[seed.site_c]
        assert a.offline_count == 1 and a.offline_seconds == pytest.approx(7200)
        assert a.refusals == {"ERR_UNSTABLE": 1}
        assert b.indicator_incidents == 1 and b.camera_incidents == 1 and b.other_incidents == 0
        assert b.refusals == {"ERR_VEHICLE_TIMEOUT": 1}  # v1 сопоставлен по legacy-адресу
        assert b.availability == pytest.approx(1.0)
        assert c.has_agent is False and c.availability is None and c.incidents == 0

    def test_offline_carries_over_period_start_and_now(
        self,
        db: sessionmaker[Session],  # noqa: F811
        seed: Seed,
    ) -> None:
        """Офлайн, начавшийся до периода и не закрытый, считается с начала периода
        и до «сейчас» (будущее в знаменатель не идёт)."""
        with db() as session:
            _event(
                session,
                seed.scale_b,
                "offline",
                MonitoringSeverity.DANGER,
                datetime(2026, 7, 30, tzinfo=UTC),
            )
            now = datetime(2026, 8, 5, 18, 0, tzinfo=UTC)  # середина периода
            rows = {r.site_id: r for r in reports.reliability_by_site(session, PERIOD, now=now)}
        b = rows[seed.site_b]
        # период с 31.07 18:00 UTC до now — 120 часов, все офлайн
        assert b.offline_seconds == pytest.approx(120 * 3600)
        assert b.availability == pytest.approx(0.0)
        assert b.offline_count == 0  # переход был до периода — уходов внутри нет

    def test_offline_seconds_pure(self) -> None:
        start = datetime(2026, 8, 1, tzinfo=UTC)
        end = datetime(2026, 8, 2, tzinfo=UTC)
        events = [
            (datetime(2026, 8, 1, 1, tzinfo=UTC), MonitoringSeverity.DANGER),
            (datetime(2026, 8, 1, 2, tzinfo=UTC), MonitoringSeverity.OK),
            (datetime(2026, 8, 1, 23, tzinfo=UTC), MonitoringSeverity.DANGER),  # не закрыт → до end
        ]
        seconds, count = reports._offline_seconds(events, False, start, end)
        assert seconds == pytest.approx(2 * 3600) and count == 2
        seconds, count = reports._offline_seconds([], True, start, end)
        assert seconds == pytest.approx(24 * 3600) and count == 0


class TestComparisonAndReport:
    def test_comparison_periods(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            current = reports.totals(session, PERIOD)
            cmp_ = reports.comparison(session, PERIOD, current)
            # период с 1-го числа: те же числа месяцем раньше (01.07–10.07 — пусто)
            assert cmp_.period == reports.Period(date(2026, 7, 1), date(2026, 7, 10))
            assert cmp_.weighings.previous == 0 and cmp_.weighings.change is None
            assert cmp_.netto_kg.diff == pytest.approx(69000)
            # произвольный отрезок 05.08–10.08 ↔ смежный 30.07–04.08
            tail = reports.Period(date(2026, 8, 5), date(2026, 8, 10))
            tail_totals = reports.totals(session, tail)
            tail_cmp = reports.comparison(session, tail, tail_totals)
        assert tail_cmp.period == reports.Period(date(2026, 7, 30), date(2026, 8, 4))
        # текущий: A 05, 06, 08, 10 + B 09 = 5; прошлый: A 31.07, 02, 03, 04 + B 02 = 5
        assert (tail_cmp.weighings.current, tail_cmp.weighings.previous) == (5, 5)
        assert tail_cmp.weighings.change == pytest.approx(0.0)
        # явно заданный прошлый период (пресет с единицей) — берётся как есть
        with db() as session:
            explicit = reports.comparison(
                session,
                PERIOD,
                current,
                previous=reports.Period(date(2026, 7, 22), date(2026, 7, 31)),
            )
        assert explicit.weighings.previous == 1  # 31.07 23:30

    def test_build_report(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            report = reports.build_report(session, PERIOD, split_sites=True, now=NOW)
        assert report.totals.weighings == 9
        assert len(report.sites) == 3 and report.dynamics.by_site
        assert report.manual and report.reliability
        assert report.generated_at == NOW
        with db() as session:
            only_c = reports.build_report(session, PERIOD, seed.site_c, now=NOW)
        assert only_c.totals == reports.Totals()
        assert [r.site_id for r in only_c.sites] == [seed.site_c]
        assert only_c.manual == [] and only_c.masses.retare == []
        assert all(p.totals.operations == 0 for p in only_c.dynamics.points)


class TestVolumeSummary:
    def test_cards_one_query(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        """Объёмы дашборда: сегодня / 7 дней / месяц с прошлым периодом для Δ."""
        with db() as session:
            cards = {
                c.key: c for c in reports.volume_summary(session, None, today=date(2026, 8, 10))
            }
        today, week, month = cards["today"], cards["week"], cards["month"]
        assert today.period == reports.Period(date(2026, 8, 10), date(2026, 8, 10))
        assert today.weighings == 1 and today.previous is None  # 10.08 23:59 по Бишкеку
        assert week.period == reports.Period(date(2026, 8, 4), date(2026, 8, 10))
        # A: 04, 05, 06 (офлайн), 08, 10 + B: 09 = 6; сторно-пара 04.08 не считается
        assert (week.weighings, week.offline, week.tarings) == (6, 1, 0)
        assert week.netto_kg == pytest.approx(14000 + 22000)
        assert week.previous == reports.Period(date(2026, 7, 28), date(2026, 8, 3))
        assert week.prev_weighings == 4  # A: 31.07, 02, 03 + B: 02
        delta = week.weighings_delta
        assert delta is not None and delta.change == pytest.approx(0.5)
        assert month.period == reports.Period(date(2026, 8, 1), date(2026, 8, 10))
        assert month.weighings == 9 and month.tarings == 1
        assert month.previous == reports.Period(date(2026, 7, 1), date(2026, 7, 10))
        assert month.prev_weighings == 0 and month.weighings_delta is not None
        assert month.weighings_delta.change is None

    def test_cards_respect_site(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            cards = {
                c.key: c
                for c in reports.volume_summary(session, seed.site_b, today=date(2026, 8, 10))
            }
            empty = reports.volume_summary(session, seed.site_c, today=date(2026, 8, 10))
        assert cards["month"].weighings == 2 and cards["week"].weighings == 1
        assert all(c.weighings == 0 and c.netto_kg == 0 for c in empty)


class TestRefusalHelpers:
    def test_refusal_scale_mapping(self) -> None:
        index = reports.ScaleIndex(site_of={5: 1, 7: 2}, legacy={("10.0.0.1", 2): 7})
        assert reports._refusal_scale("5", None, None, index) == 5
        assert reports._refusal_scale(None, "10.0.0.1", "2", index) == 7
        assert reports._refusal_scale(None, "10.0.0.9", "2", index) is None
        assert reports._refusal_scale("abc", None, None, index) is None
        assert reports._refusal_scale(None, None, None, index) is None

    def test_scale_index_and_per_site(self, db: sessionmaker[Session], seed: Seed) -> None:  # noqa: F811
        with db() as session:
            index = reports.scale_index(session)
            refusals = reports.refusals_by_scale(session, PERIOD, index=index)
        assert index.site_of[seed.scale_a] == seed.site_a
        assert index.legacy[("192.168.1.20", 3)] == seed.scale_b
        assert refusals == {
            seed.scale_a: {"ERR_UNSTABLE": 1},
            seed.scale_b: {"ERR_VEHICLE_TIMEOUT": 1},
        }
        assert reports.refusals_total(refusals) == 2
        assert reports.refusals_per_site(refusals, index) == {seed.site_a: 1, seed.site_b: 1}
