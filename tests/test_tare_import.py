# ruff: noqa: F811 — фикстуры БД импортируются из tests.test_center_panel и
# «переопределяются» параметрами тестов (тот же приём, что в test_center_reports)
"""Перенос тарирований из выгрузки АИС «СВХ» (решение Игоря 12.09.2026).

Чистая логика (разбор файла, вердикты) — без БД; запись в журнал и реестр,
идемпотентность и консольный инструмент — на временной БД панели.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from center.db import repo
from center.db.models import AuditLog, Scale, ScaleKind, TareRegistry, Weighing, WeighingAisRef
from center.tare_import import (
    BISHKEK,
    AisTaring,
    ImportTarget,
    OurTaring,
    TareImportError,
    Verdict,
    pairs_to_import,
    parse_ais_tarings,
    plan_import,
    summarize,
    summarize_by_object,
)
from shared.enums import ErrorCode, Operation, WeighingSource
from tests.test_center_panel import (  # noqa: F401 — фикстуры временной БД
    _add_site_scale,
    _make_taring,
    db,
    db_session,
    panel_db_engine,
    panel_db_url,
)
from tools import import_ais_tares

HEADER = (
    "Номер въезда;Порядковый номер тарирования;Гос.номер АТС;Гос.номер прицепа;"
    "VIN-код транспортного средства;VIN-код прицепа;Статус тарирования;Тара;"
    "Единица измерения;Наименование объекта;ФИО оператора;Дата и время тарирования;"
    "Не учитывать при взвешивании"
)
NOW = datetime(2026, 9, 12, 14, 0, tzinfo=UTC)  # 20:00 по Бишкеку


def _row(
    ref: str,
    vehicle: str = "01KG111AAA",
    trailer: str = "01KG222PA",
    massa: str = "15600",
    obj: str = "Кант",
    at: str = "10.09.2026, 12:30",
    ignored: str = "false",
    entry: str = "SVH000000001",
    status: str = "Завершенное взвешивание",
    operator: str = "Акимов Нурлан Боронбаевич",
) -> str:
    return (
        f"{entry};{ref};{vehicle};{trailer};;;{status};{massa};кг;{obj};{operator};{at};{ignored}"
    )


def _csv(*rows: str) -> str:
    return "\ufeff" + "\r\n".join([HEADER, *rows]) + "\r\n"


def _taring(ref: str = "TAR000000001", **overrides: object) -> AisTaring:
    fields: dict[str, object] = {
        "tare_ref": ref,
        "entry_ref": "SVH000000001",
        "vehicle_number": "01KG111AAA",
        "trailer_number": "01KG222PA",
        "massa": 15600.0,
        "object_name": "Кант",
        "operator": "Акимов Нурлан Боронбаевич",
        "tared_at": NOW - timedelta(days=2),
        "completed": True,
        "ignored": False,
    }
    fields.update(overrides)
    return AisTaring(**fields)  # type: ignore[arg-type]


TARGETS = {"Кант": ImportTarget("kant", 9), "Кара-Суу": ImportTarget("kara-suu", 4)}


class TestParse:
    def test_parses_bom_crlf_and_normalizes(self) -> None:
        """BOM, CRLF, «;», запятая в массе; номера — strip/upper, пустой прицеп — None;
        время выгрузки бишкекское → UTC."""
        text = _csv(
            _row("TAR000000010", vehicle=" 01kg111aaa ", trailer="", massa="15600,5"),
            _row("TAR000000011", ignored="true", at="12.09.2026, 19:11"),
        )
        tarings, problems = parse_ais_tarings(text)
        assert problems == []
        first, second = tarings
        assert first.tare_ref == "TAR000000010"
        assert first.vehicle_number == "01KG111AAA"
        assert first.trailer_number is None
        assert first.massa == pytest.approx(15600.5)
        assert first.entry_ref == "SVH000000001"
        assert first.operator == "Акимов Нурлан Боронбаевич"
        assert first.object_name == "Кант"
        assert first.completed and not first.ignored
        assert first.tared_at == datetime(2026, 9, 10, 6, 30, tzinfo=UTC)
        assert second.ignored
        assert second.tared_at == datetime(2026, 9, 12, 13, 11, tzinfo=UTC)
        assert second.pair == ("01KG111AAA", "01KG222PA")

    def test_bad_rows_are_reported_not_fatal(self) -> None:
        text = _csv(
            _row("TAR000000020", massa="нет"),
            _row("TAR000000021", at="вчера"),
            _row("", vehicle="01KG333AAA"),
            _row("TAR000000022", massa="0"),
            _row("TAR000000023"),
        )
        tarings, problems = parse_ais_tarings(text)
        assert [t.tare_ref for t in tarings] == ["TAR000000023"]
        assert len(problems) == 4
        assert any("TAR000000020" in line and "нет" in line for line in problems)
        assert any("без номера" in line for line in problems)

    def test_wrong_file_raises(self) -> None:
        with pytest.raises(TareImportError, match="нет колонок"):
            parse_ais_tarings("Дата;Номер;Масса\r\n01.01.2026;1;2\r\n")

    def test_other_status_is_kept_for_verdict(self) -> None:
        tarings, _ = parse_ais_tarings(_csv(_row("TAR000000030", status="Отменено")))
        assert tarings[0].completed is False


class TestPlan:
    def test_verdicts(self) -> None:
        """Каждая причина не переносить — своим вердиктом; порядок проверок фиксирован."""
        old = NOW - timedelta(days=100)
        tarings = [
            _taring("TAR000000101", object_name="ОсОО «Нурдан Транс»"),
            _taring("TAR000000102", tared_at=old),
            _taring("TAR000000103"),  # уже наше (по TAR)
            _taring("TAR000000104"),  # перенесено раньше
            _taring("TAR000000105", vehicle_number="01KG555AAA", massa=14000.0),  # совпало
            _taring("TAR000000106", completed=False),
            _taring("TAR000000107", ignored=True),
            _taring("TAR000000108", vehicle_number=""),
            _taring("TAR000000109", massa=25001.0),
            _taring("TAR000000110", massa=25000.0),
            _taring(
                "TAR000000111", object_name="Кара-Суу", vehicle_number="01KG555AAA", massa=14000.0
            ),
        ]
        known = {
            "TAR000000103": WeighingSource.AIS,
            "TAR000000104": WeighingSource.IMPORTED,
        }
        ours = [
            OurTaring("kant", "01KG555AAA", 14010.0, NOW - timedelta(days=2, minutes=20)),
        ]
        plan = plan_import(tarings, now=NOW, targets=TARGETS, known_refs=known, our_tarings=ours)
        assert [item.verdict for item in plan] == [
            Verdict.UNKNOWN_OBJECT,
            Verdict.TOO_OLD,
            Verdict.ALREADY_OURS,
            Verdict.ALREADY_IMPORTED,
            Verdict.MATCHED_OURS,
            Verdict.NOT_COMPLETED,
            Verdict.IGNORED,
            Verdict.NO_VEHICLE,
            Verdict.TOO_HEAVY,
            Verdict.IMPORT,
            Verdict.IMPORT,  # на другом объекте совпадения нет
        ]
        assert plan[0].scale_id is None
        assert plan[-1].scale_id == 4 and plan[-2].scale_id == 9

    def test_three_month_boundary_and_no_limit(self) -> None:
        edge = datetime(2026, 6, 12, 14, 0, tzinfo=UTC)  # ровно 3 месяца назад
        tarings = [
            _taring("TAR000000201", tared_at=edge),
            _taring("TAR000000202", tared_at=edge - timedelta(minutes=1)),
            _taring("TAR000000203", massa=40000.0),
        ]
        plan = plan_import(tarings, now=NOW, targets=TARGETS, known_refs={}, max_tare_kg=0)
        assert [item.verdict for item in plan] == [Verdict.IMPORT, Verdict.TOO_OLD, Verdict.IMPORT]

    def test_fuzzy_match_limits(self) -> None:
        """Совпадение без TAR: масса в пределах 20 кг и время в пределах 30 минут."""
        at = NOW - timedelta(days=1)
        ours = [OurTaring("kant", "01KG111AAA", 15600.0, at)]
        far_mass = _taring("TAR000000301", tared_at=at, massa=15621.0)
        far_time = _taring("TAR000000302", tared_at=at + timedelta(minutes=31), massa=15600.0)
        close = _taring("TAR000000303", tared_at=at - timedelta(minutes=29), massa=15580.0)
        plan = plan_import(
            [far_mass, far_time, close], now=NOW, targets=TARGETS, known_refs={}, our_tarings=ours
        )
        assert [item.verdict for item in plan] == [
            Verdict.IMPORT,
            Verdict.IMPORT,
            Verdict.MATCHED_OURS,
        ]

    def test_summaries(self) -> None:
        tarings = [
            _taring("TAR000000401"),
            _taring("TAR000000402"),  # та же сцепка — одна строка реестра
            _taring("TAR000000403", vehicle_number="01KG999AAA", object_name="Кара-Суу"),
            _taring("TAR000000404", ignored=True),
        ]
        plan = plan_import(tarings, now=NOW, targets=TARGETS, known_refs={})
        assert summarize(plan) == {Verdict.IMPORT: 3, Verdict.IGNORED: 1}
        assert summarize_by_object(plan) == {
            "Кант": {Verdict.IMPORT: 2, Verdict.IGNORED: 1},
            "Кара-Суу": {Verdict.IMPORT: 1},
        }
        assert pairs_to_import(plan) == 2


def _scale(session: Session, code: str = "kant", scale_name: str = "Весы №2 (тарирование)") -> int:
    _, scale = _add_site_scale(session, code, f"СВХ «{code}»", scale_name)
    session.commit()
    return scale.id


class TestRepo:
    def test_saves_journal_row_ref_and_registry(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        imported_at = NOW
        row = repo.save_imported_taring(db_session, scale_id, _taring(), imported_at=imported_at)
        db_session.commit()
        assert row is not None
        assert row.uuid == repo.imported_taring_uuid("TAR000000001")
        assert row.source is WeighingSource.IMPORTED
        assert row.operation is Operation.TARING and row.code is ErrorCode.OK
        assert row.massa == 15600.0 and row.weighed_at == NOW - timedelta(days=2)
        assert row.operator == "Акимов Нурлан Боронбаевич"
        assert row.message is not None
        assert "TAR000000001" in row.message and "SVH000000001" in row.message
        assert "UniServer" in row.message
        assert row.request_payload is not None
        trace = row.request_payload["ais_import"]
        assert isinstance(trace, dict) and trace["object"] == "Кант"
        assert len(row.checksum) == 64
        ref = db_session.execute(
            select(WeighingAisRef).where(WeighingAisRef.weighing_id == row.id)
        ).scalar_one()
        assert ref.ais_ref == "TAR000000001" and ref.origin == "import"
        registry = db_session.get(TareRegistry, ("01KG111AAA", "01KG222PA"))
        assert registry is not None
        assert registry.weighing_id == row.id and registry.tare_value == 15600.0
        # реестр отдаёт перенесённую тару агентам с uuid записи
        active = repo.find_active_tare(db_session, "01KG111AAA", "01KG222PA", now=NOW)
        assert active is not None and active.weighing_uuid == row.uuid
        assert {record.weighing_uuid for record in repo.load_tare_registry(db_session)} == {
            row.uuid
        }

    def test_idempotent_by_tar_ref(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        first = repo.save_imported_taring(db_session, scale_id, _taring(), imported_at=NOW)
        db_session.commit()
        again = repo.save_imported_taring(db_session, scale_id, _taring(massa=1.0), imported_at=NOW)
        assert first is not None and again is None
        assert db_session.execute(select(Weighing.id)).scalars().all() == [first.id]
        assert repo.known_ais_refs(db_session, ["TAR000000001", "TAR000000009"]) == {
            "TAR000000001": WeighingSource.IMPORTED
        }

    def test_ref_taken_by_our_operation_is_skipped(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        ours = _make_taring(weighed_at=NOW - timedelta(days=1))
        repo.save_weighing_record(db_session, scale_id, ours, ais_ref="TAR000000001")
        assert repo.save_imported_taring(db_session, scale_id, _taring(), imported_at=NOW) is None
        assert repo.known_ais_refs(db_session, ["TAR000000001"]) == {
            "TAR000000001": WeighingSource.AIS
        }

    def test_older_import_does_not_override_newer_tare(self, db_session: Session) -> None:
        """Строка реестра — последнее тарирование сцепки, каким бы путём оно ни пришло."""
        scale_id = _scale(db_session)
        newer = _make_taring(
            vehicle_number="01KG111AAA",
            trailer_number="01KG222PA",
            massa=15000.0,
            weighed_at=NOW - timedelta(days=1),
        )
        repo.save_weighing_record(db_session, scale_id, newer, ais_ref="TAR000000777")
        older = repo.save_imported_taring(db_session, scale_id, _taring(), imported_at=NOW)
        db_session.commit()
        assert older is not None
        registry = db_session.get(TareRegistry, ("01KG111AAA", "01KG222PA"))
        assert registry is not None and registry.tare_value == 15000.0
        # а более позднее перенесённое — затирает
        latest = repo.save_imported_taring(
            db_session,
            scale_id,
            _taring("TAR000000002", massa=15900.0, tared_at=NOW - timedelta(hours=1)),
            imported_at=NOW,
        )
        db_session.commit()
        assert latest is not None
        db_session.expire_all()
        registry = db_session.get(TareRegistry, ("01KG111AAA", "01KG222PA"))
        assert registry is not None and registry.weighing_id == latest.id

    def test_no_vehicle_is_refused(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        with pytest.raises(ValueError, match="без номера ТС"):
            repo.save_imported_taring(
                db_session, scale_id, _taring(vehicle_number=""), imported_at=NOW
            )

    def test_tarings_since_lists_our_journal(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        repo.save_weighing_record(
            db_session, scale_id, _make_taring(weighed_at=NOW - timedelta(days=1))
        )
        repo.save_weighing_record(
            db_session, scale_id, _make_taring(weighed_at=NOW - timedelta(days=200))
        )
        ours = repo.tarings_since(db_session, NOW - timedelta(days=90))
        assert [(o.site_code, o.vehicle_number, o.massa) for o in ours] == [
            ("kant", "01KG777AAA", 7500.0)
        ]

    def test_storno_of_imported_rebuilds_registry(self, db_session: Session) -> None:
        scale_id = _scale(db_session)
        row = repo.save_imported_taring(db_session, scale_id, _taring(), imported_at=NOW)
        db_session.commit()
        assert row is not None
        repo.storno_weighing(db_session, row, actor="panel:admin", reason="ошибочный перенос")
        assert db_session.get(TareRegistry, ("01KG111AAA", "01KG222PA")) is None
        assert repo.find_active_tare(db_session, "01KG111AAA", "01KG222PA", now=NOW) is None


class TestCli:
    def _write(self, tmp_path: Path, *rows: str) -> Path:
        path = tmp_path / "Тарирования в АИС СВХ.csv"
        path.write_text(_csv(*rows), encoding="utf-8")
        return path

    def _seed(self, factory: sessionmaker[Session]) -> tuple[int, int]:
        """Кант с двумя весами (как в бою — тарируют на №2) и Кара-Суу с одними."""
        with factory() as session:
            _, entry = _add_site_scale(session, "kant", "СВХ «Кант»", "Весы №1 (въезд)")
            second = Scale(
                site_id=entry.site_id,
                name="Весы №2 (тарирование)",
                kind=ScaleKind.STATIC,
                driver="cas22",
            )
            session.add(second)
            _, karasuu = _add_site_scale(session, "kara-suu", "ПЗТК «Кара-Суу»", "Весы SCS-80")
            session.commit()
            return second.id, karasuu.id

    def test_dry_run_then_apply_then_repeat(
        self, db: sessionmaker[Session], tmp_path: Path
    ) -> None:
        kant_tare_id, karasuu_id = self._seed(db)
        recent = (NOW - timedelta(days=3)).astimezone(BISHKEK)
        path = self._write(
            tmp_path,
            _row("TAR000000501", at=recent.strftime("%d.%m.%Y, %H:%M")),
            _row(
                "TAR000000502",
                obj="Кара-Суу",
                vehicle="06KG001AAA",
                at=recent.strftime("%d.%m.%Y, %H:%M"),
            ),
            _row("TAR000000503", massa="26000", at=recent.strftime("%d.%m.%Y, %H:%M")),
            _row("TAR000000504", obj="ОсОО «Нурдан Транс»", at=recent.strftime("%d.%m.%Y, %H:%M")),
            _row("TAR000000505", at="01.01.2026, 10:00"),
        )
        out = io.StringIO()
        assert import_ais_tares.main([str(path)], session_factory=db, out=out) == 0
        text = out.getvalue()
        assert "к переносу" in text and "тара тяжелее лимита" in text
        assert "объект не заведён в центре" in text and "старше 3 месяцев" in text
        assert "Проверка без записи" in text
        with db() as session:
            assert session.execute(select(Weighing.id)).scalars().all() == []

        out = io.StringIO()
        report = tmp_path / "отчёт.csv"
        assert (
            import_ais_tares.main(
                [str(path), "--apply", "--report", str(report)], session_factory=db, out=out
            )
            == 0
        )
        assert "Перенесено записей: 2" in out.getvalue()
        assert "TAR000000503" in report.read_text(encoding="utf-8-sig")
        with db() as session:
            rows = session.execute(select(Weighing).order_by(Weighing.id)).scalars().all()
            assert [(r.scale_id, r.source) for r in rows] == [
                (kant_tare_id, WeighingSource.IMPORTED),
                (karasuu_id, WeighingSource.IMPORTED),
            ]
            audit = session.execute(
                select(AuditLog).where(AuditLog.action == "tares_import")
            ).scalar_one()
            assert audit.details is not None and audit.details["imported"] == 2
            assert audit.details["by_object"] == {"Кант": 1, "Кара-Суу": 1}

        out = io.StringIO()
        assert import_ais_tares.main([str(path), "--apply"], session_factory=db, out=out) == 0
        assert "уже перенесено раньше" in out.getvalue()
        assert "Перенесено записей: 0" in out.getvalue()
        with db() as session:
            assert len(session.execute(select(Weighing.id)).scalars().all()) == 2

    def test_wrong_file_and_bad_object_option(
        self, db: sessionmaker[Session], tmp_path: Path
    ) -> None:
        path = tmp_path / "x.csv"
        path.write_text("a;b\n1;2\n", encoding="utf-8")
        out = io.StringIO()
        assert import_ais_tares.main([str(path)], session_factory=db, out=out) == 2
        assert "нет колонок" in out.getvalue()
        with pytest.raises(SystemExit):
            import_ais_tares.main(
                [str(path), "--object", "без-знака-равно"], session_factory=db, out=io.StringIO()
            )

    def test_ambiguous_scales_need_explicit_name(
        self, db: sessionmaker[Session], tmp_path: Path
    ) -> None:
        with db() as session:
            _, first = _add_site_scale(session, "kara-suu", "ПЗТК «Кара-Суу»", "Весы А")
            session.add(
                Scale(site_id=first.site_id, name="Весы Б", kind=ScaleKind.STATIC, driver="cas22")
            )
            session.commit()
        path = self._write(tmp_path, _row("TAR000000601", obj="Кара-Суу"))
        out = io.StringIO()
        assert import_ais_tares.main([str(path)], session_factory=db, out=out) == 2
        assert "несколько весов" in out.getvalue()
        out = io.StringIO()
        code = import_ais_tares.main(
            [str(path), "--object", "Кара-Суу=kara-suu/Весы Б"], session_factory=db, out=out
        )
        assert code == 0 and "к переносу" in out.getvalue()


class TestRobustness:
    def test_extra_and_missing_separators(self) -> None:
        """Лишний «;» в строке (текст с разделителем внутри) и недостающие поля не
        роняют разбор: строка либо читается, либо попадает в отчёт."""
        good = _row("TAR000000701")
        text = _csv(good + ";хвост;ещё", "SVH1;TAR000000702;01KG111AAA", good)
        tarings, problems = parse_ais_tarings(text)
        assert [t.tare_ref for t in tarings] == ["TAR000000701", "TAR000000701"]
        assert len(problems) == 1 and "TAR000000702" in problems[0]

    def test_nbsp_in_massa(self) -> None:
        tarings, problems = parse_ais_tarings(_csv(_row("TAR000000703", massa="15\u00a0600")))
        assert problems == [] and tarings[0].massa == 15600.0

    def test_agent_record_cannot_claim_imported_source(self) -> None:
        """Источник imported ставит только центр: по каналу агента он не проходит."""
        from pydantic import ValidationError

        from shared.messages import WeighingRecord

        with pytest.raises(ValidationError, match="imported"):
            WeighingRecord(
                uuid=repo.imported_taring_uuid("TAR000000704"),
                operation=Operation.TARING,
                code=ErrorCode.OK,
                massa=15600.0,
                source=WeighingSource.IMPORTED,
            )


class TestKnownSinceImport:
    def test_latest_taring_as_of_respects_import_moment(self, db_session: Session) -> None:
        """До переноса система о тарировании не знала: как-бы-на-момент его нет."""
        scale_id = _scale(db_session)
        imported_at = datetime.now(UTC)
        row = repo.save_imported_taring(
            db_session,
            scale_id,
            _taring(tared_at=imported_at - timedelta(days=10)),
            imported_at=imported_at,
        )
        db_session.commit()
        assert row is not None  # created_at — момент вставки (записи неизменяемы)
        earlier = imported_at - timedelta(days=1)
        assert repo.latest_taring_as_of(db_session, "01KG111AAA", "01KG222PA", earlier) is None
        later = datetime.now(UTC) + timedelta(seconds=1)
        found = repo.latest_taring_as_of(db_session, "01KG111AAA", "01KG222PA", later)
        assert found is not None and found.id == row.id
        # реестр и подстановка «сейчас» перенесённую тару видят как обычную
        active = repo.find_active_tare(db_session, "01KG111AAA", "01KG222PA", now=later)
        assert active is not None and active.weighing_uuid == row.uuid
