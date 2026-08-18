"""Тесты самопроверки после автообновления (agent/selfcheck.py, агент 0.4.19).

Файловый протокол с self-update.bat эмулируется в tmp_path; условия
(веб-порт, связь с центром, индикатор) — колбэками, время — ручными
часами, чтобы таймауты проверялись без ожидания.

Инварианты:
- rollback-файл при старте → доклад ``rolled_back`` (версия из файла,
  причина с подробностями из update-check.fail), файлы убраны;
- контекст с чужой to_version → доклад «не состоялось», контекст убран;
- контекст со своей версией: все условия выполнены → update-check.ok,
  контекст удалён, доклад ``installed``; условие не дождалось таймаута →
  update-check.fail с причиной, контекст ОСТАЁТСЯ (его уберёт bat);
- индикатор проверяется только при expect_indicator;
- dev-запуск (base=None) и обычный старт без файлов — ничего не делают;
- ошибка условия не роняет проверку.
"""

import asyncio
from pathlib import Path

from agent.selfcheck import UpdateSelfCheck
from agent.updater import (
    CHECK_FAIL_FILE,
    CHECK_OK_FILE,
    ROLLBACK_FILE,
    UPDATE_CONTEXT_FILE,
    UpdateContext,
    write_update_context,
)
from shared.messages import UpdateStatus, parse_agent_message

VERSION = "0.4.19"


class FakeClock:
    """Ручные часы: sleep двигает время, ожидания не тратят реального времени."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


def _run(check: UpdateSelfCheck) -> None:
    asyncio.run(check.run())


def _make(
    base: Path | None,
    *,
    web: bool = True,
    center: bool = True,
    indicator: bool = True,
    reports: list[UpdateStatus] | None = None,
    clock: FakeClock | None = None,
) -> UpdateSelfCheck:
    clock = clock or FakeClock()
    return UpdateSelfCheck(
        base,
        agent_id="agent-1",
        version=VERSION,
        web_ready=lambda: web,
        center_connected=lambda: center,
        indicator_ok=lambda: indicator,
        notify=(reports if reports is not None else []).append,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


def _write_context(base: Path, *, to_version: str = VERSION, expect_indicator: bool = True) -> None:
    write_update_context(
        base,
        UpdateContext(
            from_version="0.4.18",
            to_version=to_version,
            started_at="2026-08-18T10:00:00+00:00",
            expect_indicator=expect_indicator,
        ),
    )


class TestQuietStarts:
    def test_dev_run_does_nothing(self, tmp_path: Path) -> None:
        reports: list[UpdateStatus] = []
        _write_context(tmp_path)
        _run(_make(None, reports=reports))
        assert reports == []
        assert (tmp_path / UPDATE_CONTEXT_FILE).exists()

    def test_plain_start_without_files(self, tmp_path: Path) -> None:
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, reports=reports))
        assert reports == []
        assert not (tmp_path / CHECK_OK_FILE).exists()


class TestRollbackReport:
    def test_rollback_file_reported_and_cleaned(self, tmp_path: Path) -> None:
        (tmp_path / ROLLBACK_FILE).write_text(
            "0.4.20\r\nсамопроверка новой версии не пройдена\r\n", encoding="utf-8"
        )
        (tmp_path / CHECK_FAIL_FILE).write_text("нет связи с центром за 120 с\n", encoding="utf-8")
        _write_context(tmp_path, to_version="0.4.20")  # bat мог не успеть убрать
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, reports=reports))
        assert len(reports) == 1
        report = reports[0]
        assert report.stage == "rolled_back" and report.ok is False
        assert report.version == "0.4.20" and report.running_version == VERSION
        assert report.error is not None
        assert "откат на 0.4.19" in report.error
        assert "нет связи с центром за 120 с" in report.error
        for name in (ROLLBACK_FILE, CHECK_FAIL_FILE, UPDATE_CONTEXT_FILE):
            assert not (tmp_path / name).exists(), name
        # сообщение разбирается центром
        parsed = parse_agent_message(report.model_dump_json())
        assert isinstance(parsed, UpdateStatus) and parsed.stage == "rolled_back"


class TestForeignContext:
    def test_service_restarted_with_old_version(self, tmp_path: Path) -> None:
        """bat не смог подменить папку и запустил прежнюю версию: доклад."""
        _write_context(tmp_path, to_version="0.4.20")
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, reports=reports))
        assert len(reports) == 1
        report = reports[0]
        assert report.stage == "started" and report.ok is False
        assert report.version == "0.4.20" and report.running_version == VERSION
        assert report.error is not None and "не состоялось" in report.error
        assert not (tmp_path / UPDATE_CONTEXT_FILE).exists()


class TestSelfCheck:
    def test_all_good_writes_ok_and_reports_installed(self, tmp_path: Path) -> None:
        _write_context(tmp_path)
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, reports=reports))
        assert (tmp_path / CHECK_OK_FILE).read_text(encoding="utf-8").strip() == "ok"
        assert not (tmp_path / CHECK_FAIL_FILE).exists()
        assert not (tmp_path / UPDATE_CONTEXT_FILE).exists()
        assert len(reports) == 1
        report = reports[0]
        assert report.stage == "installed" and report.ok is True
        assert report.version == VERSION and report.running_version == VERSION

    def test_conditions_can_become_true_later(self, tmp_path: Path) -> None:
        """Связь с центром появляется через минуту — проверка дожидается."""
        _write_context(tmp_path)
        clock = FakeClock()
        state = {"center": False}

        def center() -> bool:
            if clock.now >= 60:
                state["center"] = True
            return state["center"]

        reports: list[UpdateStatus] = []
        check = UpdateSelfCheck(
            tmp_path,
            agent_id="agent-1",
            version=VERSION,
            web_ready=lambda: True,
            center_connected=center,
            indicator_ok=lambda: True,
            notify=reports.append,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )
        _run(check)
        assert (tmp_path / CHECK_OK_FILE).exists()
        assert reports and reports[0].stage == "installed"

    def test_no_center_writes_fail_marker(self, tmp_path: Path) -> None:
        _write_context(tmp_path)
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, center=False, reports=reports))
        fail = (tmp_path / CHECK_FAIL_FILE).read_text(encoding="utf-8")
        assert "нет связи с центром за 120 с" in fail
        assert not (tmp_path / CHECK_OK_FILE).exists()
        # контекст остаётся — его уберёт bat при откате
        assert (tmp_path / UPDATE_CONTEXT_FILE).exists()
        assert reports == []

    def test_web_not_up_is_first_reason(self, tmp_path: Path) -> None:
        _write_context(tmp_path)
        _run(_make(tmp_path, web=False, center=False))
        fail = (tmp_path / CHECK_FAIL_FILE).read_text(encoding="utf-8")
        assert "веб-интерфейс" in fail

    def test_silent_indicator_fails_only_when_expected(self, tmp_path: Path) -> None:
        _write_context(tmp_path, expect_indicator=True)
        _run(_make(tmp_path, indicator=False))
        fail = (tmp_path / CHECK_FAIL_FILE).read_text(encoding="utf-8")
        assert "индикатор" in fail and "до обновления шёл" in fail

        # индикатор молчал и до обновления — не повод для отката
        (tmp_path / CHECK_FAIL_FILE).unlink()
        _write_context(tmp_path, expect_indicator=False)
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, indicator=False, reports=reports))
        assert (tmp_path / CHECK_OK_FILE).exists()
        assert reports and reports[0].stage == "installed"

    def test_condition_exception_does_not_break_check(self, tmp_path: Path) -> None:
        _write_context(tmp_path, expect_indicator=False)
        calls = {"n": 0}

        def flaky_web() -> bool:
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("uvicorn not ready")
            return True

        reports: list[UpdateStatus] = []
        check = UpdateSelfCheck(
            tmp_path,
            agent_id="agent-1",
            version=VERSION,
            web_ready=flaky_web,
            center_connected=lambda: True,
            indicator_ok=lambda: False,
            notify=reports.append,
            sleep=FakeClock().sleep,
            monotonic=lambda: 0.0,
        )
        _run(check)
        assert (tmp_path / CHECK_OK_FILE).exists()
        assert reports and reports[0].stage == "installed"

    def test_fail_reason_is_cmd_safe(self, tmp_path: Path) -> None:
        """Первая строка update-check.fail без спецсимволов cmd — попадёт в echo."""
        _write_context(tmp_path)
        _run(_make(tmp_path, center=False))
        line = (tmp_path / CHECK_FAIL_FILE).read_text(encoding="utf-8").splitlines()[0]
        for ch in "()&|<>^%!":
            assert ch not in line


class TestHoldReason:
    def test_states(self, tmp_path: Path) -> None:
        """idle → pending (во время проверки) → ok (30 с удержания) / failed (навсегда)."""
        _write_context(tmp_path)
        clock = FakeClock()
        gate = {"center": False}
        seen: list[tuple[str, bool] | None] = []

        def center() -> bool:
            # заглянуть в hold_reason изнутри ожидания
            seen.append(check.hold_reason())
            return gate["center"]

        check = UpdateSelfCheck(
            tmp_path,
            agent_id="agent-1",
            version=VERSION,
            web_ready=lambda: True,
            center_connected=center,
            indicator_ok=lambda: True,
            notify=lambda _s: None,
            sleep=clock.sleep,
            monotonic=clock.monotonic,
        )
        assert check.hold_reason() is None
        gate["center"] = True
        _run(check)
        assert seen and seen[0] == ("идёт самопроверка после предыдущего обновления", False)
        hold = check.hold_reason()
        assert hold is not None and hold[1] is False  # ok, но 30 с удержания
        clock.now += 31
        assert check.hold_reason() is None

    def test_failed_holds_forever(self, tmp_path: Path) -> None:
        _write_context(tmp_path)
        check = _make(tmp_path, center=False)
        _run(check)
        hold = check.hold_reason()
        assert hold is not None and hold[1] is True and "откат" in hold[0]

    def test_no_context_never_holds(self, tmp_path: Path) -> None:
        check = _make(tmp_path)
        _run(check)
        assert check.hold_reason() is None


class TestForeignContextVariants:
    def test_wrong_build_in_archive(self, tmp_path: Path) -> None:
        """Версия ни from, ни to: в архиве не та сборка — доклад различает случаи."""
        write_update_context(
            tmp_path,
            UpdateContext(
                from_version="0.4.17",
                to_version="0.4.20",
                started_at="2026-08-18T10:00:00+00:00",
                expect_indicator=False,
            ),
        )
        reports: list[UpdateStatus] = []
        _run(_make(tmp_path, reports=reports))
        assert reports and "не та сборка" in (reports[0].error or "")
        assert reports[0].version == "0.4.20" and reports[0].running_version == VERSION
