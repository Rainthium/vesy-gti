"""Чтение хвоста журнала службы (agent/diagnostics.py).

Экран «Диагностика» должен работать на весовом ПК в любом состоянии:
лог пишет служба, файл может быть огромным, обрезанным ротацией, занятым
на запись или отсутствовать вовсе (dev-запуск). Ни один из этих случаев
не должен ронять экран — в худшем случае строк просто нет.
"""

from pathlib import Path

from agent.diagnostics import MAX_TAIL_BYTES, read_log_tail


def _write(path: Path, lines: list[str]) -> Path:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


class TestReadLogTail:
    def test_returns_last_lines_in_order(self, tmp_path: Path) -> None:
        """Отдаются последние строки в исходном порядке (старые сверху)."""
        log = _write(tmp_path / "agent.log", [f"строка {i}" for i in range(1, 11)])
        assert read_log_tail(log, lines=3) == ["строка 8", "строка 9", "строка 10"]

    def test_short_file_returned_whole(self, tmp_path: Path) -> None:
        """Файл короче запрошенного хвоста отдаётся целиком."""
        log = _write(tmp_path / "agent.log", ["одна", "две"])
        assert read_log_tail(log, lines=100) == ["одна", "две"]

    def test_missing_file_and_none(self, tmp_path: Path) -> None:
        """Файла нет или путь не задан (dev-запуск) — пустой список."""
        assert read_log_tail(tmp_path / "нет.log") == []
        assert read_log_tail(None) == []

    def test_directory_instead_of_file(self, tmp_path: Path) -> None:
        """Вместо файла каталог — не падаем."""
        assert read_log_tail(tmp_path) == []

    def test_empty_file(self, tmp_path: Path) -> None:
        """Пустой файл — пустой список, без исключений."""
        (tmp_path / "agent.log").write_bytes(b"")
        assert read_log_tail(tmp_path / "agent.log") == []

    def test_huge_file_reads_only_tail(self, tmp_path: Path) -> None:
        """Из большого файла читается только хвост, и первая строка в нём
        не обрывается посередине."""
        log = tmp_path / "agent.log"
        filler = "x" * 200
        log.write_text("\n".join(f"{filler} {i}" for i in range(5000)) + "\n", encoding="utf-8")
        assert log.stat().st_size > MAX_TAIL_BYTES

        tail = read_log_tail(log, lines=10)
        assert len(tail) == 10
        assert tail[-1].endswith("4999")
        assert all(line.startswith(filler) for line in tail), "строка обрезана посередине"

    def test_broken_bytes_do_not_crash(self, tmp_path: Path) -> None:
        """Битые байты в логе (обрыв записи) заменяются, экран живёт."""
        log = tmp_path / "agent.log"
        log.write_bytes("нормальная строка\n".encode() + b"\xff\xfe\n" + "конец\n".encode())
        tail = read_log_tail(log)
        assert tail[0] == "нормальная строка"
        assert tail[-1] == "конец"

    def test_unfinished_last_line_shown(self, tmp_path: Path) -> None:
        """Служба пишет прямо сейчас: последняя строка без перевода строки
        всё равно показывается."""
        log = tmp_path / "agent.log"
        log.write_text("готовая\nещё пишется", encoding="utf-8")
        assert read_log_tail(log) == ["готовая", "ещё пишется"]
