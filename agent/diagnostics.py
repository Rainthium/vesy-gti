"""Диагностика агента: хвост журнала службы и путь к нему.

Экран «Диагностика» в интерфейсе оператора (вопрос Игоря 10.08.2026):
разобрать сбой на объекте, не подключаясь к весовому ПК по AnyDesk и не
имея доступа к файлам. Работает и без связи с центром — это его главное
свойство, лог лежит рядом.

Лог пишет служба (nssm перенаправляет stdout/stderr в ``logs\\agent.log``
с ротацией по 10 МБ), поэтому читаем ХВОСТ файла, а не файл целиком.
"""

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_LINES = 300
MAX_TAIL_BYTES = 256 * 1024  # больше хвоста показывать в интерфейсе незачем


def default_log_path() -> Path | None:
    """Путь к логу службы при стандартной раскладке (C:/vesy-agent/logs).

    В замороженной сборке exe лежит в ``<база>/app``, лог — в
    ``<база>/logs/agent.log``. В dev-запуске файла нет: логи идут в консоль.
    """
    if not getattr(sys, "frozen", False):
        return None
    return Path(sys.executable).resolve().parent.parent / "logs" / "agent.log"


def read_log_tail(
    path: Path | None, *, lines: int = DEFAULT_LINES, max_bytes: int = MAX_TAIL_BYTES
) -> list[str]:
    """Последние строки лога; пустой список, если читать нечего.

    Файл может писаться прямо сейчас и содержать неполную строку в конце —
    это нормально, показываем как есть. Кодировка вывода службы — UTF-8
    (PYTHONIOENCODING в install-service.bat), битые байты не роняют экран.
    """
    if path is None or not path.is_file():
        return []
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > max_bytes:
                handle.seek(size - max_bytes)
                handle.readline()  # обрезанную первую строку выбрасываем
            data = handle.read()
    except OSError as exc:
        logger.warning("не удалось прочитать лог %s: %s", path, exc)
        return []
    text = data.decode("utf-8", errors="replace")
    return text.splitlines()[-lines:]
