"""Каталог инструментов для агентов (0.4.33): файлы, которые агент докачивает
сам по надобности — сегодня это ffmpeg.exe для камер, отдающих кадр только по
RTSP (урок Канта 12.09.2026: ffmpeg в мастер-папке не было, на ПК не зайти,
а класть 100 МБ в каждый релиз — раздавать его всем объектам без нужды).

Каталог — AGENT_TOOLS_DIR (на ВМ `~/vesy-gti/deploy/tools`, том `/data/tools`
только на чтение); файлы кладутся scp с рабочей машины (deploy/README.md §9а),
ship.sh каталог не синхронизирует и не удаляет. Имя файла — один сегмент из
букв, цифр, точки, дефиса, подчёркивания: обход путей невозможен. sha256
считается один раз и кэшируется как у релизов (по имени, размеру, mtime).
"""

import re
from dataclasses import dataclass
from pathlib import Path

from center.releases import file_sha256

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass(frozen=True)
class ToolFile:
    """Файл инструмента в каталоге центра."""

    filename: str
    path: Path
    sha256: str
    size_bytes: int


def tool_by_filename(tools_dir: Path, filename: str) -> ToolFile | None:
    """Файл инструмента по имени: только валидные имена, только файлы каталога."""
    if _NAME_RE.match(filename) is None or ".." in filename:
        return None
    path = tools_dir / filename
    if not path.is_file():
        return None
    return ToolFile(
        filename=filename,
        path=path,
        sha256=file_sha256(path),
        size_bytes=path.stat().st_size,
    )
