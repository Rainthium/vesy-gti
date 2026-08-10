"""Хранилище релизов агента на центре (автообновление, решение 10.08.2026).

Релизы — те же архивы ves-agent-<версия>-win64.zip, что собирает
GitHub Actions; на ВМ их кладут в каталог AGENT_RELEASES_DIR
(deploy/README.md, раздел «Автообновление агентов»). «Актуальный релиз» —
архив с максимальной версией по имени файла; sha256 считается один раз
и кэшируется по (имя, размер, mtime).
"""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

_NAME_RE = re.compile(r"^ves-agent-(\d+)\.(\d+)\.(\d+)-win64\.zip$")

# кэш контрольных сумм: (имя, размер, mtime_ns) → sha256
_sha_cache: dict[tuple[str, int, int], str] = {}


@dataclass(frozen=True)
class AgentRelease:
    """Описание выложенного релиза агента."""

    version: str
    filename: str
    path: Path
    sha256: str
    size_bytes: int


def _file_sha256(path: Path) -> str:
    stat = path.stat()
    key = (path.name, stat.st_size, stat.st_mtime_ns)
    cached = _sha_cache.get(key)
    if cached is not None:
        return cached
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    value = digest.hexdigest()
    _sha_cache[key] = value
    return value


def latest_release(releases_dir: Path) -> AgentRelease | None:
    """Найти актуальный (максимальная версия) релиз в каталоге; None — пусто."""
    if not releases_dir.is_dir():
        return None
    best: tuple[tuple[int, int, int], Path] | None = None
    for file in releases_dir.iterdir():
        match = _NAME_RE.match(file.name)
        if match is None or not file.is_file():
            continue
        version_tuple = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if best is None or version_tuple > best[0]:
            best = (version_tuple, file)
    if best is None:
        return None
    version_tuple, path = best
    return AgentRelease(
        version=".".join(str(part) for part in version_tuple),
        filename=path.name,
        path=path,
        sha256=_file_sha256(path),
        size_bytes=path.stat().st_size,
    )


def release_by_filename(releases_dir: Path, filename: str) -> AgentRelease | None:
    """Найти релиз по имени файла (только валидные имена, без обхода путей)."""
    if _NAME_RE.match(filename) is None:
        return None
    path = releases_dir / filename
    if not path.is_file():
        return None
    match = _NAME_RE.match(filename)
    assert match is not None
    return AgentRelease(
        version=f"{match.group(1)}.{match.group(2)}.{match.group(3)}",
        filename=filename,
        path=path,
        sha256=_file_sha256(path),
        size_bytes=path.stat().st_size,
    )
