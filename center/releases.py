"""Хранилище релизов агента на центре (автообновление, решение 10.08.2026;
каналы раскатки — 18.08.2026, architecture §7а).

Релизы — те же архивы ves-agent-<версия>-win64.zip, что собирает
GitHub Actions; на ВМ они лежат в каталоге AGENT_RELEASES_DIR: кладутся
scp с рабочей машины или загружаются через панель («Релизы агентов»,
deploy/README.md §9). Файл — единственный артефакт; какому каналу
(pilot/stable) он назначен и что в нём изменилось, хранит таблица
``agent_releases`` (center/db/repo.py). Здесь — только работа с файлами:
перечень, sha256 (считается один раз и кэшируется по (имя, размер, mtime)),
проверка загружаемого архива (имя по шаблону, оглавление без zip-slip,
внутри app/ves-agent.exe) и атомарная запись.
"""

import hashlib
import os
import re
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath

# каноническое имя: без ведущих нулей в номерах (иначе «0.04.19» и «0.4.19»
# были бы одной версией с двумя файлами)
_NAME_RE = re.compile(r"^ves-agent-(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)-win64\.zip$")

# кэш контрольных сумм: (имя, размер, mtime_ns) → sha256
_sha_cache: dict[tuple[str, int, int], str] = {}


class ReleaseError(Exception):
    """Загружаемый файл не похож на релиз агента (сообщение — человеку)."""


@dataclass(frozen=True)
class AgentRelease:
    """Описание выложенного релиза агента."""

    version: str
    filename: str
    path: Path
    sha256: str
    size_bytes: int


def version_key(version: str | None) -> tuple[int, int, int] | None:
    """Кортеж для сравнения версий «X.Y.Z»; None — не разбирается/пусто."""
    if not version:
        return None
    parts = version.strip().split(".")
    if len(parts) != 3:
        return None
    try:
        major, minor, patch = (int(p) for p in parts)
    except ValueError:
        return None
    return (major, minor, patch)


def release_filename(version: str) -> str:
    return f"ves-agent-{version}-win64.zip"


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


def _release_from_path(path: Path) -> AgentRelease | None:
    match = _NAME_RE.match(path.name)
    if match is None or not path.is_file():
        return None
    return AgentRelease(
        version=f"{int(match.group(1))}.{int(match.group(2))}.{int(match.group(3))}",
        filename=path.name,
        path=path,
        sha256=_file_sha256(path),
        size_bytes=path.stat().st_size,
    )


def list_releases(releases_dir: Path) -> list[AgentRelease]:
    """Все релизы каталога, новые первыми; посторонние файлы игнорируются."""
    if not releases_dir.is_dir():
        return []
    found: list[AgentRelease] = []
    for file in releases_dir.iterdir():
        release = _release_from_path(file)
        if release is not None:
            found.append(release)
    found.sort(key=lambda r: version_key(r.version) or (0, 0, 0), reverse=True)
    return found


def latest_release(releases_dir: Path) -> AgentRelease | None:
    """Найти актуальный (максимальная версия) релиз в каталоге; None — пусто."""
    releases = list_releases(releases_dir)
    return releases[0] if releases else None


def release_by_filename(releases_dir: Path, filename: str) -> AgentRelease | None:
    """Найти релиз по имени файла (только валидные имена, без обхода путей)."""
    if _NAME_RE.match(filename) is None:
        return None
    return _release_from_path(releases_dir / filename)


def release_by_version(releases_dir: Path, version: str) -> AgentRelease | None:
    return release_by_filename(releases_dir, release_filename(version))


def parse_release_filename(filename: str) -> str | None:
    """Версия из имени файла релиза или None, если имя не по шаблону."""
    match = _NAME_RE.match(filename)
    if match is None:
        return None
    return f"{int(match.group(1))}.{int(match.group(2))}.{int(match.group(3))}"


def validate_release_members(members: Iterable[str]) -> None:
    """Правила оглавления релиза — те же, что у агента перед распаковкой:
    внутри есть app/ves-agent.exe, пути членов не вырываются наружу
    (zip-slip по правилам Windows — распаковка идёт на весовом ПК)."""
    names = list(members)
    if not any(m.startswith("app/") and m.endswith("ves-agent.exe") for m in names):
        raise ReleaseError("в архиве нет app/ves-agent.exe — это не релиз агента")
    for member in names:
        path = PureWindowsPath(member)
        if path.is_absolute() or path.drive or path.root or ".." in path.parts:
            raise ReleaseError(f"подозрительный путь в архиве: {member}")


def validate_release_archive(path: Path) -> None:
    """Проверить архив на диске (после загрузки, до публикации в каталоге)."""
    try:
        with zipfile.ZipFile(path) as bundle:
            validate_release_members(bundle.namelist())
    except zipfile.BadZipFile as exc:
        raise ReleaseError("файл не читается как zip-архив") from exc


def store_release(releases_dir: Path, filename: str, source: Path) -> AgentRelease:
    """Опубликовать проверенный архив в каталоге атомарно.

    ``source`` — временный файл загрузки (после успеха удаляется). Имя
    обязано быть по шаблону релиза; существующая версия не перезаписывается
    (версии неизменяемы: пересобрал — новая версия) — гарантию даёт
    ``os.link`` в целевое имя: он атомарно падает FileExistsError, если
    кто-то опередил (двух одновременных загрузок одной версии не бывает).
    Файл вне каталога релизов (другой диск) сперва копируется в него.
    """
    if _NAME_RE.match(filename) is None:
        raise ReleaseError("имя файла должно быть ves-agent-X.Y.Z-win64.zip")
    releases_dir.mkdir(parents=True, exist_ok=True)
    target = releases_dir / filename
    if target.exists():
        raise ReleaseError(f"релиз {filename} уже выложен — версии не перезаписываются")
    validate_release_archive(source)
    staged = source
    if source.resolve().parent != releases_dir.resolve():
        staged = releases_dir / f".{filename}.part"
        with source.open("rb") as src, staged.open("wb") as dst:
            while chunk := src.read(1 << 20):
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())
    try:
        os.link(staged, target)
    except FileExistsError as exc:
        raise ReleaseError(f"релиз {filename} уже выложен — версии не перезаписываются") from exc
    finally:
        staged.unlink(missing_ok=True)
        if staged != source:
            source.unlink(missing_ok=True)
    release = _release_from_path(target)
    assert release is not None
    return release
