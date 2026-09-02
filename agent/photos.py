"""Снимки для журнала оператора: локальный файл, миниатюра, фолбэк на центр.

Журнал в интерфейсе агента показывает те же кадры, что и панель центра
(запрос Игоря 09.08.2026). Источников два, и порядок такой:

1. **локальный файл** — пока он есть, ничего никуда не ходит;
2. **центр** — после того как ретеншн убрал локальную копию
   (agent/sync/retention.py), снимок берётся у центра по токену агента.
   Центр отдаёт только снимки СВОИХ весов.

Миниатюра для строки журнала делается один раз и кладётся рядом с
оригиналом (``..._thumb.jpeg``) — как в центре. Оригинал не трогается
никогда: правило №2, sha256 связан с записью.

Без связи с центром и без локального файла снимка просто нет — журнал
покажет прочерк, а не сломается.

Ответ центра «снимка нет» (404) запоминается на MISSING_TTL_S, а полученная
из центра миниатюра сохраняется на диск: журнал переклеивается каждые 5 с,
и после уборки локальных фото (0.4.25) агент Кызыл-Кыи за четыре часа
сделал 2158 запросов к центру за снимками, которых там нет (02.09.2026).
"""

import io
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

from PIL import Image

from agent.sync.storage import AgentStorage
from shared.enums import CameraRole

logger = logging.getLogger(__name__)

THUMB_SUFFIX = "_thumb"
THUMB_MAX_SIDE = 320  # как в центре: хватает для строки журнала
THUMB_QUALITY = 80
DOWNLOAD_TIMEOUT_S = 5.0  # журнал переклеивается каждые 5 с — долго ждать нельзя
# центр ответил «нет такого снимка» — не переспрашиваем, пока не истечёт
MISSING_TTL_S = 600.0
MISSING_MAX_KEYS = 10_000  # страховка от роста памяти отказов: чистим просроченные


def thumb_path(original: Path) -> Path:
    return original.with_name(original.stem + THUMB_SUFFIX + original.suffix)


def _read(path: Path) -> bytes | None:
    """Байты файла или None (файла нет, занят, нет прав)."""
    try:
        return path.read_bytes()
    except OSError:
        return None


def _write_atomic(target: Path, data: bytes) -> None:
    """Записать файл через временный + rename; ошибка диска — только в лог."""
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".part")
        tmp.write_bytes(data)
        tmp.replace(target)
    except OSError as exc:
        logger.warning("файл %s не сохранён: %s", target, exc)


def make_thumbnail(original: bytes) -> bytes:
    """Уменьшенная копия кадра (оригинал не изменяется)."""
    image: Image.Image = Image.open(io.BytesIO(original))
    image.thumbnail((THUMB_MAX_SIDE, THUMB_MAX_SIDE))
    if image.mode != "RGB":
        image = image.convert("RGB")
        # RGB — чтобы JPEG принял кадры с альфа-каналом
    buffer = io.BytesIO()
    image.save(buffer, "JPEG", quality=THUMB_QUALITY)
    return buffer.getvalue()


def _ensure_thumb(data: bytes) -> bytes | None:
    """Данные размера миниатюры: как есть, если не больше THUMB_MAX_SIDE,
    иначе ужатая копия; None — это не картинка (или битая)."""
    try:
        with Image.open(io.BytesIO(data)) as image:
            small_enough = max(image.size) <= THUMB_MAX_SIDE
        return data if small_enough else make_thumbnail(data)
    except Exception:
        return None


class PhotoLibrary:
    """Доступ к снимкам записей журнала (локально, иначе из центра)."""

    def __init__(
        self,
        storage: AgentStorage,
        *,
        base_url: str,
        token: str,
        online: Callable[[], bool] = lambda: True,
        timeout_s: float = DOWNLOAD_TIMEOUT_S,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._storage = storage
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._online = online
        self._timeout_s = timeout_s
        self._now = now
        # (uuid, роль, миниатюра?) → момент, до которого центр не спрашиваем:
        # он уже ответил 404 (снимка в центре нет — например, запись
        # старше чистки журнала)
        self._missing: dict[tuple[UUID, CameraRole, bool], float] = {}

    def roles_of(self, weighing_uuid: UUID) -> list[CameraRole]:
        """Роли снимков записи — по журналу, а не по наличию файлов."""
        return [photo.role for photo in self._storage.photos_for(weighing_uuid)]

    def photo_bytes(
        self, weighing_uuid: UUID, role: CameraRole, *, thumb: bool = False
    ) -> bytes | None:
        """JPEG снимка или None, если его нет ни локально, ни в центре."""
        photos = {photo.role: photo for photo in self._storage.photos_for(weighing_uuid)}
        stored = photos.get(role)
        if stored is None:
            return None
        original = Path(stored.path)
        local = self._local_thumb(original) if thumb else _read(original)
        if local is not None:
            return local
        return self._from_center(weighing_uuid, role, thumb=thumb, original=original)

    def photo_available(self, weighing_uuid: UUID, role: CameraRole) -> bool:
        """Достижим ли снимок сейчас: локальный файл на месте либо есть
        связь с центром (снимок уже там — ретеншн убирает только досланные).

        Для печатной карточки: офлайн после ретеншна — вместо пустой рамки
        печатается честное предупреждение, где снимок взять.
        """
        photos = {photo.role: photo for photo in self._storage.photos_for(weighing_uuid)}
        stored = photos.get(role)
        if stored is None:
            return False
        if Path(stored.path).is_file():
            return True
        return self._online()

    def _local_thumb(self, original: Path) -> bytes | None:
        """Готовая миниатюра рядом с кадром; при отсутствии — строим её."""
        target = thumb_path(original)
        ready = _read(target)
        if ready is not None:
            return ready
        source = _read(original)
        if source is None:
            return None
        try:
            data = make_thumbnail(source)
        except Exception:
            logger.exception("не удалось построить миниатюру для %s", original)
            return source  # лучше крупный кадр, чем пустая строка
        _write_atomic(target, data)
        return data

    def _from_center(
        self, weighing_uuid: UUID, role: CameraRole, *, thumb: bool, original: Path
    ) -> bytes | None:
        """Снимок из центра: локальная копия уже убрана ретеншном.

        Без связи не ходим вовсе: журнал обновляется каждые 5 секунд, и
        десяток ожидающих таймаута запросов забил бы пул соединений
        браузера ровно тогда, когда оператору нужен ручной режим
        (замечание ревью 11.08.2026). Ответ 404 запоминается на
        MISSING_TTL_S, полученная миниатюра кладётся рядом с кадром —
        иначе каждая переклейка журнала шла бы в центр заново (02.09.2026).
        """
        if not self._online():
            return None
        key = (weighing_uuid, role, thumb)
        until = self._missing.get(key)
        if until is not None:
            if self._now() < until:
                return None
            # pop, не del: два потока роута могли прочитать один просроченный ключ
            self._missing.pop(key, None)
        first_refusal = until is None
        query = "?thumb=1" if thumb else ""
        url = f"{self._base_url}/agents/photos/{weighing_uuid}/{role.value}{query}"
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._token}"})
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_s) as response:
                if response.status != 200:
                    return None
                data: bytes = response.read()
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                # в центре снимка нет и не появится (правило №2: фото не
                # переснимаются) — молчим до истечения срока
                if len(self._missing) > MISSING_MAX_KEYS:
                    now = self._now()
                    self._missing = {k: v for k, v in self._missing.items() if v > now}
                self._missing[key] = self._now() + MISSING_TTL_S
                # в лог — один раз: повтор после срока засорял бы «Диагностику»
                log = logger.info if first_refusal else logger.debug
                log("центр не знает фото %s/%s — прочерк", weighing_uuid, role.value)
            else:
                logger.warning(
                    "центр не отдал фото %s/%s: HTTP %d", weighing_uuid, role.value, exc.code
                )
            return None
        except OSError as exc:
            # офлайн — обычное дело на объекте, журнал просто покажет прочерк
            logger.debug("центр недоступен для получения фото: %s", exc)
            return None
        if thumb:
            # центр без готовой миниатюры отдаёт ПОЛНЫЙ кадр (1–3 МБ) — под
            # именем миниатюры его хранить нельзя (замечание ревью 02.09):
            # ужимаем копию до размера миниатюры; оригинал никто не трогает
            shrunk = _ensure_thumb(data)
            if shrunk is None:
                return data  # не разобрали как картинку — отдать, но не хранить
            data = shrunk
            # миниатюра маленькая (десятки КБ) и неизменяема: оставляем на
            # диске, плановая уборка её не тронет (строка уже file_removed)
            _write_atomic(thumb_path(original), data)
        return data
