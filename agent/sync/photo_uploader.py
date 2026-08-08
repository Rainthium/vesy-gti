"""Фоновая загрузка файлов фото в центр (architecture §4.2).

Метаданные снимков уезжают вместе с записью (weigh_result/offline_sync)
и фиксируются в контрольной сумме; файлы догружает этот модуль:
- берёт фото ДОСЛАННЫХ записей с ``uploaded = 0``;
- POST ``{base_url}/agents/photos/{uuid}/{role}`` с токеном агента;
- 204 → пометка uploaded; 409 (хеш не совпал) → файл повреждён,
  оставляем на месте и шумим в лог (фото — доказательство, молча
  пропускать нельзя);
- сеть недоступна → просто следующая попытка через интервал.

Работает независимо от WebSocket-соединения: файлы уходят по HTTP.
"""

import asyncio
import contextlib
import logging
import urllib.error
import urllib.request
from pathlib import Path
from uuid import UUID

from agent.sync.storage import AgentStorage, StoredPhoto

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 5.0
DEFAULT_BATCH = 4


class PhotoUploader:
    """Цикл догрузки фото; останавливается отменой задачи run()."""

    def __init__(
        self,
        storage: AgentStorage,
        *,
        base_url: str,  # например, https://vesy.gti.kg
        token: str,
        interval_s: float = DEFAULT_INTERVAL_S,
        batch: int = DEFAULT_BATCH,
        timeout_s: float = 30.0,
    ) -> None:
        self._storage = storage
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._interval_s = interval_s
        self._batch = batch
        self._timeout_s = timeout_s

    async def run(self) -> None:
        """Бесконечный цикл: порция фото → загрузка → пауза."""
        while True:
            try:
                await self.upload_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("сбой цикла загрузки фото")
            await asyncio.sleep(self._interval_s)

    async def upload_once(self) -> int:
        """Загрузить одну порцию; вернуть число принятых центром файлов."""
        batch = await asyncio.to_thread(self._storage.photos_to_upload, self._batch)
        uploaded = 0
        for weighing_uuid, photo in batch:
            ok = await asyncio.to_thread(self._upload_photo, weighing_uuid, photo)
            if ok:
                await asyncio.to_thread(
                    self._storage.mark_photo_uploaded, weighing_uuid, photo.role
                )
                uploaded += 1
        return uploaded

    def _upload_photo(self, weighing_uuid: UUID, photo: StoredPhoto) -> bool:
        path = Path(photo.path)
        try:
            body = path.read_bytes()
        except OSError as exc:
            # файл потерян — фото-доказательство; каждый цикл будет напоминать
            logger.error("файл фото недоступен: %s (%s)", photo.path, exc)
            return False

        url = f"{self._base_url}/agents/photos/{weighing_uuid}/{photo.role.value}"
        request = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "image/jpeg",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_s) as response:
                return response.status in (200, 204)
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                logger.error(
                    "центр отверг фото %s/%s: хеш не совпал — файл повреждён?",
                    weighing_uuid,
                    photo.role.value,
                )
            elif exc.code == 404:
                logger.warning("центр не знает запись %s — фото уедет после досылки", weighing_uuid)
            else:
                logger.warning("загрузка фото: HTTP %d (%s)", exc.code, url)
            return False
        except OSError as exc:
            logger.debug("центр недоступен для загрузки фото: %s", exc)
            return False


async def run_forever(uploader: PhotoUploader) -> None:
    """Запуск до отмены с подавлением CancelledError снаружи."""
    with contextlib.suppress(asyncio.CancelledError):
        await uploader.run()
