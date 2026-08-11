"""Фоновая загрузка файлов фото в центр (architecture §4.2).

Метаданные снимков уезжают вместе с записью (weigh_result/offline_sync)
и фиксируются в контрольной сумме; файлы догружает этот модуль:
- берёт фото ДОСЛАННЫХ записей с ``uploaded = 0``;
- POST ``{base_url}/agents/photos/{uuid}/{role}`` с токеном агента;
- 204 → пометка uploaded; 409 (хеш не совпал) → файл повреждён,
  оставляем на месте и шумим в лог (фото — доказательство, молча
  пропускать нельзя);
- любая неудача (нет сети, 409, пропавший файл) считается попыткой:
  пауза до следующей удваивается до получаса, а очередь берёт сначала
  снимки с меньшим числом неудач. Иначе одно вечно падающее фото
  занимало бы порцию каждые 5 секунд и задерживало все остальные
  (находка ревью, сделано 11.08.2026). Сдаваться нельзя: фото —
  доказательство операции, оно должно уехать рано или поздно.

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
from shared.enums import CameraRole

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_S = 5.0
DEFAULT_BATCH = 4
# пауза после неудачи: 15 с, 30 с, 1 мин … до получаса
DEFAULT_RETRY_BASE_S = 15.0
DEFAULT_RETRY_MAX_S = 1800.0
STUCK_AFTER_ATTEMPTS = 5  # с этой попытки снимок считается застрявшим
STATS_EVERY_CYCLES = 120  # сводка по очереди раз в ~10 минут (при интервале 5 с)


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
        retry_base_s: float = DEFAULT_RETRY_BASE_S,
        retry_max_s: float = DEFAULT_RETRY_MAX_S,
    ) -> None:
        self._storage = storage
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._interval_s = interval_s
        self._batch = batch
        self._timeout_s = timeout_s
        self._retry_base_s = retry_base_s
        self._retry_max_s = retry_max_s

    async def run(self) -> None:
        """Бесконечный цикл: порция фото → загрузка → пауза."""
        cycle = 0
        while True:
            try:
                await self.upload_once()
                cycle += 1
                if cycle % STATS_EVERY_CYCLES == 0:
                    await self._log_queue_stats()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("сбой цикла загрузки фото")
            await asyncio.sleep(self._interval_s)

    async def _log_queue_stats(self) -> None:
        """Периодическая сводка: пока фото не уехали, о них надо напоминать."""
        total, stuck = await asyncio.to_thread(
            self._storage.photo_queue_stats, stuck_after=STUCK_AFTER_ATTEMPTS
        )
        if stuck:
            logger.warning("очередь фото: %d в ожидании, из них застряло %d", total, stuck)
        elif total:
            logger.info("очередь фото: %d в ожидании", total)

    async def upload_once(self) -> int:
        """Загрузить одну порцию; вернуть число принятых центром файлов."""
        batch = await asyncio.to_thread(
            lambda: self._storage.photos_to_upload(
                self._batch,
                # снимок с паузой дальше потолка — след скакнувших часов
                # весового ПК; такой берём в работу, иначе он завис бы навсегда
                max_pause_s=self._retry_max_s * 2,
            )
        )
        uploaded = 0
        for weighing_uuid, photo in batch:
            ok = await asyncio.to_thread(self._upload_photo, weighing_uuid, photo)
            if ok:
                await asyncio.to_thread(
                    self._storage.mark_photo_uploaded, weighing_uuid, photo.role
                )
                uploaded += 1
                continue
            attempts = await asyncio.to_thread(self._mark_failed, weighing_uuid, photo.role)
            if attempts == STUCK_AFTER_ATTEMPTS:
                # ровно один раз на снимок: дальше он ждёт долгими паузами
                # и в лог больше не шумит, но диспетчер уже предупреждён
                logger.warning(
                    "фото %s/%s не уходит в центр (%d попыток) — ушло в конец очереди",
                    weighing_uuid,
                    photo.role.value,
                    attempts,
                )
        return uploaded

    def _mark_failed(self, weighing_uuid: UUID, role: CameraRole) -> int:
        return self._storage.mark_photo_failed(
            weighing_uuid,
            role,
            base_delay_s=self._retry_base_s,
            max_delay_s=self._retry_max_s,
        )

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
                # только 204: так отвечает центр, приняв файл. Чужой 200 (прокси,
                # заглушка) означал бы «фото не уехало», а мы бы сочли его
                # доставленным и через месяц стёрли локальную копию
                return bool(response.status == 204)
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
