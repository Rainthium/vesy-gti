"""Ручная операция оператора в автономном режиме (architecture §3.4).

Поток одношаговый, по образцу UniServer/ВесыСофт (решение Игоря,
07.08.2026, см. docs/decisions.md): программа сама ждёт заезда и
стабилизации массы, оператор вводит номера и жмёт «Взвесить» — в этот
момент масса и снимки фиксируются И операция сразу записывается
в журнал (``capture_and_save``). Экран результата — информационный.

Внутри фиксация разложена на кирпичи prepare → commit (+ discard):
они пригодны и для сценариев с отдельным подтверждением.

Правила:
- №3: ручной режим доступен только без связи с центром — проверяет
  вызывающий код (веб-слой) И повторно этот модуль через колбэк;
- №4: нетто = брутто − последняя тара этого номера не старше 3 месяцев,
  из локальной реплики; нет тары → запись с признаком «нет тары»;
- №2: запись создаётся только при подтверждении и далее неизменяема;
  снимки после сохранения не пересжимаются (пишутся байты как есть).
"""

import contextlib
import logging
import secrets
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from agent.cameras.capture import CameraConfig, CameraShot
from agent.cameras.stream import CameraStreams, shots_or_capture_all
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage, StoredPhoto
from agent.weighing.shots import store_shots
from shared.enums import ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord, WeighingRecord

logger = logging.getLogger(__name__)

DEFAULT_VEHICLE_THRESHOLD_KG = 500.0  # ниже — считаем, что АТС на весах нет


class ManualFlowError(Exception):
    """Ошибка ручной операции с текстом для формы оператора."""


@dataclass(frozen=True)
class ManualPreview:
    """Превью зафиксированной операции до подтверждения оператором.

    Снимки уже лежат файлами на диске; запись в журнал появится только
    после ``commit()``.
    """

    preview_id: str
    record: WeighingRecord
    photos: list[StoredPhoto]
    tare: TareRecord | None  # найденная тара (для карточки результата)
    # последнее (устаревшее) тарирование сцепки, когда действующего нет:
    # оператор видит дату и массу на карточке результата (просьба Игоря
    # 14.08.2026); в расчёт нетто оно не подставляется (правило №4)
    expired_tare: TareRecord | None = None

    @property
    def no_valid_tare(self) -> bool:
        """Взвешивание без действующей тары — нетто не рассчитано."""
        return self.record.operation is Operation.WEIGHING and self.record.netto is None


class ManualOperationFlow:
    """Ручные операции: превью → подтверждение. Одно превью за раз.

    ``scale_state`` — снимок состояния индикатора (обычно driver.state);
    ``manual_allowed`` — правило №3 (нет связи с центром) либо разрешение
    центра при живой связи (0.4.28, объект без АИС);
    ``busy`` — идёт ли операция по команде АИС: при связи ручная фиксация
    не должна пересекаться с ней (иначе две записи на одну стоянку);
    ``photos_dir`` — корень хранения снимков (структура ГГГГ/ММ/ДД).
    """

    def __init__(
        self,
        *,
        scale_state: Callable[[], ScaleState],
        manual_allowed: Callable[[], bool],
        storage: AgentStorage,
        cameras: list[CameraConfig],
        photos_dir: str | Path,
        vehicle_threshold_kg: float = DEFAULT_VEHICLE_THRESHOLD_KG,
        ffmpeg_path: str = "ffmpeg",
        streams: CameraStreams | None = None,
        now_utc: Callable[[], datetime] | None = None,
        busy: Callable[[], bool] | None = None,
    ) -> None:
        self._scale_state = scale_state
        self._manual_allowed = manual_allowed
        self._busy = busy or (lambda: False)
        self._storage = storage
        self._cameras = cameras
        self._photos_dir = Path(photos_dir)
        self._vehicle_threshold_kg = vehicle_threshold_kg
        self._ffmpeg_path = ffmpeg_path
        # буфер потоковых камер: кадр мгновенно, без RTSP-подключения
        self._streams = streams
        # время записи: часы центра (agent/clock.py), по умолчанию локальные
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._lock = threading.Lock()
        self._pending: ManualPreview | None = None

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        """Новый список камер (настройки из центра)."""
        self._cameras = cameras

    def set_vehicle_threshold(self, threshold_kg: float) -> None:
        """Новый порог заезда (настройки из центра)."""
        self._vehicle_threshold_kg = threshold_kg

    # --- шаг 1: фиксация (кнопка «Взвесить») ---

    def ready(self) -> bool:
        """Можно ли фиксировать прямо сейчас (для активации кнопки)."""
        scale = self._scale_state()
        return (
            self._manual_allowed()
            and not self._busy()
            and scale.status is ScaleStatus.OK
            and scale.stable
            and not scale.overload
            and scale.weight_kg is not None
            and scale.weight_kg >= self._vehicle_threshold_kg
        )

    def prepare(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        """Зафиксировать вес и снимки; вернуть превью для подтверждения.

        Новое превью заменяет неподтверждённое старое (его снимки удаляются).
        """
        if not self._manual_allowed():
            raise ManualFlowError("Есть связь с центром — взвешивание проводится через АИС «СВХ»")
        if self._busy():
            raise ManualFlowError(
                "На весах выполняется операция по команде АИС «СВХ» — дождитесь её завершения"
            )
        vehicle_number = vehicle_number.strip().upper()
        trailer_number = (trailer_number or "").strip().upper() or None
        if not vehicle_number:
            raise ManualFlowError("Укажите номер головы (номер ТС)")

        scale = self._scale_state()
        if scale.status is not ScaleStatus.OK:
            raise ManualFlowError("Нет данных с весового индикатора — проверьте оборудование")
        if scale.overload:
            raise ManualFlowError("Перегруз весов — вес использовать нельзя")
        if scale.weight_kg is None or scale.weight_kg < self._vehicle_threshold_kg:
            raise ManualFlowError("АТС не на весах — дождитесь заезда")
        if not scale.stable:
            raise ManualFlowError("Масса нестабильна — дождитесь остановки АТС")

        weight = scale.weight_kg
        weighed_at = self._now_utc()
        record_uuid = uuid4()

        shots = shots_or_capture_all(self._cameras, self._streams, ffmpeg_path=self._ffmpeg_path)
        camera_errors = [shot.error or "камера недоступна" for shot in shots if not shot.ok]
        if camera_errors:
            # решение Игоря 09.08.2026: без снимков ОБЕИХ камер операция
            # не проводится (файлы не сохранялись — сохранять нечего)
            raise ManualFlowError(
                "Операция не проведена — камера недоступна: " + "; ".join(camera_errors)
            )
        photos, _ = self._store_photos(record_uuid, weighed_at, shots, weight)

        tare: TareRecord | None = None
        netto: float | None = None
        expired_tare: TareRecord | None = None
        if operation is Operation.WEIGHING:
            # правило №4 (ред. 09.08.2026): тара — только совпавшей СЦЕПКИ
            tare = self._storage.find_active_tare(vehicle_number, weighed_at, trailer_number)
            if tare is not None:
                netto = weight - tare.tare_value
            else:
                # строка реестра без действующей тары — устаревшее тарирование
                expired_tare = self._storage.latest_tare(vehicle_number, trailer_number)

        record = WeighingRecord(
            uuid=record_uuid,
            operation=operation,
            code=ErrorCode.OK,
            massa=weight,
            stable=True,
            weighed_at=weighed_at,
            vehicle_number=vehicle_number,
            trailer_number=trailer_number,  # нормализован выше
            tare_value=tare.tare_value if tare else None,
            tare_weighing_uuid=tare.weighing_uuid if tare else None,
            netto=netto,
            source=WeighingSource.LOCAL_OFFLINE,
            operator=operator,
        )
        preview = ManualPreview(
            preview_id=secrets.token_urlsafe(16),
            record=record,
            photos=photos,
            tare=tare,
            expired_tare=expired_tare,
        )
        with self._lock:
            old = self._pending
            self._pending = preview
        if old is not None:
            self._remove_files(old)
        logger.info(
            "ручная операция подготовлена: %s %s %.0f кг (оператор %s)",
            operation.value,
            vehicle_number,
            weight,
            operator,
        )
        return preview

    def capture_and_save(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        """Одношаговая операция как в ВесыСофт: фиксация и запись сразу.

        Нажатие кнопки оператором — это и есть подтверждение: масса
        и снимки фиксируются и операция немедленно пишется в журнал.
        """
        preview = self.prepare(
            operation,
            vehicle_number=vehicle_number,
            trailer_number=trailer_number,
            operator=operator,
        )
        self.commit(preview.preview_id)
        return preview

    # --- шаг 2 (кирпичи): подтверждение / отмена ---

    def pending(self) -> ManualPreview | None:
        with self._lock:
            return self._pending

    def commit(self, preview_id: str) -> WeighingRecord:
        """Сохранить превью в журнал (после этого запись неизменяема)."""
        with self._lock:
            preview = self._pending
            if preview is None or preview.preview_id != preview_id:
                raise ManualFlowError("Операция устарела — повторите фиксацию")
            # превью очищается только ПОСЛЕ успешной записи: сбой SQLite
            # (диск, блокировка) не должен потерять зафиксированную операцию
            self._storage.save_weighing(preview.record, preview.photos)
            self._pending = None
        logger.info("ручная операция записана: %s", preview.record.uuid)
        return preview.record

    def discard(self, preview_id: str) -> None:
        """Отменить превью: записи не будет, снимки удаляются."""
        with self._lock:
            preview = self._pending
            if preview is None or preview.preview_id != preview_id:
                return  # уже отменено или заменено — отмена идемпотентна
            self._pending = None
        self._remove_files(preview)
        logger.info("ручная операция отменена: %s", preview.record.vehicle_number)

    # --- внутреннее ---

    def _store_photos(
        self,
        record_uuid: UUID,
        weighed_at: datetime,
        shots: list[CameraShot],
        weight_kg: float | None,
    ) -> tuple[list[StoredPhoto], list[str]]:
        """Сохранить снимки с оверлеем (общий кирпич — shots.store_shots)."""
        return store_shots(self._photos_dir, record_uuid, weighed_at, shots, weight_kg=weight_kg)

    @staticmethod
    def _remove_files(preview: ManualPreview) -> None:
        """Удалить файлы неподтверждённого превью (записи в журнале нет)."""
        for photo in preview.photos:
            with contextlib.suppress(OSError):
                Path(photo.path).unlink()
