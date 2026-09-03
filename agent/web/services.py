"""Слой данных локального веб-интерфейса оператора (architecture §3.4).

Веб-приложение не знает ни о драйвере, ни о WebSocket-клиенте напрямую —
только об этом протоколе. Реализует его оркестратор агента (собирается
в задаче упаковки); в тестах — фейк.
"""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from agent.cameras.capture import CameraShot
from agent.drivers.base import ScaleState
from agent.weighing.manual import ManualPreview
from shared.enums import CameraRole, Operation
from shared.messages import TareRecord, VerificationInfo, WeighingRecord


@dataclass(frozen=True)
class AgentInfo:
    """Статичные сведения агента для шапки и экрана «Оборудование»."""

    site_name: str  # СВХ «Кызыл-Кыя»
    scale_name: str  # Весы SCS-80
    indicator_model: str  # CAS CI-201A
    driver_name: str  # cas22
    port_label: str  # COM3 · 9600 · 8-N-1
    agent_version: str
    center_url: str  # wss://vesy.gti.kg (для экрана «Оборудование»)


class AgentServices(Protocol):
    """Что веб-интерфейсу нужно от агента."""

    @property
    def info(self) -> AgentInfo: ...

    def scale_state(self) -> ScaleState:
        """Мгновенное состояние индикатора (вес, стабильность, статус)."""
        ...

    def center_connected(self) -> bool:
        """Есть ли связь с центром (правило режимов №3)."""
        ...

    def manual_allowed(self) -> bool:
        """Доступен ли оператору ручной режим: нет связи с центром (правило №3)
        либо центр разрешил ручной режим при связи (0.4.28, объект без АИС)."""
        ...

    def manual_allowed_by_center(self) -> bool:
        """Разрешил ли центр ручной режим при живой связи (подсказка оператору)."""
        ...

    def pending_count(self) -> int:
        """Очередь недосланных офлайн-записей."""
        ...

    def tare_registry_size(self) -> int:
        """Размер локальной реплики реестра тарирований."""
        ...

    def recent_weighings(self, limit: int = 50) -> list[tuple[WeighingRecord, bool]]:
        """Журнал объекта, новые первыми; второй элемент пары — дослано ли в центр."""
        ...

    def camera_roles(self) -> list[CameraRole]:
        """Настроенные камеры (обычно front и rear)."""
        ...

    def preview_interval_ms(self) -> int:
        """Период опроса превью браузером: 1000 при лёгком preview_url, иначе 2000."""
        ...

    def photo_roles(self, weighing_uuid: UUID) -> list[CameraRole]:
        """Роли снимков записи журнала (по журналу, не по наличию файлов)."""
        ...

    def photo_bytes(
        self, weighing_uuid: UUID, role: CameraRole, *, thumb: bool = False
    ) -> bytes | None:
        """Снимок записи: локальный файл, а после ретеншна — из центра."""
        ...

    # --- печатная весовая карточка (задача 13.08.2026) ---

    def record_by_uuid(self, weighing_uuid: UUID) -> WeighingRecord | None:
        """Запись журнала по uuid (для печати карточки любой давности)."""
        ...

    def tare_by_weighing_uuid(self, weighing_uuid: UUID) -> TareRecord | None:
        """Строка реплики реестра тар по uuid исходного тарирования."""
        ...

    def verification(self) -> VerificationInfo | None:
        """Свидетельство о поверке весов из снимка настроек центра."""
        ...

    def photo_available(self, weighing_uuid: UUID, role: CameraRole) -> bool:
        """Достижим ли снимок сейчас (локально либо с центра при связи)."""
        ...

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        """Свежий кадр камеры (может занимать до пары секунд)."""
        ...

    def verify_operator(self, login: str, password: str) -> str | None:
        """Проверка входа; отображаемое имя оператора либо None."""
        ...

    def operator_stamp(self, login: str) -> str | None:
        """Штамп текущего пароля активной учётки; None — учётки нет/заблокирована.
        Сверяется на каждом запросе живой сессии (смена пароля выбивает)."""
        ...

    def reopen_port(self) -> None:
        """Принудительно переоткрыть порт индикатора (кнопка на «Оборудовании»)."""
        ...

    # --- диагностика (экран «Диагностика», работает и без связи с центром) ---

    def photo_queue(self) -> tuple[int, int]:
        """Очередь загрузки снимков: (всего ждёт, из них застряло)."""
        ...

    def clock_offset_s(self) -> float | None:
        """Смещение часов ПК относительно центра; None — время не получено."""
        ...

    def log_tail(self, lines: int = 300) -> list[str]:
        """Последние строки журнала службы (пусто — файл недоступен)."""
        ...

    def log_location(self) -> str:
        """Где лежит журнал службы — подсказка оператору."""
        ...

    # --- ручной режим (автономный, правило №3; поток agent/weighing/manual.py) ---

    def manual_ready(self) -> bool:
        """Можно ли фиксировать вес сейчас (офлайн + стабильная масса ≥ порога)."""
        ...

    def manual_capture(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        """Одношаговая операция (как в ВесыСофт): фиксация массы и снимков
        с немедленной записью в журнал. ManualFlowError — текст для формы."""
        ...

    def find_active_tare(
        self, vehicle_number: str, trailer_number: str | None = None
    ) -> TareRecord | None:
        """Действующая тара СЦЕПКИ из локальной реплики (подсказка в форме);
        подставляется только при совпадении обоих номеров (решение 09.08.2026)."""
        ...

    def latest_tare(
        self, vehicle_number: str, trailer_number: str | None = None
    ) -> TareRecord | None:
        """Последнее тарирование сцепки БЕЗ проверки срока — для примечаний
        об устаревшей таре (карта, подсказка формы); в расчёт не подставлять."""
        ...
