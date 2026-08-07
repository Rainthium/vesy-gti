"""Слой данных локального веб-интерфейса оператора (architecture §3.4).

Веб-приложение не знает ни о драйвере, ни о WebSocket-клиенте напрямую —
только об этом протоколе. Реализует его оркестратор агента (собирается
в задаче упаковки); в тестах — фейк.
"""

from dataclasses import dataclass
from typing import Protocol

from agent.cameras.capture import CameraShot
from agent.drivers.base import ScaleState
from agent.weighing.manual import ManualPreview
from shared.enums import CameraRole, Operation
from shared.messages import TareRecord, WeighingRecord


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

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        """Свежий кадр камеры (может занимать до пары секунд)."""
        ...

    def verify_operator(self, login: str, password: str) -> str | None:
        """Проверка входа; отображаемое имя оператора либо None."""
        ...

    def reopen_port(self) -> None:
        """Принудительно переоткрыть порт индикатора (кнопка на «Оборудовании»)."""
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

    def find_active_tare(self, vehicle_number: str) -> TareRecord | None:
        """Действующая тара номера ТС из локальной реплики (подсказка в форме)."""
        ...
