"""Интерфейс драйвера весового индикатора (architecture §3.2).

Драйверы — сменные модули: логика взвешивания видит только ``ScaleDriver``
и ``ScaleState`` и ничего не знает о конкретном протоколе.
"""

from dataclasses import dataclass
from typing import Protocol

from shared.enums import ScaleStatus


@dataclass(frozen=True)
class ScaleState:
    """Мгновенный снимок состояния индикатора (неизменяемая копия).

    ``weight_kg is None`` — данных нет или перегруз (см. ``overload``).
    ``last_packet_at`` — ``time.monotonic()`` последнего разобранного пакета.
    """

    status: ScaleStatus
    weight_kg: float | None = None
    stable: bool = False
    overload: bool = False
    error: str | None = None  # текст последней ошибки порта (диагностика)
    last_packet_at: float | None = None


class ScaleDriver(Protocol):
    """Контракт драйвера индикатора.

    Реализация обязана быть устойчивой: сбой порта не роняет процесс,
    драйвер бесконечно переоткрывает порт сам (правило проекта №6).
    """

    def start(self) -> None:
        """Открыть порт и начать фоновое чтение."""
        ...

    def stop(self) -> None:
        """Остановить чтение и закрыть порт."""
        ...

    @property
    def state(self) -> ScaleState:
        """Текущее состояние индикатора (потокобезопасно)."""
        ...

    def zero(self) -> bool:
        """Обнулить весы, если протокол это поддерживает; вернуть успех."""
        ...


class SerialScaleDriver(ScaleDriver, Protocol):
    """Драйвер на последовательном порту, перенастраиваемый из центра.

    Расширение контракта для агента: смена COM-порта со страницы
    «Настройки» весов (SettingsManager) требует port_url/baudrate
    и set_port. Реализации — Cas22Driver, VesarDriver.
    """

    @property
    def port_url(self) -> str:
        """Текущий адрес порта («COM4» либо pyserial-URL)."""
        ...

    @property
    def baudrate(self) -> int:
        """Текущая скорость порта."""
        ...

    def set_port(self, port_url: str, baudrate: int | None = None) -> None:
        """Переключить порт/скорость: остановить чтение и запустить заново."""
        ...
