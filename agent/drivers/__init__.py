"""Реестр драйверов весовых индикаторов (architecture §3.2).

Логика взвешивания видит только контракт ScaleDriver; конкретный
протокол выбирается по ``scale.driver`` конфига агента.
"""

from agent.drivers.base import SerialScaleDriver
from agent.drivers.cas22 import Cas22Driver
from agent.drivers.vesar import VesarDriver

# имя в конфиге → класс драйвера; новые индикаторы добавляются сюда
DRIVERS: dict[str, type[Cas22Driver] | type[VesarDriver]] = {
    "cas22": Cas22Driver,
    "vesar": VesarDriver,
}


def create_driver(name: str, port_url: str, *, baudrate: int) -> SerialScaleDriver:
    """Создать драйвер по имени протокола из конфига агента.

    Неизвестное имя не должно случаться (конфиг валидируется Literal'ом),
    но ошибка при старте понятнее KeyError.
    """
    try:
        driver_cls = DRIVERS[name]
    except KeyError:
        raise ValueError(f"неизвестный драйвер индикатора: {name!r}") from None
    return driver_cls(port_url, baudrate=baudrate)
