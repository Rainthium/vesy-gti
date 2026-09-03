"""Реестр драйверов весовых индикаторов (architecture §3.2).

Логика взвешивания видит только контракт ScaleDriver; конкретный
протокол выбирается по ``scale.driver`` конфига агента.
"""

from agent.drivers.base import SerialScaleDriver
from agent.drivers.cas22 import Cas22Driver
from agent.drivers.vesar import VesarDriver
from agent.drivers.xk3190 import Xk3190Driver

# имя в конфиге → класс драйвера; новые индикаторы добавляются сюда
DRIVERS: dict[str, type[Cas22Driver] | type[VesarDriver]] = {
    "cas22": Cas22Driver,
    "vesar": VesarDriver,
    "xk3190": Xk3190Driver,
}


def create_driver(
    name: str,
    port_url: str,
    *,
    baudrate: int,
    weight_divisor: float | None = None,
    discrete_kg: float | None = None,
) -> SerialScaleDriver:
    """Создать драйвер по имени протокола из конфига агента.

    Неизвестное имя не должно случаться (конфиг валидируется Literal'ом),
    но ошибка при старте понятнее KeyError. ``weight_divisor`` и
    ``discrete_kg`` понимают только драйверы семейства vesar/xk3190;
    None — умолчание драйвера.
    """
    try:
        driver_cls = DRIVERS[name]
    except KeyError:
        raise ValueError(f"неизвестный драйвер индикатора: {name!r}") from None
    if issubclass(driver_cls, VesarDriver):
        return driver_cls(
            port_url,
            baudrate=baudrate,
            weight_divisor=weight_divisor,
            discrete_kg=discrete_kg,
        )
    if weight_divisor is not None or discrete_kg is not None:
        raise ValueError(f"драйвер {name!r} не использует weight_divisor/discrete_kg")
    return driver_cls(port_url, baudrate=baudrate)
