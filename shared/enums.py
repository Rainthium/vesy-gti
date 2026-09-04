"""Перечисления статусов, кодов ошибок и типов операций (architecture §4.1, §5).

Строковые значения — часть внешних контрактов (API АИС, протокол агент↔центр,
поля БД). Менять их нельзя без согласования.
"""

from enum import StrEnum


class ErrorCode(StrEnum):
    """Код завершения операции взвешивания (контракт АИС, architecture §4.1)."""

    OK = "OK"
    ERR_AGENT_OFFLINE = "ERR_AGENT_OFFLINE"  # нет связи центра с агентом объекта
    ERR_SCALE_OFFLINE = "ERR_SCALE_OFFLINE"  # агент на связи, но нет данных от индикатора
    ERR_NOT_ZERO = "ERR_NOT_ZERO"  # весы не пусты перед началом операции
    ERR_VEHICLE_TIMEOUT = "ERR_VEHICLE_TIMEOUT"  # АТС не заехало за отведённое время
    ERR_UNSTABLE = "ERR_UNSTABLE"  # вес не стабилизировался за таймаут
    ERR_CAMERA = "ERR_CAMERA"  # камера недоступна — операция не проведена (реш. 09.08.2026)
    ERR_BUSY = "ERR_BUSY"  # на этих весах уже выполняется операция
    # тарирование гружёной машины: масса больше лимита тары весов (решение
    # Игоря 04.09.2026, Кокчо-Коз) — операция не проводится, записи нет
    ERR_TARE_TOO_HEAVY = "ERR_TARE_TOO_HEAVY"
    ERR_INTERNAL = "ERR_INTERNAL"  # прочее, детали в поле message


class Operation(StrEnum):
    """Тип операции: взвешивание гружёного АТС (брутто) или тарирование пустого."""

    WEIGHING = "weighing"
    TARING = "taring"


class WeighingSource(StrEnum):
    """Источник операции: команда АИС «СВХ» или ручной офлайн-режим оператора."""

    AIS = "ais"
    LOCAL_OFFLINE = "local_offline"


class CameraRole(StrEnum):
    """Положение камеры относительно АТС."""

    FRONT = "front"
    REAR = "rear"


class ScaleStatus(StrEnum):
    """Состояние весового индикатора в самодиагностике агента."""

    OK = "ok"  # поток пакетов идёт
    NO_DATA = "no_data"  # порт открыт, но нет пакетов дольше таймаута (3 с)
    PORT_ERROR = "port_error"  # порт не открывается (например, CH340, ошибка 31)
