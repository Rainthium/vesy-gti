"""Конфиг агента весового ПК: TOML-файл рядом со службой.

Формат — TOML (stdlib ``tomllib``, без внешних зависимостей — удобно
для PyInstaller). Образец с данными Кызыл-Кыи — ``agent/config.example.toml``.

Правило №7: боевой конфиг содержит секреты (токен агента, пароли камер
в URL) и живёт ТОЛЬКО на весовом ПК — в git не попадает.

Параметры объекта (architecture §3.5): порог заезда, время стабильности,
таймауты, адреса камер, порт индикатора, тип драйвера, адрес центра.
"""

import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, PositiveFloat, model_validator

from agent.cameras.capture import DEFAULT_TIMEOUT_S, CameraConfig
from agent.weighing.cycle import CycleConfig
from shared.enums import CameraRole


class _Section(BaseModel):
    """Общие настройки разбора: неизвестные ключи — ошибка (опечатки в конфиге
    должны быть видны при старте, а не молча игнорироваться)."""

    model_config = ConfigDict(extra="forbid")


class ScaleSection(_Section):
    """Весовой индикатор."""

    # Literal: опечатка в имени драйвера — ошибка при старте, а не тихий
    # запуск cas22; реестр — agent/drivers/__init__.py (DRIVERS)
    driver: Literal["cas22", "vesar", "xk3190"] = "cas22"
    port: str  # «COM5» либо pyserial-URL («socket://127.0.0.1:4001» — эмулятор)
    baudrate: int = 9600
    # только vesar/xk3190 (0.4.27): делитель 7 цифр кадра (10 — десятые кг,
    # 1 — целые кг) и дискрета табло для квантования; None — умолчание
    # драйвера (vesar 10/10, xk3190 1/20)
    weight_divisor: PositiveFloat | None = None
    discrete_kg: PositiveFloat | None = None


class CycleSection(_Section):
    """Параметры цикла взвешивания (дефолты — Кызыл-Кыя, выгрузка 07.08.2026)."""

    zero_threshold_kg: float = 200.0  # НмПВ UniServer: 20 дискрет × 10 кг
    vehicle_threshold_kg: float = 500.0
    zero_timeout_s: float = 10.0
    vehicle_timeout_s: float = 90.0  # в UniServer явного нет — своё значение
    stable_duration_s: float = 5.0  # фиксация после 5 с неизменной массы
    stable_timeout_s: float = 30.0
    no_data_timeout_s: float = 5.0

    def to_cycle_config(self) -> CycleConfig:
        return CycleConfig(**self.model_dump())


class CameraSection(_Section):
    """Одна камера; snapshot_url (ISAPI) — основной путь, rtsp_url — запасной.

    preview_url — лёгкий кадр только для превью оператора (суб-поток
    камеры); задан → превью обновляется чаще, фото операций — по-прежнему
    с основного URL.
    """

    role: CameraRole
    snapshot_url: str | None = None
    rtsp_url: str | None = None
    preview_url: str | None = None
    timeout_s: float = DEFAULT_TIMEOUT_S

    @model_validator(mode="after")
    def _at_least_one_url(self) -> Self:
        """Ошибка видна при старте, а не при первом снимке."""
        if not self.snapshot_url and not self.rtsp_url:
            raise ValueError(f"камера {self.role}: не задан ни snapshot_url, ни rtsp_url")
        return self

    def to_camera_config(self) -> CameraConfig:
        return CameraConfig(
            role=self.role,
            snapshot_url=self.snapshot_url,
            rtsp_url=self.rtsp_url,
            preview_url=self.preview_url,
            timeout_s=self.timeout_s,
        )


class CenterSection(_Section):
    """Подключение к центру."""

    url: str = Field(pattern=r"^wss?://")  # ws(s)://vesy.gti.kg/agents/ws
    token: str = Field(min_length=16)  # выпускает центр: tools/center_admin create-agent
    heartbeat_interval_s: float = 5.0


class StorageSection(_Section):
    """Локальные данные агента."""

    db_path: Path
    photos_dir: Path
    # через сколько дней после подтверждённой загрузки в центр убирать
    # локальный файл снимка; 0 — не убирать никогда (см. agent/sync/retention.py)
    photo_retention_days: int = Field(default=30, ge=0)


class WebSection(_Section):
    """Локальный веб-интерфейс оператора."""

    host: str = "127.0.0.1"  # наружу не открываем: оператор работает на этом ПК
    port: int = 8090  # 8087 занят UniServer на время параллельной работы
    session_secret: str = Field(min_length=16)


class AgentConfig(_Section):
    """Полный конфиг агента."""

    site_name: str
    scale_name: str
    indicator_model: str = ""
    agent_id: str = Field(min_length=1)
    scale: ScaleSection
    cycle: CycleSection = Field(default_factory=CycleSection)
    cameras: list[CameraSection] = Field(min_length=1)
    center: CenterSection
    storage: StorageSection
    web: WebSection
    ffmpeg_path: str = "ffmpeg"  # для RTSP-запасного пути
    # период фоновой проверки камер (статусы для heartbeat/дашборда центра)
    camera_check_interval_s: float = Field(default=60.0, gt=0)

    def camera_configs(self) -> list[CameraConfig]:
        return [camera.to_camera_config() for camera in self.cameras]


def load_config(path: str | Path) -> AgentConfig:
    """Прочитать и провалидировать конфиг; ошибки — понятным текстом при старте."""
    with open(path, "rb") as fh:
        data = tomllib.load(fh)
    return AgentConfig.model_validate(data)
