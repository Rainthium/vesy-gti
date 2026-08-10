"""Тесты конфига агента (agent/config.py) и сборки оркестратора (agent/main.py).

Покрытие:
- config.example.toml (образец Кызыл-Кыи) парсится моделью — защита
  от рассинхронизации примера с кодом; значения выгрузки на месте;
- опечатки (неизвестные ключи), короткий токен, кривой URL центра — ошибки;
- дефолты цикла соответствуют выгрузке (порог 200 кг, стабильность 5 с);
- http_base_url: ws→http, wss→https, путь отбрасывается;
- уборка снимков-сирот: чужие файлы удаляются, снимки записей — нет;
- build_runtime: сервисы собираются, инфо/камеры/тара работают,
  правило №3 — ручной режим доступен только без связи с центром.
"""

import asyncio
import contextlib
import socket
import time
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent.cameras.capture import CameraConfig
from agent.config import AgentConfig, load_config
from agent.main import (
    CameraHealth,
    build_runtime,
    cleanup_orphan_photos,
    http_base_url,
    run_agent,
)
from agent.sync.storage import AgentStorage, StoredPhoto
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import TareRecord, WeighingRecord

EXAMPLE = Path(__file__).resolve().parents[1] / "agent" / "config.example.toml"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def config_data(**overrides: object) -> dict[str, object]:
    """Минимальный валидный конфиг; overrides — точечные замены секций."""
    data: dict[str, object] = {
        "site_name": "Тестовый объект",
        "scale_name": "Весы",
        "agent_id": "test-1",
        "scale": {"port": "socket://127.0.0.1:4001"},
        "cameras": [{"role": "front", "snapshot_url": "http://127.0.0.1:1/pic"}],
        "center": {"url": "ws://127.0.0.1:8080/agents/ws", "token": "t" * 24},
        "storage": {"db_path": "agent.sqlite3", "photos_dir": "photos"},
        "web": {"session_secret": "s" * 32},
    }
    data.update(overrides)
    return data


class TestConfigModel:
    def test_example_config_parses(self) -> None:
        """Образец Кызыл-Кыи всегда валиден (пример не отстаёт от модели)."""
        config = load_config(EXAMPLE)
        assert config.site_name == "СВХ «Кызыл-Кыя»"
        assert config.scale.port == "COM5"
        assert config.scale.baudrate == 9600
        assert config.cycle.zero_threshold_kg == 200.0  # НмПВ из выгрузки
        assert config.cycle.stable_duration_s == 5.0
        assert [c.role for c in config.cameras] == [CameraRole.FRONT, CameraRole.REAR]
        assert all(c.snapshot_url and "/ISAPI/" in c.snapshot_url for c in config.cameras)
        assert config.web.port == 8090  # 8087 занят UniServer

    def test_unknown_key_is_error(self) -> None:
        """Опечатка в конфиге видна при старте, а не игнорируется молча."""
        with pytest.raises(ValidationError, match="zero_treshold_kg"):
            AgentConfig.model_validate(
                config_data(cycle={"zero_treshold_kg": 100.0})  # опечатка
            )

    def test_short_token_rejected(self) -> None:
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(
                config_data(center={"url": "ws://x/agents/ws", "token": "short"})
            )

    def test_center_url_must_be_websocket(self) -> None:
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(
                config_data(center={"url": "http://x/agents/ws", "token": "t" * 24})
            )

    def test_cameras_required(self) -> None:
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(config_data(cameras=[]))

    def test_camera_without_urls_rejected(self) -> None:
        """Камера без единого URL — ошибка при старте, а не при первом снимке."""
        with pytest.raises(ValidationError, match="ни snapshot_url, ни rtsp_url"):
            AgentConfig.model_validate(config_data(cameras=[{"role": "front"}]))

    def test_unknown_driver_rejected(self) -> None:
        """Опечатка в имени драйвера не запускает cas22 молча."""
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(config_data(scale={"port": "COM5", "driver": "cas-22"}))

    def test_cycle_defaults_match_survey(self) -> None:
        """Дефолты цикла — значения выгрузки Кызыл-Кыи."""
        config = AgentConfig.model_validate(config_data())
        cycle = config.cycle.to_cycle_config()
        assert cycle.zero_threshold_kg == 200.0
        assert cycle.stable_duration_s == 5.0
        assert cycle.vehicle_timeout_s == 90.0


class TestHttpBaseUrl:
    def test_ws_to_http(self) -> None:
        assert http_base_url("ws://127.0.0.1:8080/agents/ws") == "http://127.0.0.1:8080"

    def test_wss_to_https(self) -> None:
        assert http_base_url("wss://vesy.gti.kg/agents/ws") == "https://vesy.gti.kg"


def _saved_record_with_photo(storage: AgentStorage, photos_dir: Path) -> Path:
    """Запись журнала со снимком-файлом; вернуть путь снимка."""
    record_uuid = uuid4()
    day_dir = photos_dir / "2026/08/09"
    day_dir.mkdir(parents=True)
    photo_path = day_dir / f"{record_uuid.hex}_photo1.jpeg"
    photo_path.write_bytes(b"\xff\xd8\xff\xe0known\xff\xd9")
    record = WeighingRecord(
        uuid=record_uuid,
        operation=Operation.WEIGHING,
        code=ErrorCode.OK,
        massa=12000.0,
        stable=True,
        weighed_at=datetime.now(UTC),
        vehicle_number="01KG111AAA",
        source=WeighingSource.AIS,
    )
    storage.save_weighing(
        record,
        [StoredPhoto(role=CameraRole.FRONT, path=str(photo_path), sha256="a" * 64, size_bytes=1)],
    )
    return photo_path


class TestCleanupOrphanPhotos:
    def test_orphans_removed_known_kept(self, tmp_path: Path) -> None:
        storage = AgentStorage(tmp_path / "agent.sqlite3")
        photos_dir = tmp_path / "photos"
        known = _saved_record_with_photo(storage, photos_dir)
        orphan = photos_dir / "2026/08/09" / f"{uuid4().hex}_photo1.jpeg"
        orphan.write_bytes(b"\xff\xd8\xff\xe0orphan\xff\xd9")
        stray_root = photos_dir / "stray.jpeg"
        stray_root.write_bytes(b"x")

        removed = cleanup_orphan_photos(storage, photos_dir)

        assert removed == 2
        assert known.exists()  # снимок записи неприкосновенен (правило №2)
        assert not orphan.exists() and not stray_root.exists()
        storage.close()

    def test_missing_dir_is_noop(self, tmp_path: Path) -> None:
        storage = AgentStorage(tmp_path / "agent.sqlite3")
        assert cleanup_orphan_photos(storage, tmp_path / "нет-такого") == 0
        storage.close()


class TestBuildRuntime:
    def test_services_glued(self, tmp_path: Path) -> None:
        """Сервисы собираются из конфига; инфо, камеры, тара, правило №3."""
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, _driver, storage, _client, _uploader, _camera_health, _watcher, _auto = (
            build_runtime(config)
        )
        try:
            info = runtime.info
            assert info.site_name == "Тестовый объект"
            assert "socket://127.0.0.1:4001" in info.port_label
            assert runtime.camera_roles() == [CameraRole.FRONT]
            assert runtime.pending_count() == 0

            # правило №3: без связи с центром ручной режим разрешён
            # (кнопка неактивна лишь из-за отсутствия стабильного веса)
            assert not runtime.center_connected()

            # тара находится по нормализованному номеру
            storage.replace_tare_registry(
                [
                    TareRecord(
                        vehicle_number="01KG777AAA",
                        tare_value=8000.0,
                        tared_at=datetime.now(UTC) - timedelta(days=1),
                        weighing_uuid=uuid4(),
                    )
                ]
            )
            tare = runtime.find_active_tare("  01kg777aaa ")
            assert tare is not None and tare.tare_value == 8000.0
        finally:
            storage.close()

    def test_run_agent_boots_and_serves_login(self, tmp_path: Path) -> None:
        """Дымовой старт всего агента по конфигу: веб оператора отвечает.

        Индикатор и центр недоступны (драйвер и клиент бесконечно
        переподключаются — это штатно), но служба живёт и логин отдаётся.
        """
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            web_port = int(sock.getsockname()[1])
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                },
                web={"port": web_port, "session_secret": "s" * 32},
            )
        )

        async def scenario() -> None:
            task = asyncio.create_task(run_agent(config))
            try:

                def login_ok() -> bool:
                    try:
                        with urllib.request.urlopen(
                            f"http://127.0.0.1:{web_port}/login", timeout=2
                        ) as response:
                            return bool(response.status == 200)
                    except OSError:
                        return False

                deadline = time.monotonic() + 15
                while not await asyncio.to_thread(login_ok):
                    assert time.monotonic() < deadline, "веб оператора не поднялся"
                    await asyncio.sleep(0.2)
            finally:
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task

        asyncio.run(asyncio.wait_for(scenario(), timeout=30))

    def test_camera_snapshot_unknown_role_raises(self, tmp_path: Path) -> None:
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, _, storage, _, _, _, _, _ = build_runtime(config)
        try:
            with pytest.raises(ValueError, match="не настроена"):
                runtime.camera_snapshot(CameraRole.REAR)
        finally:
            storage.close()


class TestCameraHealth:
    def test_unreachable_camera_reported_unavailable(self) -> None:
        """Недоступная камера видна в статусах (heartbeat → дашборд центра)."""
        health = CameraHealth(
            [
                CameraConfig(
                    role=CameraRole.FRONT,
                    snapshot_url=f"http://127.0.0.1:{_free_port()}/pic",
                    timeout_s=0.3,
                )
            ],
            interval_s=60.0,
            ffmpeg_path="ffmpeg",
        )
        asyncio.run(health.check_once())
        [status] = health.statuses
        assert status.role is CameraRole.FRONT
        assert status.available is False
        assert status.last_snapshot_at is None  # удачного снимка ещё не было
