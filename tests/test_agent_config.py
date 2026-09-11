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

from agent.cameras.capture import CameraConfig, CameraShot
from agent.config import AgentConfig, load_config
from agent.main import (
    AgentRuntime,
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
        # лимит тары (решение Игоря 04.09.2026): 25 т по умолчанию
        assert cycle.max_tare_kg == 25000.0

    def test_max_tare_from_config(self) -> None:
        """[cycle] max_tare_kg переопределяет лимит; 0 — лимит выключен."""
        config = AgentConfig.model_validate(config_data(cycle={"max_tare_kg": 0}))
        assert config.cycle.to_cycle_config().max_tare_kg == 0.0
        config = AgentConfig.model_validate(config_data(cycle={"max_tare_kg": 30000}))
        assert config.cycle.to_cycle_config().max_tare_kg == 30000.0


class TestPhotoRetentionOption:
    """storage.photo_retention_days: срок жизни локальных копий снимков."""

    def _storage(self, **extra: object) -> dict[str, object]:
        return {"db_path": "agent.sqlite3", "photos_dir": "photos", **extra}

    def test_default_is_thirty_days(self) -> None:
        """Конфиг объекта, поставленного раньше, ключа не знает — берётся 30."""
        config = AgentConfig.model_validate(config_data())
        assert config.storage.photo_retention_days == 30

    def test_example_config_keeps_thirty(self) -> None:
        """Образец Кызыл-Кыи не расходится с моделью."""
        assert load_config(EXAMPLE).storage.photo_retention_days == 30

    def test_zero_allowed_as_switch_off(self) -> None:
        """0 — уборка выключена совсем (снимки лежат на ПК бессрочно)."""
        config = AgentConfig.model_validate(
            config_data(storage=self._storage(photo_retention_days=0))
        )
        assert config.storage.photo_retention_days == 0

    def test_negative_rejected(self) -> None:
        """Отрицательный срок — опечатка, видна при старте службы."""
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(config_data(storage=self._storage(photo_retention_days=-1)))

    def test_unknown_storage_key_rejected(self) -> None:
        """Опечатка в имени ключа не проходит молча (уборка не «выключится»)."""
        with pytest.raises(ValidationError, match="photo_retention_day"):
            AgentConfig.model_validate(config_data(storage=self._storage(photo_retention_day=30)))


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

    def test_thumbnail_of_known_photo_survives(self, tmp_path: Path) -> None:
        """Миниатюра журнала живёт, пока жив её кадр.

        Она не числится в журнале, и без учёта родства уборка стирала бы
        кэш при каждом старте агента (находка ревью 11.08.2026).
        """
        storage = AgentStorage(tmp_path / "agent.sqlite3")
        photos_dir = tmp_path / "photos"
        known = _saved_record_with_photo(storage, photos_dir)
        thumb = known.with_name(known.stem + "_thumb" + known.suffix)
        thumb.write_bytes(b"\xff\xd8thumb\xff\xd9")

        assert cleanup_orphan_photos(storage, photos_dir) == 0
        assert known.exists() and thumb.exists()
        storage.close()

    def test_thumbnail_of_orphan_removed(self, tmp_path: Path) -> None:
        """Миниатюра снимка-сироты уходит вместе с ним — мусор не копится."""
        storage = AgentStorage(tmp_path / "agent.sqlite3")
        photos_dir = tmp_path / "photos"
        photos_dir.mkdir(parents=True, exist_ok=True)
        orphan = photos_dir / f"{uuid4().hex}_photo1.jpeg"
        orphan.write_bytes(b"\xff\xd8orphan\xff\xd9")
        orphan_thumb = orphan.with_name(orphan.stem + "_thumb" + orphan.suffix)
        orphan_thumb.write_bytes(b"\xff\xd8thumb\xff\xd9")

        assert cleanup_orphan_photos(storage, photos_dir) == 2
        assert not orphan.exists() and not orphan_thumb.exists()
        storage.close()


class TestBuildRuntime:
    def test_info_port_label_follows_driver(self, tmp_path: Path) -> None:
        """Строка порта на экранах — живая, из драйвера: после смены порта из
        центра (driver.set_port) экран показывает новый порт без перезапуска
        (Кара-Суу 07.09.2026: до перезапуска висел socket://…)."""
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, driver, storage, _client, _uploader, _camera_health, _watcher, _auto, streams = (
            build_runtime(config)
        )
        streams.stop_all()
        try:
            assert runtime.info.port_label == "socket://127.0.0.1:4001 · 9600 · 8-N-1"
            driver.set_port("COM4", 19200)
            assert runtime.info.port_label == "COM4 · 19200 · 8-N-1"
            # прочие сведения не затронуты
            assert runtime.info.site_name == "Тестовый объект"
        finally:
            driver.stop()
            storage.close()

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
        runtime, _driver, storage, _client, _uploader, _camera_health, _watcher, _auto, streams = (
            build_runtime(config)
        )
        streams.stop_all()
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

    def test_disabled_retention_does_not_stop_agent(self, tmp_path: Path) -> None:
        """photo_retention_days = 0 не должен останавливать службу.

        run_agent ждёт задачи через FIRST_COMPLETED, а выключенный ретеншн
        завершает run() сразу — агент снимает все остальные задачи и
        выходит через доли секунды после старта (на Windows служба уходит
        в цикл перезапусков).
        """
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            web_port = int(sock.getsockname()[1])
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                    "photo_retention_days": 0,  # уборка выключена на объекте
                },
                web={"port": web_port, "session_secret": "s" * 32},
            )
        )

        async def scenario() -> None:
            task = asyncio.create_task(run_agent(config))
            await asyncio.sleep(1.0)
            alive = not task.done()
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
            assert alive, "агент остановился сам: выключенный ретеншн снял остальные задачи"

        asyncio.run(asyncio.wait_for(scenario(), timeout=30))

    def test_center_cameras_reach_preview_live(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Смена камер из центра доезжает до превью БЕЗ рестарта службы.

        Боевой урок Кызыл-Кыи 14.08.2026: свап ролей камер из панели доехал
        до съёмки операций, а превью оператора продолжало снимать по
        локальному config.toml — оператор видел прежние камеры и считал,
        что настройка не сработала.
        """
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, _, storage, _, _, _, _, _, streams = build_runtime(config)
        streams.stop_all()
        try:
            captured: list[str] = []

            def fake_capture(camera: CameraConfig, *, ffmpeg_path: str) -> CameraShot:
                captured.append(camera.snapshot_url or "")
                return CameraShot(role=camera.role, jpeg=b"\xff\xd8", captured_at=datetime.now(UTC))

            monkeypatch.setattr("agent.main.capture", fake_capture)
            runtime.camera_snapshot(CameraRole.FRONT)  # кадр лёг в кэш превью
            runtime.set_cameras(
                [
                    CameraConfig(role=CameraRole.FRONT, snapshot_url="http://u:p@10.9.9.9/new"),
                    CameraConfig(role=CameraRole.REAR, snapshot_url="http://u:p@10.9.9.8/new"),
                ]
            )
            # кэш сброшен: следующий кадр идёт сразу с НОВОЙ камеры
            runtime.camera_snapshot(CameraRole.FRONT)
            assert captured[-1] == "http://u:p@10.9.9.9/new"
            # роль, которой не было в локальном конфиге, появилась в превью
            assert runtime.camera_roles() == [CameraRole.FRONT, CameraRole.REAR]
        finally:
            storage.close()

    def test_preview_url_used_for_preview_and_speeds_up_polling(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Камера с preview_url (лёгкий кадр суб-потока, запрос Игоря
        20.08.2026 для Аламедина): превью снимается по нему, опрос браузера
        учащается до секунды; без preview_url всё по-старому (2 с)."""
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, _, storage, _, _, _, _, _, streams = build_runtime(config)
        streams.stop_all()
        try:
            assert runtime.preview_interval_ms() == 2000
            captured: list[str] = []

            def fake_capture(camera: CameraConfig, *, ffmpeg_path: str) -> CameraShot:
                captured.append(camera.snapshot_url or "")
                return CameraShot(role=camera.role, jpeg=b"\xff\xd8", captured_at=datetime.now(UTC))

            monkeypatch.setattr("agent.main.capture", fake_capture)
            runtime.set_cameras(
                [
                    CameraConfig(
                        role=CameraRole.FRONT,
                        snapshot_url="http://u:p@10.9.9.9/ch101",
                        preview_url="http://u:p@10.9.9.9/ch102",
                    )
                ]
            )
            assert runtime.preview_interval_ms() == 1000
            runtime.camera_snapshot(CameraRole.FRONT)
            # превью пошло с лёгкого URL, основной остался для фото операций
            assert captured == ["http://u:p@10.9.9.9/ch102"]
        finally:
            storage.close()

    def test_camera_snapshot_unknown_role_raises(self, tmp_path: Path) -> None:
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                }
            )
        )
        runtime, _, storage, _, _, _, _, _, streams = build_runtime(config)
        streams.stop_all()
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


class TestRuntimeRetention:
    def test_build_runtime_wires_retention_from_config(self, tmp_path: Path) -> None:
        """Уборка собирается в build_runtime со сроком из config.toml и живёт
        в runtime — цикл запускает run_agent, срок меняет центр (0.4.25)."""
        config = AgentConfig.model_validate(
            config_data(
                storage={
                    "db_path": str(tmp_path / "agent.sqlite3"),
                    "photos_dir": str(tmp_path / "photos"),
                    "photo_retention_days": 12,
                }
            )
        )
        runtime, _driver, storage, _client, _uploader, _health, _watcher, _auto, streams = (
            build_runtime(config)
        )
        streams.stop_all()
        try:
            assert runtime.retention is not None
            assert runtime.retention.retention_days == 12
            assert runtime.retention.enabled
        finally:
            storage.close()


class TestManualPermitRestore:
    def test_permit_restored_from_stored_snapshot(self, tmp_path: Path) -> None:
        """0.4.28: разрешение ручного режима из снимка центра переживает рестарт
        службы — объект без АИС не остаётся без кнопки до следующего hello."""
        db_path = tmp_path / "agent.sqlite3"
        config = AgentConfig.model_validate(
            config_data(storage={"db_path": str(db_path), "photos_dir": str(tmp_path / "photos")})
        )
        seed = AgentStorage(str(db_path))
        seed.save_center_settings('{"manual_allowed": true}')
        seed.close()
        runtime, _, storage, _, _, _, _, _, streams = build_runtime(config)
        streams.stop_all()
        try:
            assert runtime.manual_allowed_by_center() is True
            runtime.set_manual_allowed(False)
            assert runtime.manual_allowed_by_center() is False
        finally:
            storage.close()

    def test_snapshot_without_field_gives_no_permit(self, tmp_path: Path) -> None:
        """Снимок от центра до 0.4.28 (поля нет) — правило №3 как прежде."""
        db_path = tmp_path / "agent.sqlite3"
        config = AgentConfig.model_validate(
            config_data(storage={"db_path": str(db_path), "photos_dir": str(tmp_path / "photos")})
        )
        seed = AgentStorage(str(db_path))
        seed.save_center_settings('{"indicator_model": "CAS"}')
        seed.close()
        runtime, _, storage, _, _, _, _, _, streams = build_runtime(config)
        streams.stop_all()
        try:
            assert runtime.manual_allowed_by_center() is False
        finally:
            storage.close()


# --- превью 0.4.31 (урок Канта 11.09.2026): ужатие, запасной кадр, лог, замок ---


def _big_jpeg() -> bytes:
    import io

    from PIL import Image

    buffer = io.BytesIO()
    Image.new("RGB", (1600, 900), (10, 20, 30)).save(buffer, "JPEG", quality=90)
    return buffer.getvalue()


def _runtime_with_cameras(tmp_path: Path) -> tuple[AgentRuntime, AgentStorage, CameraHealth]:
    config = AgentConfig.model_validate(
        config_data(
            storage={
                "db_path": str(tmp_path / "agent.sqlite3"),
                "photos_dir": str(tmp_path / "photos"),
            }
        )
    )
    runtime, _, storage, _, _, camera_health, _, _, streams = build_runtime(config)
    streams.stop_all()
    return runtime, storage, camera_health


class TestPreviewRobustness:
    def test_preview_frame_is_shrunk(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Кадр превью ужимается агентом до 640 px: Hikvision Канта отдаёт по
        «лёгкому» каналу полный кадр 2560×1440, Dahua уменьшенного не имеет."""
        import io

        from PIL import Image

        runtime, storage, _ = _runtime_with_cameras(tmp_path)
        try:
            big = _big_jpeg()
            monkeypatch.setattr(
                "agent.main.capture",
                lambda camera, *, ffmpeg_path: CameraShot(
                    role=camera.role, jpeg=big, captured_at=datetime.now(UTC)
                ),
            )
            shot = runtime.camera_snapshot(CameraRole.FRONT)
            assert shot.ok and shot.jpeg is not None
            with Image.open(io.BytesIO(shot.jpeg)) as image:
                assert image.size == (640, 360)
            assert len(shot.jpeg) < len(big)
        finally:
            storage.close()

    def test_failure_serves_last_frame_then_error_and_logs_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Срыв съёмки: пока последний удачный кадр моложе PREVIEW_STALE_MAX_S —
        отдаётся он (оператор не видит «Нет сигнала» от единственной осечки);
        старше — ошибка. Срывы подряд пишутся в лог один раз (PREVIEW_WARN_EVERY_S)."""
        runtime, storage, _ = _runtime_with_cameras(tmp_path)
        try:
            monkeypatch.setattr("agent.main.PREVIEW_TTL_S", 0.0)  # каждый запрос — новая съёмка
            outcomes: list[bool] = [True, False, False]

            def fake_capture(camera: CameraConfig, *, ffmpeg_path: str) -> CameraShot:
                ok = outcomes.pop(0) if outcomes else False
                if ok:
                    return CameraShot(
                        role=camera.role, jpeg=_big_jpeg(), captured_at=datetime.now(UTC)
                    )
                return CameraShot(
                    role=camera.role,
                    jpeg=None,
                    captured_at=datetime.now(UTC),
                    error="front: таймаут",
                )

            monkeypatch.setattr("agent.main.capture", fake_capture)
            first = runtime.camera_snapshot(CameraRole.FRONT)
            assert first.ok
            with caplog.at_level("WARNING", logger="agent.main"):
                second = runtime.camera_snapshot(CameraRole.FRONT)
                third = runtime.camera_snapshot(CameraRole.FRONT)
            # два срыва подряд — оператору по-прежнему последний удачный кадр
            assert second.ok and second.jpeg == first.jpeg
            assert third.ok and third.jpeg == first.jpeg
            warnings = [r for r in caplog.records if "превью камеры: front" in r.getMessage()]
            assert len(warnings) == 1, "срывы подряд должны попадать в лог один раз"
            assert "таймаут" in warnings[0].getMessage()
            # запасной кадр устарел — отдаётся ошибка (браузер покажет «Нет сигнала»)
            monkeypatch.setattr("agent.main.PREVIEW_STALE_MAX_S", -1.0)
            stale = runtime.camera_snapshot(CameraRole.FRONT)
            assert not stale.ok and stale.error == "front: таймаут"
        finally:
            storage.close()

    def test_set_cameras_forgets_last_frame(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """После смены камер из центра запасной кадр прежней камеры не отдаётся."""
        runtime, storage, _ = _runtime_with_cameras(tmp_path)
        try:
            monkeypatch.setattr("agent.main.PREVIEW_TTL_S", 0.0)
            monkeypatch.setattr(
                "agent.main.capture",
                lambda camera, *, ffmpeg_path: CameraShot(
                    role=camera.role, jpeg=_big_jpeg(), captured_at=datetime.now(UTC)
                ),
            )
            assert runtime.camera_snapshot(CameraRole.FRONT).ok
            runtime.set_cameras(
                [CameraConfig(role=CameraRole.FRONT, snapshot_url="http://u:p@10.9.9.9/new")]
            )
            monkeypatch.setattr(
                "agent.main.capture",
                lambda camera, *, ffmpeg_path: CameraShot(
                    role=camera.role, jpeg=None, captured_at=datetime.now(UTC), error="front: нет"
                ),
            )
            assert not runtime.camera_snapshot(CameraRole.FRONT).ok
        finally:
            storage.close()


class TestCameraHealthLock:
    def test_probe_waits_for_preview_lock(self) -> None:
        """Проба камеры не ходит к камере, пока идёт съёмка превью (тот же замок)."""
        import threading

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
        lock = threading.Lock()
        health.set_capture_lock(lambda role: lock)
        lock.acquire()  # «съёмка превью идёт»
        worker = threading.Thread(target=lambda: asyncio.run(health.check_once()), daemon=True)
        worker.start()
        try:
            worker.join(0.4)
            assert worker.is_alive(), "проба должна ждать замок съёмки"
            assert health.statuses == []
        finally:
            lock.release()
        worker.join(5.0)
        assert not worker.is_alive()
        [status] = health.statuses
        assert status.available is False  # порт закрыт: камера недоступна, но проба прошла

    def test_build_runtime_shares_lock_with_preview(self, tmp_path: Path) -> None:
        """build_runtime связывает пробу CameraHealth с замком превью рантайма."""
        import threading

        runtime, storage, camera_health = _runtime_with_cameras(tmp_path)
        try:
            lock = runtime.preview_lock(CameraRole.FRONT)
            lock.acquire()
            worker = threading.Thread(
                target=lambda: asyncio.run(camera_health.check_once()), daemon=True
            )
            worker.start()
            try:
                worker.join(0.4)
                assert worker.is_alive()
            finally:
                lock.release()
            worker.join(10.0)
            assert not worker.is_alive()
        finally:
            storage.close()
