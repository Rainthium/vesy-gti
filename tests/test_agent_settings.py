"""Тесты применения настроек центра на агенте (agent/settings.py).

Покрытие:
- merge_center_settings: наложение сохранённого снимка на AgentConfig —
  каждое поле отдельно и вместе; None-поля не трогают конфиг; пустой
  payload возвращает тот же объект конфига;
- SettingsManager.handle (с фейковыми потребителями вместо реальных
  driver/runner/manual): параметры цикла доходят до watcher/runner/manual,
  наблюдение сбрасывается в WAIT_EMPTY; камеры расходятся по трём
  потребителям; COM-порт: тот же порт не трогается, живой новый порт
  применяется, молчащий — откатывается с rolled_back=True и снимком БЕЗ
  порта; исключение внутри применения → ConfigStatus(ok=False), клиент
  не падает; успешный снимок сохраняется в storage и переживает рестарт.

Реального ожидания 12 с нет: PORT_CHECK_TIMEOUT_S ужимается monkeypatch.
"""

import asyncio
from pathlib import Path
from typing import Any

import pytest

import agent.settings as agent_settings
from agent.cameras.capture import DEFAULT_TIMEOUT_S, CameraConfig
from agent.config import (
    AgentConfig,
    CameraSection,
    CenterSection,
    ScaleSection,
    StorageSection,
    WebSection,
)
from agent.drivers.base import ScaleState
from agent.settings import SettingsManager, merge_center_settings
from agent.sync.storage import AgentStorage
from agent.weighing.cycle import CycleConfig
from agent.weighing.watcher import ScaleWatcher, WatcherPhase
from shared.enums import CameraRole, ScaleStatus
from shared.messages import (
    CameraSettings,
    ConfigStatus,
    CycleSettings,
    ScaleConfigUpdate,
    ScaleSettingsPayload,
)

# --- построение исходных данных ---


def make_cycle_settings(**overrides: Any) -> CycleSettings:
    """Полный набор параметров цикла, заметно отличающийся от дефолтов."""
    fields: dict[str, Any] = {
        "zero_threshold_kg": 111.0,
        "vehicle_threshold_kg": 999.0,
        "zero_timeout_s": 7.0,
        "vehicle_timeout_s": 77.0,
        "stable_duration_s": 3.0,
        "stable_timeout_s": 33.0,
        "no_data_timeout_s": 4.0,
    }
    fields.update(overrides)
    return CycleSettings(**fields)


def make_agent_config() -> AgentConfig:
    """Минимальный валидный конфиг агента (локальный, до вмешательства центра)."""
    return AgentConfig(
        site_name="Тестовый объект",
        scale_name="Весы",
        agent_id="agent-test",
        scale=ScaleSection(port="COM5", baudrate=9600),
        cameras=[CameraSection(role=CameraRole.FRONT, snapshot_url="http://local/front")],
        center=CenterSection(url="ws://127.0.0.1/agents/ws", token="x" * 32),
        storage=StorageSection(db_path=Path("agent.db"), photos_dir=Path("photos")),
        web=WebSection(session_secret="s" * 32),
    )


# --- merge_center_settings ---


class TestMergeCenterSettings:
    def test_empty_payload_returns_same_config(self) -> None:
        """Пустой снимок (все поля None) не трогает конфиг вовсе —
        возвращается тот же объект."""
        config = make_agent_config()
        merged = merge_center_settings(config, ScaleSettingsPayload())
        assert merged is config

    def test_cycle_only_overrides_cycle_section(self) -> None:
        """cycle из снимка замещает секцию цикла; остальное не тронуто."""
        config = make_agent_config()
        cycle = make_cycle_settings()
        merged = merge_center_settings(config, ScaleSettingsPayload(cycle=cycle))
        assert merged.cycle.model_dump() == cycle.model_dump()
        # секции вне управления центра — исходные
        assert merged.scale is config.scale
        assert merged.cameras == config.cameras
        assert merged.center is config.center

    def test_cameras_only_replace_camera_sections(self) -> None:
        """cameras из снимка замещают список камер (URL с паролями — из центра);
        preview_url (лёгкий кадр превью, 20.08.2026) едет вместе с ними."""
        config = make_agent_config()
        payload = ScaleSettingsPayload(
            cameras=[
                CameraSettings(
                    role=CameraRole.FRONT,
                    snapshot_url="http://u:p@10.0.0.5/front",
                    preview_url="http://u:p@10.0.0.5/light",
                ),
                CameraSettings(role=CameraRole.REAR, rtsp_url="rtsp://u:p@10.0.0.6/rear"),
            ]
        )
        merged = merge_center_settings(config, payload)
        assert [(c.role, c.snapshot_url, c.rtsp_url, c.preview_url) for c in merged.cameras] == [
            (CameraRole.FRONT, "http://u:p@10.0.0.5/front", None, "http://u:p@10.0.0.5/light"),
            (CameraRole.REAR, None, "rtsp://u:p@10.0.0.6/rear", None),
        ]
        assert merged.cycle == config.cycle
        assert merged.scale is config.scale

    def test_center_cameras_inherit_local_timeout_by_role(self) -> None:
        """Камеры из центра наследуют timeout_s локальной камеры той же роли:
        таймаут — свойство площадки, центр им не управляет (урок Джалал-Абада
        12.08.2026 — снимок центра затирал поднятый локально таймаут).
        Роль без локальной пары получает дефолт."""
        config = make_agent_config()
        config = config.model_copy(
            update={
                "cameras": [
                    CameraSection(
                        role=CameraRole.FRONT,
                        snapshot_url="http://local/front",
                        timeout_s=25.0,
                    )
                ]
            }
        )
        payload = ScaleSettingsPayload(
            cameras=[
                CameraSettings(role=CameraRole.FRONT, snapshot_url="http://u:p@10.0.0.5/front"),
                CameraSettings(role=CameraRole.REAR, rtsp_url="rtsp://u:p@10.0.0.6/rear"),
            ]
        )
        merged = merge_center_settings(config, payload)
        assert [(c.role, c.timeout_s) for c in merged.cameras] == [
            (CameraRole.FRONT, 25.0),
            (CameraRole.REAR, DEFAULT_TIMEOUT_S),
        ]

    def test_port_only_keeps_local_baudrate_and_driver(self) -> None:
        """scale_port без baudrate: порт из центра, скорость и драйвер локальные."""
        config = make_agent_config()
        merged = merge_center_settings(config, ScaleSettingsPayload(scale_port="COM11"))
        assert merged.scale.port == "COM11"
        assert merged.scale.baudrate == 9600
        assert merged.scale.driver == "cas22"

    def test_port_with_baudrate_overrides_both(self) -> None:
        """Порт и скорость из снимка применяются вместе."""
        config = make_agent_config()
        merged = merge_center_settings(
            config, ScaleSettingsPayload(scale_port="COM11", baudrate=19200)
        )
        assert merged.scale.port == "COM11"
        assert merged.scale.baudrate == 19200

    def test_baudrate_without_port_is_ignored(self) -> None:
        """Скорость без порта не применяется: портом управляет локальный
        конфиг, и скорость к чужому порту не подсовывается."""
        config = make_agent_config()
        merged = merge_center_settings(config, ScaleSettingsPayload(baudrate=19200))
        assert merged.scale.baudrate == 9600
        assert merged.scale.port == "COM5"

    def test_full_payload_overrides_everything_managed(self) -> None:
        """Полный снимок: цикл, камеры, порт и скорость применяются вместе."""
        config = make_agent_config()
        cycle = make_cycle_settings()
        payload = ScaleSettingsPayload(
            cycle=cycle,
            cameras=[CameraSettings(role=CameraRole.REAR, snapshot_url="http://c/rear")],
            scale_port="COM7",
            baudrate=38400,
        )
        merged = merge_center_settings(config, payload)
        assert merged.cycle.model_dump() == cycle.model_dump()
        assert [(c.role, c.snapshot_url) for c in merged.cameras] == [
            (CameraRole.REAR, "http://c/rear")
        ]
        assert (merged.scale.port, merged.scale.baudrate) == ("COM7", 38400)
        # исходный конфиг не мутировал (model_copy)
        assert config.scale.port == "COM5"
        assert config.cycle.zero_threshold_kg == 200.0


# --- фейковые потребители настроек для SettingsManager ---


class FakeDriver:
    """Фейк Cas22Driver: помнит вызовы set_port; «оживает» только на портах
    из alive_ports (state → OK), на остальных молчит (NO_DATA)."""

    def __init__(self, port: str = "COM5", baudrate: int = 9600) -> None:
        self._port = port
        self._baudrate = baudrate
        self.alive_ports: set[str] = set()
        self.set_port_calls: list[tuple[str, int | None]] = []
        self.state = ScaleState(status=ScaleStatus.OK)

    @property
    def port_url(self) -> str:
        return self._port

    @property
    def baudrate(self) -> int:
        return self._baudrate

    def set_port(self, port_url: str, baudrate: int | None = None) -> None:
        self.set_port_calls.append((port_url, baudrate))
        self._port = port_url
        if baudrate is not None:
            self._baudrate = baudrate
        status = ScaleStatus.OK if port_url in self.alive_ports else ScaleStatus.NO_DATA
        self.state = ScaleState(status=status)


class FakeWatcher:
    def __init__(self) -> None:
        self.reconfigured: list[CycleConfig] = []

    def reconfigure(self, config: CycleConfig) -> None:
        self.reconfigured.append(config)


class FakeRunner:
    def __init__(self) -> None:
        self.cycles: list[CycleConfig] = []
        self.cameras: list[list[CameraConfig]] = []

    def set_cycle(self, cycle: CycleConfig) -> None:
        self.cycles.append(cycle)

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        self.cameras.append(cameras)


class FakeManual:
    def __init__(self) -> None:
        self.thresholds: list[float] = []
        self.max_tares: list[float] = []
        self.cameras: list[list[CameraConfig]] = []

    def set_vehicle_threshold(self, threshold_kg: float) -> None:
        self.thresholds.append(threshold_kg)

    def set_max_tare(self, max_tare_kg: float) -> None:
        self.max_tares.append(max_tare_kg)

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        self.cameras.append(cameras)


class FakeCameraHealth:
    def __init__(self) -> None:
        self.cameras: list[list[CameraConfig]] = []

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        self.cameras.append(cameras)


class FakePreview:
    """Превью веб-интерфейса (боевой урок Кызыл-Кыи 14.08.2026)."""

    def __init__(self) -> None:
        self.cameras: list[list[CameraConfig]] = []

    def set_cameras(self, cameras: list[CameraConfig]) -> None:
        self.cameras.append(cameras)


class FakeInfoSink:
    """Шапка веб-интерфейса: подпись индикатора из центра (20.08.2026)."""

    def __init__(self) -> None:
        self.models: list[str] = []
        self.manual: list[bool] = []

    def set_indicator_model(self, model: str) -> None:
        self.models.append(model)

    def set_manual_allowed(self, allowed: bool) -> None:
        self.manual.append(allowed)


class FakeRetention:
    """Уборка локальных фото: срок из центра применяется на лету (02.09.2026)."""

    def __init__(self) -> None:
        self.days: list[int] = []

    def set_retention_days(self, days: int) -> None:
        self.days.append(days)


class ManagerEnv:
    """Собранный SettingsManager с фейками и памятью на диске (in-memory)."""

    def __init__(
        self,
        watcher: Any = None,
        local_camera_timeouts: dict[CameraRole, float] | None = None,
    ) -> None:
        self.driver = FakeDriver()
        # Any: сюда подставляется и FakeWatcher, и реальный ScaleWatcher
        self.watcher: Any = watcher if watcher is not None else FakeWatcher()
        self.runner = FakeRunner()
        self.manual = FakeManual()
        self.camera_health = FakeCameraHealth()
        self.preview = FakePreview()
        self.storage = AgentStorage(":memory:")
        self.manager = SettingsManager(
            driver=self.driver,  # type: ignore[arg-type]
            watcher=self.watcher,  # type: ignore[arg-type]
            runner=self.runner,  # type: ignore[arg-type]
            manual=self.manual,  # type: ignore[arg-type]
            camera_health=self.camera_health,
            storage=self.storage,
            local_camera_timeouts=local_camera_timeouts,
        )
        # как в build_runtime: превью и шапка подписываются отдельным шагом
        self.manager.set_preview(self.preview)
        self.info_sink = FakeInfoSink()
        self.manager.set_info_sink(self.info_sink)
        self.retention = FakeRetention()
        self.manager.set_retention(self.retention)

    def close(self) -> None:
        self.storage.close()

    def handle(self, payload: ScaleSettingsPayload) -> ConfigStatus:
        """Синхронно прогнать handle() (внутри — asyncio)."""
        try:
            return asyncio.run(self.manager.handle(ScaleConfigUpdate(settings=payload)))
        finally:
            pass

    def stored_payload(self) -> ScaleSettingsPayload | None:
        raw = self.storage.load_center_settings()
        if raw is None:
            return None
        return ScaleSettingsPayload.model_validate_json(raw)


@pytest.fixture
def env() -> Any:
    environment = ManagerEnv()
    yield environment
    environment.close()


@pytest.fixture
def fast_port_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Не ждать реальные 12 с проверки порта: ужать таймаут до долей секунды."""
    monkeypatch.setattr(agent_settings, "PORT_CHECK_TIMEOUT_S", 0.3)
    monkeypatch.setattr(agent_settings, "PORT_CHECK_INTERVAL_S", 0.02)


# --- SettingsManager: параметры цикла ---


class TestManagerCycle:
    def test_cycle_reaches_watcher_runner_and_manual(self, env: ManagerEnv) -> None:
        """Цикл доходит до наблюдателя, авторежима и порога ручного режима."""
        cycle = make_cycle_settings()
        status = env.handle(ScaleSettingsPayload(cycle=cycle))
        assert status.ok is True
        assert status.rolled_back is False
        assert isinstance(env.watcher, FakeWatcher)
        expected = CycleConfig(**cycle.model_dump())
        assert env.watcher.reconfigured == [expected]
        assert env.runner.cycles == [expected]
        assert env.manual.thresholds == [999.0]
        # лимит тары (0.4.29) доезжает до ручного режима тем же снимком
        assert env.manual.max_tares == [cycle.max_tare_kg]

    def test_cycle_resets_real_watcher_to_wait_empty(self) -> None:
        """С реальным ScaleWatcher: применение цикла сбрасывает наблюдение
        в WAIT_EMPTY — готовая фиксация стоящей машины аннулируется."""
        clock = {"now": 0.0}
        watcher = ScaleWatcher(CycleConfig(stable_duration_s=1.0), clock=lambda: clock["now"])
        # доводим наблюдение до READY: пустые весы → заезд → стабилизация
        watcher.tick(ScaleState(status=ScaleStatus.OK, weight_kg=0.0, stable=True))
        clock["now"] = 1.0
        watcher.tick(ScaleState(status=ScaleStatus.OK, weight_kg=12000.0, stable=True))
        clock["now"] = 2.0
        watcher.tick(ScaleState(status=ScaleStatus.OK, weight_kg=12000.0, stable=True))
        clock["now"] = 4.0
        watcher.tick(ScaleState(status=ScaleStatus.OK, weight_kg=12000.0, stable=True))
        # фазы читаются в переменные: сужение типа литералом фазы мешало бы
        # mypy сравнить фазу до и после применения настроек
        phase_before: WatcherPhase = watcher.phase
        assert phase_before is WatcherPhase.READY
        assert watcher.fixation is not None

        environment = ManagerEnv(watcher=watcher)
        try:
            status = environment.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
            assert status.ok is True
            phase_after: WatcherPhase = watcher.phase
            assert phase_after is WatcherPhase.WAIT_EMPTY
            assert watcher.fixation is None
        finally:
            environment.close()

    def test_cycle_snapshot_persisted(self, env: ManagerEnv) -> None:
        """Применённый снимок сохранён в SQLite и восстановим после рестарта."""
        cycle = make_cycle_settings()
        env.handle(ScaleSettingsPayload(cycle=cycle))
        stored = env.stored_payload()
        assert stored is not None
        assert stored.cycle is not None
        assert stored.cycle.model_dump() == cycle.model_dump()
        assert stored.scale_port is None

    def test_exception_inside_apply_reports_not_ok(self, env: ManagerEnv) -> None:
        """Исключение при применении → ConfigStatus(ok=False, error=...),
        наружу ничего не вылетает (клиент не падает)."""
        assert isinstance(env.watcher, FakeWatcher)

        def boom(config: CycleConfig) -> None:
            raise RuntimeError("watcher сломался")

        env.watcher.reconfigure = boom  # type: ignore[method-assign]
        status = env.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert status.ok is False
        assert status.error and "watcher сломался" in status.error
        # неудачный снимок не сохранён
        assert env.stored_payload() is None


# --- SettingsManager: камеры ---


class TestManagerCameras:
    def test_cameras_delivered_to_all_consumers(self, env: ManagerEnv) -> None:
        """Камеры расходятся в авторежим, ручной режим, фоновую проверку
        и превью оператора (превью — боевой урок Кызыл-Кыи 14.08.2026)."""
        payload = ScaleSettingsPayload(
            cameras=[
                CameraSettings(
                    role=CameraRole.FRONT,
                    snapshot_url="http://u:p@10.0.0.5/f",
                    preview_url="http://u:p@10.0.0.5/light",
                ),
                CameraSettings(role=CameraRole.REAR, rtsp_url="rtsp://u:p@10.0.0.6/r"),
            ]
        )
        status = env.handle(payload)
        assert status.ok is True
        consumers = (
            env.runner.cameras,
            env.manual.cameras,
            env.camera_health.cameras,
            env.preview.cameras,
        )
        for consumer in consumers:
            assert len(consumer) == 1
            assert [(c.role, c.snapshot_url, c.rtsp_url, c.preview_url) for c in consumer[0]] == [
                (CameraRole.FRONT, "http://u:p@10.0.0.5/f", None, "http://u:p@10.0.0.5/light"),
                (CameraRole.REAR, None, "rtsp://u:p@10.0.0.6/r", None),
            ]
        stored = env.stored_payload()
        assert stored is not None and stored.cameras is not None
        assert len(stored.cameras) == 2

    def test_cameras_none_leaves_consumers_untouched(self, env: ManagerEnv) -> None:
        """cameras=None в снимке — камеры остаются локальными, вызовов нет."""
        env.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert env.runner.cameras == []
        assert env.manual.cameras == []
        assert env.camera_health.cameras == []
        assert env.preview.cameras == []

    def test_indicator_model_applied_and_persisted(self, env: ManagerEnv) -> None:
        """Подпись индикатора из центра доезжает до шапки на лету и в снимок."""
        status = env.handle(ScaleSettingsPayload(indicator_model="CAS CI-201A (весы SCS-80, 80 т)"))
        assert status.ok is True
        assert env.info_sink.models == ["CAS CI-201A (весы SCS-80, 80 т)"]
        stored = env.stored_payload()
        assert stored is not None
        assert stored.indicator_model == "CAS CI-201A (весы SCS-80, 80 т)"

    def test_indicator_model_none_leaves_header_untouched(self, env: ManagerEnv) -> None:
        """None в поле — подписью продолжает управлять локальный конфиг."""
        env.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert env.info_sink.models == []

    def test_indicator_model_merge_on_start(self) -> None:
        """merge_center_settings: подпись из снимка главнее локального конфига."""
        config = make_agent_config()
        merged = merge_center_settings(
            config, ScaleSettingsPayload(indicator_model="VESAR (весы SCS-80, 80 т)")
        )
        assert merged.indicator_model == "VESAR (весы SCS-80, 80 т)"
        # None — локальное значение остаётся
        merged = merge_center_settings(config, ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert merged.indicator_model == config.indicator_model

    def test_without_preview_subscription_still_ok(self) -> None:
        """Менеджер без подписанного превью применяет камеры без ошибок
        (тесты и сборки, зовущие SettingsManager напрямую)."""
        environment = ManagerEnv.__new__(ManagerEnv)
        environment.driver = FakeDriver()
        environment.watcher = FakeWatcher()
        environment.runner = FakeRunner()
        environment.manual = FakeManual()
        environment.camera_health = FakeCameraHealth()
        environment.storage = AgentStorage(":memory:")
        environment.manager = SettingsManager(
            driver=environment.driver,  # type: ignore[arg-type]
            watcher=environment.watcher,
            runner=environment.runner,  # type: ignore[arg-type]
            manual=environment.manual,  # type: ignore[arg-type]
            camera_health=environment.camera_health,
            storage=environment.storage,
        )
        try:
            payload = ScaleSettingsPayload(
                cameras=[CameraSettings(role=CameraRole.FRONT, snapshot_url="http://u:p@x/f")]
            )
            status = environment.handle(payload)
            assert status.ok is True
            assert len(environment.runner.cameras) == 1
        finally:
            environment.close()

    def test_cameras_inherit_local_timeout_by_role(self) -> None:
        """Живое применение тоже наследует локальный таймаут по роли
        (зеркало merge_center_settings: та же логика при работе онлайн)."""
        environment = ManagerEnv(local_camera_timeouts={CameraRole.FRONT: 25.0})
        try:
            payload = ScaleSettingsPayload(
                cameras=[
                    CameraSettings(role=CameraRole.FRONT, snapshot_url="http://u:p@10.0.0.5/f"),
                    CameraSettings(role=CameraRole.REAR, rtsp_url="rtsp://u:p@10.0.0.6/r"),
                ]
            )
            status = environment.handle(payload)
            assert status.ok is True
            assert [(c.role, c.timeout_s) for c in environment.runner.cameras[0]] == [
                (CameraRole.FRONT, 25.0),
                (CameraRole.REAR, DEFAULT_TIMEOUT_S),
            ]
        finally:
            environment.close()


# --- SettingsManager: COM-порт ---


class TestManagerPort:
    def test_same_port_not_reopened(self, env: ManagerEnv) -> None:
        """Тот же порт и скорость: драйвер не перезапускается (машина могла
        стоять на весах — лишний рестарт потока чтения ни к чему)."""
        status = env.handle(ScaleSettingsPayload(scale_port="COM5", baudrate=9600))
        assert status.ok is True
        assert env.driver.set_port_calls == []
        # снимок сохранён с портом: после рестарта порт останется под центром
        stored = env.stored_payload()
        assert stored is not None and stored.scale_port == "COM5"

    def test_same_port_without_baudrate_not_reopened(self, env: ManagerEnv) -> None:
        """Тот же порт без указания скорости — тоже без перезапуска."""
        status = env.handle(ScaleSettingsPayload(scale_port="COM5"))
        assert status.ok is True
        assert env.driver.set_port_calls == []

    def test_new_port_alive_applied(self, env: ManagerEnv, fast_port_check: None) -> None:
        """Новый порт, индикатор ожил: порт применён, снимок с портом."""
        env.driver.alive_ports = {"COM11"}
        status = env.handle(ScaleSettingsPayload(scale_port="COM11", baudrate=19200))
        assert status.ok is True
        assert status.rolled_back is False
        assert env.driver.set_port_calls == [("COM11", 19200)]
        assert env.driver.port_url == "COM11"
        assert env.driver.baudrate == 19200
        stored = env.stored_payload()
        assert stored is not None
        assert stored.scale_port == "COM11"
        assert stored.baudrate == 19200

    def test_new_port_silent_rolled_back(self, env: ManagerEnv, fast_port_check: None) -> None:
        """Новый порт молчит: откат на старый порт/скорость, rolled_back=True,
        снимок сохранён БЕЗ порта (после рестарта мёртвый порт не слушается)."""
        cycle = make_cycle_settings()
        status = env.handle(ScaleSettingsPayload(cycle=cycle, scale_port="COM99", baudrate=19200))
        assert status.ok is False
        assert status.rolled_back is True
        assert status.error and "COM99" in status.error
        # два вызова: попытка нового порта и откат на прежние параметры
        assert env.driver.set_port_calls == [("COM99", 19200), ("COM5", 9600)]
        assert env.driver.port_url == "COM5"
        assert env.driver.baudrate == 9600
        # снимок сохранён, но без порта; цикл при этом применён и сохранён
        stored = env.stored_payload()
        assert stored is not None
        assert stored.scale_port is None
        assert stored.baudrate is None
        assert stored.cycle is not None
        assert stored.cycle.model_dump() == cycle.model_dump()
        assert isinstance(env.watcher, FakeWatcher)
        assert len(env.watcher.reconfigured) == 1

    def test_no_port_in_payload_driver_untouched(self, env: ManagerEnv) -> None:
        """scale_port=None: драйвер не трогается вовсе."""
        status = env.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert status.ok is True
        assert env.driver.set_port_calls == []


# --- срок хранения локальных фото из центра (0.4.25, 02.09.2026) ---


class TestRetentionFromCenter:
    def test_retention_days_applied_and_persisted(self, env: ManagerEnv) -> None:
        """Срок из снимка доезжает до уборки на лету и сохраняется в снимке."""
        status = env.handle(ScaleSettingsPayload(photo_retention_days=7))
        assert status.ok is True
        assert env.retention.days == [7]
        stored = env.stored_payload()
        assert stored is not None
        assert stored.photo_retention_days == 7

    def test_zero_from_center_disables_purge(self, env: ManagerEnv) -> None:
        """0 — «не убирать»: это управление, а не «не задано»."""
        env.handle(ScaleSettingsPayload(photo_retention_days=0))
        assert env.retention.days == [0]

    def test_none_leaves_retention_untouched(self, env: ManagerEnv) -> None:
        env.handle(ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert env.retention.days == []

    def test_merge_on_start(self) -> None:
        """merge_center_settings: срок из снимка главнее config.toml,
        остальная секция хранилища не тронута."""
        config = make_agent_config()
        merged = merge_center_settings(config, ScaleSettingsPayload(photo_retention_days=3))
        assert merged.storage.photo_retention_days == 3
        assert merged.storage.db_path == config.storage.db_path
        assert merged.storage.photos_dir == config.storage.photos_dir
        merged = merge_center_settings(config, ScaleSettingsPayload(photo_retention_days=0))
        assert merged.storage.photo_retention_days == 0
        merged = merge_center_settings(config, ScaleSettingsPayload(cycle=make_cycle_settings()))
        assert merged.storage.photo_retention_days == config.storage.photo_retention_days


# --- ручной режим при связи с центром (0.4.28, 03.09.2026) ---


class TestManualAllowedFromCenter:
    def test_true_reaches_runtime_and_is_persisted(self, env: ManagerEnv) -> None:
        """Разрешение доезжает до веб-интерфейса на лету и остаётся в снимке —
        после рестарта службы объект без АИС не теряет кнопку."""
        status = env.handle(ScaleSettingsPayload(manual_allowed=True))
        assert status.ok is True
        assert env.info_sink.manual == [True]
        stored = env.stored_payload()
        assert stored is not None
        assert stored.manual_allowed is True

    def test_false_revokes(self, env: ManagerEnv) -> None:
        """False из центра — тоже управление: разрешение снимается."""
        env.handle(ScaleSettingsPayload(manual_allowed=True))
        env.handle(ScaleSettingsPayload(manual_allowed=False))
        assert env.info_sink.manual == [True, False]

    def test_none_leaves_permit_untouched(self, env: ManagerEnv) -> None:
        env.handle(ScaleSettingsPayload(indicator_model="CAS"))
        assert env.info_sink.manual == []
