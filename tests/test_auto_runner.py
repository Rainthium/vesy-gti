"""Тесты оркестратора автоматического режима (agent/weighing/auto.py).

Семантика UniServer (решение Игоря 10.08.2026): runner ЗАЕЗДА НЕ ЖДЁТ,
работает по готовой фиксации ScaleWatcher. Покрытие handle():

- мгновенная операция по фиксации: code OK, снимки обеих камер в момент
  команды, запись в журнале, файлы на диске байт-в-байт (правило №2),
  тара/нетто по реплике реестра (правило №4, сцепка голова+прицеп);
- пустая платформа / заезд не засвидетельствован → немедленный
  ERR_VEHICLE_TIMEOUT без записи и без ожиданий (проверяется время);
- индикатор молчит → немедленный ERR_SCALE_OFFLINE, даже при
  формально живой фиксации наблюдателя;
- STABILIZING: runner дожидается фиксации → OK; не дождался за
  stable_timeout_s → ERR_UNSTABLE; машина съехала за время ожидания →
  ERR_VEHICLE_TIMEOUT; присланный request.timeout_s короче потолка →
  ожидание ограничено им;
- сбой камеры при готовой фиксации → ERR_CAMERA, вес НЕ возвращается,
  записи нет (решение 09.08.2026);
- ERR_BUSY при параллельной команде, первая операция не прерывается,
  номера в ответе ERR_BUSY нормализованы;
- тарирование не подставляет тару; просроченная тара игнорируется;
- нормализация номеров (upper/strip, пустые → None) — и в отказах тоже;
- weighed_at из подменяемых часов now_utc («время от центра», 10.08.2026).

Железо не используется: состояние индикатора — подменяемый держатель,
watcher тикается вручную фейковыми часами, камеры — monkeypatch
agent.weighing.auto.shots_or_capture_all. Асинхронность — через asyncio.run.
"""

import asyncio
import hashlib
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from agent.cameras.capture import CameraConfig, CameraShot
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage
from agent.weighing import auto
from agent.weighing.auto import AutoConfig, AutoOperationRunner
from agent.weighing.cycle import CycleConfig
from agent.weighing.watcher import ScaleWatcher, WatcherPhase
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord, WeighingRecord, WeighRequest, WeighResult

# Общий лимит на сценарий: ожидания короткие, реальное время — миллисекунды.
SCENARIO_TIMEOUT_S = 10.0
# Немедленные отказы обязаны возвращаться без ожиданий (порог с запасом).
IMMEDIATE_S = 1.0

CFG = CycleConfig()  # пороги 50/500 кг, выдержка 2 с — часы у watcher фейковые

VEHICLE = "01KG777AAA"
TRAILER = "BD123AB"
GROSS_KG = 43310.0
TARE_KG = 15300.0

# разные тела снимков — проверяем соответствие камера → файл (без пересжатия)
FRONT_JPEG = b"\xff\xd8\xff\xe0" + b"front-camera-frame" + b"\xff\xd9"
REAR_JPEG = b"\xff\xd8\xff\xe0" + b"rear-camera-frame-bytes" + b"\xff\xd9"


def ok(weight: float, *, stable: bool = True, overload: bool = False) -> ScaleState:
    """Снимок индикатора с идущим потоком данных (status OK)."""
    return ScaleState(status=ScaleStatus.OK, weight_kg=weight, stable=stable, overload=overload)


NO_DATA = ScaleState(status=ScaleStatus.NO_DATA)


class FakeClock:
    """Управляемые монотонные часы для watcher (runner живёт в реальном времени)."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScaleHolder:
    """Текущее состояние индикатора: runner читает его через scale_state()."""

    def __init__(self) -> None:
        self.state: ScaleState = ok(0.0)

    def __call__(self) -> ScaleState:
        return self.state


def good_shots() -> list[CameraShot]:
    """Удачные кадры обеих камер (порядок соответствует списку камер)."""
    now = datetime.now(UTC)
    return [
        CameraShot(role=CameraRole.FRONT, jpeg=FRONT_JPEG, captured_at=now),
        CameraShot(role=CameraRole.REAR, jpeg=REAR_JPEG, captured_at=now),
    ]


class CaptureMock:
    """Замена shots_or_capture_all (0.4.7): в сеть не ходит, фиксирует
    вызовы, отдаёт заготовку."""

    def __init__(self) -> None:
        self.shots: list[CameraShot] = good_shots()
        self.calls: list[list[CameraRole]] = []

    def __call__(
        self,
        configs: list[CameraConfig],
        streams: object = None,
        *,
        ffmpeg_path: str = "ffmpeg",
        max_age_s: float = 3.0,
    ) -> list[CameraShot]:
        self.calls.append([config.role for config in configs])
        return list(self.shots)


class RunnerEnv:
    """Собранный runner с фейками: watcher на фейковых часах, мок камер, SQLite."""

    def __init__(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        auto_config: AutoConfig | None = None,
        now_utc: Callable[[], datetime] | None = None,
    ) -> None:
        config = auto_config or AutoConfig(cycle=CFG, tick_interval_s=0.005)
        self.watcher_clock = FakeClock()
        self.watcher = ScaleWatcher(config.cycle, clock=self.watcher_clock)
        self.scale = ScaleHolder()
        self.storage = AgentStorage(tmp_path / "agent.db")
        self.photos_dir = tmp_path / "photos"
        self.capture = CaptureMock()
        monkeypatch.setattr(auto, "shots_or_capture_all", self.capture)
        self.runner = AutoOperationRunner(
            scale_state=self.scale,
            watcher=self.watcher,
            storage=self.storage,
            cameras=[
                CameraConfig(role=CameraRole.FRONT, snapshot_url="http://127.0.0.1:9/front.jpg"),
                CameraConfig(role=CameraRole.REAR, snapshot_url="http://127.0.0.1:9/rear.jpg"),
            ],
            photos_dir=self.photos_dir,
            config=config,
            now_utc=now_utc,
        )

    def drive_to_ready(self, weight: float = GROSS_KG) -> None:
        """Засвидетельствовать полный заезд: watcher в READY, фиксация готова."""
        self.watcher.tick(ok(0.0))  # пустые стабильные весы
        self.watcher.tick(ok(weight))  # заезд
        self.watcher.tick(ok(weight))  # кандидат неизменности
        self.watcher_clock.advance(CFG.stable_duration_s)
        self.watcher.tick(ok(weight))  # выдержка набрана → READY
        assert self.watcher.phase is WatcherPhase.READY
        self.scale.state = ok(weight)

    def drive_to_stabilizing(self, weight: float = GROSS_KG) -> None:
        """Машина только что заехала: STABILIZING, выдержка ещё не набрана."""
        self.watcher.tick(ok(0.0))
        self.watcher.tick(ok(weight))
        self.watcher.tick(ok(weight))  # кандидат есть, но выдержки нет
        assert self.watcher.phase is WatcherPhase.STABILIZING
        self.scale.state = ok(weight)


@pytest.fixture
def make_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., RunnerEnv]]:
    """Фабрика окружений (для тестов с нестандартным AutoConfig)."""
    created: list[RunnerEnv] = []

    def factory(
        auto_config: AutoConfig | None = None,
        now_utc: Callable[[], datetime] | None = None,
    ) -> RunnerEnv:
        environment = RunnerEnv(tmp_path, monkeypatch, auto_config, now_utc)
        created.append(environment)
        return environment

    yield factory
    for environment in created:
        environment.storage.close()


@pytest.fixture
def env(make_env: Callable[..., RunnerEnv]) -> RunnerEnv:
    return make_env()


def make_request(**overrides: Any) -> WeighRequest:
    """Команда центра с типичными полями; overrides — точечные замены."""
    fields: dict[str, Any] = {
        "request_id": uuid4(),
        "operation": Operation.WEIGHING,
        "vehicle_number": VEHICLE,
    }
    fields.update(overrides)
    return WeighRequest(**fields)


def run_handle(env: RunnerEnv, request: WeighRequest) -> WeighResult:
    """Выполнить одну команду с общим лимитом времени сценария."""
    return asyncio.run(asyncio.wait_for(env.runner.handle(request), timeout=SCENARIO_TIMEOUT_S))


def put_tare(
    storage: AgentStorage,
    vehicle_number: str = VEHICLE,
    tare_value: float = TARE_KG,
    tared_at: datetime | None = None,
    trailer_number: str | None = None,
) -> TareRecord:
    """Положить в реплику реестра единственную тару и вернуть её."""
    record = TareRecord(
        vehicle_number=vehicle_number,
        trailer_number=trailer_number,
        tare_value=tare_value,
        tared_at=tared_at or datetime.now(UTC) - timedelta(days=10),
        weighing_uuid=uuid4(),
    )
    storage.replace_tare_registry([record])
    return record


def stored_record(env: RunnerEnv, record: WeighingRecord) -> WeighingRecord | None:
    """Запись из журнала (в журнале метаданные фото лежат отдельной таблицей)."""
    return env.storage.get_weighing(record.uuid)


# --- мгновенная операция по готовой фиксации ---


def test_instant_operation_by_fixation(env: RunnerEnv) -> None:
    """АТС стоит, фиксация готова: команда срабатывает мгновенно — code OK,
    вес из фиксации, снимки обеих камер с sha256, запись в журнале,
    файлы на диске байт-в-байт, тара/нетто по реплике (сцепка совпала)."""
    tare = put_tare(env.storage, trailer_number=TRAILER)
    env.drive_to_ready()
    request = make_request(trailer_number=TRAILER)

    started = time.perf_counter()
    result = run_handle(env, request)
    assert time.perf_counter() - started < IMMEDIATE_S  # никаких ожиданий

    assert result.request_id == request.request_id
    record = result.record
    assert record.code is ErrorCode.OK
    assert record.operation is Operation.WEIGHING
    assert record.massa == GROSS_KG
    assert record.stable is True
    assert record.source is WeighingSource.AIS
    assert record.vehicle_number == VEHICLE
    assert record.trailer_number == TRAILER
    assert record.message is None
    assert record.weighed_at is not None and record.weighed_at.tzinfo == UTC

    # правило №4: тара реплики, нетто = брутто − тара, ссылка на тарирование
    assert record.tare_value == TARE_KG
    assert record.tare_weighing_uuid == tare.weighing_uuid
    assert record.netto == GROSS_KG - TARE_KG

    # метаданные снимков: роли, имена, sha256, размеры
    assert [photo.role for photo in record.photos] == [CameraRole.FRONT, CameraRole.REAR]
    day_dir = env.photos_dir / record.weighed_at.strftime("%Y/%m/%d")
    for photo, jpeg, index in (
        (record.photos[0], FRONT_JPEG, 1),
        (record.photos[1], REAR_JPEG, 2),
    ):
        assert photo.filename == f"{record.uuid.hex}_photo{index}.jpeg"
        assert photo.sha256 == hashlib.sha256(jpeg).hexdigest()
        assert photo.size_bytes == len(jpeg)
        # файл на диске байт-в-байт (правило №2 — без пересжатия)
        assert (day_dir / photo.filename).read_bytes() == jpeg

    # запись сохранена локально до отправки результата (synced=0)
    saved = stored_record(env, record)
    assert saved == record.model_copy(update={"photos": []})
    assert env.storage.pending_count() == 1
    assert [p.role for p in env.storage.photos_for(record.uuid)] == [
        CameraRole.FRONT,
        CameraRole.REAR,
    ]
    # камеры дёрнуты один раз, в порядке конфига — в момент команды
    assert env.capture.calls == [[CameraRole.FRONT, CameraRole.REAR]]


def test_operator_from_request_stamped_into_record(env: RunnerEnv) -> None:
    """ФИО оператора из команды центра (запрос АИС, контракт 13.08.2026)
    попадает в запись — печатается на весовой карточке."""
    env.drive_to_ready()
    record = run_handle(env, make_request(operator="Акимов Нурлан Боронбаевич")).record
    assert record.code is ErrorCode.OK
    assert record.operator == "Акимов Нурлан Боронбаевич"
    saved = stored_record(env, record)
    assert saved is not None and saved.operator == "Акимов Нурлан Боронбаевич"


def test_operator_absent_or_blank_is_none(env: RunnerEnv) -> None:
    """Запрос без оператора (или с пустым) — в записи None, как раньше."""
    env.drive_to_ready()
    record = run_handle(env, make_request(operator="   ")).record
    assert record.code is ErrorCode.OK
    assert record.operator is None


def test_weighed_at_taken_from_injected_clock(make_env: Callable[..., RunnerEnv]) -> None:
    """«Время от центра»: weighed_at берётся из now_utc (часы CenterClock),
    а не из локальных часов ПК — и в результате, и в записи журнала."""
    fixed_now = datetime(2026, 8, 10, 6, 30, 15, 123456, tzinfo=UTC)
    env = make_env(now_utc=lambda: fixed_now)
    env.drive_to_ready()

    record = run_handle(env, make_request()).record
    assert record.code is ErrorCode.OK
    assert record.weighed_at == fixed_now

    saved = stored_record(env, record)
    assert saved is not None
    assert saved.weighed_at == fixed_now
    # снимки легли в каталог даты по времени центра, не локальному
    day_dir = env.photos_dir / fixed_now.strftime("%Y/%m/%d")
    assert sorted(p.name for p in day_dir.iterdir()) == [
        f"{record.uuid.hex}_photo1.jpeg",
        f"{record.uuid.hex}_photo2.jpeg",
    ]


def test_fixation_survives_multiple_commands(env: RunnerEnv) -> None:
    """АТС продолжает стоять: повторная команда снова срабатывает мгновенно
    по той же фиксации (наблюдение вечное, фиксация не одноразовая)."""
    env.drive_to_ready()
    first = run_handle(env, make_request()).record
    second = run_handle(env, make_request()).record
    assert first.code is ErrorCode.OK and second.code is ErrorCode.OK
    assert first.massa == second.massa == GROSS_KG
    assert first.uuid != second.uuid
    assert env.storage.pending_count() == 2


# --- пустая платформа: немедленный отказ ---


@pytest.mark.parametrize("witnessed_empty", [False, True], ids=["wait-empty", "wait-vehicle"])
def test_no_vehicle_immediate_refusal(env: RunnerEnv, witnessed_empty: bool) -> None:
    """Платформа пуста (WAIT_EMPTY или WAIT_VEHICLE): немедленный
    ERR_VEHICLE_TIMEOUT без записи, камер и ожиданий; номера в отказе
    нормализованы (upper/strip)."""
    if witnessed_empty:
        env.watcher.tick(ok(0.0))
        assert env.watcher.phase is WatcherPhase.WAIT_VEHICLE
    env.scale.state = ok(0.0)
    request = make_request(vehicle_number="  01kg777aaa ", trailer_number=" bd123ab ")

    started = time.perf_counter()
    result = run_handle(env, request)
    assert time.perf_counter() - started < IMMEDIATE_S  # отказ без ожиданий

    record = result.record
    assert result.request_id == request.request_id
    assert record.code is ErrorCode.ERR_VEHICLE_TIMEOUT
    assert record.massa is None
    assert record.message is not None and "загоните" in record.message
    assert record.vehicle_number == VEHICLE  # нормализованы и в отказе
    assert record.trailer_number == TRAILER
    assert env.storage.pending_count() == 0
    assert stored_record(env, record) is None
    assert env.capture.calls == []


def test_unwitnessed_vehicle_refused(env: RunnerEnv) -> None:
    """Машина стоит на весах, но заезд не засвидетельствован (агент
    перезапущен при стоящей машине): отказ ERR_VEHICLE_TIMEOUT — фиксация
    без цепочки «пустые весы → заезд» не выдаётся."""
    env.watcher.tick(ok(GROSS_KG))  # стоит с самого старта
    assert env.watcher.phase is WatcherPhase.WAIT_EMPTY
    env.scale.state = ok(GROSS_KG)
    record = run_handle(env, make_request()).record
    assert record.code is ErrorCode.ERR_VEHICLE_TIMEOUT
    assert env.storage.pending_count() == 0


# --- индикатор молчит ---


def test_scale_offline_immediate_refusal(env: RunnerEnv) -> None:
    """Индикатор не отдаёт данных в момент команды → немедленный
    ERR_SCALE_OFFLINE без записи, даже если watcher ещё держит фиксацию."""
    env.drive_to_ready()
    env.scale.state = NO_DATA  # поток оборвался только что

    started = time.perf_counter()
    record = run_handle(env, make_request()).record
    assert time.perf_counter() - started < IMMEDIATE_S

    assert record.code is ErrorCode.ERR_SCALE_OFFLINE
    assert record.massa is None
    assert record.vehicle_number == VEHICLE
    assert env.storage.pending_count() == 0
    assert env.capture.calls == []


# --- ожидание стабилизации ---


def test_stabilizing_waits_for_fixation_then_ok(env: RunnerEnv) -> None:
    """Команда пришла к только что заехавшей машине (STABILIZING): runner
    дожидается фиксации и завершает операцию OK с её весом."""

    async def scenario() -> None:
        env.drive_to_stabilizing()
        task = asyncio.create_task(env.runner.handle(make_request()))
        await asyncio.sleep(0.05)
        assert not task.done()  # ждёт стабилизации, а не отказывает

        # вес выстоял выдержку (фейковое время watcher) → READY
        env.watcher_clock.advance(CFG.stable_duration_s)
        env.watcher.tick(ok(GROSS_KG))
        result = await task

        record = result.record
        assert record.code is ErrorCode.OK
        assert record.massa == GROSS_KG
        assert stored_record(env, record) is not None
        assert env.storage.pending_count() == 1

    asyncio.run(asyncio.wait_for(scenario(), timeout=SCENARIO_TIMEOUT_S))


def test_stabilizing_timeout_gives_unstable(make_env: Callable[..., RunnerEnv]) -> None:
    """Вес так и не стабилизировался за stable_timeout_s → ERR_UNSTABLE,
    записи нет, камеры не дёргались."""
    env = make_env(
        AutoConfig(
            cycle=CycleConfig(stable_timeout_s=0.1),  # реальный потолок ожидания мал
            tick_interval_s=0.01,
        )
    )
    env.drive_to_stabilizing()  # фиксации нет и не появится: watcher не тикают

    record = run_handle(env, make_request()).record
    assert record.code is ErrorCode.ERR_UNSTABLE
    assert record.massa is None
    assert record.vehicle_number == VEHICLE
    assert env.storage.pending_count() == 0
    assert env.capture.calls == []


def test_request_timeout_caps_stabilizing_wait(make_env: Callable[..., RunnerEnv]) -> None:
    """request.timeout_s короче stable_timeout_s: ожидание стабилизации
    ограничено присланным тайм-аутом — ERR_UNSTABLE приходит быстро,
    полный потолок конфига (заведомо больше лимита сценария) не отрабатывается."""
    env = make_env(
        AutoConfig(
            cycle=CycleConfig(stable_timeout_s=30.0),  # без ограничения тест бы завис
            tick_interval_s=0.01,
        )
    )
    env.drive_to_stabilizing()  # фиксации нет и не появится: watcher не тикают

    started = time.perf_counter()
    record = run_handle(env, make_request(timeout_s=0.1)).record
    elapsed = time.perf_counter() - started

    assert record.code is ErrorCode.ERR_UNSTABLE
    assert record.massa is None
    assert elapsed < IMMEDIATE_S  # ждали ~0.1 с, а не 30 с
    assert env.storage.pending_count() == 0
    assert env.capture.calls == []


def test_vehicle_leaves_during_stabilizing(env: RunnerEnv) -> None:
    """Машина съехала, пока runner ждал стабилизации: фаза ушла из
    STABILIZING без фиксации → ERR_VEHICLE_TIMEOUT, записи нет."""

    async def scenario() -> None:
        env.drive_to_stabilizing()
        task = asyncio.create_task(env.runner.handle(make_request()))
        await asyncio.sleep(0.05)
        assert not task.done()

        env.watcher.tick(ok(0.0, stable=False))  # съезд
        assert env.watcher.phase is WatcherPhase.WAIT_EMPTY
        result = await task
        assert result.record.code is ErrorCode.ERR_VEHICLE_TIMEOUT
        assert result.record.massa is None
        assert env.storage.pending_count() == 0
        assert env.capture.calls == []

    asyncio.run(asyncio.wait_for(scenario(), timeout=SCENARIO_TIMEOUT_S))


# --- ERR_CAMERA ---


def test_camera_failure_rejects_operation(env: RunnerEnv) -> None:
    """Сбой одной камеры при готовой фиксации: операция НЕ проводится
    (решение 09.08.2026) — ERR_CAMERA без веса, записи и файлов нет."""
    env.drive_to_ready()
    env.capture.shots = [
        CameraShot(role=CameraRole.FRONT, jpeg=FRONT_JPEG, captured_at=datetime.now(UTC)),
        CameraShot(
            role=CameraRole.REAR,
            jpeg=None,
            captured_at=datetime.now(UTC),
            error="rear: камера недоступна",
        ),
    ]
    record = run_handle(env, make_request()).record

    assert record.code is ErrorCode.ERR_CAMERA
    assert record.massa is None  # вес НЕ возвращается: операции не было
    assert record.message is not None and "не проведена" in record.message
    assert "rear" in record.message
    assert record.photos == []
    assert stored_record(env, record) is None  # журнал пуст
    assert list(env.photos_dir.rglob("*.jpeg")) == []  # файлов не осталось


# --- ERR_BUSY ---


def test_parallel_command_gets_err_busy(make_env: Callable[..., RunnerEnv]) -> None:
    """Пока первая команда ждёт стабилизации, вторая сразу получает ERR_BUSY
    и не пишется в журнал; первая доводится до своего исхода (OK)."""
    env = make_env(AutoConfig(cycle=CycleConfig(stable_timeout_s=5.0), tick_interval_s=0.01))

    async def scenario() -> None:
        env.drive_to_stabilizing()
        first_request = make_request()
        first_task = asyncio.create_task(env.runner.handle(first_request))
        await asyncio.sleep(0.05)  # операция стартовала и ждёт фиксации
        assert not first_task.done()

        busy_request = make_request(vehicle_number="05KG254AEA")
        busy_result = await env.runner.handle(busy_request)
        assert busy_result.request_id == busy_request.request_id
        assert busy_result.record.code is ErrorCode.ERR_BUSY
        assert busy_result.record.massa is None
        assert busy_result.record.message is not None
        assert not first_task.done()  # первая операция не прервана

        # отпускаем первую: вес выстоял → фиксация → OK
        env.watcher_clock.advance(CFG.stable_duration_s)
        env.watcher.tick(ok(GROSS_KG))
        first_result = await first_task
        assert first_result.request_id == first_request.request_id
        assert first_result.record.code is ErrorCode.OK

        # ERR_BUSY журнал не пополнил — там только первая операция
        assert env.storage.pending_count() == 1

    asyncio.run(asyncio.wait_for(scenario(), timeout=SCENARIO_TIMEOUT_S))


def test_err_busy_carries_normalized_numbers(make_env: Callable[..., RunnerEnv]) -> None:
    """ERR_BUSY отвечает на отклонённую команду её же номерами, причём
    нормализованными (upper/strip, пустые → None) — как в остальных отказах."""
    env = make_env(AutoConfig(cycle=CycleConfig(stable_timeout_s=5.0), tick_interval_s=0.01))

    async def scenario() -> None:
        env.drive_to_stabilizing()
        first_task = asyncio.create_task(env.runner.handle(make_request()))
        await asyncio.sleep(0.05)  # первая операция заняла весы
        assert not first_task.done()

        busy = await env.runner.handle(
            make_request(vehicle_number="  05kg254aea ", trailer_number=" bd777xy ")
        )
        assert busy.record.code is ErrorCode.ERR_BUSY
        assert busy.record.vehicle_number == "05KG254AEA"
        assert busy.record.trailer_number == "BD777XY"

        blank_busy = await env.runner.handle(make_request(vehicle_number="   "))
        assert blank_busy.record.code is ErrorCode.ERR_BUSY
        assert blank_busy.record.vehicle_number is None  # пробельный номер → None
        assert blank_busy.record.trailer_number is None

        # доводим первую операцию, чтобы сценарий завершился чисто
        env.watcher_clock.advance(CFG.stable_duration_s)
        env.watcher.tick(ok(GROSS_KG))
        first_result = await first_task
        assert first_result.record.code is ErrorCode.OK

    asyncio.run(asyncio.wait_for(scenario(), timeout=SCENARIO_TIMEOUT_S))


# --- тара и нетто (правило №4) ---


def test_taring_does_not_apply_tare(env: RunnerEnv) -> None:
    """Тарирование: massa и есть тара этого ТС — тара из реестра не подставляется."""
    put_tare(env.storage)  # действующая тара есть, но не нужна
    env.drive_to_ready()
    record = run_handle(env, make_request(operation=Operation.TARING)).record

    assert record.operation is Operation.TARING
    assert record.code is ErrorCode.OK
    assert record.massa == GROSS_KG
    assert record.tare_value is None
    assert record.tare_weighing_uuid is None
    assert record.netto is None
    assert stored_record(env, record) is not None


def test_stale_tare_is_ignored(env: RunnerEnv) -> None:
    """Тара старше 3 месяцев не действует: признак «нет тары», нетто нет."""
    put_tare(env.storage, tared_at=datetime.now(UTC) - timedelta(days=120))
    env.drive_to_ready()
    record = run_handle(env, make_request()).record

    assert record.code is ErrorCode.OK
    assert record.tare_value is None
    assert record.tare_weighing_uuid is None
    assert record.netto is None


def test_tare_of_other_trailer_not_applied(env: RunnerEnv) -> None:
    """Правило №4 (ред. 09.08.2026): смена прицепа = нет действующей тары —
    тара старой сцепки не подставляется ни другому прицепу, ни соло-машине."""
    put_tare(env.storage, trailer_number="OLD01AB")
    env.drive_to_ready()
    record = run_handle(env, make_request(trailer_number="NEW02CD")).record
    assert record.code is ErrorCode.OK
    assert record.tare_value is None and record.netto is None  # чужая сцепка

    record = run_handle(env, make_request()).record
    assert record.tare_value is None and record.netto is None  # без прицепа


def test_weighing_without_vehicle_number_no_netto(env: RunnerEnv) -> None:
    """Без номера ТС тару искать не по чему: нетто не считается."""
    put_tare(env.storage)
    env.drive_to_ready()
    record = run_handle(env, make_request(vehicle_number=None)).record

    assert record.code is ErrorCode.OK
    assert record.vehicle_number is None
    assert record.tare_value is None
    assert record.netto is None


# --- нормализация номеров ---


def test_vehicle_and_trailer_normalized(env: RunnerEnv) -> None:
    """Номера приводятся к upper и strip; тара СЦЕПКИ ищется уже по
    нормализованной паре; нормализованные значения попадают и в запись."""
    put_tare(env.storage, vehicle_number=VEHICLE, trailer_number=TRAILER)
    env.drive_to_ready()
    record = run_handle(
        env, make_request(vehicle_number="  01kg777aaa ", trailer_number=" bd123ab ")
    ).record

    assert record.vehicle_number == VEHICLE
    assert record.trailer_number == TRAILER
    assert record.tare_value == TARE_KG  # тара нашлась по нормализованной паре
    saved = stored_record(env, record)
    assert saved is not None
    assert saved.vehicle_number == VEHICLE
    assert saved.trailer_number == TRAILER


@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
def test_blank_numbers_become_none(env: RunnerEnv, blank: str) -> None:
    """Пустые и пробельные номера сохраняются как None, а не пустая строка."""
    env.drive_to_ready()
    record = run_handle(env, make_request(vehicle_number=blank, trailer_number=blank)).record

    assert record.code is ErrorCode.OK
    assert record.vehicle_number is None
    assert record.trailer_number is None
    assert record.netto is None  # без номера нетто не считается


# --- агент 0.4.17: тара из команды центра и номер документа АИС в записи ---


def test_tare_from_center_overrides_replica(env: RunnerEnv) -> None:
    """Центр прислал действующую тару в команде (tare_resolved) — она подставляется,
    даже если реплика на весовом ПК ничего не знает (реплика отстала)."""
    env.drive_to_ready()
    hint = TareRecord(
        vehicle_number=VEHICLE,
        trailer_number=TRAILER,
        tare_value=TARE_KG,
        tared_at=datetime.now(UTC) - timedelta(days=3),
        weighing_uuid=uuid4(),
    )
    record = run_handle(
        env, make_request(trailer_number=TRAILER, tare=hint, tare_resolved=True)
    ).record
    assert record.code is ErrorCode.OK
    assert record.tare_value == TARE_KG
    assert record.tare_weighing_uuid == hint.weighing_uuid
    assert record.netto == GROSS_KG - TARE_KG


def test_center_says_no_tare_wins_over_replica(env: RunnerEnv) -> None:
    """Центр искал и не нашёл (tare_resolved без tare) — реплика не опрашивается,
    даже если в ней лежит устаревший снимок с действующей тарой."""
    put_tare(env.storage, trailer_number=TRAILER)
    env.drive_to_ready()
    record = run_handle(
        env, make_request(trailer_number=TRAILER, tare=None, tare_resolved=True)
    ).record
    assert record.tare_value is None and record.netto is None


def test_without_hint_replica_is_used(env: RunnerEnv) -> None:
    """Старый центр без подсказки — тара по реплике, как раньше."""
    tare = put_tare(env.storage, trailer_number=TRAILER)
    env.drive_to_ready()
    record = run_handle(env, make_request(trailer_number=TRAILER)).record
    assert record.tare_weighing_uuid == tare.weighing_uuid


def test_ais_ref_travels_with_record(env: RunnerEnv) -> None:
    """Номер документа АИС из команды v2 сохраняется в записи и уходит с ней
    (weigh_result и досылка offline_sync)."""
    env.drive_to_ready()
    result = run_handle(env, make_request(ais_ref="WEI000094176"))
    assert result.record.ais_ref == "WEI000094176"
    stored = stored_record(env, result.record)
    assert stored is not None and stored.ais_ref == "WEI000094176"
    pending = env.storage.pending_records()
    assert [r.ais_ref for r in pending] == ["WEI000094176"]


def test_refusal_echoes_ais_ref(env: RunnerEnv) -> None:
    """Отказ (нет машины) тоже несёт номер — АИС свяжет ответ со своей командой."""
    result = run_handle(env, make_request(ais_ref="WEI000094177"))
    assert result.record.code is ErrorCode.ERR_VEHICLE_TIMEOUT
    assert result.record.ais_ref == "WEI000094177"
