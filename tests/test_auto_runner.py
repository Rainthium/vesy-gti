"""Тесты оркестратора автоматического режима (agent/weighing/auto.py).

Покрытие AutoOperationRunner.handle():
- счастливый путь: цикл до CAPTURE, снимки обеих камер, code OK,
  запись в журнале, файлы на диске байт-в-байт (правило №2);
- ERR_CAMERA: без снимков обеих камер операция не проводится (решение 09.08.2026);
- ошибки до фиксации веса (ERR_NOT_ZERO, ERR_SCALE_OFFLINE): результат
  возвращается, но локально НЕ сохраняется, камеры не дёргаются;
- ERR_BUSY: параллельная команда отклоняется сразу и не пишется в журнал;
- общий тайм-аут request.timeout_s: честный код по фазе
  (WAIT_ZERO → ERR_NOT_ZERO, WAIT_VEHICLE → ERR_VEHICLE_TIMEOUT,
  WAIT_STABLE → ERR_UNSTABLE);
- нетто по правилу №4: тара из реплики реестра, тарирование и запись
  без номера тару не подставляют, просроченная тара не действует;
- нормализация номеров: upper/strip, пустые → None.

Железо не используется: индикатор — скриптованная фабрика ScaleState
с управляемыми часами (clock инъецируется в runner), камеры — monkeypatch
agent.weighing.auto.capture_all. Асинхронность — через asyncio.run
по образцу tests/test_ws_client.py (без pytest-asyncio).
"""

import asyncio
import hashlib
from collections import deque
from collections.abc import Iterator
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
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord, WeighingRecord, WeighRequest, WeighResult

# Общий лимит на сценарий: время цикла фейковое, реальное время — миллисекунды.
SCENARIO_TIMEOUT_S = 10.0

CFG = CycleConfig()  # пороги 50/500 кг, таймауты 10/60/30 с, выдержка 2 с

VEHICLE = "01KG777AAA"
GROSS_KG = 43310.0
TARE_KG = 15300.0

# разные тела снимков — проверяем соответствие камера → файл (без пересжатия)
FRONT_JPEG = b"\xff\xd8\xff\xe0" + b"front-camera-frame" + b"\xff\xd9"
REAR_JPEG = b"\xff\xd8\xff\xe0" + b"rear-camera-frame-bytes" + b"\xff\xd9"


def ok(weight: float | None, *, stable: bool = True, overload: bool = False) -> ScaleState:
    """Снимок индикатора с идущим потоком данных (status OK)."""
    return ScaleState(status=ScaleStatus.OK, weight_kg=weight, stable=stable, overload=overload)


NO_DATA = ScaleState(status=ScaleStatus.NO_DATA)


class FakeClock:
    """Управляемые монотонные часы: инъецируются и в runner, и в цикл."""

    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScaleScript:
    """Сценарий индикатора: очередь шагов (сдвиг часов, снимок).

    Каждый опрос сдвигает часы и отдаёт очередной снимок; когда очередь
    исчерпана, повторяется ``hold`` — им же управляем «подвисшей» операцией.
    """

    def __init__(self, clock: FakeClock, hold: tuple[float, ScaleState]) -> None:
        self._clock = clock
        self._steps: deque[tuple[float, ScaleState]] = deque()
        self.hold = hold

    def push(self, advance_s: float, state: ScaleState) -> None:
        self._steps.append((advance_s, state))

    def __call__(self) -> ScaleState:
        advance_s, state = self._steps.popleft() if self._steps else self.hold
        self._clock.advance(advance_s)
        return state


def good_shots() -> list[CameraShot]:
    """Удачные кадры обеих камер (порядок соответствует списку камер)."""
    now = datetime.now(UTC)
    return [
        CameraShot(role=CameraRole.FRONT, jpeg=FRONT_JPEG, captured_at=now),
        CameraShot(role=CameraRole.REAR, jpeg=REAR_JPEG, captured_at=now),
    ]


class CaptureMock:
    """Замена capture_all: в сеть не ходит, фиксирует вызовы, отдаёт заготовку."""

    def __init__(self) -> None:
        self.shots: list[CameraShot] = good_shots()
        self.calls: list[list[CameraRole]] = []

    def __call__(
        self, configs: list[CameraConfig], *, ffmpeg_path: str = "ffmpeg"
    ) -> list[CameraShot]:
        self.calls.append([config.role for config in configs])
        return list(self.shots)


class RunnerEnv:
    """Собранный runner с фейками: часы, скрипт индикатора, мок камер, SQLite."""

    def __init__(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.clock = FakeClock()
        self.script = ScaleScript(self.clock, hold=(0.5, ok(0.0)))
        self.storage = AgentStorage(tmp_path / "agent.db")
        self.photos_dir = tmp_path / "photos"
        self.capture = CaptureMock()
        monkeypatch.setattr(auto, "capture_all", self.capture)
        self.runner = AutoOperationRunner(
            scale_state=self.script,
            storage=self.storage,
            cameras=[
                CameraConfig(role=CameraRole.FRONT, snapshot_url="http://127.0.0.1:9/front.jpg"),
                CameraConfig(role=CameraRole.REAR, snapshot_url="http://127.0.0.1:9/rear.jpg"),
            ],
            photos_dir=self.photos_dir,
            config=AutoConfig(tick_interval_s=0.001),
            clock=self.clock,
        )


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[RunnerEnv]:
    environment = RunnerEnv(tmp_path, monkeypatch)
    yield environment
    environment.storage.close()


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


def drive_to_capture(script: ScaleScript, weight: float = GROSS_KG) -> None:
    """Заскриптовать полный проезд: пусто → заезд → стабилизация → фиксация."""
    script.push(0.05, ok(0.0))  # пустые стабильные весы → WAIT_VEHICLE
    script.push(0.05, ok(weight, stable=False))  # заезд → WAIT_STABLE
    script.push(0.05, ok(weight))  # первый кандидат неизменности
    script.push(CFG.stable_duration_s, ok(weight))  # выдержка набрана → CAPTURE
    script.hold = (0.05, ok(weight))


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


# --- счастливый путь ---


def test_happy_path_weighing_ok(env: RunnerEnv) -> None:
    """Полный цикл: OK, вес, снимки обеих камер с sha256, запись в журнале,
    файлы на диске байт-в-байт, request_id результата совпадает с командой."""
    drive_to_capture(env.script)
    request = make_request()
    result = run_handle(env, request)

    assert result.request_id == request.request_id
    record = result.record
    assert record.code is ErrorCode.OK
    assert record.operation is Operation.WEIGHING
    assert record.massa == GROSS_KG
    assert record.stable is True
    assert record.source is WeighingSource.AIS
    assert record.vehicle_number == VEHICLE
    assert record.message is None
    assert record.weighed_at is not None and record.weighed_at.tzinfo == UTC

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
    # камеры дёрнуты один раз, в порядке конфига
    assert env.capture.calls == [[CameraRole.FRONT, CameraRole.REAR]]


# --- ERR_CAMERA ---


def test_one_camera_failed_rejects_operation(env: RunnerEnv) -> None:
    """Сбой одной камеры: операция НЕ проводится (решение 09.08.2026) —
    code=ERR_CAMERA без веса, запись не создана, файлов на диске нет."""
    drive_to_capture(env.script)
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


# --- ошибки до фиксации веса: локально не сохраняются ---


def test_not_zero_error_not_saved_locally(env: RunnerEnv) -> None:
    """Весы не пустеют → ERR_NOT_ZERO по таймауту фазы: результат вернулся,
    но журнал пуст и камеры не дёргались (вес не был зафиксирован)."""
    env.script.hold = (1.0, ok(3000.0))  # весы заняты всё время
    request = make_request()
    result = run_handle(env, request)

    record = result.record
    assert result.request_id == request.request_id
    assert record.code is ErrorCode.ERR_NOT_ZERO
    assert record.massa is None
    assert record.stable is False
    assert record.weighed_at is None
    assert record.photos == []
    assert env.storage.pending_count() == 0
    assert stored_record(env, record) is None
    assert env.capture.calls == []


def test_scale_offline_error_not_saved_locally(env: RunnerEnv) -> None:
    """Обрыв потока данных дольше no_data_timeout_s → ERR_SCALE_OFFLINE,
    запись локально не сохраняется."""
    env.script.hold = (1.0, NO_DATA)
    record = run_handle(env, make_request()).record
    assert record.code is ErrorCode.ERR_SCALE_OFFLINE
    assert record.massa is None
    assert env.storage.pending_count() == 0
    assert env.capture.calls == []


# --- ERR_BUSY ---


def test_parallel_command_gets_err_busy(env: RunnerEnv) -> None:
    """Пока первая команда висит в стабилизации, вторая сразу получает
    ERR_BUSY и не пишется в журнал; первая доводится до своего исхода."""

    async def scenario() -> None:
        # первая операция застревает в WAIT_STABLE: вес прыгает, часы стоят
        env.script.push(0.0, ok(0.0))
        env.script.push(0.0, ok(GROSS_KG, stable=False))
        env.script.hold = (0.0, ok(GROSS_KG, stable=False))

        first_request = make_request()
        first_task = asyncio.create_task(env.runner.handle(first_request))
        await asyncio.sleep(0.05)  # операция стартовала и висит
        assert not first_task.done()

        busy_request = make_request(vehicle_number="05KG254AEA")
        busy_result = await env.runner.handle(busy_request)
        assert busy_result.request_id == busy_request.request_id
        assert busy_result.record.code is ErrorCode.ERR_BUSY
        assert busy_result.record.massa is None
        assert busy_result.record.message is not None
        assert not first_task.done()  # первая операция не прервана

        # отпускаем первую: время идёт → честный ERR_UNSTABLE по фазе
        env.script.hold = (5.0, ok(GROSS_KG, stable=False))
        first_result = await first_task
        assert first_result.request_id == first_request.request_id
        assert first_result.record.code is ErrorCode.ERR_UNSTABLE

        # ни ERR_BUSY, ни безвесовая первая операция журнал не пополнили
        assert env.storage.pending_count() == 0

    asyncio.run(asyncio.wait_for(scenario(), timeout=SCENARIO_TIMEOUT_S))


# --- общий тайм-аут операции (request.timeout_s) ---


def prepare_phase(script: ScaleScript, phase: str) -> None:
    """Заскриптовать зависание в нужной фазе: часы идут, фаза не меняется."""
    if phase == "wait_zero":
        script.hold = (1.0, ok(3000.0))  # весы не пустеют
    elif phase == "wait_vehicle":
        script.push(0.05, ok(0.0))
        script.hold = (1.0, ok(0.0))  # никто не заезжает
    else:  # wait_stable
        script.push(0.05, ok(0.0))
        script.push(0.05, ok(GROSS_KG, stable=False))
        script.hold = (1.0, ok(GROSS_KG, stable=False))  # вес не стабилизируется


@pytest.mark.parametrize(
    ("phase", "expected_code"),
    [
        pytest.param("wait_zero", ErrorCode.ERR_NOT_ZERO, id="wait-zero"),
        pytest.param("wait_vehicle", ErrorCode.ERR_VEHICLE_TIMEOUT, id="wait-vehicle"),
        pytest.param("wait_stable", ErrorCode.ERR_UNSTABLE, id="wait-stable"),
    ],
)
def test_overall_timeout_maps_to_phase_code(
    env: RunnerEnv, phase: str, expected_code: ErrorCode
) -> None:
    """request.timeout_s истёк раньше фазовых таймаутов → честный код по фазе
    и сообщение про общий тайм-аут; запись локально не сохраняется."""
    prepare_phase(env.script, phase)
    # 5 с меньше любого фазового таймаута (10/60/30) — сработает общий потолок
    record = run_handle(env, make_request(timeout_s=5.0)).record

    assert record.code is expected_code
    assert record.message == "превышен общий тайм-аут операции"
    assert record.massa is None
    assert env.storage.pending_count() == 0
    assert env.capture.calls == []


# --- нетто по правилу №4 ---


def test_weighing_with_active_tare_computes_netto(env: RunnerEnv) -> None:
    """Действующая тара из реплики реестра: tare_value, netto = брутто − тара,
    ссылка на операцию тарирования; всё это же — в сохранённой записи."""
    tare = put_tare(env.storage)
    drive_to_capture(env.script)
    record = run_handle(env, make_request()).record

    assert record.tare_value == TARE_KG
    assert record.tare_weighing_uuid == tare.weighing_uuid
    assert record.netto == GROSS_KG - TARE_KG
    saved = stored_record(env, record)
    assert saved is not None
    assert saved.tare_value == TARE_KG
    assert saved.netto == GROSS_KG - TARE_KG


def test_taring_does_not_apply_tare(env: RunnerEnv) -> None:
    """Тарирование: massa и есть тара этого ТС — тара из реестра не подставляется."""
    put_tare(env.storage)  # действующая тара есть, но не нужна
    drive_to_capture(env.script)
    record = run_handle(env, make_request(operation=Operation.TARING)).record

    assert record.operation is Operation.TARING
    assert record.code is ErrorCode.OK
    assert record.massa == GROSS_KG
    assert record.tare_value is None
    assert record.tare_weighing_uuid is None
    assert record.netto is None
    assert stored_record(env, record) is not None


def test_weighing_without_vehicle_number_no_netto(env: RunnerEnv) -> None:
    """Без номера ТС тару искать не по чему: нетто не считается."""
    put_tare(env.storage)
    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=None)).record

    assert record.code is ErrorCode.OK
    assert record.vehicle_number is None
    assert record.tare_value is None
    assert record.netto is None


def test_stale_tare_is_ignored(env: RunnerEnv) -> None:
    """Тара старше 3 месяцев не действует: признак «нет тары», нетто нет."""
    put_tare(env.storage, tared_at=datetime.now(UTC) - timedelta(days=120))
    drive_to_capture(env.script)
    record = run_handle(env, make_request()).record

    assert record.code is ErrorCode.OK
    assert record.tare_value is None
    assert record.tare_weighing_uuid is None
    assert record.netto is None


# --- нормализация номеров ---


def test_vehicle_and_trailer_normalized(env: RunnerEnv) -> None:
    """Номера приводятся к upper и strip; тара СЦЕПКИ ищется уже по
    нормализованной паре; нормализованные значения попадают и в запись."""
    put_tare(env.storage, vehicle_number=VEHICLE, trailer_number="BD123AB")
    drive_to_capture(env.script)
    record = run_handle(
        env, make_request(vehicle_number="  01kg777aaa ", trailer_number=" bd123ab ")
    ).record

    assert record.vehicle_number == VEHICLE
    assert record.trailer_number == "BD123AB"
    assert record.tare_value == TARE_KG  # тара нашлась по нормализованному номеру
    saved = stored_record(env, record)
    assert saved is not None
    assert saved.vehicle_number == VEHICLE
    assert saved.trailer_number == "BD123AB"


def test_tare_of_other_trailer_not_applied(env: RunnerEnv) -> None:
    """Правило №4 (ред. 09.08.2026): смена прицепа = нет действующей тары.

    Тарирование было со старым прицепом — при взвешивании с другим прицепом
    (и при взвешивании вовсе без прицепа) тара НЕ подставляется."""
    put_tare(env.storage, vehicle_number=VEHICLE, trailer_number="OLD01AB")
    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=VEHICLE, trailer_number="NEW02CD")).record

    assert record.code is ErrorCode.OK
    assert record.tare_value is None and record.netto is None  # чужая сцепка

    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=VEHICLE)).record
    assert record.tare_value is None and record.netto is None  # без прицепа


def test_tare_without_trailer_only_for_solo_vehicle(env: RunnerEnv) -> None:
    """Тарирование без прицепа действует только для машины без прицепа."""
    put_tare(env.storage, vehicle_number=VEHICLE, trailer_number=None)
    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=VEHICLE, trailer_number="BD123AB")).record
    assert record.tare_value is None  # с прицепом — соло-тара не подходит

    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=VEHICLE)).record
    assert record.tare_value == TARE_KG  # без прицепа — подошла
    assert record.netto == GROSS_KG - TARE_KG


@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
def test_blank_numbers_become_none(env: RunnerEnv, blank: str) -> None:
    """Пустые и пробельные номера сохраняются как None, а не пустая строка."""
    drive_to_capture(env.script)
    record = run_handle(env, make_request(vehicle_number=blank, trailer_number=blank)).record

    assert record.code is ErrorCode.OK
    assert record.vehicle_number is None
    assert record.trailer_number is None
    assert record.netto is None  # без номера нетто не считается
