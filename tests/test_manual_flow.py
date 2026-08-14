"""Тесты ручного офлайн-режима (agent/weighing/manual.py).

Покрытие ManualOperationFlow:
- ready(): каждый фактор готовности по отдельности ломает кнопку;
- prepare(): отказы (ManualFlowError, превью и файлов нет) и успех —
  подстановка тары из реплики реестра, расчёт нетто (правило №4),
  нормализация номеров, снимки байт-в-байт (правило №2), отказ без камер
  при сбое одной камеры, замена неподтверждённого превью;
- commit(): запись в журнале, фото привязаны, файлы не удаляются,
  повторный/чужой id → ManualFlowError без записи;
- discard(): файлы удалены, записи нет; чужой id и отмена после
  commit — идемпотентные no-op.

Железо не используется: весы — управляемое замыкание scale_state,
камеры — локальный http.server (как в tests/test_cameras.py).
"""

import hashlib
import http.server
import socket
import sqlite3
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from agent.cameras.capture import CameraConfig
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage
from agent.weighing.manual import ManualFlowError, ManualOperationFlow, ManualPreview
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord

# разные тела снимков, чтобы проверить соответствие камера → файл
FRONT_JPEG = b"\xff\xd8\xff\xe0" + b"front-camera-frame" + b"\xff\xd9"
REAR_JPEG = b"\xff\xd8\xff\xe0" + b"rear-camera-frame-bytes" + b"\xff\xd9"

OPERATOR = "А. Осмонов"
VEHICLE = "01KG777AAA"


class FlowEnv:
    """Управляемое окружение потока: замыкания scale_state и manual_allowed."""

    def __init__(self, storage: AgentStorage, photos_dir: Path) -> None:
        self.storage = storage
        self.photos_dir = photos_dir
        self.allowed = True  # правило №3: связи с центром нет
        self.scale = ScaleState(status=ScaleStatus.OK, weight_kg=43310.0, stable=True)

    def set_scale(self, **overrides: Any) -> None:
        """Собрать состояние индикатора из «хорошей» базы с точечными заменами."""
        base: dict[str, Any] = {
            "status": ScaleStatus.OK,
            "weight_kg": 43310.0,
            "stable": True,
            "overload": False,
        }
        base.update(overrides)
        self.scale = ScaleState(**base)

    def make_flow(
        self,
        cameras: list[CameraConfig] | None = None,
        threshold: float = 500.0,
        now_utc: Callable[[], datetime] | None = None,
    ) -> ManualOperationFlow:
        return ManualOperationFlow(
            scale_state=lambda: self.scale,
            manual_allowed=lambda: self.allowed,
            storage=self.storage,
            cameras=list(cameras or []),
            photos_dir=self.photos_dir,
            vehicle_threshold_kg=threshold,
            now_utc=now_utc,
        )


@pytest.fixture
def storage() -> Iterator[AgentStorage]:
    store = AgentStorage(":memory:")
    yield store
    store.close()


@pytest.fixture
def env(storage: AgentStorage, tmp_path: Path) -> FlowEnv:
    return FlowEnv(storage, tmp_path / "photos")


def put_tare(
    storage: AgentStorage,
    vehicle_number: str = VEHICLE,
    tare_value: float = 15300.0,
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


def prepare_weighing(
    flow: ManualOperationFlow,
    vehicle_number: str = VEHICLE,
    trailer_number: str | None = None,
) -> ManualPreview:
    return flow.prepare(
        Operation.WEIGHING,
        vehicle_number=vehicle_number,
        trailer_number=trailer_number,
        operator=OPERATOR,
    )


# --- камеры: локальный HTTP-сервер вместо железа ---


@pytest.fixture
def http_camera() -> Iterator[str]:
    """Локальный snapshot-эндпоинт: /front.jpg и /rear.jpg с разными телами."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/front.jpg":
                code, body = 200, FRONT_JPEG
            elif self.path == "/rear.jpg":
                code, body = 200, REAR_JPEG
            else:
                code, body = 404, b"Not Found"
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            # не засорять вывод pytest логами сервера
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def free_port() -> int:
    """Порт, который заведомо никто не слушает (для «упавшей» камеры)."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def two_cameras(base_url: str) -> list[CameraConfig]:
    return [
        CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{base_url}/front.jpg"),
        CameraConfig(role=CameraRole.REAR, snapshot_url=f"{base_url}/rear.jpg"),
    ]


# --- ready() ---


class TestReady:
    @pytest.mark.parametrize(
        ("allowed", "scale_overrides", "expected"),
        [
            pytest.param(True, {}, True, id="all-good"),
            pytest.param(False, {}, False, id="center-online"),
            pytest.param(
                True, {"status": ScaleStatus.NO_DATA, "weight_kg": None}, False, id="no-data"
            ),
            pytest.param(
                True, {"status": ScaleStatus.PORT_ERROR, "weight_kg": None}, False, id="port-error"
            ),
            pytest.param(True, {"stable": False}, False, id="unstable"),
            pytest.param(True, {"overload": True}, False, id="overload"),
            pytest.param(True, {"weight_kg": None}, False, id="no-weight"),
            pytest.param(True, {"weight_kg": 499.9}, False, id="below-threshold"),
            pytest.param(True, {"weight_kg": 500.0}, True, id="exact-threshold"),
        ],
    )
    def test_each_factor_breaks_readiness(
        self, env: FlowEnv, allowed: bool, scale_overrides: dict[str, Any], expected: bool
    ) -> None:
        """Готовность требует всех факторов сразу; каждый по отдельности её ломает."""
        env.allowed = allowed
        env.set_scale(**scale_overrides)
        assert env.make_flow().ready() is expected

    def test_threshold_comes_from_config(self, env: FlowEnv) -> None:
        """Порог пустых весов берётся из параметра конструктора, не захардкожен."""
        flow = env.make_flow(threshold=8000.0)
        env.set_scale(weight_kg=5000.0)  # выше дефолтных 500, но ниже конфига
        assert flow.ready() is False
        env.set_scale(weight_kg=8000.0)
        assert flow.ready() is True


# --- prepare(): отказы ---


class TestPrepareRejected:
    @pytest.mark.parametrize(
        ("allowed", "scale_overrides", "vehicle_number", "match"),
        [
            pytest.param(False, {}, VEHICLE, "связь с центром", id="center-online"),
            pytest.param(True, {}, "", "номер головы", id="empty-number"),
            pytest.param(True, {}, "   ", "номер головы", id="whitespace-number"),
            pytest.param(
                True,
                {"status": ScaleStatus.NO_DATA, "weight_kg": None},
                VEHICLE,
                "Нет данных",
                id="no-data",
            ),
            pytest.param(
                True,
                {"status": ScaleStatus.PORT_ERROR, "weight_kg": None},
                VEHICLE,
                "Нет данных",
                id="port-error",
            ),
            pytest.param(True, {"overload": True}, VEHICLE, "Перегруз", id="overload"),
            pytest.param(True, {"weight_kg": 320.0}, VEHICLE, "не на весах", id="below-threshold"),
            pytest.param(True, {"weight_kg": None}, VEHICLE, "не на весах", id="no-weight"),
            pytest.param(True, {"stable": False}, VEHICLE, "нестабильна", id="unstable"),
        ],
    )
    def test_rejected_without_preview_and_files(
        self,
        env: FlowEnv,
        allowed: bool,
        scale_overrides: dict[str, Any],
        vehicle_number: str,
        match: str,
    ) -> None:
        """Отказ — осмысленный текст; превью не создаётся, файлов на диске нет."""
        env.allowed = allowed
        env.set_scale(**scale_overrides)
        flow = env.make_flow()
        with pytest.raises(ManualFlowError, match=match):
            prepare_weighing(flow, vehicle_number=vehicle_number)
        assert flow.pending() is None
        # каталог снимков даже не создавался
        assert not env.photos_dir.exists()

    def test_low_weight_uses_configured_threshold(self, env: FlowEnv) -> None:
        """Порог заезда в prepare тоже берётся из конфига."""
        env.set_scale(weight_kg=5000.0)
        flow = env.make_flow(threshold=8000.0)
        with pytest.raises(ManualFlowError, match="не на весах"):
            prepare_weighing(flow)


# --- prepare(): успех ---


class TestPrepareSuccess:
    def test_weighing_with_active_tare(self, env: FlowEnv) -> None:
        """Тара из реестра подставлена, нетто = брутто − тара (правило №4)."""
        tare = put_tare(env.storage, tare_value=15300.0)
        env.set_scale(weight_kg=43310.0)
        flow = env.make_flow()
        preview = prepare_weighing(flow)

        record = preview.record
        assert record.operation is Operation.WEIGHING
        assert record.code is ErrorCode.OK
        assert record.massa == 43310.0
        assert record.stable is True
        assert record.tare_value == 15300.0
        assert record.tare_weighing_uuid == tare.weighing_uuid
        assert record.netto == 43310.0 - 15300.0
        assert record.source is WeighingSource.LOCAL_OFFLINE
        assert record.operator == OPERATOR
        assert record.message is None
        assert record.weighed_at is not None and record.weighed_at.tzinfo == UTC
        assert preview.tare == tare
        assert preview.no_valid_tare is False
        assert preview.expired_tare is None  # действующая тара — устаревшую не ищем
        assert preview.photos == []  # камер нет — снимков нет, но это не ошибка
        assert flow.pending() is preview

    def test_weighed_at_taken_from_injected_clock(self, env: FlowEnv) -> None:
        """«Время от центра» (10.08.2026): weighed_at берётся из now_utc
        (часы CenterClock), а не из локальных часов ПК — и в превью,
        и в сохранённой записи журнала."""
        fixed_now = datetime(2026, 8, 10, 6, 30, 15, 123456, tzinfo=UTC)
        flow = env.make_flow(now_utc=lambda: fixed_now)
        preview = prepare_weighing(flow)
        assert preview.record.weighed_at == fixed_now
        flow.commit(preview.preview_id)
        saved = env.storage.get_weighing(preview.record.uuid)
        assert saved is not None
        assert saved.weighed_at == fixed_now

    def test_vehicle_and_trailer_normalized(self, env: FlowEnv) -> None:
        """Номера приводятся к верхнему регистру и обрезаются по краям;
        тара СЦЕПКИ ищется уже по нормализованной паре."""
        put_tare(env.storage, vehicle_number=VEHICLE, trailer_number="BD123AB")
        flow = env.make_flow()
        preview = prepare_weighing(flow, vehicle_number="  01kg777aaa ", trailer_number=" bd123ab ")
        assert preview.record.vehicle_number == VEHICLE
        assert preview.record.trailer_number == "BD123AB"
        assert preview.record.tare_value == 15300.0  # тара нашлась по upper-паре

    def test_tare_of_other_trailer_not_applied(self, env: FlowEnv) -> None:
        """Правило №4 (ред. 09.08.2026): смена прицепа = нет действующей тары."""
        put_tare(env.storage, vehicle_number=VEHICLE, trailer_number="OLD01AB")
        preview = prepare_weighing(env.make_flow(), trailer_number="NEW02CD")
        assert preview.record.tare_value is None
        assert preview.no_valid_tare is True

    def test_tare_without_trailer_only_for_solo_vehicle(self, env: FlowEnv) -> None:
        """Соло-тарирование действует только для машины без прицепа
        (зеркало кейса авторежима из test_auto_runner)."""
        put_tare(env.storage, vehicle_number=VEHICLE, trailer_number=None)
        # с прицепом соло-тара не подставляется, нетто не считается
        preview = prepare_weighing(env.make_flow(), trailer_number="BD123AB")
        assert preview.record.tare_value is None
        assert preview.record.netto is None
        assert preview.no_valid_tare is True
        # без прицепа — тара подошла, нетто = брутто − тара
        preview = prepare_weighing(env.make_flow())
        assert preview.record.tare_value == 15300.0
        assert preview.record.netto == 43310.0 - 15300.0
        assert preview.no_valid_tare is False

    @pytest.mark.parametrize("trailer", [None, "", "   "])
    def test_empty_trailer_becomes_none(self, env: FlowEnv, trailer: str | None) -> None:
        """Пустой/пробельный прицеп сохраняется как None, а не пустая строка."""
        preview = prepare_weighing(env.make_flow(), trailer_number=trailer)
        assert preview.record.trailer_number is None

    def test_weighing_without_tare(self, env: FlowEnv) -> None:
        """Тары в реестре нет: нетто не считается, признак «нет тары»."""
        preview = prepare_weighing(env.make_flow())
        record = preview.record
        assert record.code is ErrorCode.OK
        assert record.tare_value is None
        assert record.tare_weighing_uuid is None
        assert record.netto is None
        assert preview.tare is None
        assert preview.no_valid_tare is True
        assert preview.expired_tare is None  # сцепка не тарировалась вовсе

    def test_weighing_with_stale_tare(self, env: FlowEnv) -> None:
        """Тара старше 3 месяцев не действует: как будто тары нет (правило №4),
        но устаревшее тарирование попадает в превью — карточка результата
        покажет его дату и массу (просьба Игоря 14.08.2026)."""
        stale_at = datetime.now(UTC) - timedelta(days=120)
        stale = put_tare(env.storage, tared_at=stale_at)
        preview = prepare_weighing(env.make_flow())
        assert preview.record.tare_value is None
        assert preview.record.netto is None
        assert preview.tare is None
        assert preview.no_valid_tare is True
        assert preview.expired_tare == stale

    def test_taring_does_not_lookup_tare(self, env: FlowEnv) -> None:
        """При тарировании тара не ищется: massa и есть тара этого ТС."""
        put_tare(env.storage)  # действующая тара есть, но она не нужна
        flow = env.make_flow()
        preview = flow.prepare(
            Operation.TARING,
            vehicle_number=VEHICLE,
            trailer_number=None,
            operator=OPERATOR,
        )
        record = preview.record
        assert record.operation is Operation.TARING
        assert record.massa == 43310.0
        assert record.tare_value is None
        assert record.tare_weighing_uuid is None
        assert record.netto is None
        assert preview.tare is None
        assert preview.no_valid_tare is False  # для тарирования признак не ставится

    def test_photos_saved_byte_for_byte(self, env: FlowEnv, http_camera: str) -> None:
        """Снимки лежат в ГГГГ/ММ/ДД под hex-uuid записи; байты, sha256 и размер
        совпадают со снимком камеры без пересжатия (правило №2)."""
        flow = env.make_flow(two_cameras(http_camera))
        preview = prepare_weighing(flow)
        record = preview.record
        assert record.code is ErrorCode.OK
        assert record.weighed_at is not None
        day_dir = env.photos_dir / record.weighed_at.strftime("%Y/%m/%d")

        assert [photo.role for photo in preview.photos] == [CameraRole.FRONT, CameraRole.REAR]
        front, rear = preview.photos
        assert Path(front.path) == day_dir / f"{record.uuid.hex}_photo1.jpeg"
        assert Path(rear.path) == day_dir / f"{record.uuid.hex}_photo2.jpeg"
        for photo, jpeg in ((front, FRONT_JPEG), (rear, REAR_JPEG)):
            assert Path(photo.path).read_bytes() == jpeg  # байт-в-байт
            assert photo.sha256 == hashlib.sha256(jpeg).hexdigest()
            assert photo.size_bytes == len(jpeg)

    def test_one_camera_failed_rejects_operation(self, env: FlowEnv, http_camera: str) -> None:
        """Сбой одной камеры: операция НЕ проводится (решение 09.08.2026) —
        ManualFlowError с текстом для оператора, файлов и записи нет."""
        cameras = [
            CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{http_camera}/front.jpg"),
            CameraConfig(
                role=CameraRole.REAR,
                snapshot_url=f"http://127.0.0.1:{free_port()}/rear.jpg",
                timeout_s=0.5,
            ),
        ]
        flow = env.make_flow(cameras)
        with pytest.raises(ManualFlowError, match="не проведена"):
            prepare_weighing(flow)
        assert flow.pending() is None  # превью не создано
        assert list(env.photos_dir.rglob("*.jpeg")) == []  # файлов не осталось

    def test_new_prepare_replaces_old_preview(self, env: FlowEnv, http_camera: str) -> None:
        """Повторная фиксация заменяет неподтверждённое превью,
        файлы старых снимков удаляются с диска."""
        flow = env.make_flow(two_cameras(http_camera))
        first = prepare_weighing(flow)
        first_paths = [Path(photo.path) for photo in first.photos]
        assert all(path.exists() for path in first_paths)

        second = prepare_weighing(flow, vehicle_number="05KG254AEA")
        assert flow.pending() is second
        assert all(not path.exists() for path in first_paths)
        assert all(Path(photo.path).exists() for photo in second.photos)


# --- commit() ---


class TestCommit:
    def test_commit_saves_record_and_photos(self, env: FlowEnv, http_camera: str) -> None:
        """Подтверждение: запись в журнале, фото привязаны, превью очищено,
        файлы снимков живы (правило №2 — фото не удаляются)."""
        put_tare(env.storage)
        flow = env.make_flow(two_cameras(http_camera))
        preview = prepare_weighing(flow)

        committed = flow.commit(preview.preview_id)
        assert committed == preview.record

        stored = env.storage.get_weighing(preview.record.uuid)
        assert stored == preview.record
        assert env.storage.photos_for(preview.record.uuid) == preview.photos
        assert flow.pending() is None
        for photo in preview.photos:
            assert Path(photo.path).exists(), "файлы снимков не должны удаляться после commit"

    def test_commit_twice_raises(self, env: FlowEnv) -> None:
        """Повторный commit того же id — ошибка «операция устарела»."""
        flow = env.make_flow()
        preview = prepare_weighing(flow)
        flow.commit(preview.preview_id)
        with pytest.raises(ManualFlowError, match="устарела"):
            flow.commit(preview.preview_id)

    def test_commit_wrong_id_keeps_pending(self, env: FlowEnv) -> None:
        """Чужой/устаревший id: ошибка, записи нет, актуальное превью не теряется."""
        flow = env.make_flow()
        preview = prepare_weighing(flow)
        with pytest.raises(ManualFlowError):
            flow.commit("stale-or-forged-id")
        assert env.storage.get_weighing(preview.record.uuid) is None
        assert flow.pending() is preview
        # верный id после этого по-прежнему принимается
        flow.commit(preview.preview_id)
        assert env.storage.get_weighing(preview.record.uuid) is not None

    def test_commit_without_pending_raises(self, env: FlowEnv) -> None:
        """Без подготовленного превью commit невозможен."""
        with pytest.raises(ManualFlowError):
            env.make_flow().commit("anything")


# --- discard() ---


class TestDiscard:
    def test_discard_removes_files_and_no_record(self, env: FlowEnv, http_camera: str) -> None:
        """Отмена: файлы снимков удалены, записи в журнале нет, превью очищено."""
        flow = env.make_flow(two_cameras(http_camera))
        preview = prepare_weighing(flow)
        paths = [Path(photo.path) for photo in preview.photos]

        flow.discard(preview.preview_id)
        assert flow.pending() is None
        assert all(not path.exists() for path in paths)
        assert env.storage.get_weighing(preview.record.uuid) is None

    def test_discard_wrong_id_is_noop(self, env: FlowEnv, http_camera: str) -> None:
        """Чужой id: превью и файлы не трогаются (отмена идемпотентна)."""
        flow = env.make_flow(two_cameras(http_camera))
        preview = prepare_weighing(flow)
        flow.discard("stale-or-forged-id")
        assert flow.pending() is preview
        assert all(Path(photo.path).exists() for photo in preview.photos)

    def test_discard_after_commit_is_noop(self, env: FlowEnv, http_camera: str) -> None:
        """Отмена после подтверждения ничего не делает: запись и файлы живы."""
        flow = env.make_flow(two_cameras(http_camera))
        preview = prepare_weighing(flow)
        flow.commit(preview.preview_id)

        flow.discard(preview.preview_id)
        assert env.storage.get_weighing(preview.record.uuid) is not None
        assert all(Path(photo.path).exists() for photo in preview.photos)


def test_commit_survives_storage_failure(env: FlowEnv, monkeypatch: pytest.MonkeyPatch) -> None:
    """Сбой записи в БД при подтверждении не теряет превью: повторный commit проходит.

    Правило «взвешивания не теряются»: превью очищается только после
    успешного save_weighing.
    """
    flow = env.make_flow()
    preview = flow.prepare(
        Operation.WEIGHING, vehicle_number=VEHICLE, trailer_number=None, operator=OPERATOR
    )

    original_save = env.storage.save_weighing
    calls = {"n": 0}

    def failing_save(record: Any, photos: Any = ()) -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        original_save(record, photos)

    monkeypatch.setattr(env.storage, "save_weighing", failing_save)

    with pytest.raises(sqlite3.OperationalError):
        flow.commit(preview.preview_id)
    # превью не потеряно, записи в журнале нет
    pending = flow.pending()
    assert pending is not None
    assert pending.preview_id == preview.preview_id
    assert env.storage.get_weighing(preview.record.uuid) is None

    # повторный commit того же превью успешен
    record = flow.commit(preview.preview_id)
    assert record.uuid == preview.record.uuid
    assert env.storage.get_weighing(preview.record.uuid) is not None
    assert flow.pending() is None


def test_capture_and_save_writes_immediately(env: FlowEnv) -> None:
    """Одношаговая операция (как в ВесыСофт): нажатие = фиксация + запись сразу."""
    put_tare(env.storage)
    flow = env.make_flow()
    result = flow.capture_and_save(
        Operation.WEIGHING, vehicle_number=VEHICLE, trailer_number=None, operator=OPERATOR
    )
    saved = env.storage.get_weighing(result.record.uuid)
    assert saved is not None
    assert saved.massa is not None
    assert saved.netto == saved.massa - 15300.0
    assert flow.pending() is None  # подтверждать нечего — уже записано
