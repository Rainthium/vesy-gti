"""Сквозной тест этапа 1: эмулятор весов → агент → центр → запрос «как от АИС».

Все компоненты настоящие, как на пилоте:
- индикатор — эмулятор CAS 22 byte (tools/cas22_emulator) на TCP-порту,
  крутит цикл «пусто → заезд → стабильный вес → съезд»;
- агент — Cas22Driver через pyserial socket://, AutoOperationRunner
  (автомат цикла + снимки с двух HTTP-камер), AgentStorage (SQLite),
  CenterClient (WebSocket) и PhotoUploader (HTTP);
- центр — create_app() из переменных окружения (как в docker), настоящий
  uvicorn на локальном порту, временный PostgreSQL с миграциями alembic;
- АИС — POST /api/v1/weigh с учёткой v1 (как tools/ais_client), затем
  скачивание фото по ссылкам из ответа с сервисным токеном.

Сценарий: тарирование (8 000 кг) → реестр тар доезжает до реплики агента
→ взвешивание (20 000 кг) → ответ с tare/netto по правилу №4 → файлы фото
догружаются и скачиваются байт-в-байт.

Тест небыстрый (~15–25 с реального времени): эмулятор шлёт пакеты
в реальном темпе, циклы взвешивания проживаются целиком.
"""

import asyncio
import http.server
import io
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import uvicorn
from PIL import Image
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from agent.cameras.capture import CameraConfig
from agent.drivers.cas22 import Cas22Driver
from agent.sync.photo_uploader import PhotoUploader
from agent.sync.storage import AgentStorage
from agent.sync.ws_client import CenterClient, ClientConfig
from agent.weighing.auto import AutoConfig, AutoOperationRunner
from agent.weighing.cycle import CycleConfig
from agent.weighing.watcher import ScaleWatcher
from center.app import create_app
from center.db import repo
from center.db.models import Agent, Scale, ScaleKind, Site, Weighing
from center.db.session import database_url
from shared.enums import CameraRole, ScaleStatus, WeighingSource
from shared.messages import EquipmentStatus
from tests.test_center_db import _upgrade_head
from tools.cas22_emulator import (
    PacketBuilder,
    Step,
    drive_off,
    drive_on,
    empty_scale,
    serve,
    stable_weight,
)

AGENT_TOKEN = "e2e-agent-token-" + "x" * 16
AIS_TOKEN = "e2e-ais-photo-token"
V1_USERNAME = "ais-e2e"
V1_PASSWORD = "e2e-v1-secret"
LEGACY_IP = "192.168.150.185"
LEGACY_PORT = 8087
LEGACY_AUTOSCALE = 2
VEHICLE = "01KG777AAA"

TARE_KG = 8000.0
BRUTTO_KG = 20000.0

EMULATOR_RATE = 25.0  # пакетов/с — быстрее реального индикатора, тест короче

# снимки «камер»: настоящие JPEG разного содержимого, чтобы сверить байты


def _jpeg(color: str) -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (64, 48), color).save(buffer, "JPEG", quality=90)
    return buffer.getvalue()


FRONT_JPEG = _jpeg("red")
REAR_JPEG = _jpeg("blue")


# ---------------------------------------------------------------------------
# Инфраструктура: временная БД, HTTP-камеры, свободные порты
# ---------------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="module")
def e2e_db_url() -> Iterator[URL]:
    """Одноразовая БД ves_test_e2e_<pid> с миграциями alembic."""
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip("PostgreSQL недоступен — сквозной тест пропущен")

    db_name = f"ves_test_e2e_{os.getpid()}"
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    test_url = admin_url.set(database=db_name)
    _upgrade_head(test_url)
    yield test_url
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
    admin_engine.dispose()


@pytest.fixture()
def camera_server() -> Iterator[str]:
    """Локальный HTTP-сервер «камер»: /front и /rear отдают разные JPEG."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = {"/front": FRONT_JPEG, "/rear": REAR_JPEG}.get(self.path)
            if body is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "image/jpeg")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    thread.join(timeout=5)


def _seed_center(factory: sessionmaker[Session]) -> int:
    """Объект + весы с legacy-маршрутом + агент с токеном; вернуть scale_id."""
    with factory() as session:
        site = Site(code="e2e-site", name="Сквозной объект")
        session.add(site)
        session.flush()
        scale = Scale(
            site_id=site.id,
            name="Весы e2e",
            kind=ScaleKind.STATIC,
            driver="cas22",
            legacy_ip=LEGACY_IP,
            legacy_port=LEGACY_PORT,
            legacy_autoscale=LEGACY_AUTOSCALE,
        )
        session.add(scale)
        session.flush()
        session.add(Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(AGENT_TOKEN)))
        scale_id = scale.id
        session.commit()
    return scale_id


def _post_v1(base_url: str, operation: str) -> dict[str, Any]:
    """Запрос «как от АИС» (тот же формат, что шлёт tools/ais_client)."""
    payload = {
        "ip_address": LEGACY_IP,
        "port": LEGACY_PORT,
        "username": V1_USERNAME,
        "password": V1_PASSWORD,
        "autoscale": LEGACY_AUTOSCALE,
        "operation": operation,
        "vehicle_number": VEHICLE,
    }
    request = urllib.request.Request(
        f"{base_url}/api/v1/weigh",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        return dict(json.loads(response.read()))


def _get_photo(base_url: str, path: str) -> tuple[int, bytes]:
    request = urllib.request.Request(
        f"{base_url}{path}", headers={"Authorization": f"Bearer {AIS_TOKEN}"}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return int(response.status), bytes(response.read())
    except urllib.error.HTTPError as error:
        return int(error.code), b""


async def _wait_for(
    predicate: Callable[[], bool], *, timeout_s: float, what: str, interval_s: float = 0.2
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval_s)
    raise AssertionError(f"не дождались за {timeout_s} с: {what}")


# ---------------------------------------------------------------------------
# Сам сквозной сценарий
# ---------------------------------------------------------------------------


def test_full_chain_emulator_agent_center_ais(
    e2e_db_url: URL,
    camera_server: str,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    center_port = _free_port()
    emulator_port = _free_port()

    # центр конфигурируется так же, как в docker, — через окружение
    monkeypatch.setenv("DATABASE_URL", e2e_db_url.render_as_string(hide_password=False))
    monkeypatch.setenv("PHOTOS_DIR", str(tmp_path / "center_photos"))
    monkeypatch.setenv("AIS_PHOTO_TOKEN", AIS_TOKEN)
    monkeypatch.setenv("V1_USERNAME", V1_USERNAME)
    monkeypatch.setenv("V1_PASSWORD", V1_PASSWORD)
    monkeypatch.setenv("V1_WEIGH_TIMEOUT_S", "60")

    engine = create_engine(e2e_db_url, poolclass=NullPool)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    _seed_center(factory)
    base_url = f"http://127.0.0.1:{center_port}"

    # эмулятор крутит цикл; целевой вес меняется между операциями
    target = {"kg": TARE_KG}

    def scenario() -> Iterator[Step]:
        builder = PacketBuilder()
        weight = target["kg"]
        yield from empty_scale(builder, 0.8, EMULATOR_RATE)
        yield from drive_on(builder, weight, 0.5, EMULATOR_RATE)
        # окно стабильности с запасом: команда шлётся по готовой фиксации
        # наблюдателя и должна успеть отработать до съезда
        yield from stable_weight(builder, weight, 4.0, EMULATOR_RATE)
        yield from drive_off(builder, weight, 0.4, EMULATOR_RATE)

    async def run_scenario() -> None:
        app = create_app()
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=center_port, log_level="warning")
        )
        server_task = asyncio.create_task(server.serve())
        emulator_task = asyncio.create_task(
            serve("127.0.0.1", emulator_port, scenario, loop_forever=True)
        )
        await _wait_for(lambda: server.started, timeout_s=10, what="старт uvicorn центра")

        # --- агент, как он собран на весовом ПК ---
        driver = Cas22Driver(f"socket://127.0.0.1:{emulator_port}")
        driver.start()
        storage = AgentStorage(tmp_path / "agent.sqlite3")
        cycle_config = CycleConfig(
            zero_threshold_kg=200.0,
            vehicle_threshold_kg=500.0,
            zero_timeout_s=15.0,
            vehicle_timeout_s=15.0,
            stable_duration_s=0.5,
            stable_timeout_s=15.0,
            no_data_timeout_s=3.0,
        )
        # наблюдатель платформы — как в боевом main.py: тикается фоном,
        # команды срабатывают по его готовой фиксации
        watcher = ScaleWatcher(cycle_config)

        async def tick_watcher() -> None:
            while True:
                watcher.tick(driver.state)
                await asyncio.sleep(0.05)

        watcher_task = asyncio.create_task(tick_watcher())
        runner = AutoOperationRunner(
            scale_state=lambda: driver.state,
            watcher=watcher,
            storage=storage,
            cameras=[
                CameraConfig(role=CameraRole.FRONT, snapshot_url=f"{camera_server}/front"),
                CameraConfig(role=CameraRole.REAR, snapshot_url=f"{camera_server}/rear"),
            ],
            photos_dir=tmp_path / "agent_photos",
            config=AutoConfig(cycle=cycle_config, tick_interval_s=0.05),
        )

        def equipment() -> EquipmentStatus:
            state = driver.state
            return EquipmentStatus(
                scale_status=state.status,
                current_weight=state.weight_kg,
                stable=state.stable,
                pending_sync_count=storage.pending_count(),
            )

        client = CenterClient(
            ClientConfig(
                url=f"ws://127.0.0.1:{center_port}/agents/ws",
                token=AGENT_TOKEN,
                agent_id="e2e-agent",
                version="e2e",
                driver="cas22",
                heartbeat_interval_s=0.3,
            ),
            storage,
            equipment_status=equipment,
            on_weigh_request=runner.handle,
        )
        client_task = asyncio.create_task(client.run())
        uploader = PhotoUploader(storage, base_url=base_url, token=AGENT_TOKEN, interval_s=0.3)
        uploader_task = asyncio.create_task(uploader.run())

        try:
            await _wait_for(lambda: client.connected, timeout_s=10, what="агент онлайн")
            await _wait_for(
                lambda: driver.state.status is ScaleStatus.OK,
                timeout_s=10,
                what="поток данных с эмулятора",
            )

            # --- 1. тарирование: АИС фиксирует тару 8 000 кг ---
            # команда заезда не ждёт (решение 10.08.2026): шлём её, когда
            # машина стоит на платформе с готовой фиксацией наблюдателя
            await _wait_for(
                lambda: watcher.fixation is not None,
                timeout_s=15,
                what="фиксация тары на платформе",
            )
            taring = await asyncio.to_thread(_post_v1, base_url, "taring")
            assert taring["code"] == "OK", taring
            assert taring["massa"] == TARE_KG
            assert taring["unit_meas"] == "kg"
            assert str(taring["weighing_datetime"]).endswith("+06:00")
            assert "tare" not in taring  # тарирование — без полей тары
            assert taring["front_image"] and taring["rear_image"]

            # реестр тар центра доезжает до локальной реплики агента
            await _wait_for(
                lambda: storage.find_active_tare(VEHICLE, datetime.now(UTC)) is not None,
                timeout_s=10,
                what="реплика реестра тарирований на агенте",
            )

            # --- 2. взвешивание: брутто 20 000, нетто по правилу №4 ---
            target["kg"] = BRUTTO_KG
            await _wait_for(
                lambda: (fx := watcher.fixation) is not None and fx.weight_kg == BRUTTO_KG,
                timeout_s=15,
                what="фиксация брутто на платформе",
            )
            weighing = await asyncio.to_thread(_post_v1, base_url, "weighing")
            assert weighing["code"] == "OK", weighing
            assert weighing["massa"] == BRUTTO_KG
            assert weighing["tare"] == TARE_KG
            assert weighing["netto"] == BRUTTO_KG - TARE_KG
            front_path = weighing["front_image"]
            rear_path = weighing["rear_image"]
            assert front_path.startswith("/vesy/") and front_path.endswith("_photo1.jpeg")
            assert rear_path.endswith("_photo2.jpeg")

            # --- 3. файлы фото догружаются агентом и скачиваются «АИС» ---
            # (urllib — только через to_thread: uvicorn живёт в этом же loop'е)
            deadline = time.monotonic() + 20
            while True:
                status, front_bytes = await asyncio.to_thread(_get_photo, base_url, front_path)
                if status == 200:
                    break
                assert time.monotonic() < deadline, "не дождались догрузки файлов фото в центр"
                await asyncio.sleep(0.3)
            rear_status, rear_bytes = await asyncio.to_thread(_get_photo, base_url, rear_path)
            assert rear_status == 200

            # правило №2: в центре лежит байт-в-байт то, что зафиксировал агент
            record_uuid = UUID(hex=Path(front_path).name.split("_")[0])
            local = {p.role: p for p in storage.photos_for(record_uuid)}
            assert front_bytes == Path(local[CameraRole.FRONT].path).read_bytes()
            assert rear_bytes == Path(local[CameraRole.REAR].path).read_bytes()
            # оверлей прожжён при фиксации: кадр отличается от исходного
            # с камеры, но остался JPEG родного разрешения
            assert front_bytes != FRONT_JPEG and rear_bytes != REAR_JPEG
            assert Image.open(io.BytesIO(front_bytes)).size == (64, 48)

            # --- 4. журнал центра: обе операции источника «АИС» ---
            with factory() as session:
                rows = (
                    session.execute(select(Weighing).order_by(Weighing.created_at)).scalars().all()
                )
                assert len(rows) == 2
                assert all(row.source is WeighingSource.AIS for row in rows)
                assert rows[1].netto == BRUTTO_KG - TARE_KG
                assert rows[1].tare_weighing_id == rows[0].id
        finally:
            # драйвер первым: обработчик эмулятора увидит разрыв соединения
            await asyncio.to_thread(driver.stop)
            for task in (client_task, uploader_task, emulator_task, watcher_task):
                task.cancel()
            await asyncio.wait_for(
                asyncio.gather(
                    client_task, uploader_task, emulator_task, watcher_task, return_exceptions=True
                ),
                timeout=15,
            )
            storage.close()
            server.should_exit = True
            await asyncio.wait_for(server_task, timeout=10)
            engine.dispose()

    asyncio.run(asyncio.wait_for(run_scenario(), timeout=120))
