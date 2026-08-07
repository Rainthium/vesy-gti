"""Запуск интерфейса оператора с демо-данными — для вёрстки и ручной отладки.

Не трогает реальное железо: вместо драйвера и камер — фейковые сервисы
с правдоподобными данными (вес «дышит», журнал как на макетах).

Запуск:
    uv run python -m tools.dev_operator_ui [--port 8077] [--offline] [--no-data]

Вход: логин ``operator`` / пароль ``operator``.
"""

import argparse
import math
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import uvicorn

from agent.cameras.capture import CameraShot
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage
from agent.web.app import create_app
from agent.web.services import AgentInfo
from agent.weighing.manual import ManualOperationFlow, ManualPreview
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord, WeighingRecord

# однотонный серый JPEG 32×24 — заглушка кадра камеры
_GRAY_JPEG = bytes.fromhex(
    "ffd8ffe000104a46494600010100004800480000ffe1004c4578696600004d4d002a00000008"
    "000187690004000000010000001a000000000003a00100030000000100010000a00200040000"
    "000100000040a0030004000000010000003000000000ffed003850686f746f73686f7020332e"
    "30003842494d04040000000000003842494d0425000000000010d41d8cd98f00b204e9800998"
    "ecf8427effc00011080030004003012200021101031101ffc4001f0000010501010101010100"
    "000000000000000102030405060708090a0bffc400b510000201030302040305050404000001"
    "7d01020300041105122131410613516107227114328191a1082342b1c11552d1f02433627282"
    "090a161718191a25262728292a3435363738393a434445464748494a535455565758595a6364"
    "65666768696a737475767778797a838485868788898a92939495969798999aa2a3a4a5a6a7a8"
    "a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae1e2e3e4e5e6e7e8e9"
    "eaf1f2f3f4f5f6f7f8f9faffc4001f0100030101010101010101010000000000000102030405"
    "060708090a0bffc400b511000201020404030407050404000102770001020311040521310612"
    "41510761711322328108144291a1b1c109233352f0156272d10a162434e125f11718191a2627"
    "28292a35363738393a434445464748494a535455565758595a636465666768696a7374757677"
    "78797a82838485868788898a92939495969798999aa2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9"
    "bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae2e3e4e5e6e7e8e9eaf2f3f4f5f6f7f8f9faff"
    "db00430001010101010102010102030202020304030303030406040404040406070606060606"
    "06070707070707070708080808080809090909090b0b0b0b0b0b0b0b0b0bffdb004301020202"
    "030303050303050b0806080b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b"
    "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0bffdd00040004ffda000c0301000211"
    "0311003f00feb828a28aec39c28a28a0028a28a0028a28a00fffd0feb828a28aec39c28a28a0"
    "028a28a0028a28a00fffd1feb828a28aec39c28a28a0028a28a0028a28a00fffd9"
)


def _demo_journal() -> list[WeighingRecord]:
    """Журнал, повторяющий данные макета operator-main."""
    now = datetime.now(UTC)
    rows = [
        (
            "01KG777AAA",
            "01KG500AB",
            Operation.WEIGHING,
            43310.0,
            15300.0,
            28010.0,
            WeighingSource.AIS,
        ),
        ("28BAHE03KG", None, Operation.TARING, 14890.0, None, None, WeighingSource.AIS),
        (
            "05KG123BBB",
            "05KG881AC",
            Operation.WEIGHING,
            38050.0,
            12400.0,
            25650.0,
            WeighingSource.LOCAL_OFFLINE,
        ),
        ("01KG254AEA", None, Operation.WEIGHING, 27480.0, None, None, WeighingSource.AIS),
        (
            "07KG090ABB",
            "07KG112AB",
            Operation.WEIGHING,
            41205.0,
            15020.0,
            26185.0,
            WeighingSource.AIS,
        ),
        ("B455UBM01", None, Operation.TARING, 13750.0, None, None, WeighingSource.AIS),
    ]
    records = []
    for i, (vehicle, trailer, op, massa, tare, netto, source) in enumerate(rows):
        records.append(
            WeighingRecord(
                uuid=uuid4(),
                operation=op,
                code=ErrorCode.OK,
                massa=massa,
                stable=True,
                weighed_at=now - timedelta(minutes=11 + i * 19),
                vehicle_number=vehicle,
                trailer_number=trailer,
                tare_value=tare,
                netto=netto,
                source=source,
                operator="А. Осмонов" if source is WeighingSource.LOCAL_OFFLINE else None,
            )
        )
    return records


class DemoServices:
    """Фейковые сервисы с живым «дыханием» веса."""

    def __init__(self, *, offline: bool, no_data: bool) -> None:
        self._offline = offline
        self._no_data = no_data
        self._journal = _demo_journal()
        # настоящий поток ручной операции поверх БД в памяти (камер в демо нет)
        self._storage = AgentStorage(":memory:")
        self._storage.replace_tare_registry(
            [
                TareRecord(
                    vehicle_number="01KG777AAA",
                    tare_value=15300.0,
                    tared_at=datetime(2026, 6, 12, tzinfo=UTC),
                    weighing_uuid=uuid4(),
                )
            ]
        )
        self._flow = ManualOperationFlow(
            scale_state=self.scale_state,
            manual_allowed=lambda: self._offline,
            storage=self._storage,
            cameras=[],
            photos_dir=Path(tempfile.mkdtemp(prefix="ves-demo-photos-")),
        )
        self.info = AgentInfo(
            site_name="СВХ «Кызыл-Кыя»",
            scale_name="Весы SCS-80",
            indicator_model="CAS CI-201A",
            driver_name="cas22 · пакет 22 байта",
            port_label="COM3 · 9600 · 8-N-1",
            agent_version="0.1.0 (dev)",
            center_url="wss://ves.gti.kg",
        )

    def scale_state(self) -> ScaleState:
        if self._no_data:
            return ScaleState(status=ScaleStatus.NO_DATA, error="поток отсутствует 45 сек")
        # каждые ~20 секунд «заезжает машина»: рост, качание, стабилизация
        phase = (time.monotonic() % 20.0) / 20.0
        if phase < 0.3:
            weight, stable = 0.0, True
        elif phase < 0.5:
            weight, stable = 43310.0 * (phase - 0.3) / 0.2, False
        elif phase < 0.7:
            weight = 43310.0 + 40 * math.sin(phase * 80)
            stable = False
        else:
            weight, stable = 43310.0, True
        return ScaleState(
            status=ScaleStatus.OK,
            weight_kg=round(weight / 10) * 10,
            stable=stable,
            last_packet_at=time.monotonic(),
        )

    def center_connected(self) -> bool:
        return not self._offline

    def pending_count(self) -> int:
        return 3 if self._offline else 0

    def tare_registry_size(self) -> int:
        return 1812

    def recent_weighings(self, limit: int = 50) -> list[tuple[WeighingRecord, bool]]:
        # сначала записи, сделанные вручную в демо, затем статичный журнал макета;
        # офлайн-записи показываются как недосланные (⧗)
        saved = self._storage.recent_weighings_synced(limit)
        demo = [(r, r.source is not WeighingSource.LOCAL_OFFLINE) for r in self._journal]
        return (saved + demo)[:limit]

    def manual_ready(self) -> bool:
        return self._flow.ready()

    def manual_capture(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        return self._flow.capture_and_save(
            operation,
            vehicle_number=vehicle_number,
            trailer_number=trailer_number,
            operator=operator,
        )

    def find_active_tare(self, vehicle_number: str) -> TareRecord | None:
        return self._storage.find_active_tare(vehicle_number, datetime.now(UTC))

    def camera_roles(self) -> list[CameraRole]:
        return [CameraRole.FRONT, CameraRole.REAR]

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        return CameraShot(role=role, jpeg=_GRAY_JPEG, captured_at=datetime.now(UTC))

    def verify_operator(self, login: str, password: str) -> str | None:
        return "А. Осмонов" if (login, password) == ("operator", "operator") else None

    def reopen_port(self) -> None:
        print("демо: переоткрытие порта")


def main() -> None:
    parser = argparse.ArgumentParser(description="Интерфейс оператора с демо-данными")
    parser.add_argument("--port", type=int, default=8077)
    parser.add_argument(
        "--offline", action="store_true", help="автономный режим (центр недоступен)"
    )
    parser.add_argument("--no-data", action="store_true", help="нет данных с индикатора")
    args = parser.parse_args()

    services = DemoServices(offline=args.offline, no_data=args.no_data)
    app = create_app(services, session_secret="dev-only-secret")
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
