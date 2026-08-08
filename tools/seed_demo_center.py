"""Демо-данные центра для вёрстки и ручной отладки панели.

Создаёт (идемпотентно, только в пустой БД): три объекта с весами и
агентами (разные статусы), пользователя панели ``demo``/``demo1234``,
журнал из ~40 операций за последние двое суток, действующие тары.

    uv run python -m tools.seed_demo_center        # засеять dev-БД
    uv run uvicorn center.app:create_app --factory --port 8080
    открыть http://127.0.0.1:8080/panel/  (вход: demo / demo1234)

Только для dev-БД! На боевой базе запускаться откажется, если в ней
уже есть объекты.
"""

import random
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select

from center.db import repo
from center.db.models import Agent, AgentStatus, Camera, Scale, ScaleKind, Site, User, UserRole
from center.db.session import make_engine, make_session_factory
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord
from shared.passwords import hash_password

SITES = [
    ("kyzyl-kyia", "СВХ «Кызыл-Кыя»", "Весы SCS-80", AgentStatus.ONLINE, "0.1.0"),
    ("kant", "СВХ «КАНТ»", "Весы SCS-80 22-3", AgentStatus.OFFLINE, "0.1.0"),
    ("kara-suu", "ПЗТК «Кара-Суу»", "Весы SCS-80", AgentStatus.OFFLINE, None),
]

VEHICLES = [
    ("01KG777AAA", "01KG500AB"),
    ("28BAHE03KG", None),
    ("05KG123BBB", "05KG881AC"),
    ("T076AB40UZ", "0108BA40"),
    ("259AFX03KG", "321PE03KG"),
    ("07KG090ABB", "07KG112AB"),
]


def main() -> None:
    engine = make_engine()
    session_factory = make_session_factory(engine)
    rng = random.Random(20260808)

    with session_factory() as session:
        if session.execute(select(Site.id).limit(1)).scalar_one_or_none() is not None:
            sys.exit("БД не пуста — демо-сеятель работает только с чистой dev-БД.")

        session.add(
            User(
                login="demo",
                pw_hash=hash_password("demo1234"),
                full_name="Демо Диспетчер",
                role=UserRole.DISPATCHER,
            )
        )

        scale_ids = []
        for index, (code, name, scale_name, status, version) in enumerate(SITES):
            site = Site(code=code, name=name)
            session.add(site)
            session.flush()
            scale = Scale(
                site_id=site.id,
                name=scale_name,
                kind=ScaleKind.STATIC,
                driver="cas22",
                legacy_ip=f"192.168.150.{185 + index}",
                legacy_port=8087,
                legacy_autoscale=2 + index,
            )
            session.add(scale)
            session.flush()
            session.add_all(
                [
                    Camera(scale_id=scale.id, role=CameraRole.FRONT, rtsp_url="rtsp://demo/1"),
                    Camera(scale_id=scale.id, role=CameraRole.REAR, rtsp_url="rtsp://demo/2"),
                ]
            )
            session.add(
                Agent(
                    scale_id=scale.id,
                    token_hash=repo.hash_agent_token(f"demo-token-{code}"),
                    status=status,
                    version=version,
                    last_seen_at=datetime.now(UTC)
                    - (
                        timedelta(seconds=5)
                        if status is AgentStatus.ONLINE
                        else timedelta(minutes=42)
                    ),
                )
            )
            scale_ids.append(scale.id)
        session.commit()

        # журнал: сначала тарирования, затем взвешивания с расчётом нетто
        now = datetime.now(UTC)
        for vehicle, _ in VEHICLES:
            tare_at = now - timedelta(hours=rng.randint(24, 45))
            record = WeighingRecord(
                uuid=uuid4(),
                operation=Operation.TARING,
                code=ErrorCode.OK,
                massa=float(rng.randrange(12000, 18000, 10)),
                stable=True,
                weighed_at=tare_at,
                vehicle_number=vehicle,
                source=WeighingSource.AIS,
            )
            repo.save_weighing_record(session, rng.choice(scale_ids), record)

        for _step in range(34):
            vehicle, trailer = rng.choice(VEHICLES)
            at = now - timedelta(minutes=rng.randint(5, 2700))
            scale_id = rng.choice(scale_ids)
            tare = repo.find_active_tare(session, vehicle, now=at)
            massa = float(rng.randrange(27000, 44000, 10))
            source = WeighingSource.AIS if rng.random() < 0.8 else WeighingSource.LOCAL_OFFLINE
            code = ErrorCode.OK
            record = WeighingRecord(
                uuid=uuid4(),
                operation=Operation.WEIGHING,
                code=code,
                massa=massa,
                stable=True,
                weighed_at=at,
                vehicle_number=vehicle,
                trailer_number=trailer,
                tare_value=tare.tare_value if tare else None,
                tare_weighing_uuid=tare.weighing_uuid if tare else None,
                netto=(massa - tare.tare_value) if tare else None,
                source=source,
                operator="А. Осмонов" if source is WeighingSource.LOCAL_OFFLINE else None,
                message=None,
            )
            repo.save_weighing_record(session, scale_id, record)

    print("Демо-данные засеяны. Панель: http://127.0.0.1:8080/panel/  (demo / demo1234)")


if __name__ == "__main__":
    main()
