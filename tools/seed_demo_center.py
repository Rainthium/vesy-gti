"""Демо-данные центра для вёрстки и ручной отладки панели.

Создаёт (идемпотентно, только в пустой БД): три объекта с весами и
агентами (разные статусы), пользователя панели ``demo``/``demo1234``,
журнал из ~40 операций за последние двое суток, действующие тары.

    uv run python -m tools.seed_demo_center        # засеять dev-БД
    uv run uvicorn center.app:create_app --factory --port 8080
    открыть http://127.0.0.1:8080/panel/  (вход: demo / demo1234)

История для экрана «Отчёты» (этап 4): ``--history 90`` досеивает журнал
за N последних дней (будни/выходные, тарирования — в том числе устаревшие,
ручные операции с операторами, номера АИС у части офлайн-операций,
события мониторинга, отказы АИС в audit_log). На пустой БД сперва идёт
базовый посев; на уже засеянной demo-БД (есть пользователь ``demo``) —
только история. Повторный запуск добавит ещё одну порцию.

Только для dev-БД! На боевой базе запускаться откажется: базовый посев —
если в ней уже есть объекты, история — если нет пользователя ``demo``.
"""

import argparse
import random
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from center.db import repo
from center.db.models import (
    Agent,
    AgentStatus,
    AuditLog,
    Camera,
    MonitoringEvent,
    MonitoringSeverity,
    Scale,
    ScaleKind,
    Site,
    User,
    UserRole,
    Weighing,
    WeighingAisRef,
)
from center.db.session import make_engine, make_session_factory
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import WeighingRecord
from shared.passwords import hash_password
from shared.tare import three_months_before

BISHKEK = ZoneInfo("Asia/Bishkek")

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

# история: базовый суточный поток по объектам (взвешиваний в будний день)
HISTORY_DAILY = {"kyzyl-kyia": 14, "kant": 10, "kara-suu": 7}
WEEKDAY_FACTOR = (1.0, 1.0, 1.05, 1.0, 0.95, 0.55, 0.3)  # пн … вс
OPERATORS = ("А. Осмонов", "Б. Джумабаев", "d.ivanov")
REFUSAL_CODES = (
    ErrorCode.ERR_UNSTABLE,
    ErrorCode.ERR_VEHICLE_TIMEOUT,
    ErrorCode.ERR_BUSY,
    ErrorCode.ERR_SCALE_OFFLINE,
)


def seed_base(session: Session, rng: random.Random) -> list[int]:
    """Объекты, весы, камеры, агенты, пользователь demo и короткий журнал."""
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
            # привязка v2 (контракт 17.08.2026): демо-идентификаторы СВХ
            ais_object=f"{9000 + index:04d}",
            ais_scale_no=1,
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
                - (timedelta(seconds=5) if status is AgentStatus.ONLINE else timedelta(minutes=42)),
            )
        )
        scale_ids.append(scale.id)
    session.commit()

    # журнал: сначала тарирования, затем взвешивания с расчётом нетто
    now = datetime.now(UTC)
    for vehicle, trailer in VEHICLES:
        tare_at = now - timedelta(hours=rng.randint(24, 45))
        record = WeighingRecord(
            uuid=uuid4(),
            operation=Operation.TARING,
            code=ErrorCode.OK,
            massa=float(rng.randrange(12000, 18000, 10)),
            stable=True,
            weighed_at=tare_at,
            vehicle_number=vehicle,
            trailer_number=trailer,  # тара привязана к сцепке (правило №4)
            source=WeighingSource.AIS,
        )
        repo.save_weighing_record(session, rng.choice(scale_ids), record)

    for _step in range(34):
        vehicle, trailer = rng.choice(VEHICLES)
        at = now - timedelta(minutes=rng.randint(5, 2700))
        scale_id = rng.choice(scale_ids)
        tare = repo.find_active_tare(session, vehicle, trailer, now=at)
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
    return scale_ids


def _plate(rng: random.Random, regions: tuple[str, ...], letters: str, tail: int) -> str:
    """Демо-номер вида 01KG123ABC (tail — сколько букв в хвосте)."""
    suffix = "".join(rng.choice(letters) for _ in range(tail))
    return f"{rng.choice(regions)}KG{rng.randint(100, 999)}{suffix}"


def _history_vehicles(rng: random.Random) -> list[tuple[str, str | None]]:
    """Парк для истории: ~40 сцепок, часть без прицепа."""
    fleet: list[tuple[str, str | None]] = list(VEHICLES)
    regions = ("01", "02", "03", "05", "07", "08", "09")
    letters = "ABCEHKMOPTXY"
    while len(fleet) < 40:
        head = _plate(rng, regions, letters, 3)
        trailer = None if rng.random() < 0.3 else _plate(rng, regions, letters, 2)
        fleet.append((head, trailer))
    return fleet


def _tare_as_of(
    session: Session, vehicle: str, trailer: str | None, moment: datetime
) -> tuple[float, UUID] | None:
    """Действующая на ``moment`` тара сцепки: (масса, uuid тарирования) или None."""
    row = repo.latest_taring_as_of(session, vehicle, trailer, moment)
    if row is None or row.weighed_at is None or row.massa is None:
        return None
    if row.weighed_at < three_months_before(moment):
        return None
    return float(row.massa), row.uuid


def seed_history(session: Session, rng: random.Random, days: int) -> int:
    """Досеять журнал за ``days`` последних дней; вернуть число операций."""
    scales = list(
        session.execute(
            select(Scale, Site).join(Site, Site.id == Scale.site_id).order_by(Scale.id)
        ).all()
    )
    if not scales:
        sys.exit("В БД нет весов — сперва базовый посев.")
    scale_by_code = {site.code: scale for scale, site in scales}
    fleet = _history_vehicles(rng)
    now = datetime.now(UTC)
    start_day = (now.astimezone(BISHKEK) - timedelta(days=days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    total = 0

    def add(record: WeighingRecord, scale_id: int) -> None:
        nonlocal total
        repo.save_weighing_record(session, scale_id, record)
        total += 1

    # тарирования до начала истории: у 70 % парка есть тара, у трети из них — устаревшая
    for vehicle, trailer in fleet:
        roll = rng.random()
        if roll < 0.3:
            continue  # без тарирования — взвешивания пойдут без нетто
        if roll < 0.55:
            tare_at = start_day - timedelta(days=rng.randint(100, 160))  # устареет
        else:
            tare_at = start_day - timedelta(days=rng.randint(1, 60))
        add(
            WeighingRecord(
                uuid=uuid4(),
                operation=Operation.TARING,
                code=ErrorCode.OK,
                massa=float(rng.randrange(9000, 18000, 10)),
                stable=True,
                weighed_at=tare_at.astimezone(UTC),
                vehicle_number=vehicle,
                trailer_number=trailer,
                source=WeighingSource.AIS,
            ),
            rng.choice(list(scale_by_code.values())).id,
        )

    for day_index in range(days):
        day = start_day + timedelta(days=day_index)
        factor = WEEKDAY_FACTOR[day.weekday()]
        for code, base in HISTORY_DAILY.items():
            scale = scale_by_code.get(code)
            if scale is None:
                continue
            count = max(0, round(base * factor * rng.uniform(0.7, 1.3)))
            for _ in range(count):
                at = day + timedelta(minutes=rng.randint(7 * 60, 21 * 60))
                if at > now.astimezone(BISHKEK):
                    continue
                at_utc = at.astimezone(UTC)
                # изредка сцепку перетаривают прямо в потоке
                if rng.random() < 0.03:
                    vehicle, trailer = rng.choice(fleet)
                    add(
                        WeighingRecord(
                            uuid=uuid4(),
                            operation=Operation.TARING,
                            code=ErrorCode.OK,
                            massa=float(rng.randrange(9000, 18000, 10)),
                            stable=True,
                            weighed_at=at_utc,
                            vehicle_number=vehicle,
                            trailer_number=trailer,
                            source=WeighingSource.AIS,
                        ),
                        scale.id,
                    )
                    continue
                # отказ команды АИС — только след в audit_log (записи нет)
                if rng.random() < 0.03:
                    session.add(
                        AuditLog(
                            actor="ais:demo",
                            action="weigh_request_v2",
                            at=at_utc,
                            details={
                                "code": rng.choice(REFUSAL_CODES).value,
                                "scale_id": scale.id,
                                "request": {},
                            },
                        )
                    )
                    continue
                vehicle, trailer = rng.choice(fleet)
                if rng.random() < 0.02:
                    vehicle, trailer = "", None  # номер не передан
                # тара «на тот момент» — по журналу тарирований не позже момента
                # (реестр помнит только последнее, в т.ч. из «будущего» базового посева)
                tare = _tare_as_of(session, vehicle, trailer, at_utc) if vehicle else None
                massa = float(rng.randrange(24000, 46000, 10))
                offline = rng.random() < 0.08
                record = WeighingRecord(
                    uuid=uuid4(),
                    operation=Operation.WEIGHING,
                    code=ErrorCode.OK,
                    massa=massa,
                    stable=True,
                    weighed_at=at_utc,
                    vehicle_number=vehicle or None,
                    trailer_number=trailer,
                    tare_value=tare[0] if tare else None,
                    tare_weighing_uuid=tare[1] if tare else None,
                    netto=(massa - tare[0]) if tare else None,
                    source=WeighingSource.LOCAL_OFFLINE if offline else WeighingSource.AIS,
                    operator=rng.choice(OPERATORS) if offline else None,
                )
                add(record, scale.id)
                if offline and rng.random() < 0.6:
                    # АИС уже сообщила номер документа по офлайн-операции
                    session.flush()
                    weighing_id = session.execute(
                        select(Weighing.id).where(Weighing.uuid == record.uuid)
                    ).scalar_one()
                    session.add(
                        WeighingAisRef(
                            weighing_id=weighing_id,
                            ais_ref=f"WEI{rng.randint(10**8, 10**9 - 1)}",
                            origin="callback",
                        )
                    )
            session.commit()

        # события мониторинга: изредка офлайн на 20–180 минут, индикатор, камеры
        for code, scale in scale_by_code.items():
            if rng.random() < 0.12:
                since = day + timedelta(minutes=rng.randint(6 * 60, 22 * 60))
                length = timedelta(minutes=rng.randint(20, 180))
                session.add_all(
                    [
                        MonitoringEvent(
                            scale_id=scale.id,
                            kind="offline",
                            severity=MonitoringSeverity.DANGER,
                            message=f"{code}: агент офлайн",
                            created_at=since.astimezone(UTC),
                        ),
                        MonitoringEvent(
                            scale_id=scale.id,
                            kind="offline",
                            severity=MonitoringSeverity.OK,
                            message=f"{code}: агент снова на связи",
                            created_at=(since + length).astimezone(UTC),
                        ),
                    ]
                )
            if rng.random() < 0.06:
                kind = rng.choice(("no_data", "camera_front", "camera_rear"))
                session.add(
                    MonitoringEvent(
                        scale_id=scale.id,
                        kind=kind,
                        severity=MonitoringSeverity.WARNING
                        if kind.startswith("camera")
                        else MonitoringSeverity.DANGER,
                        message=f"{code}: {kind}",
                        created_at=(day + timedelta(hours=rng.randint(7, 20))).astimezone(UTC),
                    )
                )
        session.commit()
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Демо-посев dev-БД центра")
    parser.add_argument(
        "--history",
        type=int,
        metavar="DAYS",
        help="досеять историю операций за N дней (для экрана «Отчёты»)",
    )
    args = parser.parse_args()

    engine = make_engine()
    session_factory = make_session_factory(engine)
    rng = random.Random(20260808)

    with session_factory() as session:
        has_sites = session.execute(select(Site.id).limit(1)).scalar_one_or_none() is not None
        if not has_sites:
            seed_base(session, rng)
            print("Демо-данные засеяны. Панель: http://127.0.0.1:8080/panel/  (demo / demo1234)")
        elif args.history is None:
            sys.exit("БД не пуста — демо-сеятель работает только с чистой dev-БД.")
        if args.history is not None:
            is_demo = (
                session.execute(select(User.id).where(User.login == "demo")).scalar_one_or_none()
                is not None
            )
            if not is_demo:
                sys.exit("История сеется только в demo-БД (нет пользователя demo).")
            history_rng = random.Random(f"history-{datetime.now(UTC).date().isoformat()}")
            total = seed_history(session, history_rng, max(1, args.history))
            print(f"История досеяна: {total} операций за {args.history} дней.")


if __name__ == "__main__":
    main()
