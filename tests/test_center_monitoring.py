"""Тесты мониторинга центра (center/monitoring.py, этап 2, 13.08.2026).

MonitoringService: детекторы с антидребезгом (проблема должна продержаться
порог, прежде чем стать алертом), события на переходах, заморозка детекторов
оборудования у офлайн-агента, разграничение активных алертов по объекту.
TelegramNotifier: доставка по порядку, отметка notified_at по одному,
недоступный Telegram не теряет события, окно доставки отсекает древность.

Часы детекторов — фейковые (now-колбэк), БД настоящая (как в остальных
тестах центра: одноразовая PostgreSQL с миграциями).
"""

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, create_engine, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from center.agents_ws.hub import AgentHub
from center.db import repo
from center.db.models import (
    Agent,
    AgentStatus,
    MonitoringEvent,
    MonitoringSeverity,
    Scale,
    ScaleKind,
    Site,
)
from center.db.session import database_url, make_session_factory
from center.monitoring import MonitoringService, MonitoringThresholds, TelegramNotifier
from shared.enums import CameraRole, ScaleStatus
from shared.messages import CameraStatus, EquipmentStatus
from tests.test_center_db import ALL_TABLES, _upgrade_head

# ---------------------------------------------------------------------------
# Инфраструктура: временная БД + миграции (по образцу test_center_panel)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def monitoring_db_url() -> Iterator[URL]:
    admin_url = make_url(database_url())
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
    try:
        with admin_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except (OperationalError, DBAPIError):
        pytest.skip(
            "PostgreSQL недоступен (контейнер ves-postgres не запущен?) — "
            "тесты мониторинга пропущены"
        )

    db_name = f"ves_test_monitoring_{os.getpid()}"
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    test_url = admin_url.set(database=db_name)
    _upgrade_head(test_url)
    yield test_url
    with admin_engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
    admin_engine.dispose()


@pytest.fixture(scope="session")
def monitoring_db_engine(monitoring_db_url: URL) -> Iterator[Engine]:
    engine = create_engine(monitoring_db_url, poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def db(monitoring_db_engine: Engine) -> Iterator[sessionmaker[Session]]:
    with monitoring_db_engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(ALL_TABLES)} RESTART IDENTITY CASCADE"))
    yield make_session_factory(monitoring_db_engine)


# ---------------------------------------------------------------------------
# Посев и часы
# ---------------------------------------------------------------------------

T0 = datetime(2026, 8, 13, 6, 0, 0, tzinfo=UTC)


class FakeClock:
    """Управляемое время детекторов."""

    def __init__(self, start: datetime = T0) -> None:
        self.current = start

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)


def _seed_scale(
    db: sessionmaker[Session],
    code: str = "kyzyl-kyia",
    site_name: str = "СВХ «Кызыл-Кыя»",
    scale_name: str = "Весы SCS-80",
) -> tuple[int, int, int]:
    """Объект + весы + агент; вернуть (site_id, scale_id, agent_id)."""
    with db() as session:
        site = Site(code=code, name=site_name)
        session.add(site)
        session.flush()
        scale = Scale(site_id=site.id, name=scale_name, kind=ScaleKind.STATIC, driver="cas22")
        session.add(scale)
        session.flush()
        agent = Agent(scale_id=scale.id, token_hash=repo.hash_agent_token(f"tok-{code}"))
        session.add(agent)
        session.flush()
        ids = (site.id, scale.id, agent.id)
        session.commit()
    return ids


def _set_agent(
    db: sessionmaker[Session],
    agent_id: int,
    *,
    status: AgentStatus,
    last_seen_at: datetime | None,
) -> None:
    with db() as session:
        agent = session.get(Agent, agent_id)
        assert agent is not None
        agent.status = status
        agent.last_seen_at = last_seen_at
        session.commit()


def _events(db: sessionmaker[Session]) -> list[MonitoringEvent]:
    with db() as session:
        return list(session.execute(select(MonitoringEvent).order_by(MonitoringEvent.id)).scalars())


def _equipment(**overrides: object) -> EquipmentStatus:
    fields: dict[str, object] = {
        "scale_status": ScaleStatus.OK,
        "cameras": [
            CameraStatus(role=CameraRole.FRONT, available=True),
            CameraStatus(role=CameraRole.REAR, available=True),
        ],
        "pending_sync_count": 0,
    }
    fields.update(overrides)
    return EquipmentStatus.model_validate(fields)


def _make_service(db: sessionmaker[Session], hub: AgentHub, clock: FakeClock) -> MonitoringService:
    return MonitoringService(db, hub, now=clock.now)


THRESHOLDS = MonitoringThresholds()


# ---------------------------------------------------------------------------
# Детектор офлайна
# ---------------------------------------------------------------------------


class TestOfflineDetector:
    def test_alert_after_threshold_and_recovery(self, db: sessionmaker[Session]) -> None:
        """Офлайн дольше порога → одно danger-событие и активный алерт;
        возврат на связь → ok-событие, активных алертов нет."""
        _site_id, _scale_id, agent_id = _seed_scale(db)
        clock = FakeClock()
        service = _make_service(db, AgentHub(), clock)
        _set_agent(db, agent_id, status=AgentStatus.OFFLINE, last_seen_at=clock.current)

        service.tick()  # первый раз увидели проблему — антидребезг молчит
        assert _events(db) == []
        assert service.active_alerts() == []

        clock.advance(THRESHOLDS.offline_after_s + 1)
        service.tick()
        events = _events(db)
        assert [e.kind for e in events] == ["offline"]
        assert events[0].severity is MonitoringSeverity.DANGER
        assert "СВХ «Кызыл-Кыя»" in events[0].message
        assert "Весы SCS-80" in events[0].message
        alerts = service.active_alerts()
        assert len(alerts) == 1 and alerts[0].kind == "offline"

        # повторные тики не плодят события
        clock.advance(60)
        service.tick()
        assert len(_events(db)) == 1

        # восстановление — сразу, без выдержки
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        events = _events(db)
        assert [e.severity for e in events] == [MonitoringSeverity.DANGER, MonitoringSeverity.OK]
        assert service.active_alerts() == []

    def test_short_offline_never_alerts(self, db: sessionmaker[Session]) -> None:
        """WS-реконнект (офлайн меньше порога) не будит никого."""
        _, _, agent_id = _seed_scale(db)
        clock = FakeClock()
        service = _make_service(db, AgentHub(), clock)
        _set_agent(db, agent_id, status=AgentStatus.OFFLINE, last_seen_at=clock.current)
        service.tick()
        clock.advance(60)  # меньше offline_after_s
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        assert _events(db) == []

    def test_stale_heartbeat_is_offline(self, db: sessionmaker[Session]) -> None:
        """Статус online при молчащем heartbeat — тоже офлайн (полуживой TCP)."""
        _, _, agent_id = _seed_scale(db)
        clock = FakeClock()
        service = _make_service(db, AgentHub(), clock)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        clock.advance(THRESHOLDS.stale_heartbeat_s + 10)
        service.tick()  # bad замечен
        clock.advance(THRESHOLDS.offline_after_s + 1)
        service.tick()
        assert [e.kind for e in _events(db)] == ["offline"]


# ---------------------------------------------------------------------------
# Детекторы оборудования (по самодиагностике из хаба)
# ---------------------------------------------------------------------------


def _bring_online(
    db: sessionmaker[Session], hub: AgentHub, agent_id: int, scale_id: int, clock: FakeClock
) -> None:
    _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
    hub.update_equipment(scale_id, _equipment())


class TestEquipmentDetectors:
    def test_no_data_alert(self, db: sessionmaker[Session]) -> None:
        _, scale_id, agent_id = _seed_scale(db)
        clock = FakeClock()
        hub = AgentHub()
        service = _make_service(db, hub, clock)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        hub.update_equipment(scale_id, _equipment(scale_status=ScaleStatus.NO_DATA))
        service.tick()
        clock.advance(THRESHOLDS.no_data_after_s + 1)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        events = _events(db)
        assert [e.kind for e in events] == ["no_data"]
        assert "индикатор не отдаёт вес" in events[0].message

    def test_camera_alert_survives_agent_offline(self, db: sessionmaker[Session]) -> None:
        """Пока агент офлайн, алерт камеры не закрывается «за отсутствием
        данных» — закрытие честное, только по наблюдению работающей камеры."""
        _, scale_id, agent_id = _seed_scale(db)
        clock = FakeClock()
        hub = AgentHub()
        service = _make_service(db, hub, clock)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        hub.update_equipment(
            scale_id,
            _equipment(
                cameras=[
                    CameraStatus(role=CameraRole.FRONT, available=False),
                    CameraStatus(role=CameraRole.REAR, available=True),
                ]
            ),
        )
        service.tick()
        clock.advance(THRESHOLDS.camera_after_s + 1)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        assert [e.kind for e in _events(db)] == ["camera_front"]
        assert "ПЕРЕД" in _events(db)[0].message

        # агент пропал: камера-алерт остаётся, офлайн добавляется своим чередом
        _set_agent(db, agent_id, status=AgentStatus.OFFLINE, last_seen_at=clock.current)
        service.tick()
        clock.advance(THRESHOLDS.offline_after_s + 1)
        service.tick()
        kinds = {a.kind for a in service.active_alerts()}
        assert kinds == {"camera_front", "offline"}

    def test_backlog_photo_and_disk_metrics(self, db: sessionmaker[Session]) -> None:
        """Очереди и диск: photo_backlog с выдержкой, disk_low сразу."""
        _, scale_id, agent_id = _seed_scale(db)
        clock = FakeClock()
        hub = AgentHub()
        service = _make_service(db, hub, clock)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        hub.update_equipment(
            scale_id,
            _equipment(
                pending_sync_count=3,
                pending_photos_count=THRESHOLDS.photo_backlog_min,
                disk_free_mb=1024,
            ),
        )
        service.tick()
        # диск — сразу (не дребезжит), очереди — только после выдержки
        assert [e.kind for e in _events(db)] == ["disk_low"]
        clock.advance(THRESHOLDS.backlog_after_s + 1)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        kinds = sorted(e.kind for e in _events(db))
        assert kinds == ["disk_low", "photo_backlog", "sync_backlog"]

    def test_old_agent_without_metrics_is_silent(self, db: sessionmaker[Session]) -> None:
        """Агент ≤0.4.12 не шлёт метрик (None) — детекторы очередей и диска
        по нему молчат, а не считают None нулём или бесконечностью."""
        _, scale_id, agent_id = _seed_scale(db)
        clock = FakeClock()
        hub = AgentHub()
        service = _make_service(db, hub, clock)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        hub.update_equipment(scale_id, _equipment(pending_photos_count=None, disk_free_mb=None))
        service.tick()
        clock.advance(THRESHOLDS.backlog_after_s + 1)
        _set_agent(db, agent_id, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()
        assert _events(db) == []


# ---------------------------------------------------------------------------
# Активные алерты для дашборда
# ---------------------------------------------------------------------------


class TestActiveAlerts:
    def test_scope_filter_and_order(self, db: sessionmaker[Session]) -> None:
        """danger первыми; site_scope отдаёт только алерты своего объекта."""
        site_a, _, agent_a = _seed_scale(db)
        site_b, scale_b, agent_b = _seed_scale(
            db, code="jalal-abad", site_name="ПЗТК «Джалал-Абад»", scale_name="Весы 80т"
        )
        clock = FakeClock()
        hub = AgentHub()
        service = _make_service(db, hub, clock)
        # объект A офлайн (danger), объект B — очередь снимков (warning)
        _set_agent(db, agent_a, status=AgentStatus.OFFLINE, last_seen_at=clock.current)
        _set_agent(db, agent_b, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        hub.update_equipment(scale_b, _equipment(pending_photos_count=100))
        service.tick()
        clock.advance(THRESHOLDS.backlog_after_s + 1)
        _set_agent(db, agent_b, status=AgentStatus.ONLINE, last_seen_at=clock.current)
        service.tick()

        alerts = service.active_alerts()
        assert [a.kind for a in alerts] == ["offline", "photo_backlog"]
        assert [a.severity for a in alerts] == [
            MonitoringSeverity.DANGER,
            MonitoringSeverity.WARNING,
        ]
        only_b = service.active_alerts(site_b)
        assert [a.kind for a in only_b] == ["photo_backlog"]
        assert service.active_alerts(site_a + site_b + 100) == []


# ---------------------------------------------------------------------------
# Доставка в Telegram
# ---------------------------------------------------------------------------


def _add_event(
    db: sessionmaker[Session],
    scale_id: int,
    *,
    kind: str = "offline",
    severity: MonitoringSeverity = MonitoringSeverity.DANGER,
    message: str = "тестовое событие",
    created_at: datetime | None = None,
) -> int:
    with db() as session:
        event = MonitoringEvent(scale_id=scale_id, kind=kind, severity=severity, message=message)
        if created_at is not None:
            event.created_at = created_at
        session.add(event)
        session.commit()
        return event.id


class TestTelegramNotifier:
    def test_disabled_without_secrets(self, db: sessionmaker[Session]) -> None:
        assert TelegramNotifier(db).enabled is False
        assert TelegramNotifier(db, token="x").enabled is False
        assert TelegramNotifier(db, token="x", chat_id="1").enabled is True

    def test_delivers_in_order_with_emoji(self, db: sessionmaker[Session]) -> None:
        _, scale_id, _ = _seed_scale(db)
        clock = FakeClock()
        _add_event(db, scale_id, message="агент пропал", created_at=clock.current)
        _add_event(
            db,
            scale_id,
            severity=MonitoringSeverity.OK,
            message="агент вернулся",
            created_at=clock.current,
        )
        sent: list[str] = []
        notifier = TelegramNotifier(db, token="t", chat_id="c", send=sent.append, now=clock.now)
        assert notifier.deliver_once() == 2
        assert sent == ["🔴 агент пропал", "✅ агент вернулся"]
        assert all(e.notified_at is not None for e in _events(db))
        # повторный цикл ничего не шлёт
        assert notifier.deliver_once() == 0
        assert len(sent) == 2

    def test_http_error_body_reaches_log(
        self, db: sessionmaker[Session], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Тело ответа Telegram при HTTP-ошибке попадает в текст исключения:
        боевой урок 14.08.2026 — группа мигрировала в супергруппу, а в логе
        был голый «400 Bad Request» без description и нового chat_id."""
        import io
        import urllib.error

        body = (
            b'{"ok":false,"error_code":400,'
            b'"description":"Bad Request: group chat was upgraded to a supergroup chat",'
            b'"parameters":{"migrate_to_chat_id":-1004389188992}}'
        )

        def raising_urlopen(request: object, timeout: float = 0) -> None:
            raise urllib.error.HTTPError(
                "https://api.telegram.org/", 400, "Bad Request", None, io.BytesIO(body)
            )

        monkeypatch.setattr("urllib.request.urlopen", raising_urlopen)
        notifier = TelegramNotifier(db, token="t", chat_id="-5485263200")
        with pytest.raises(RuntimeError) as excinfo:
            notifier._send("проверка")
        assert "group chat was upgraded to a supergroup chat" in str(excinfo.value)
        assert "-1004389188992" in str(excinfo.value)

    def test_failure_keeps_rest_for_retry(self, db: sessionmaker[Session]) -> None:
        """Telegram упал на втором сообщении: первое помечено, остальные
        ждут следующего цикла — порядок доставки сохраняется."""
        _, scale_id, _ = _seed_scale(db)
        clock = FakeClock()
        for n in (1, 2, 3):
            _add_event(db, scale_id, message=f"событие {n}", created_at=clock.current)
        sent: list[str] = []
        failures = iter([True])  # «событие 2» падает ровно один раз

        def flaky(text: str) -> None:
            if "событие 2" in text and next(failures, False):
                raise OSError("timeout")
            sent.append(text)

        notifier = TelegramNotifier(db, token="t", chat_id="c", send=flaky, now=clock.now)
        assert notifier.deliver_once() == 1
        assert [e.notified_at is not None for e in _events(db)] == [True, False, False]
        # следующий цикл добирает остаток с того же места
        assert notifier.deliver_once() == 2
        assert [e.notified_at is not None for e in _events(db)] == [True, True, True]
        assert sent == ["🔴 событие 1", "🔴 событие 2", "🔴 событие 3"]

    def test_window_skips_ancient_events(self, db: sessionmaker[Session]) -> None:
        """События старше окна не шлются: подключённый через сутки токен
        не обрушивает на чат поток древних алертов."""
        _, scale_id, _ = _seed_scale(db)
        clock = FakeClock()
        _add_event(
            db,
            scale_id,
            message="древнее",
            created_at=clock.current - timedelta(hours=7),
        )
        _add_event(db, scale_id, message="свежее", created_at=clock.current)
        sent: list[str] = []
        notifier = TelegramNotifier(db, token="t", chat_id="c", send=sent.append, now=clock.now)
        assert notifier.deliver_once() == 1
        assert sent == ["🔴 свежее"]
