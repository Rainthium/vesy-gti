"""Мониторинг объектов: детекторы проблем и доставка алертов в Telegram.

Этап 2 (одобрено Игорем 13.08.2026). Устройство:

- ``MonitoringService`` — фоновый цикл в процессе центра (один воркер
  uvicorn, поэтому и детекторы одни). Раз в ``tick_s`` сверяет состояние
  каждых весов (строка агента в БД + последняя самодиагностика из
  ``AgentHub``) с порогами и держит в памяти АКТИВНЫЕ проблемы — их
  показывает дашборд панели. На ПЕРЕХОДАХ (проблема появилась/закрылась)
  пишет событие в ``monitoring_events`` — это журнал экрана «События»
  и очередь доставки уведомлений.
- ``TelegramNotifier`` — отдельный фоновый цикл: забирает события без
  отметки ``notified_at`` и шлёт их в чат ботом (secrets из env, правило
  №7). Недоступный Telegram не теряет события: отметки нет — попробуем
  в следующем цикле; окно доставки ограничено, чтобы после долгого
  простоя не хлынул поток древних событий.

Активные проблемы живут в памяти: после рестарта центра детекторы
восстановят их за первый же тик (повторный алерт после деплоя — цена,
которую платим за простоту; лучше повтор, чем молчание).

Антидребезг: проблема должна продержаться ``*_after_s``, прежде чем
станет алертом (WS-реконнекты и переоткрытия порта — не повод будить
людей). Восстановление сообщается сразу. Пока агент офлайн, детекторы
по его оборудованию замирают (не открывают и не закрывают ничего):
самодиагностики нет, а закрыть алерт камеры «за отсутствием данных»
значило бы соврать.
"""

import asyncio
import json
import logging
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub
from center.db.models import Agent, AgentStatus, MonitoringEvent, MonitoringSeverity, Scale, Site
from shared.enums import CameraRole, ScaleStatus

logger = logging.getLogger(__name__)

BISHKEK = ZoneInfo("Asia/Bishkek")

TELEGRAM_TIMEOUT_S = 15.0


@dataclass(frozen=True)
class MonitoringThresholds:
    """Пороги детекторов; значения подобраны под пилот (см. decisions).

    В env не вынесены сознательно: настроек и так много, а менять пороги
    приходится не чаще, чем выкатывать центр.
    """

    tick_s: float = 30.0  # период прохода детекторов
    # heartbeat агента идёт раз в ~5 с; молчание дольше этого срока
    # означает полуживое соединение даже при status=online в БД
    stale_heartbeat_s: float = 120.0
    offline_after_s: float = 300.0  # офлайн 5 минут — алерт
    no_data_after_s: float = 180.0  # индикатор молчит 3 минуты — алерт
    camera_after_s: float = 180.0  # камера пропала (проверки раз в 60 с)
    backlog_after_s: float = 600.0  # очереди не рассасываются 10 минут
    photo_backlog_min: int = 20  # снимков в очереди при живой связи
    disk_low_mb: int = 5120  # меньше 5 ГБ на диске с фото — алерт


# подписи типов проблем — пилюли дашборда и экрана «События»
KIND_LABELS = {
    "offline": "Офлайн",
    "no_data": "Индикатор",
    "camera_front": "Камера ПЕРЕД",
    "camera_rear": "Камера ЗАД",
    "sync_backlog": "Досылка",
    "photo_backlog": "Снимки",
    "disk_low": "Диск",
}


@dataclass(frozen=True)
class ActiveAlert:
    """Активная (незакрытая) проблема — строка блока алертов на дашборде."""

    scale_id: int
    site_id: int
    kind: str
    severity: MonitoringSeverity
    message: str
    started_at: datetime

    @property
    def kind_label(self) -> str:
        return KIND_LABELS.get(self.kind, self.kind)


@dataclass
class _DetectorState:
    """Память одного детектора одних весов (антидребезг + активность)."""

    first_bad_at: datetime
    active: bool = False
    started_at: datetime | None = None
    severity: MonitoringSeverity = MonitoringSeverity.WARNING
    message: str = ""


@dataclass(frozen=True)
class _ScaleSnapshot:
    """Всё, что детекторам нужно знать об одних весах на этом тике."""

    scale_id: int
    site_id: int
    title: str  # «СВХ «Кызыл-Кыя» · Весы SCS-80» — префикс сообщений


def _fmt_hm(value: datetime | None, *, now: datetime) -> str:
    """Время для замороженного сообщения алерта: не сегодняшнее — с датой.

    Сообщение фиксируется при активации и живёт, пока проблема не
    закрыта: «14:22:10» без даты спустя сутки офлайна читалось бы как
    сегодняшнее.
    """
    if value is None:
        return "никогда"
    local = value.astimezone(BISHKEK)
    if local.date() == now.astimezone(BISHKEK).date():
        return local.strftime("%H:%M:%S")
    return local.strftime("%d.%m %H:%M:%S")


class MonitoringService:
    """Детекторы состояния объектов; активные алерты — в памяти."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        hub: AgentHub,
        *,
        thresholds: MonitoringThresholds | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._hub = hub
        self._thresholds = thresholds or MonitoringThresholds()
        self._now = now or (lambda: datetime.now(UTC))
        self._states: dict[tuple[int, str], _DetectorState] = {}
        # site_id по весам — обновляется каждым тиком (scope-фильтр алертов)
        self._site_ids: dict[int, int] = {}

    # --- чтение для панели ---

    def active_alerts(self, site_scope: int | None = None) -> list[ActiveAlert]:
        """Активные проблемы (для дашборда); danger первыми, застарелые выше.

        ``site_scope`` — разграничение видимости по объекту, как везде
        в панели (правило PanelScope). Вызывается из потока event loop,
        пока tick() пишет из worker-потока (asyncio.to_thread) — поэтому
        итерация по копии словаря (сама копия атомарна под GIL), иначе
        одновременный тик ронял бы запрос дашборда «dictionary changed
        size during iteration».
        """
        states = self._states.copy()
        site_ids = self._site_ids
        alerts = [
            ActiveAlert(
                scale_id=scale_id,
                site_id=state_site_id,
                kind=kind,
                severity=state.severity,
                message=state.message,
                started_at=state.started_at or state.first_bad_at,
            )
            for (scale_id, kind), state in states.items()
            if state.active and (state_site_id := site_ids.get(scale_id)) is not None
        ]
        if site_scope is not None:
            alerts = [a for a in alerts if a.site_id == site_scope]
        alerts.sort(key=lambda a: (a.severity is not MonitoringSeverity.DANGER, a.started_at))
        return alerts

    # --- цикл ---

    async def run(self) -> None:
        """Фоновая задача центра: детекторы каждые tick_s, ошибки не роняют."""
        logger.info("мониторинг запущен (тик %.0f с)", self._thresholds.tick_s)
        while True:
            try:
                await asyncio.to_thread(self.tick)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("проход детекторов мониторинга не удался")
            await asyncio.sleep(self._thresholds.tick_s)

    def tick(self) -> None:
        """Один проход детекторов по всем весам с агентами (синхронный)."""
        now = self._now()
        with self._session_factory() as session:
            rows = session.execute(
                select(Scale, Site, Agent)
                .join(Site, Site.id == Scale.site_id)
                .join(Agent, Agent.scale_id == Scale.id)
                .order_by(Scale.id)
            ).all()
            events: list[MonitoringEvent] = []
            seen_scale_ids = set()
            site_ids: dict[int, int] = {}
            for scale, site, agent in rows:
                seen_scale_ids.add(scale.id)
                site_ids[scale.id] = site.id
                snapshot = _ScaleSnapshot(
                    scale_id=scale.id,
                    site_id=site.id,
                    title=f"{site.name} · {scale.name}",
                )
                events.extend(self._check_scale(snapshot, agent, now))
            self._site_ids = site_ids
            # весы, удалённые из справочника (или лишившиеся агента),
            # не должны вечно висеть активным алертом
            for key in [k for k in self._states if k[0] not in seen_scale_ids]:
                del self._states[key]
            for event in events:
                session.add(event)
            if events:
                session.commit()

    # --- детекторы ---

    def _check_scale(
        self, snap: _ScaleSnapshot, agent: Agent, now: datetime
    ) -> list[MonitoringEvent]:
        thresholds = self._thresholds
        events: list[MonitoringEvent] = []

        last_seen = agent.last_seen_at
        if last_seen is not None and last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=UTC)
        heartbeat_fresh = (
            last_seen is not None
            and (now - last_seen).total_seconds() <= thresholds.stale_heartbeat_s
        )
        # «на связи» = статус online И свежий heartbeat: полуживой TCP
        # с молчащим агентом — это тоже офлайн, хоть detach и не случился
        online = agent.status == AgentStatus.ONLINE and heartbeat_fresh

        events.extend(
            self._transition(
                snap,
                "offline",
                bad=not online,
                delay_s=thresholds.offline_after_s,
                severity=MonitoringSeverity.DANGER,
                problem=(
                    f"{snap.title}: агент не выходит на связь "
                    f"(последний раз {_fmt_hm(last_seen, now=now)}) — взвешивания недоступны"
                ),
                recovery=f"{snap.title}: связь с агентом восстановлена",
                now=now,
            )
        )

        equipment = self._hub.equipment(snap.scale_id)
        if not online or equipment is None:
            # самодиагностики нет — детекторы оборудования замирают:
            # закрыть алерт камеры «за отсутствием данных» значило бы соврать
            return events

        scale_ok = equipment.scale_status == ScaleStatus.OK
        status_label = (
            "ошибка порта" if equipment.scale_status == ScaleStatus.PORT_ERROR else "нет данных"
        )
        events.extend(
            self._transition(
                snap,
                "no_data",
                bad=not scale_ok,
                delay_s=thresholds.no_data_after_s,
                severity=MonitoringSeverity.DANGER,
                problem=f"{snap.title}: индикатор не отдаёт вес ({status_label})",
                recovery=f"{snap.title}: индикатор снова отдаёт вес",
                now=now,
            )
        )

        for camera in equipment.cameras:
            role_label = "ПЕРЕД" if camera.role == CameraRole.FRONT else "ЗАД"
            events.extend(
                self._transition(
                    snap,
                    f"camera_{camera.role.value}",
                    bad=not camera.available,
                    delay_s=thresholds.camera_after_s,
                    severity=MonitoringSeverity.WARNING,
                    problem=f"{snap.title}: камера {role_label} недоступна",
                    recovery=f"{snap.title}: камера {role_label} снова работает",
                    now=now,
                )
            )

        events.extend(
            self._transition(
                snap,
                "sync_backlog",
                bad=equipment.pending_sync_count > 0,
                delay_s=thresholds.backlog_after_s,
                severity=MonitoringSeverity.WARNING,
                problem=(
                    f"{snap.title}: {equipment.pending_sync_count} офлайн-записей "
                    "не досылаются при живой связи"
                ),
                recovery=f"{snap.title}: очередь досылки записей опустела",
                now=now,
            )
        )

        # метрики 0.4.13: старый агент их не шлёт (None) — детекторы молчат
        if equipment.pending_photos_count is not None:
            events.extend(
                self._transition(
                    snap,
                    "photo_backlog",
                    bad=equipment.pending_photos_count >= self._thresholds.photo_backlog_min,
                    delay_s=thresholds.backlog_after_s,
                    severity=MonitoringSeverity.WARNING,
                    problem=(
                        f"{snap.title}: очередь недосланных снимков растёт "
                        f"({equipment.pending_photos_count})"
                    ),
                    recovery=f"{snap.title}: очередь снимков рассосалась",
                    now=now,
                )
            )
        if equipment.disk_free_mb is not None:
            free_gb = equipment.disk_free_mb / 1024
            events.extend(
                self._transition(
                    snap,
                    "disk_low",
                    bad=equipment.disk_free_mb < self._thresholds.disk_low_mb,
                    delay_s=0.0,  # диск не дребезжит, ждать нечего
                    severity=MonitoringSeverity.WARNING,
                    problem=(
                        f"{snap.title}: на диске весового ПК осталось {free_gb:.1f} ГБ — "
                        "скоро станет некуда писать фото"
                    ),
                    recovery=f"{snap.title}: место на диске весового ПК снова в норме",
                    now=now,
                )
            )
        return events

    def _transition(
        self,
        snap: _ScaleSnapshot,
        kind: str,
        *,
        bad: bool,
        delay_s: float,
        severity: MonitoringSeverity,
        problem: str,
        recovery: str,
        now: datetime,
    ) -> list[MonitoringEvent]:
        """Свести наблюдение с памятью детектора; вернуть события переходов."""
        key = (snap.scale_id, kind)
        state = self._states.get(key)
        if bad:
            if state is None:
                state = _DetectorState(first_bad_at=now)
                self._states[key] = state
            if not state.active and (now - state.first_bad_at).total_seconds() >= delay_s:
                # active — последним: active_alerts читает из другого потока
                # и не должен увидеть активный алерт с пустым сообщением
                state.started_at = now
                state.severity = severity
                state.message = problem
                state.active = True
                logger.warning("мониторинг: %s", problem)
                return [
                    MonitoringEvent(
                        scale_id=snap.scale_id, kind=kind, severity=severity, message=problem
                    )
                ]
            return []
        if state is not None:
            was_active = state.active
            del self._states[key]
            if was_active:
                logger.info("мониторинг: %s", recovery)
                return [
                    MonitoringEvent(
                        scale_id=snap.scale_id,
                        kind=kind,
                        severity=MonitoringSeverity.OK,
                        message=recovery,
                    )
                ]
        return []


# эмодзи в сообщении Telegram — по важности события
_SEVERITY_EMOJI = {
    MonitoringSeverity.DANGER: "🔴",
    MonitoringSeverity.WARNING: "🟠",
    MonitoringSeverity.OK: "✅",
}


@dataclass
class TelegramNotifier:
    """Доставка событий мониторинга в Telegram-чат.

    Токен бота и chat_id — из env (правило №7); без них нотификатор
    выключен, события просто копятся в журнале. Отметка ``notified_at``
    ставится по одному событию за раз: частичная доставка не теряется,
    недоступный Telegram — повтор в следующем цикле.
    """

    session_factory: Callable[[], Session]
    token: str = ""
    chat_id: str = ""
    interval_s: float = 20.0
    # события старше окна не шлём: после долгого простоя нотификатора
    # (токен появился позже, центр лежал) поток древних алертов не нужен
    window: timedelta = timedelta(hours=6)
    batch_limit: int = 20
    send: Callable[[str], None] | None = None  # подмена транспорта в тестах
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def run(self) -> None:
        logger.info("Telegram-уведомления включены (chat_id %s)", self.chat_id)
        while True:
            try:
                await asyncio.to_thread(self.deliver_once)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("цикл доставки Telegram-уведомлений не удался")
            await asyncio.sleep(self.interval_s)

    def deliver_once(self) -> int:
        """Отправить недоставленные события (по порядку); вернуть число."""
        now = self.now()
        sent = 0
        with self._session() as session:
            events = (
                session.execute(
                    select(MonitoringEvent)
                    .where(
                        MonitoringEvent.notified_at.is_(None),
                        MonitoringEvent.created_at > now - self.window,
                    )
                    .order_by(MonitoringEvent.id)
                    .limit(self.batch_limit)
                )
                .scalars()
                .all()
            )
            for event in events:
                emoji = _SEVERITY_EMOJI.get(event.severity, "")
                try:
                    self._send(f"{emoji} {event.message}".strip())
                except Exception as exc:
                    # порядок доставки важнее полноты пачки: остановились,
                    # следующий цикл начнёт с этого же события
                    logger.warning("Telegram не принял сообщение: %s", exc)
                    break
                event.notified_at = now
                session.commit()
                sent += 1
        return sent

    def _session(self) -> Session:
        return self.session_factory()

    def _send(self, text: str) -> None:
        if self.send is not None:
            self.send(text)
            return
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self.token}/sendMessage",
            data=json.dumps({"chat_id": self.chat_id, "text": text}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=TELEGRAM_TIMEOUT_S) as response:
            # Telegram отвечает 200 с ok=true; иные коды бросают HTTPError
            response.read()
