"""Автовыкат агентов по каналам pilot → stable (architecture §7а, 18.08.2026).

Три части:

1. **Каталог релизов** — файлы каталога AGENT_RELEASES_DIR (center/releases.py)
   объединяются со строками ``agent_releases`` (канал, описание, кто
   назначил). Канал у одной версии на канал: перевод новой версии в канал
   снимает его с прежней (та становится «архивом»). Цель агента — релиз
   его канала; агенту канала pilot без назначенного pilot-релиза достаётся
   stable. Раскатка только ВВЕРХ: версия ниже целевой ставится, выше —
   нет (отзыв релиза не откатывает уже обновлённых).

2. **Журнал раскатки** ``agent_updates`` — строка на пару (агент, версия):
   commanded → started → installed | failed | rolled_back. Переходы делает
   WS-роутер по сообщениям агента (hello с новой версией = installed;
   update_status со стадиями 0.4.19) — здесь функции ``note_agent_hello``
   и ``apply_update_status``; исходы становятся событиями мониторинга
   (успех — ok, отказ/откат — warning → «События» и Telegram).

3. **Движок** ``RolloutService`` — фоновая задача центра (раз в 30 с):
   агентам на связи, чья версия ниже цели канала, шлёт update_command;
   не больше ``max_in_flight`` одновременно (сбойный релиз не должен лечь
   на все объекты разом), отказы повторяет не чаще раза в полчаса и не
   больше ``max_attempts`` раз, откат (rolled_back) — терминален: дальше
   только человек кнопкой «Повторить». Зависшая команда без ответа
   ``stale_after`` — тоже повтор. Офлайн-агенты получают команду при связи.
"""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentHubError
from center.db import repo
from center.db.models import (
    Agent,
    AgentRelease,
    AgentUpdate,
    AgentUpdateStatus,
    MonitoringSeverity,
    ReleaseChannel,
    Scale,
    Site,
)
from center.releases import AgentRelease as ReleaseFile
from center.releases import (
    ReleaseError,
    list_releases,
    release_by_version,
    release_filename,
    version_key,
)
from shared.messages import UpdateCommand, UpdateStatus, supports_update_stages

logger = logging.getLogger(__name__)

ORIGIN_AUTO = "auto"
ORIGIN_MANUAL = "manual"
NOTE_SELF_CHECK = "самопроверка пройдена"
NOTE_HELLO = "версия подтверждена агентом"
_ALREADY_INSTALLED = "уже установлена"
_ALREADY_RUNNING = "уже выполняется"  # повторная команда поверх идущего обновления

IN_FLIGHT = frozenset({AgentUpdateStatus.COMMANDED, AgentUpdateStatus.STARTED})


# ---------------------------------------------------------------------------
# Каталог релизов и каналы
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReleaseInfo:
    """Релиз глазами панели и движка: файл + строка каталога."""

    version: str
    filename: str
    sha256: str
    size_bytes: int
    present: bool  # файл лежит в каталоге (без файла раскатка невозможна)
    channel: ReleaseChannel | None
    notes: str
    released_at: datetime | None
    channel_changed_at: datetime | None
    published_by: str | None

    @property
    def sha_short(self) -> str:
        return f"{self.sha256[:6]}…{self.sha256[-4:]}" if self.sha256 else "—"

    @property
    def size_mb(self) -> str:
        return f"{self.size_bytes / (1024 * 1024):.1f} МБ" if self.size_bytes else "—"


def _file_mtime(release: ReleaseFile | None) -> datetime | None:
    if release is None:
        return None
    try:
        return datetime.fromtimestamp(release.path.stat().st_mtime, UTC)
    except OSError:
        return None


def release_catalog(session: Session, releases_dir: Path) -> list[ReleaseInfo]:
    """Все известные релизы (файлы ∪ строки), новые первыми."""
    files = {r.version: r for r in list_releases(releases_dir)}
    rows = {row.version: row for row in session.execute(select(AgentRelease)).scalars()}
    catalog: list[ReleaseInfo] = []
    for version in set(files) | set(rows):
        file = files.get(version)
        row = rows.get(version)
        catalog.append(
            ReleaseInfo(
                version=version,
                filename=file.filename if file else (row.file_path if row else ""),
                sha256=file.sha256 if file else (row.sha256 if row else ""),
                size_bytes=file.size_bytes if file else int((row.size_bytes if row else 0) or 0),
                present=file is not None,
                channel=row.channel if row else None,
                notes=row.notes if row else "",
                # дата артефакта — mtime файла; строка каталога могла появиться позже
                released_at=_file_mtime(file) or (row.released_at if row else None),
                channel_changed_at=row.channel_changed_at if row else None,
                published_by=row.published_by if row else None,
            )
        )
    catalog.sort(key=lambda r: version_key(r.version) or (0, 0, 0), reverse=True)
    return catalog


def channel_targets(catalog: list[ReleaseInfo]) -> dict[ReleaseChannel, ReleaseInfo]:
    """Релиз каждого канала (только с файлом на месте)."""
    return {info.channel: info for info in catalog if info.channel is not None and info.present}


def target_for(
    channel: ReleaseChannel, targets: dict[ReleaseChannel, ReleaseInfo]
) -> ReleaseInfo | None:
    """Цель агента: релиз его канала; pilot без своего релиза берёт stable."""
    if channel is ReleaseChannel.PILOT:
        return targets.get(ReleaseChannel.PILOT) or targets.get(ReleaseChannel.STABLE)
    return targets.get(ReleaseChannel.STABLE)


def _ensure_row(
    session: Session, releases_dir: Path, version: str, *, by: str | None
) -> AgentRelease:
    row = session.execute(
        select(AgentRelease).where(AgentRelease.version == version)
    ).scalar_one_or_none()
    file = release_by_version(releases_dir, version)
    if row is None:
        row = AgentRelease(
            version=version,
            channel=None,
            file_path=file.filename if file else release_filename(version),
            sha256=file.sha256 if file else "",
            size_bytes=file.size_bytes if file else None,
            published_by=by,
        )
        session.add(row)
        session.flush()
    elif file is not None and (row.sha256 != file.sha256 or row.size_bytes != file.size_bytes):
        # файл появился/пересобран после создания строки — обновить описание
        row.sha256 = file.sha256
        row.size_bytes = file.size_bytes
        row.file_path = file.filename
    return row


def register_release(
    session: Session, releases_dir: Path, version: str, *, by: str | None
) -> AgentRelease:
    """Строка каталога для выложенного файла (после загрузки через панель)."""
    row = _ensure_row(session, releases_dir, version, by=by)
    session.commit()
    return row


def _known_release(session: Session, releases_dir: Path, version: str) -> bool:
    """Есть файл или строка каталога (иначе правки создали бы фантом)."""
    if release_by_version(releases_dir, version) is not None:
        return True
    return (
        session.execute(
            select(AgentRelease.id).where(AgentRelease.version == version)
        ).scalar_one_or_none()
        is not None
    )


def set_release_channel(
    session: Session,
    releases_dir: Path,
    version: str,
    channel: ReleaseChannel | None,
    *,
    by: str,
    now: datetime | None = None,
) -> None:
    """Назначить релизу канал (или снять — «отозвать»).

    Назначение требует файла в каталоге. Прежний релиз того же канала
    теряет канал (архив): у канала всегда одна версия — цель раскатки.
    """
    now = now or datetime.now(UTC)
    if channel is not None and release_by_version(releases_dir, version) is None:
        raise ReleaseError(f"файла релиза {version} нет в каталоге — назначить канал нельзя")
    if channel is None and not _known_release(session, releases_dir, version):
        raise ReleaseError(f"релиз {version} не найден")
    row = _ensure_row(session, releases_dir, version, by=by)
    if channel is not None:
        others = session.execute(
            select(AgentRelease).where(
                AgentRelease.channel == channel, AgentRelease.version != version
            )
        ).scalars()
        for other in others:
            other.channel = None
            other.channel_changed_at = now
    row.channel = channel
    row.channel_changed_at = now
    row.published_by = by
    session.commit()
    logger.info("релиз агента %s: канал %s (%s)", version, channel.value if channel else "снят", by)


def set_release_notes(
    session: Session, releases_dir: Path, version: str, notes: str, *, by: str
) -> None:
    if not _known_release(session, releases_dir, version):
        raise ReleaseError(f"релиз {version} не найден")
    row = _ensure_row(session, releases_dir, version, by=by)
    row.notes = notes.strip()[:500]
    session.commit()


def delete_release(session: Session, releases_dir: Path, version: str, *, by: str) -> None:
    """Удалить релиз из каталога: zip с диска и строку ``agent_releases`` разом
    (иначе каталог, собираемый из файлы ∪ строки, оставит фантом).

    Версия на канале не удаляется — её прямо сейчас раздаёт раскатка; сперва
    «Отозвать» или перевод другой версии. Журнал ``agent_updates`` остаётся:
    это история обновлений агентов, а не описание версии. Вернуть удалённую
    версию можно только пересборкой из git-тега.
    """
    if not _known_release(session, releases_dir, version):
        raise ReleaseError(f"релиз {version} не найден")
    row = session.execute(
        select(AgentRelease).where(AgentRelease.version == version)
    ).scalar_one_or_none()
    if row is not None and row.channel is not None:
        raise ReleaseError(
            f"релиз {version} назначен каналу {row.channel.value} — сначала снимите его с канала"
        )
    file = release_by_version(releases_dir, version)
    # файл стирается до commit: если диск откажет, строка каталога уцелеет
    if file is not None:
        file.path.unlink(missing_ok=True)
    if row is not None:
        session.delete(row)
    session.commit()
    logger.info("релиз агента %s удалён из каталога (%s)", version, by)


# ---------------------------------------------------------------------------
# Журнал раскатки: переходы состояний
# ---------------------------------------------------------------------------


def get_agent_update(session: Session, agent_id: int, version: str) -> AgentUpdate | None:
    return session.execute(
        select(AgentUpdate).where(AgentUpdate.agent_id == agent_id, AgentUpdate.version == version)
    ).scalar_one_or_none()


def updates_by_agent(session: Session) -> dict[int, list[AgentUpdate]]:
    """Все строки журнала, сгруппированные по агенту (новые первыми)."""
    grouped: dict[int, list[AgentUpdate]] = {}
    for row in session.execute(
        select(AgentUpdate).order_by(AgentUpdate.updated_at.desc())
    ).scalars():
        grouped.setdefault(row.agent_id, []).append(row)
    return grouped


def mark_commanded(
    session: Session,
    agent_id: int,
    version: str,
    *,
    origin: str,
    now: datetime | None = None,
) -> AgentUpdate:
    """Команда отправлена: новая строка или повтор (attempts += 1)."""
    now = now or datetime.now(UTC)
    row = get_agent_update(session, agent_id, version)
    if row is None:
        row = AgentUpdate(
            agent_id=agent_id,
            version=version,
            status=AgentUpdateStatus.COMMANDED,
            origin=origin,
            attempts=0,
            commanded_at=now,
        )
        session.add(row)
    row.status = AgentUpdateStatus.COMMANDED
    row.origin = origin
    row.attempts += 1
    row.commanded_at = now
    row.updated_at = now
    row.error = None
    row.note = None
    row.running_version = None
    session.commit()
    return row


def mark_send_failed(
    session: Session,
    agent_id: int,
    version: str,
    error: str,
    *,
    now: datetime | None = None,
) -> None:
    """Команда не ушла (агент отвалился между проверкой и отправкой): строка
    отмечена заранее — вернуть попытку и записать причину."""
    row = get_agent_update(session, agent_id, version)
    if row is None:
        return
    row.status = AgentUpdateStatus.FAILED
    row.error = f"команда не отправлена: {error}"
    row.attempts = max(0, row.attempts - 1)
    row.updated_at = now or datetime.now(UTC)
    session.commit()


def _row_for_report(session: Session, agent_id: int, version: str, now: datetime) -> AgentUpdate:
    row = get_agent_update(session, agent_id, version)
    if row is None:
        # доклад без команды в журнале (кнопка старого центра, ручной запуск
        # bat на объекте) — заводим строку, чтобы исход был виден
        row = AgentUpdate(
            agent_id=agent_id,
            version=version,
            status=AgentUpdateStatus.COMMANDED,
            origin=ORIGIN_MANUAL,
            attempts=1,
            commanded_at=now,
        )
        session.add(row)
    return row


def apply_update_status(
    session: Session,
    agent_id: int,
    scale_id: int,
    status: UpdateStatus,
    *,
    now: datetime | None = None,
) -> AgentUpdate:
    """Отчёт агента (update_status) → строка журнала + событие мониторинга."""
    now = now or datetime.now(UTC)
    if not status.ok and status.stage == "started" and _ALREADY_RUNNING in (status.error or ""):
        # повторная команда поверх идущего обновления (движок и кнопка,
        # двойной клик): не отказ — первое обновление продолжается
        row = get_agent_update(session, agent_id, status.version)
        if row is None:
            row = _row_for_report(session, agent_id, status.version, now)
            session.commit()
        return row
    row = _row_for_report(session, agent_id, status.version, now)
    row.updated_at = now
    row.running_version = status.running_version
    if status.stage == "installed":
        # ok-событие — по самопроверке (агент 0.4.19+ подтверждает себя сам;
        # hello такой версии событие не пишет, см. note_agent_hello)
        first_time = row.note != NOTE_SELF_CHECK
        row.status = AgentUpdateStatus.INSTALLED
        row.note = NOTE_SELF_CHECK
        row.error = None
        if first_time:
            repo.record_update_event(
                session,
                scale_id,
                f"агент обновлён до {status.version}, самопроверка пройдена",
                severity=MonitoringSeverity.OK,
                commit=False,
            )
    elif status.stage == "rolled_back":
        row.status = AgentUpdateStatus.ROLLED_BACK
        row.error = status.error or "откат без указания причины"
        repo.record_update_event(
            session,
            scale_id,
            f"автообновление агента до {status.version} не удалось, {row.error}",
            commit=False,
        )
    elif status.ok:
        # started, ok: архив проверен, служба перезапускается
        if row.status not in (AgentUpdateStatus.INSTALLED, AgentUpdateStatus.ROLLED_BACK):
            row.status = AgentUpdateStatus.STARTED
        row.error = None
    else:
        error = status.error or "без подробностей"
        if _ALREADY_INSTALLED in error:
            # команда пришла агенту, у которого эта версия уже стоит
            row.status = AgentUpdateStatus.INSTALLED
            row.note = row.note or NOTE_HELLO
            row.error = None
        else:
            row.status = AgentUpdateStatus.FAILED
            row.error = error
            repo.record_update_event(
                session,
                scale_id,
                f"автообновление агента до {status.version} не выполнено — {error}",
                commit=False,
            )
    session.commit()
    return row


def note_agent_hello(
    session: Session,
    agent_id: int,
    scale_id: int,
    version: str,
    *,
    now: datetime | None = None,
) -> list[AgentUpdate]:
    """hello агента: подтвердить установку целевой версии; поймать откат без доклада.

    Возвращает изменённые строки (для логов вызывающего).
    """
    now = now or datetime.now(UTC)
    current = version_key(version)
    # агент 0.4.19+ докладывает исход сам (installed/rolled_back с причиной):
    # события по hello для него не пишем — иначе Telegram получал бы три
    # сообщения на один откат (замечание ревью 18.08.2026)
    self_reporting = supports_update_stages(version)
    changed: list[AgentUpdate] = []
    rows = session.execute(select(AgentUpdate).where(AgentUpdate.agent_id == agent_id)).scalars()
    for row in rows:
        target = version_key(row.version)
        confirmable = row.status in IN_FLIGHT or row.status is AgentUpdateStatus.FAILED
        if confirmable and row.version == version:
            # факт сильнее истории: агент пришёл с целевой версией — установлено
            # (в т.ч. после ложного «отказа» повторной команды)
            row.status = AgentUpdateStatus.INSTALLED
            row.note = row.note or NOTE_HELLO
            row.error = None
            row.running_version = version
            row.updated_at = now
            if not self_reporting:
                repo.record_update_event(
                    session,
                    scale_id,
                    f"агент обновлён до {version}",
                    severity=MonitoringSeverity.OK,
                    commit=False,
                )
            changed.append(row)
        elif (
            row.status is AgentUpdateStatus.INSTALLED
            and current is not None
            and target is not None
            and current < target
        ):
            # ставили новее, а агент снова на прежней: откат — терминально.
            # Агент 0.4.19+ следом пришлёт rolled_back с причиной (событие
            # тогда); старый агент или папка, возвращённая руками, — событие сейчас
            row.status = AgentUpdateStatus.ROLLED_BACK
            row.error = f"после установки агент снова на версии {version} — откат без доклада"
            row.running_version = version
            row.updated_at = now
            if not self_reporting:
                repo.record_update_event(
                    session,
                    scale_id,
                    f"автообновление агента до {row.version} не удержалось: {row.error}",
                    commit=False,
                )
            changed.append(row)
    if changed:
        session.commit()
    return changed


# ---------------------------------------------------------------------------
# Движок автовыката
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RolloutPlan:
    """Кому что слать в этом проходе (посчитано в потоке БД)."""

    agent_id: int
    scale_id: int
    release: ReleaseInfo


@dataclass
class RolloutService:
    session_factory: Callable[[], Session]
    hub: AgentHub
    releases_dir: Path
    interval_s: float = 30.0
    max_in_flight: int = 3  # одновременно обновляющихся агентов
    max_attempts: int = 3  # автоматических попыток на пару (агент, версия)
    retry_after: timedelta = timedelta(minutes=30)  # пауза после отказа
    stale_after: timedelta = timedelta(minutes=30)  # команда без ответа → повтор
    # после установки агенту дают устаканиться: bat ещё ждёт самопроверку
    # (до 6 мин) — следующую версию в это окно не шлём (агент 0.4.19 и сам
    # подождёт, это защита в глубину для любых версий)
    settle_after: timedelta = timedelta(minutes=10)
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))

    async def run(self) -> None:
        logger.info("автовыкат агентов по каналам запущен (проход раз в %.0f с)", self.interval_s)
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("автовыкат агентов: проход не удался")
            await asyncio.sleep(self.interval_s)

    async def tick(self) -> list[RolloutPlan]:
        """Один проход: посчитать план и разослать команды; вернуть отправленное."""
        plan = await asyncio.to_thread(self._plan)
        sent: list[RolloutPlan] = []
        for item in plan:
            command = UpdateCommand(
                version=item.release.version,
                url_path=f"/agents/releases/{item.release.filename}",
                sha256=item.release.sha256,
                size_bytes=item.release.size_bytes,
            )
            # строка журнала — ДО отправки: мгновенный ответ агента (отказ
            # dev-запуска, «уже выполняется») не должен обгонять запись
            await asyncio.to_thread(self._mark, item)
            try:
                await self.hub.send_update_command(item.scale_id, command)
            except AgentHubError as exc:
                logger.info(
                    "автовыкат: весы %d — команда %s не отправлена: %s",
                    item.scale_id,
                    item.release.version,
                    exc,
                )
                await asyncio.to_thread(self._unmark, item, str(exc))
                continue
            logger.info(
                "автовыкат: весы %d — команда обновления до %s отправлена (канал)",
                item.scale_id,
                item.release.version,
            )
            sent.append(item)
        return sent

    def _mark(self, item: RolloutPlan) -> None:
        # часы движка (self.now) — и в план, и в отметки: иначе повтор по
        # stale_after считался бы от другого времени, чем сам план
        with self.session_factory() as session:
            mark_commanded(
                session, item.agent_id, item.release.version, origin=ORIGIN_AUTO, now=self.now()
            )

    def _unmark(self, item: RolloutPlan, error: str) -> None:
        with self.session_factory() as session:
            mark_send_failed(session, item.agent_id, item.release.version, error, now=self.now())

    def _plan(self) -> list[RolloutPlan]:
        now = self.now()
        with self.session_factory() as session:
            targets = channel_targets(release_catalog(session, self.releases_dir))
            if not targets:
                return []
            agents = list(session.execute(select(Agent)).scalars())
            updates = updates_by_agent(session)
            in_flight = sum(
                1
                for rows in updates.values()
                for row in rows
                if row.status in IN_FLIGHT and now - _aware(row.updated_at) <= self.stale_after
            )
            plan: list[RolloutPlan] = []
            for agent in agents:
                if not self.hub.connected(agent.scale_id):
                    continue
                target = target_for(agent.channel, targets)
                if target is None:
                    continue
                current = version_key(agent.version)
                wanted = version_key(target.version)
                if current is None or wanted is None or current >= wanted:
                    continue
                agent_rows = updates.get(agent.id, [])
                if any(
                    r.status is AgentUpdateStatus.INSTALLED
                    and now - _aware(r.updated_at) < self.settle_after
                    for r in agent_rows
                ):
                    continue  # только что обновился — пусть устаканится
                row = next((r for r in agent_rows if r.version == target.version), None)
                if not self._should_send(row, now):
                    continue
                plan.append(RolloutPlan(agent.id, agent.scale_id, target))
            room = max(0, self.max_in_flight - in_flight)
            if len(plan) > room:
                logger.info(
                    "автовыкат: %d агентов ждут очереди (лимит одновременных %d)",
                    len(plan) - room,
                    self.max_in_flight,
                )
            return plan[:room]

    def _should_send(self, row: AgentUpdate | None, now: datetime) -> bool:
        if row is None:
            return True
        age = now - _aware(row.updated_at)
        if row.status in IN_FLIGHT:
            return age > self.stale_after and row.attempts < self.max_attempts
        if row.status is AgentUpdateStatus.FAILED:
            return row.attempts < self.max_attempts and age >= self.retry_after
        # INSTALLED (агент ниже цели — hello переведёт в rolled_back) и
        # ROLLED_BACK — только человек кнопкой
        return False


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Обзор для панели: экран «Релизы агентов»
# ---------------------------------------------------------------------------

# подписи статусов установки (пилюли по макету center-releases): ключ →
# (текст, класс пилюли)
STATUS_PILLS: dict[str, tuple[str, str]] = {
    "current": ("Актуально", "pill-ok"),
    "installed": ("Установлено", "pill-ok"),
    "commanded": ("Команда отправлена", "pill-info"),
    "started": ("Устанавливается", "pill-info"),
    "pending": ("Ждёт очереди", "pill-info"),
    "offline": ("Офлайн · получит при связи", ""),
    "failed": ("Ошибка", "pill-err"),
    "rolled_back": ("Откат", "pill-err"),
    "no_target": ("Каналу релиз не назначен", ""),
    "no_version": ("Версия неизвестна", ""),
}


@dataclass(frozen=True)
class AgentRolloutRow:
    """Строка блока «Раскатка»: агент, его цель и состояние установки."""

    agent_id: int
    scale_id: int
    site_name: str
    scale_name: str
    channel: ReleaseChannel
    version: str | None
    online: bool
    target: ReleaseInfo | None
    update: AgentUpdate | None  # строка журнала по целевой версии (или по текущей)
    status_key: str
    detail: str  # подпись под пилюлей: ошибка, заметка, номер попытки
    updated_at: datetime | None

    @property
    def status_text(self) -> str:
        return STATUS_PILLS.get(self.status_key, (self.status_key, ""))[0]

    @property
    def pill_class(self) -> str:
        return STATUS_PILLS.get(self.status_key, ("", ""))[1]

    @property
    def can_command(self) -> bool:
        """Кнопка «Обновить сейчас»/«Повторить»: есть цель выше текущей, агент на связи."""
        return (
            self.online
            and self.target is not None
            and self.status_key in {"pending", "failed", "rolled_back"}
        )


def _agent_status(
    agent: Agent, target: ReleaseInfo | None, rows: list[AgentUpdate], online: bool
) -> tuple[str, str, AgentUpdate | None]:
    """(ключ статуса, подпись, строка журнала) для агента относительно цели."""
    current = version_key(agent.version)
    if target is None:
        row = next((r for r in rows if r.version == agent.version), None)
        return ("no_target" if current is not None else "no_version"), "", row
    if current is None:
        return "no_version", "", None
    wanted = version_key(target.version)
    row = next((r for r in rows if r.version == target.version), None)
    if wanted is not None and current >= wanted:
        if row is not None and row.status is AgentUpdateStatus.INSTALLED:
            return "installed", row.note or "", row
        return "current", "", row
    if row is None:
        return ("pending" if online else "offline"), "", None
    if row.status is AgentUpdateStatus.FAILED:
        detail = row.error or ""
        if row.attempts > 1:
            detail = f"{detail} (попытка {row.attempts})"
        return "failed", detail, row
    if row.status is AgentUpdateStatus.ROLLED_BACK:
        detail = row.error or ""
        if row.running_version:
            detail = f"на {row.running_version}: {detail}"
        return "rolled_back", detail, row
    if row.status is AgentUpdateStatus.INSTALLED:
        # журнал говорит «установлено», а версия агента ниже: ждём hello
        return "installed", "ждём подтверждения версии от агента", row
    key = "started" if row.status is AgentUpdateStatus.STARTED else "commanded"
    if not online:
        return "offline", "команда отправлена, агент пропал со связи", row
    detail = f"попытка {row.attempts}" if row.attempts > 1 else ""
    return key, detail, row


def rollout_overview(
    session: Session, releases_dir: Path, *, connected: set[int]
) -> tuple[list[ReleaseInfo], dict[ReleaseChannel, ReleaseInfo], list[AgentRolloutRow]]:
    """Каталог, цели каналов и строки раскатки по всем агентам."""
    catalog = release_catalog(session, releases_dir)
    targets = channel_targets(catalog)
    updates = updates_by_agent(session)
    rows: list[AgentRolloutRow] = []
    query = (
        select(Agent, Scale, Site)
        .join(Scale, Agent.scale_id == Scale.id)
        .join(Site, Scale.site_id == Site.id)
        .order_by(Site.name, Scale.name)
    )
    for agent, scale, site in session.execute(query).all():
        online = agent.scale_id in connected
        target = target_for(agent.channel, targets)
        key, detail, row = _agent_status(agent, target, updates.get(agent.id, []), online)
        rows.append(
            AgentRolloutRow(
                agent_id=agent.id,
                scale_id=agent.scale_id,
                site_name=site.name,
                scale_name=scale.name,
                channel=agent.channel,
                version=agent.version,
                online=online,
                target=target,
                update=row,
                status_key=key,
                detail=detail,
                updated_at=row.updated_at if row is not None else None,
            )
        )
    return catalog, targets, rows


def release_stats(rows: list[AgentRolloutRow], version: str) -> dict[str, int]:
    """Сколько агентов на версии / с ошибкой или откатом по ней — для строки каталога."""
    on_version = sum(1 for r in rows if r.version == version)
    trouble = sum(
        1
        for r in rows
        if r.update is not None
        and r.update.version == version
        and r.update.status in (AgentUpdateStatus.FAILED, AgentUpdateStatus.ROLLED_BACK)
    )
    return {"on_version": on_version, "trouble": trouble}
