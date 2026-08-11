"""Запросы БД для веб-панели диспетчера (только чтение + вход).

Синхронные функции; маршруты панели зовут их через ``asyncio.to_thread``.
"""

import calendar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, desc, func, or_, select
from sqlalchemy.orm import Session

from center.db.models import (
    Agent,
    Camera,
    Scale,
    Site,
    TareRegistry,
    User,
    Weighing,
    WeighingPhoto,
)
from shared.enums import ErrorCode, WeighingSource
from shared.passwords import verify_password
from shared.tare import three_months_before


def verify_user(session: Session, login: str, password: str) -> User | None:
    """Вход в панель; None — неверные данные или пользователь отключён."""
    user = session.execute(
        select(User).where(User.login == login, User.is_active)
    ).scalar_one_or_none()
    if user is None or not verify_password(password, user.pw_hash):
        return None
    return user


@dataclass(frozen=True)
class DashboardScale:
    """Карточка весов на дашборде объектов."""

    site: Site
    scale: Scale
    agent: Agent | None
    last_weighing: Weighing | None


def dashboard_scales(session: Session) -> list[DashboardScale]:
    """Сводка по всем весам: агент, статус, последняя операция."""
    rows = session.execute(
        select(Scale, Site, Agent)
        .join(Site, Site.id == Scale.site_id)
        .outerjoin(Agent, Agent.scale_id == Scale.id)
        .order_by(Site.name, Scale.name)
    ).all()
    result = []
    for scale, site, agent in rows:
        last = session.execute(
            select(Weighing)
            .where(Weighing.scale_id == scale.id)
            .order_by(desc(Weighing.created_at))
            .limit(1)
        ).scalar_one_or_none()
        result.append(DashboardScale(site=site, scale=scale, agent=agent, last_weighing=last))
    return result


@dataclass(frozen=True)
class JournalFilters:
    """Фильтры журнала (architecture §4.3)."""

    site_id: int | None = None
    scale_id: int | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    vehicle: str | None = None  # подстрока номера ТС/прицепа
    source: WeighingSource | None = None


def _journal_query(filters: JournalFilters) -> Select[tuple[Weighing, Scale, Site]]:
    query = (
        select(Weighing, Scale, Site)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        # только состоявшиеся операции (решение Игоря 10.08.2026): отказы
        # больше не сохраняются, а исторические ERR-строки скрываем
        .where(Weighing.code == ErrorCode.OK)
    )
    if filters.site_id is not None:
        query = query.where(Site.id == filters.site_id)
    if filters.scale_id is not None:
        query = query.where(Scale.id == filters.scale_id)
    moment = func.coalesce(Weighing.weighed_at, Weighing.created_at)
    if filters.date_from is not None:
        query = query.where(moment >= filters.date_from)
    if filters.date_to is not None:
        # включительно по конец суток
        query = query.where(moment < filters.date_to + timedelta(days=1))
    if filters.vehicle:
        needle = f"%{filters.vehicle.strip().upper()}%"
        query = query.where(
            or_(Weighing.vehicle_number.like(needle), Weighing.trailer_number.like(needle))
        )
    if filters.source is not None:
        query = query.where(Weighing.source == filters.source)
    return query


def journal_page(
    session: Session, filters: JournalFilters, *, limit: int = 50, offset: int = 0
) -> tuple[list[tuple[Weighing, Scale, Site]], int]:
    """Страница журнала (новые первыми) и общее число записей под фильтром."""
    query = _journal_query(filters)
    total = session.execute(select(func.count()).select_from(query.subquery())).scalar_one()
    rows = session.execute(
        query.order_by(
            desc(func.coalesce(Weighing.weighed_at, Weighing.created_at)), desc(Weighing.id)
        )
        .limit(limit)
        .offset(offset)
    ).all()
    return [tuple(row) for row in rows], int(total)


@dataclass(frozen=True)
class WeighingCard:
    """Карточка записи журнала: запись, весы, объект, фото, связанная тара."""

    weighing: Weighing
    scale: Scale
    site: Site
    photos: list[WeighingPhoto]
    tare_weighing: Weighing | None
    storno_of: Weighing | None


def weighing_card(session: Session, weighing_id: int) -> WeighingCard | None:
    row = session.execute(
        select(Weighing, Scale, Site)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .where(Weighing.id == weighing_id)
    ).one_or_none()
    if row is None:
        return None
    weighing, scale, site = row
    photos = list(
        session.execute(
            select(WeighingPhoto)
            .where(WeighingPhoto.weighing_id == weighing.id)
            .order_by(WeighingPhoto.role)
        ).scalars()
    )
    tare = session.get(Weighing, weighing.tare_weighing_id) if weighing.tare_weighing_id else None
    storno = session.get(Weighing, weighing.storno_of) if weighing.storno_of else None
    return WeighingCard(
        weighing=weighing,
        scale=scale,
        site=site,
        photos=photos,
        tare_weighing=tare,
        storno_of=storno,
    )


def tare_expires_at(tared_at: datetime) -> datetime:
    """Когда тара перестанет действовать (3 календарных месяца вперёд).

    День сохраняется с поджатием к длине целевого месяца (31 авг → 30 ноя) —
    симметрично shared.tare.three_months_before.
    """
    month = tared_at.month + 3
    year = tared_at.year
    if month > 12:
        month -= 12
        year += 1
    last_day = calendar.monthrange(year, month)[1]
    return tared_at.replace(year=year, month=month, day=min(tared_at.day, last_day))


def tare_list(
    session: Session, *, search: str | None = None, limit: int = 100, offset: int = 0
) -> tuple[list[tuple[TareRegistry, Weighing, Scale, Site]], int]:
    """Реестр активных тар (действующие сверху, просроченные не показываем)."""
    threshold = three_months_before(datetime.now(UTC))
    query = (
        select(TareRegistry, Weighing, Scale, Site)
        .join(Weighing, Weighing.id == TareRegistry.weighing_id)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .where(TareRegistry.tared_at >= threshold)
    )
    if search:
        query = query.where(TareRegistry.vehicle_number.like(f"%{search.strip().upper()}%"))
    total = session.execute(select(func.count()).select_from(query.subquery())).scalar_one()
    rows = session.execute(
        query.order_by(desc(TareRegistry.tared_at)).limit(limit).offset(offset)
    ).all()
    return [tuple(row) for row in rows], int(total)


def photos_for_weighings(session: Session, weighing_ids: list[int]) -> dict[int, dict[str, str]]:
    """Пути фото для строк списков: weighing_id → {'front': path, 'rear': path}.

    Миниатюра выводится подстановкой суффикса _thumb в шаблоне
    (center/photos: миниатюра строится один раз при приёме файла).
    """
    if not weighing_ids:
        return {}
    result: dict[int, dict[str, str]] = {}
    rows = session.execute(
        select(WeighingPhoto).where(WeighingPhoto.weighing_id.in_(weighing_ids))
    ).scalars()
    for photo in rows:
        result.setdefault(photo.weighing_id, {})[photo.role.value] = photo.path
    return result


@dataclass(frozen=True)
class RefsData:
    """Справочники: объекты, весы, камеры, агенты."""

    sites: list[Site]
    scales: list[tuple[Scale, Site]]
    cameras: list[tuple[Camera, Scale]]
    agents: list[tuple[Agent, Scale]]


def refs_data(session: Session, site_id: int | None = None) -> RefsData:
    """Справочники; ``site_id`` сужает весы/камеры/агентов до одного объекта
    (фильтр экрана, запрос Игоря 11.08.2026 — на 13 объектах без него
    страница нечитаема). Список sites всегда полный: он нужен селекторам.
    """
    sites = list(session.execute(select(Site).order_by(Site.name)).scalars())
    scales_query = select(Scale, Site).join(Site, Site.id == Scale.site_id).order_by(Site.name)
    cameras_query = select(Camera, Scale).join(Scale, Scale.id == Camera.scale_id)
    agents_query = select(Agent, Scale).join(Scale, Scale.id == Agent.scale_id)
    if site_id is not None:
        scales_query = scales_query.where(Scale.site_id == site_id)
        cameras_query = cameras_query.where(Scale.site_id == site_id)
        agents_query = agents_query.where(Scale.site_id == site_id)
    scales = [tuple(r) for r in session.execute(scales_query).all()]
    cameras = [tuple(r) for r in session.execute(cameras_query.order_by(Scale.id)).all()]
    agents = [tuple(r) for r in session.execute(agents_query.order_by(Scale.id)).all()]
    return RefsData(sites=sites, scales=scales, cameras=cameras, agents=agents)
