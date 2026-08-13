"""Запросы БД для веб-панели диспетчера (только чтение + вход).

Синхронные функции; маршруты панели зовут их через ``asyncio.to_thread``.
"""

import calendar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import Select, desc, func, or_, select
from sqlalchemy.orm import Session

from center.db.models import (
    Agent,
    Camera,
    MonitoringEvent,
    Scale,
    Site,
    TareRegistry,
    User,
    Weighing,
    WeighingPhoto,
)
from shared.enums import ErrorCode, Operation, WeighingSource
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


def dashboard_scales(session: Session, site_scope: int | None = None) -> list[DashboardScale]:
    """Сводка по весам: агент, статус, последняя операция.

    ``site_scope`` — объект, которым ограничен пользователь панели
    (диспетчер объекта видит только свои весы, решение 11.08.2026);
    None — видно всё (администратор и пользователи без привязки).
    """
    query = (
        select(Scale, Site, Agent)
        .join(Site, Site.id == Scale.site_id)
        .outerjoin(Agent, Agent.scale_id == Scale.id)
        .order_by(Site.name, Scale.name)
    )
    if site_scope is not None:
        query = query.where(Scale.site_id == site_scope)
    rows = session.execute(query).all()
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


_BISHKEK = ZoneInfo("Asia/Bishkek")


def weighings_today(session: Session, site_scope: int | None = None) -> tuple[int, int]:
    """Операций с начала бишкекских суток: (всего, из них тарирований).

    Счётчик дашборда (макет center-dashboard). Считаются успешные записи
    (иных в БД и нет) по моменту взвешивания.
    """
    day_start = (
        datetime.now(_BISHKEK).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(UTC)
    )
    query = (
        select(
            func.count(Weighing.id),
            func.count(Weighing.id).filter(Weighing.operation == Operation.TARING),
        )
        .select_from(Weighing)
        .where(Weighing.weighed_at >= day_start)
    )
    if site_scope is not None:
        query = query.join(Scale, Scale.id == Weighing.scale_id).where(Scale.site_id == site_scope)
    total, tarings = session.execute(query).one()
    return int(total), int(tarings)


def monitoring_events_page(
    session: Session,
    *,
    site_id: int | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[tuple[MonitoringEvent, Scale, Site]], int]:
    """Страница журнала событий мониторинга, новые первыми.

    ``site_id`` здесь — уже РАЗРЕШЁННЫЙ фильтр: маршрут сводит выбор
    пользователя с PanelScope до вызова (как в журнале взвешиваний).
    """
    query = (
        select(MonitoringEvent, Scale, Site)
        .join(Scale, Scale.id == MonitoringEvent.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .order_by(desc(MonitoringEvent.id))
    )
    count_query = select(func.count(MonitoringEvent.id))
    if site_id is not None:
        query = query.where(Scale.site_id == site_id)
        count_query = (
            count_query.select_from(MonitoringEvent)
            .join(Scale, Scale.id == MonitoringEvent.scale_id)
            .where(Scale.site_id == site_id)
        )
    total = int(session.execute(count_query).scalar_one())
    rows = session.execute(query.offset((page - 1) * page_size).limit(page_size)).all()
    return [tuple(row) for row in rows], total


@dataclass(frozen=True)
class JournalFilters:
    """Фильтры журнала (architecture §4.3)."""

    site_id: int | None = None
    scale_id: int | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    vehicle: str | None = None  # подстрока номера ТС/прицепа
    source: WeighingSource | None = None


def _journal_query(
    filters: JournalFilters, site_scope: int | None = None
) -> Select[tuple[Weighing, Scale, Site]]:
    query = (
        select(Weighing, Scale, Site)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        # только состоявшиеся операции (решение Игоря 10.08.2026): отказы
        # больше не сохраняются, а исторические ERR-строки скрываем
        .where(Weighing.code == ErrorCode.OK)
    )
    if site_scope is not None:
        # ограничение пользователя сильнее фильтра экрана
        query = query.where(Site.id == site_scope)
    elif filters.site_id is not None:
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
    session: Session,
    filters: JournalFilters,
    *,
    limit: int = 50,
    offset: int = 0,
    site_scope: int | None = None,
) -> tuple[list[tuple[Weighing, Scale, Site]], int]:
    """Страница журнала (новые первыми) и общее число записей под фильтром."""
    query = _journal_query(filters, site_scope)
    total = session.execute(select(func.count()).select_from(query.subquery())).scalar_one()
    rows = session.execute(
        query.order_by(
            desc(func.coalesce(Weighing.weighed_at, Weighing.created_at)), desc(Weighing.id)
        )
        .limit(limit)
        .offset(offset)
    ).all()
    return [tuple(row) for row in rows], int(total)


def journal_export_rows(
    session: Session,
    filters: JournalFilters,
    *,
    limit: int,
    site_scope: int | None = None,
) -> list[tuple[Weighing, Scale, Site]]:
    """Строки журнала для выгрузки: те же фильтры, но без страниц.

    ``limit`` — потолок выгрузки: файл на миллион строк не нужен ни Excel,
    ни браузеру; о срезе экран предупреждает.
    """
    query = _journal_query(filters, site_scope)
    rows = session.execute(
        query.order_by(
            desc(func.coalesce(Weighing.weighed_at, Weighing.created_at)), desc(Weighing.id)
        ).limit(limit)
    ).all()
    return [tuple(row) for row in rows]


@dataclass(frozen=True)
class WeighingCard:
    """Карточка записи журнала: запись, весы, объект, фото, связанная тара."""

    weighing: Weighing
    scale: Scale
    site: Site
    photos: list[WeighingPhoto]
    tare_weighing: Weighing | None
    storno_of: Weighing | None


def weighing_card(
    session: Session, weighing_id: int, *, site_scope: int | None = None
) -> WeighingCard | None:
    """Карточка записи; ``site_scope`` — объект пользователя: чужая запись
    для него не существует (404), а не «нет доступа»."""
    query = (
        select(Weighing, Scale, Site)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .where(Weighing.id == weighing_id)
    )
    if site_scope is not None:
        query = query.where(Site.id == site_scope)
    row = session.execute(query).one_or_none()
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
    session: Session,
    *,
    search: str | None = None,
    site_id: int | None = None,
    scale_id: int | None = None,
    limit: int = 100,
    offset: int = 0,
    site_scope: int | None = None,
) -> tuple[list[tuple[TareRegistry, Weighing, Scale, Site]], int]:
    """Реестр активных тар (действующие сверху, просроченные не показываем).

    ``site_id``/``scale_id`` — фильтры экрана: на объекте бывает несколько
    весов (решение 11.08.2026), и тарирования разных весов нужно разделять.
    """
    threshold = three_months_before(datetime.now(UTC))
    query = (
        select(TareRegistry, Weighing, Scale, Site)
        .join(Weighing, Weighing.id == TareRegistry.weighing_id)
        .join(Scale, Scale.id == Weighing.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .where(TareRegistry.tared_at >= threshold)
    )
    if site_scope is not None:
        # ограничение пользователя сильнее фильтра экрана
        query = query.where(Site.id == site_scope)
    elif site_id is not None:
        query = query.where(Site.id == site_id)
    if scale_id is not None:
        query = query.where(Scale.id == scale_id)
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
class FilterOptions:
    """Данные селекторов «Объект» и «Весы» для списков панели.

    Отдельно от RefsData: спискам не нужны камеры и агенты, а URL камер
    содержат пароли — в контекст чужих экранов их класть незачем.
    """

    sites: list[Site]
    scales: list[tuple[Scale, Site]]


def _scales_with_sites(site_id: int | None = None) -> Select[tuple[Scale, Site]]:
    """Весы с их объектами в предсказуемом порядке (объект, затем весы)."""
    query = (
        select(Scale, Site)
        .join(Site, Site.id == Scale.site_id)
        .order_by(Site.name, Scale.name, Scale.id)
    )
    if site_id is not None:
        query = query.where(Scale.site_id == site_id)
    return query


def filter_options(session: Session, site_scope: int | None = None) -> FilterOptions:
    """Объекты и весы для фильтров журнала и реестра тарирований.

    При ограничении по объекту в списках остаётся только он: показывать
    диспетчеру чужие объекты в селекторе незачем.
    """
    sites_query = select(Site).order_by(Site.name)
    scales_query = _scales_with_sites()
    if site_scope is not None:
        sites_query = sites_query.where(Site.id == site_scope)
        scales_query = scales_query.where(Scale.site_id == site_scope)
    sites = list(session.execute(sites_query).scalars())
    scales = [tuple(row) for row in session.execute(scales_query).all()]
    return FilterOptions(sites=sites, scales=scales)


@dataclass(frozen=True)
class RefsData:
    """Справочники: объекты, весы, камеры, агенты."""

    sites: list[Site]
    scales: list[tuple[Scale, Site]]
    # объект в строках камер и агентов нужен явно: названия весов на разных
    # объектах совпадают («Весы SCS-80»), без объекта строки неоднозначны
    cameras: list[tuple[Camera, Scale, Site]]
    agents: list[tuple[Agent, Scale, Site]]


def refs_data(
    session: Session, site_id: int | None = None, *, site_scope: int | None = None
) -> RefsData:
    """Справочники; ``site_id`` сужает весы/камеры/агентов до одного объекта
    (фильтр экрана, запрос Игоря 11.08.2026 — на 13 объектах без него
    страница нечитаема). Список sites всегда полный: он нужен селекторам.
    """
    # пользователь, привязанный к объекту, видит только его — в том числе
    # в селекторах (решение 11.08.2026)
    if site_scope is not None:
        site_id = site_scope
    sites_query = select(Site).order_by(Site.name)
    if site_scope is not None:
        sites_query = sites_query.where(Site.id == site_scope)
    sites = list(session.execute(sites_query).scalars())
    # порядок строк фиксируем до имени весов: сортировки только по объекту
    # не хватало — двое весов одного объекта возвращались в произвольном
    # порядке и прыгали между обновлениями страницы (11.08.2026)
    scales_query = _scales_with_sites(site_id)
    cameras_query = (
        select(Camera, Scale, Site)
        .join(Scale, Scale.id == Camera.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .order_by(Site.name, Scale.name, Scale.id, Camera.role)
    )
    agents_query = (
        select(Agent, Scale, Site)
        .join(Scale, Scale.id == Agent.scale_id)
        .join(Site, Site.id == Scale.site_id)
        .order_by(Site.name, Scale.name, Scale.id)
    )
    if site_id is not None:
        cameras_query = cameras_query.where(Scale.site_id == site_id)
        agents_query = agents_query.where(Scale.site_id == site_id)
    scales = [tuple(r) for r in session.execute(scales_query).all()]
    cameras = [tuple(r) for r in session.execute(cameras_query).all()]
    agents = [tuple(r) for r in session.execute(agents_query).all()]
    return RefsData(sites=sites, scales=scales, cameras=cameras, agents=agents)
