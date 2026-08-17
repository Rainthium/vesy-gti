"""Веб-панель диспетчера (FastAPI + Jinja2 + HTMX, architecture §4.3).

Экраны: вход, дашборд объектов, журнал с фильтрами и карточкой записи,
реестр тарирований, справочники (чтение; правило №2: записанные операции
неизменны, редактирование справочников пока в CLI ``tools/center_admin.py``)
и администрирование пользователей (``/panel/users``, только admin —
права сверяются с БД на каждый запрос, см. ``users_admin``).

Доступ по учёткам users (роли dispatcher/operator видят одно и то же —
разграничение по объектам появится с ролями операторов).
Фото пользователям панели отдаются через ``/panel/photos/...`` со своей
сессией (сервисные токены /vesy/... — только для интеграторов).
"""

import asyncio
import contextlib
import csv
import io
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentHubError
from center.db import repo
from center.db.models import (
    Agent,
    AuditLog,
    MonitoringSeverity,
    ReleaseChannel,
    Scale,
    ScaleKind,
    Site,
    TareRegistry,
    UserRole,
    Weighing,
)
from center.monitoring import KIND_LABELS, ActiveAlert, MonitoringService, MonitoringThresholds
from center.releases import AgentRelease, latest_release
from center.web import queries, refs_admin, users_admin
from shared import card as weight_card
from shared.enums import CameraRole, ErrorCode, Operation, WeighingSource
from shared.messages import (
    CycleSettings,
    EquipmentStatus,
    OperatorsRegistryUpdate,
    ScaleConfigUpdate,
    UpdateCommand,
    VerificationInfo,
    supports_log_tail,
    supports_secure_sync,
)
from shared.tare import three_months_before

logger = logging.getLogger(__name__)

LOG_TAIL_LINES = 200  # сколько строк журнала просить у агента
EXPORT_LIMIT = 20_000  # потолок строк выгрузки (Excel и браузер не резиновые)

SessionFactory = Callable[[], Session]

TEMPLATES_DIR = Path(__file__).parent / "templates"
BISHKEK = ZoneInfo("Asia/Bishkek")
PAGE_SIZE = 50
MAX_DB_ID = 2**31 - 1  # id таблиц — int4; больше в запрос пускать нельзя


def _csv_number(value: float | None) -> str:
    """Число для CSV: пусто вместо None, вес целыми килограммами."""
    if value is None:
        return ""
    return f"{value:.0f}"


def _csv_text(value: str | None) -> str:
    """Текст для CSV с защитой от формул Excel.

    Номера ТС приходят из АИС и ручного ввода оператора: значение вроде
    «=1+1» Excel вычислил бы при открытии файла (находка ревью
    11.08.2026). Апостроф впереди заставляет показать текст как есть.
    """
    text = (value or "").strip()
    if text[:1] in {"=", "+", "-", "@"}:  # табуляция и \r срезаны strip
        return "'" + text
    return text


def _fmt_dt(value: datetime | None) -> str:
    """ДД.ММ.ГГГГ ЧЧ:ММ:СС в бишкекском времени (как журнал UniServer)."""
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BISHKEK).strftime("%d.%m.%Y %H:%M:%S")


def _fmt_time(value: datetime | None) -> str:
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BISHKEK).strftime("%H:%M")


def _fmt_kg(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:,.0f}".replace(",", " ")


def _plural_ru(n: int, one: str, few: str, many: str) -> str:
    """Русское склонение при числительном: 1 запись, 2 записи, 5 записей."""
    if n % 10 == 1 and n % 100 != 11:
        return one
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return few
    return many


def _tare_status(weighing: Weighing, registry: TareRegistry | None, threshold: datetime) -> str:
    """Статус записи в истории тарирований (пилюля экрана).

    Строка реестра есть только у последнего тарирования своей сцепки;
    тарирование без номера ТС в реестр не попадает и в расчёте нетто
    не участвует никогда.
    """
    if not weighing.vehicle_number:
        return "no_vehicle"
    if registry is None:
        return "replaced"
    return "active" if registry.tared_at >= threshold else "expired"


def create_panel_router(
    session_factory: SessionFactory,
    hub: AgentHub,
    *,
    photos_dir: Path,
    releases_dir: Path | None = None,
    monitor: MonitoringService | None = None,
) -> APIRouter:
    """Маршруты панели; сессии — общий SessionMiddleware приложения центра.

    ``monitor`` — сервис мониторинга (активные алерты и счётчики дашборда);
    None допустим в тестах отдельных экранов — блок алертов тогда пуст.
    """
    router = APIRouter(prefix="/panel")

    # второй каталог — общая с агентом печатная форма весовой карточки
    templates = Jinja2Templates(directory=[str(TEMPLATES_DIR), str(weight_card.TEMPLATES_DIR)])
    templates.env.filters["fmt_dt"] = _fmt_dt
    templates.env.filters["fmt_time"] = _fmt_time
    templates.env.filters["fmt_kg"] = _fmt_kg
    # подписи типов событий мониторинга (экран «События», блок алертов)
    templates.env.filters["kind_label"] = lambda kind: KIND_LABELS.get(kind, kind)
    templates.env.globals["expires"] = queries.tare_expires_at
    # метка старта процесса — сброс браузерного кэша статики при деплое
    templates.env.globals["static_v"] = str(int(datetime.now(UTC).timestamp()))

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    def _safe_next_path(raw: str) -> str | None:
        """Внутренний путь панели для возврата после входа либо None.

        Только пути под /panel/ без обратных слэшей (браузеры нормализуют
        «\\» в «/») и без сегментов «..» (нормализация вывела бы путь
        из-под панели) — форма входа не должна уметь уводить на чужой
        адрес (open redirect); замечания ревью 13.08.2026.
        """
        if not raw.startswith("/panel/") or "\\" in raw:
            return None
        if ".." in raw.split("/"):
            return None
        return raw

    # служебные пути не годятся в next: после входа человек увидел бы
    # голый HTMX-фрагмент или JPEG вместо страницы (замечание ревью)
    _NO_NEXT_PREFIXES = ("/panel/fragments/", "/panel/photos/")

    def _login_redirect(request: Request) -> HTTPException:
        """303 на вход; запрошенный путь уезжает в ?next= — после входа
        пользователь попадает туда, куда шёл (например, на печать
        карточки из новой вкладки), а не на дашборд."""
        if request.headers.get("hx-request"):
            # HTMX-опрос (дашборд каждые N секунд) с протухшей сессией:
            # 303 браузер разворачивает прозрачно, и htmx вставил бы форму
            # входа ВНУТРЬ фрагмента; HX-Redirect уводит на вход целиком
            return HTTPException(status_code=200, headers={"HX-Redirect": "/panel/login"})
        location = "/panel/login"
        path = request.url.path
        if path not in ("/panel/", "/panel") and not path.startswith(_NO_NEXT_PREFIXES):
            location += f"?next={quote(path, safe='/')}"
        return HTTPException(status_code=303, headers={"Location": location})

    def current_user(request: Request) -> str:
        user = request.session.get("panel_user")
        if not user:
            raise _login_redirect(request)
        return str(user)

    PanelUser = Annotated[str, Depends(current_user)]

    async def current_scope(request: Request) -> int | None:
        """Объект, которым ограничен пользователь панели (None — все).

        Права берём из БД на каждый запрос, как и у админа: смена
        привязки или роли применяется сразу, без перевхода. Админ видит
        все объекты; остальные — только свой, если он задан (решение
        11.08.2026, перед тиражом на 13 объектов).
        """
        login = request.session.get("panel_login")
        if not login:
            raise _login_redirect(request)
        active, site_id = await asyncio.to_thread(
            _db, lambda s: users_admin.visible_site_id(s, str(login))
        )
        if not active:
            # учётку отключили при живой сессии — на вход, а не «видно всё»
            raise _login_redirect(request)
        return site_id

    PanelScope = Annotated[int | None, Depends(current_scope)]

    def render(template: str, request: Request, **extra: Any) -> HTMLResponse:
        context: dict[str, Any] = {"request": request, **extra}
        return templates.TemplateResponse(request, template, context)

    # --- вход/выход ---

    @router.get("/login", response_class=HTMLResponse)
    def login_page(request: Request, next: str = "") -> HTMLResponse:
        return render("login.html", request, error=None, next=_safe_next_path(next) or "")

    @router.post("/login", response_class=HTMLResponse)
    async def login_submit(
        request: Request,
        login: Annotated[str, Form()],
        password: Annotated[str, Form()],
        next: Annotated[str, Form()] = "",
    ) -> Response:
        user = await asyncio.to_thread(
            _db, lambda s: queries.verify_user(s, login.strip(), password)
        )
        if user is None:
            logger.warning("панель: неудачный вход %s", login.strip())
            return render(
                "login.html",
                request,
                error="Неверный логин или пароль",
                next=_safe_next_path(next) or "",
            )
        request.session["panel_user"] = user.full_name or user.login
        request.session["panel_login"] = user.login
        request.session["panel_role"] = user.role.value
        logger.info("панель: вход %s (%s)", user.login, user.role.value)
        return RedirectResponse(_safe_next_path(next) or "/panel/", status_code=303)

    @router.post("/logout")
    def logout(request: Request) -> RedirectResponse:
        request.session.pop("panel_user", None)
        request.session.pop("panel_login", None)
        request.session.pop("panel_role", None)
        return RedirectResponse("/panel/login", status_code=303)

    async def current_admin(request: Request) -> str:
        """Логин администратора; права проверяются по БД, не по сессии —
        разжалованный или отключённый админ теряет экран сразу."""
        login = request.session.get("panel_login")
        if not login:
            raise _login_redirect(request)
        ok = await asyncio.to_thread(_db, lambda s: users_admin.is_active_admin(s, str(login)))
        if not ok:
            raise HTTPException(status_code=403, detail="Доступ только администраторам")
        return str(login)

    PanelAdmin = Annotated[str, Depends(current_admin)]

    # --- экраны ---

    def _equipment_map(scales: list[queries.DashboardScale]) -> dict[int, EquipmentStatus]:
        """Последняя самодиагностика агентов (индикатор + камеры) по весам."""
        return {
            item.scale.id: equipment
            for item in scales
            if (equipment := hub.equipment(item.scale.id)) is not None
        }

    async def _latest_release() -> AgentRelease | None:
        if releases_dir is None:
            return None
        return await asyncio.to_thread(latest_release, releases_dir)

    def _dashboard_counters(
        scales: list[queries.DashboardScale],
        online_map: dict[int, bool],
        equipment: dict[int, EquipmentStatus],
        alerts: list[ActiveAlert],
        today: tuple[int, int],
    ) -> list[dict[str, str]]:
        """Четыре счётчика дашборда (по макету center-dashboard)."""
        with_agent = [item for item in scales if item.agent]
        offline = [item for item in with_agent if not online_map.get(item.scale.id)]
        if not offline:
            online_sub = "все агенты онлайн" if with_agent else "агенты не заведены"
        else:
            online_sub = f"{offline[0].site.name} офлайн"
            if len(offline) > 1:
                online_sub += f" и ещё {len(offline) - 1}"
        total_today, tarings_today = today
        danger_count = sum(1 for alert in alerts if alert.severity is MonitoringSeverity.DANGER)
        backlog_records = sum(eq.pending_sync_count for eq in equipment.values())
        backlog_photos = sum(eq.pending_photos_count or 0 for eq in equipment.values())
        backlog = backlog_records + backlog_photos
        return [
            {
                "k": "Весов на связи",
                "v": f"{len(with_agent) - len(offline)} / {len(with_agent)}",
                "cls": "warn" if offline else "ok",
                "sub": online_sub,
            },
            {
                "k": "Взвешиваний сегодня",
                "v": str(total_today),
                "cls": "",
                "sub": (
                    f"из них {tarings_today} "
                    + _plural_ru(tarings_today, "тарирование", "тарирования", "тарирований")
                ),
            },
            {
                "k": "Активных алертов",
                "v": str(len(alerts)),
                "cls": "danger" if danger_count else ("warn" if alerts else "ok"),
                "sub": (
                    f"{danger_count} "
                    + _plural_ru(danger_count, "критичный", "критичных", "критичных")
                    if danger_count
                    else "всё спокойно"
                ),
            },
            {
                "k": "Очередь досылки",
                "v": str(backlog),
                "cls": "warn" if backlog else "ok",
                # очереди видны только по агентам на связи (heartbeat)
                "sub": (
                    f"записей: {backlog_records} · снимков: {backlog_photos}"
                    if backlog
                    else "всё синхронизировано"
                ),
            },
        ]

    async def _dashboard_context(scope: int | None) -> dict[str, Any]:
        """Общий контекст дашборда и его HTMX-фрагмента (счётчики + алерты
        обновляются тем же опросом, что и сетка объектов)."""
        scales = await asyncio.to_thread(_db, lambda s: queries.dashboard_scales(s, scope))
        today = await asyncio.to_thread(_db, lambda s: queries.weighings_today(s, scope))
        equipment = _equipment_map(scales)
        alerts = monitor.active_alerts(scope) if monitor is not None else []
        # «на связи» — как у детектора офлайна: статус online И свежий
        # heartbeat; иначе счётчик показывал бы «все агенты онлайн» рядом
        # с danger-алертом о молчащем агенте (замечание ревью 13.08.2026)
        now = datetime.now(UTC)
        stale_s = MonitoringThresholds().stale_heartbeat_s
        online_map: dict[int, bool] = {}
        for item in scales:
            last_seen = item.agent.last_seen_at if item.agent else None
            if last_seen is not None and last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=UTC)
            online_map[item.scale.id] = bool(
                item.agent
                and item.agent.status.value == "online"
                and last_seen is not None
                and (now - last_seen).total_seconds() <= stale_s
            )
        return {
            "scales": scales,
            "equipment": equipment,
            "alerts": alerts,
            "online_map": online_map,
            "counters": _dashboard_counters(scales, online_map, equipment, alerts, today),
            "all_good": not alerts
            and all(item.agent is None or online_map.get(item.scale.id) for item in scales),
            "release": await _latest_release(),
        }

    @router.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request, user: PanelUser, scope: PanelScope) -> HTMLResponse:
        context = await _dashboard_context(scope)
        return render(
            "dashboard.html",
            request,
            user=user,
            update_note=request.query_params.get("update_note"),
            **context,
        )

    @router.get("/fragments/dashboard", response_class=HTMLResponse)
    async def dashboard_fragment(
        request: Request, user: PanelUser, scope: PanelScope
    ) -> HTMLResponse:
        context = await _dashboard_context(scope)
        return render("fragments/dashboard_grid.html", request, user=user, **context)

    @router.post("/scales/{scale_id}/update-agent")
    async def update_agent(request: Request, admin: PanelAdmin, scale_id: int) -> RedirectResponse:
        """Разослать агенту команду автообновления до актуального релиза.

        Только админ: перезапуск службы — простой весов, и запускать его
        чужому объекту по подобранному scale_id нельзя (находка ревью
        11.08.2026: прежде маршрут требовал лишь входа в панель).
        """
        release = await _latest_release()
        if release is None:
            note = "релизы агента на центр не выложены"
        elif not hub.connected(scale_id):
            note = "агент не в сети — обновление невозможно"
        else:
            command = UpdateCommand(
                version=release.version,
                url_path=f"/agents/releases/{release.filename}",
                sha256=release.sha256,
                size_bytes=release.size_bytes,
            )
            try:
                await hub.send_update_command(scale_id, command)
                note = f"команда обновления до v{release.version} отправлена агенту"
                logger.info("панель (%s): весы %d — %s", admin, scale_id, note)
            except AgentHubError as exc:
                note = str(exc)
        return RedirectResponse(f"/panel/?update_note={quote(note)}", status_code=303)

    def _agent_of(session: Session, scale_id: int) -> Agent | None:
        return session.execute(select(Agent).where(Agent.scale_id == scale_id)).scalar_one_or_none()

    def _photo_belongs_to_site(session: Session, file_path: str, site_id: int) -> bool:
        """Принадлежит ли снимок объекту (uuid из имени файла → весы → объект).

        Канонический путь центра — ``/vesy/ГГГГ/ММ/ДД/<uuid.hex>_photoN.jpeg``,
        поэтому запись находится по uuid: он уникален и проиндексирован, а
        поиск по самому пути был бы полным просмотром таблицы (замечание
        ревью 11.08.2026). Миниатюры (``..._thumb.jpeg``) в БД не числятся —
        они производные от кадра и живут по правам оригинала.
        """
        name = Path(file_path).name
        hex_part = name.split("_", 1)[0]
        try:
            record_uuid = UUID(hex=hex_part)
        except ValueError:
            return False
        return (
            session.execute(
                select(Weighing.id)
                .join(Scale, Scale.id == Weighing.scale_id)
                .where(Weighing.uuid == record_uuid, Scale.site_id == site_id)
            ).first()
            is not None
        )

    def _audit_log_view(session: Session, user: str, scale_id: int) -> None:
        """Кто и когда смотрел журнал объекта (как у скачивания фото)."""
        session.add(
            AuditLog(
                actor=f"panel:{user}",
                action="agent_log_view",
                details={"scale_id": scale_id},
            )
        )
        session.commit()

    @router.post("/scales/{scale_id}/log", response_class=HTMLResponse)
    async def agent_log_page(
        request: Request, user: PanelUser, admin: PanelAdmin, scale_id: int
    ) -> HTMLResponse:
        """Журнал агента, запрошенный у него по WS (удалённая диагностика).

        Только админ: в строках лога может оказаться чувствительное
        (адреса камер, диагностика сети объекта). Каждый запрос пишется
        в аудит — кто и когда смотрел журнал объекта.

        POST, не GET: у запроса побочные эффекты (WS-команда агенту +
        запись в аудит), а SameSite=Lax шлёт cookie при top-level GET
        по кросс-сайтовой ссылке — чужая страница могла бы дёргать
        журнал от имени залогиненного админа (закрытие риска, принятого
        на пилоте 13.08.2026).
        """
        row = await asyncio.to_thread(
            _db,
            lambda s: (
                None
                if (scale := s.get(Scale, scale_id)) is None
                else (scale, s.get(Site, scale.site_id), _agent_of(s, scale_id))
            ),
        )
        if row is None:
            raise HTTPException(status_code=404)
        scale, site, agent = row
        lines: list[str] = []
        location = ""
        error: str | None = None
        if agent is None:
            error = "агент для этих весов не заведён"
        elif not hub.connected(scale_id):
            error = "агент не в сети — журнал доступен только при связи"
        elif not supports_log_tail(agent.version):
            error = (
                f"агент версии {agent.version or '—'} не умеет присылать журнал "
                "(нужна 0.4.5 или новее — обновите агента кнопкой на дашборде)"
            )
        else:
            try:
                response = await hub.request_log_tail(scale_id, lines=LOG_TAIL_LINES)
                lines, location = response.lines, response.location
                await asyncio.to_thread(_db, lambda s: _audit_log_view(s, admin, scale_id))
                logger.info("панель (%s): запрошен журнал агента весов %d", admin, scale_id)
            except AgentHubError as exc:
                error = str(exc)
        return render(
            "agent_log.html",
            request,
            user=user,
            scale=scale,
            site=site,
            lines=lines,
            location=location,
            error=error,
        )

    def _parse_id(raw: str) -> int | None:
        """Числовой параметр фильтра из строки запроса.

        Селекты формы шлют пустое значение («Все объекты»), а параметр
        типа ``int | None`` на пустой строке даёт 422 — поэтому фильтры
        принимаем строками. Мусор трактуем как «без фильтра».
        """
        raw = raw.strip()
        # isascii: юникодные «цифры» (³, ٢) проходят isdigit, но роняют int
        if not raw or not (raw.isascii() and raw.isdigit()):
            return None
        value = int(raw)
        # id в БД — int4: без верхней границы запрос падал бы 500-й
        # (NumericValueOutOfRange), а не показывал список без фильтра
        if not 0 < value <= MAX_DB_ID:
            return None
        return value

    def _scale_of_site(
        refs: queries.FilterOptions, scale_id: int | None, site_id: int | None
    ) -> int | None:
        """Сбросить выбор весов, если они не с выбранного объекта.

        Иначе после смены объекта в фильтре оставался бы висеть scale_id
        соседнего объекта и список молча оказывался бы пустым.
        """
        if scale_id is None or site_id is None:
            return scale_id
        if any(scale.id == scale_id and scale.site_id == site_id for scale, _ in refs.scales):
            return scale_id
        return None

    def _parse_filters(
        site_id: int | None,
        scale_id: int | None,
        date_from: str | None,
        date_to: str | None,
        vehicle: str | None,
        source: str | None,
    ) -> queries.JournalFilters:
        def parse_date(raw: str | None) -> datetime | None:
            if not raw:
                return None
            try:
                return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=BISHKEK)
            except ValueError:
                return None

        parsed_source = None
        if source in (WeighingSource.AIS.value, WeighingSource.LOCAL_OFFLINE.value):
            parsed_source = WeighingSource(source)
        return queries.JournalFilters(
            site_id=site_id,
            scale_id=scale_id,
            date_from=parse_date(date_from),
            date_to=parse_date(date_to),
            vehicle=vehicle or None,
            source=parsed_source,
        )

    @router.get("/journal", response_class=HTMLResponse)
    async def journal(
        request: Request,
        user: PanelUser,
        scope: PanelScope,
        site_id: str = "",
        scale_id: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        vehicle: str | None = None,
        source: str | None = None,
        page: int = 1,
    ) -> HTMLResponse:
        refs = await asyncio.to_thread(_db, lambda s: queries.filter_options(s, scope))
        site = _parse_id(site_id)
        scale = _scale_of_site(refs, _parse_id(scale_id), site)
        filters = _parse_filters(site, scale, date_from, date_to, vehicle, source)
        page = max(1, page)
        rows, total = await asyncio.to_thread(
            _db,
            lambda s: queries.journal_page(
                s, filters, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE, site_scope=scope
            ),
        )
        photos = await asyncio.to_thread(
            _db, lambda s: queries.photos_for_weighings(s, [w.id for w, _, _ in rows])
        )
        return render(
            "journal.html",
            request,
            user=user,
            rows=rows,
            photos=photos,
            total=total,
            page=page,
            pages=max(1, -(-total // PAGE_SIZE)),
            refs=refs,
            filters={
                "site_id": site,
                "scale_id": scale,
                "date_from": date_from or "",
                "date_to": date_to or "",
                "vehicle": vehicle or "",
                "source": source or "",
            },
        )

    @router.get("/journal/export.csv")
    async def journal_export(
        request: Request,
        user: PanelUser,
        scope: PanelScope,
        site_id: str = "",
        scale_id: str = "",
        date_from: str | None = None,
        date_to: str | None = None,
        vehicle: str | None = None,
        source: str | None = None,
    ) -> Response:
        """Выгрузка журнала под текущими фильтрами (architecture §4.3).

        CSV с BOM и точкой с запятой: Excel открывает такой файл двойным
        щелчком, без мастера импорта. Ограничения пользователя по объекту
        действуют и здесь — выгрузить чужой объект нельзя.
        """
        refs = await asyncio.to_thread(_db, lambda s: queries.filter_options(s, scope))
        site = _parse_id(site_id)
        scale = _scale_of_site(refs, _parse_id(scale_id), site)
        filters = _parse_filters(site, scale, date_from, date_to, vehicle, source)
        rows = await asyncio.to_thread(
            _db,
            lambda s: queries.journal_export_rows(s, filters, limit=EXPORT_LIMIT, site_scope=scope),
        )
        logger.info("панель (%s): выгрузка журнала, строк %d", user, len(rows))
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=";", lineterminator="\r\n")
        writer.writerow(
            [
                "Дата и время",
                "Объект",
                "Весы",
                "Номер ТС",
                "Прицеп",
                "Операция",
                "Брутто, кг",
                "Тара, кг",
                "Нетто, кг",
                "Источник",
                "Оператор",
            ]
        )
        for weighing, scale_row, site_row in rows:
            moment = weighing.weighed_at or weighing.created_at
            writer.writerow(
                [
                    _fmt_dt(moment),
                    _csv_text(site_row.name),
                    _csv_text(scale_row.name),
                    _csv_text(weighing.vehicle_number),
                    _csv_text(weighing.trailer_number),
                    "Взвешивание" if weighing.operation is Operation.WEIGHING else "Тарирование",
                    _csv_number(weighing.massa),
                    _csv_number(weighing.tare_value),
                    _csv_number(weighing.netto),
                    "АИС" if weighing.source is WeighingSource.AIS else "Вручную (офлайн)",
                    _csv_text(weighing.operator),
                ]
            )
        if len(rows) == EXPORT_LIMIT:
            # молча обрезанный отчёт хуже отсутствующего: пишем в файл
            writer.writerow([f"ВНИМАНИЕ: показаны первые {EXPORT_LIMIT} строк — сузьте фильтры"])
            logger.warning("панель (%s): выгрузка обрезана по потолку", user)
        stamp = datetime.now(BISHKEK).strftime("%Y-%m-%d_%H-%M")
        body = "\ufeff" + buffer.getvalue()  # BOM: Excel понимает UTF-8
        return Response(
            content=body.encode("utf-8"),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="vzveshivaniya_{stamp}.csv"'},
        )

    @router.get("/journal/{weighing_id}", response_class=HTMLResponse)
    async def journal_card(
        request: Request, user: PanelUser, scope: PanelScope, weighing_id: int
    ) -> HTMLResponse:
        card = await asyncio.to_thread(
            _db, lambda s: queries.weighing_card(s, weighing_id, site_scope=scope)
        )
        if card is None:
            raise HTTPException(status_code=404)
        return render("record.html", request, user=user, card=card)

    def _print_card_context(card: queries.WeighingCard) -> dict[str, object]:
        """Контекст печатной карточки из карточки записи журнала.

        Рамки фото ПЕРЕД/ЗАД печатаются всегда; снимок, чей файл ещё не
        долетел с объекта (запись пришла досылкой, PhotoUploader дольёт
        позже), оставляет рамку пустой с предупреждением, что фото появятся
        после восстановления связи.
        """
        w = card.weighing
        assert w.weighed_at is not None and w.massa is not None  # проверено маршрутом
        urls: dict[CameraRole, str] = {}
        waiting = False
        for photo in card.photos:
            if (photos_dir / photo.path.lstrip("/")).is_file():
                urls[photo.role] = f"/panel/photos{photo.path}"
            else:
                waiting = True
        note = None
        if waiting:
            note = (
                "Часть снимков ещё не дослана с объекта — "
                "они появятся после восстановления связи с ним."
            )
        verification = None
        if card.scale.verif_number:
            verification = VerificationInfo(
                number=card.scale.verif_number,
                verified_on=card.scale.verif_date,
                valid_until=card.scale.verif_until,
            )
        return weight_card.build_card(
            operation=w.operation,
            weighed_at=w.weighed_at,
            site_name=card.site.name,
            scale_name=card.scale.name,
            vehicle_number=w.vehicle_number,
            trailer_number=w.trailer_number,
            massa=w.massa,
            tare_value=w.tare_value,
            netto=w.netto,
            tared_at=card.tare_weighing.weighed_at if card.tare_weighing else None,
            operator=w.operator,
            verification=verification,
            photo_front_url=urls.get(CameraRole.FRONT),
            photo_rear_url=urls.get(CameraRole.REAR),
            photos_note=note,
            record_uuid=str(w.uuid),
            code_ok=w.code is ErrorCode.OK,
            latest_tared_at=card.expired_tare.weighed_at if card.expired_tare else None,
            latest_tare_value=card.expired_tare.massa if card.expired_tare else None,
        )

    @router.get("/journal/{weighing_id}/card", response_class=HTMLResponse)
    async def journal_print_card(
        request: Request, user: PanelUser, scope: PanelScope, weighing_id: int
    ) -> HTMLResponse:
        """Печатная весовая карточка записи (открывается в новой вкладке).

        Видимость — как у самой записи: чужой объект для ограниченного
        пользователя не существует (PanelScope, 404).
        """
        card = await asyncio.to_thread(
            _db, lambda s: queries.weighing_card(s, weighing_id, site_scope=scope)
        )
        if card is None or card.weighing.weighed_at is None or card.weighing.massa is None:
            raise HTTPException(status_code=404)
        return render(
            "card.html",
            request,
            card=_print_card_context(card),
            logo=weight_card.logo_data_uri(),
        )

    @router.get("/tares", response_class=HTMLResponse)
    async def tares(
        request: Request,
        user: PanelUser,
        scope: PanelScope,
        search: str | None = None,
        show: str = "",
        site_id: str = "",
        scale_id: str = "",
        page: int = 1,
    ) -> HTMLResponse:
        """Реестр тарирований; ``show=all`` — вся история из журнала.

        Реестр хранит одну строку на сцепку, поэтому историю машины —
        вместе с истёкшими и заменёнными тарированиями — видно только
        по журналу (запрос Игоря 14.08.2026).
        """
        refs = await asyncio.to_thread(_db, lambda s: queries.filter_options(s, scope))
        site = _parse_id(site_id)
        scale = _scale_of_site(refs, _parse_id(scale_id), site)
        page = max(1, page)
        history = show == "all"
        rows: list[dict[str, Any]]
        if history:
            raw_history, total = await asyncio.to_thread(
                _db,
                lambda s: queries.tare_history(
                    s,
                    search=search,
                    site_id=site,
                    scale_id=scale,
                    limit=PAGE_SIZE,
                    offset=(page - 1) * PAGE_SIZE,
                    site_scope=scope,
                ),
            )
            threshold = three_months_before(datetime.now(UTC))
            rows = [
                {
                    "weighing": weighing,
                    "site": row_site,
                    "scale": row_scale,
                    "vehicle_number": weighing.vehicle_number,
                    "trailer_number": weighing.trailer_number,
                    "tare_value": weighing.massa,
                    "tared_at": weighing.weighed_at or weighing.created_at,
                    "status": _tare_status(weighing, registry, threshold),
                }
                for weighing, row_scale, row_site, registry in raw_history
            ]
        else:
            raw_active, total = await asyncio.to_thread(
                _db,
                lambda s: queries.tare_list(
                    s,
                    search=search,
                    site_id=site,
                    scale_id=scale,
                    limit=PAGE_SIZE,
                    offset=(page - 1) * PAGE_SIZE,
                    site_scope=scope,
                ),
            )
            rows = [
                {
                    "weighing": weighing,
                    "site": row_site,
                    "scale": row_scale,
                    "vehicle_number": tare.vehicle_number,
                    "trailer_number": tare.trailer_number,
                    "tare_value": tare.tare_value,
                    "tared_at": tare.tared_at,
                    "status": "active",
                }
                for tare, weighing, row_scale, row_site in raw_active
            ]
        photos = await asyncio.to_thread(
            _db, lambda s: queries.photos_for_weighings(s, [r["weighing"].id for r in rows])
        )
        return render(
            "tares.html",
            request,
            user=user,
            rows=rows,
            photos=photos,
            total=total,
            page=page,
            pages=max(1, -(-total // PAGE_SIZE)),
            search=search or "",
            show="all" if history else "",
            refs=refs,
            filters={"site_id": site, "scale_id": scale},
        )

    @router.get("/events", response_class=HTMLResponse)
    async def events_page(
        request: Request,
        user: PanelUser,
        scope: PanelScope,
        site_id: str = "",
        page: int = 1,
    ) -> HTMLResponse:
        """Журнал событий мониторинга (переходы детекторов, этап 2)."""
        refs = await asyncio.to_thread(_db, lambda s: queries.filter_options(s, scope))
        site = _parse_id(site_id)
        if scope is not None:
            # разграничение по объекту сильнее фильтра экрана (как везде)
            site = scope
        page = max(1, page)
        rows, total = await asyncio.to_thread(
            _db,
            lambda s: queries.monitoring_events_page(
                s, site_id=site, page=page, page_size=PAGE_SIZE
            ),
        )
        return render(
            "events.html",
            request,
            user=user,
            rows=rows,
            total=total,
            page=page,
            pages=max(1, -(-total // PAGE_SIZE)),
            refs=refs,
            filters={"site_id": site},
        )

    @router.get("/refs", response_class=HTMLResponse)
    async def refs(
        request: Request, user: PanelUser, scope: PanelScope, site: str = ""
    ) -> HTMLResponse:
        # общий фильтр по объекту: сужает весы/агентов/камеры разом
        # (запрос Игоря 11.08.2026); мусорное значение = «все объекты»
        site_filter, site_ok = _parse_site_id(site)
        if not site_ok:
            site_filter = None
        data = await asyncio.to_thread(
            _db, lambda s: queries.refs_data(s, site_filter, site_scope=scope)
        )
        # токен агента из flash-сессии: показывается ОДИН раз после выпуска
        # (в URL секретам не место, поэтому не query-параметром)
        agent_token = request.session.pop("refs_agent_token", None)
        response = render(
            "refs.html",
            request,
            user=user,
            refs=data,
            site_filter=site_filter,
            can_edit=request.session.get("panel_role") == "admin",
            agent_token=agent_token,
            scale_kinds=list(ScaleKind),
            camera_roles=list(CameraRole),
            channels=list(ReleaseChannel),
            note=request.query_params.get("note"),
        )
        if agent_token is not None:
            # страница с токеном не должна оседать в кэше/bfcache браузера
            response.headers["Cache-Control"] = "no-store"
        return response

    # --- редактирование справочников (только администратор) ---

    def _refs_redirect(note: str) -> RedirectResponse:
        return RedirectResponse(f"/panel/refs?note={quote(note)}", status_code=303)

    def _parse_opt_int(raw: str) -> tuple[int | None, bool]:
        """(значение, ok): пустая строка — None, мусор — ошибка."""
        raw = raw.strip()
        if not raw:
            return None, True
        try:
            return int(raw), True
        except ValueError:
            return None, False

    @router.post("/refs/sites/create")
    async def refs_site_create(
        request: Request,
        admin: PanelAdmin,
        code: Annotated[str, Form()],
        name: Annotated[str, Form()],
    ) -> RedirectResponse:
        error = await asyncio.to_thread(
            _db, lambda s: refs_admin.create_site(s, code=code, name=name)
        )
        if error is None:
            logger.info("панель (%s): создан объект %s", admin, code.strip())
            return _refs_redirect(f"объект {name.strip()} создан")
        return _refs_redirect(error)

    @router.post("/refs/sites/{site_id}/edit")
    async def refs_site_edit(
        request: Request,
        admin: PanelAdmin,
        site_id: int,
        name: Annotated[str, Form()],
    ) -> RedirectResponse:
        error = await asyncio.to_thread(
            _db, lambda s: refs_admin.update_site(s, site_id, name=name)
        )
        if error is None:
            logger.info("панель (%s): объект id=%d переименован", admin, site_id)
            return _refs_redirect("объект переименован")
        return _refs_redirect(error)

    def _parse_scale_form(
        kind: str, legacy_port: str, legacy_autoscale: str
    ) -> tuple[ScaleKind | None, int | None, int | None, str | None]:
        try:
            parsed_kind = ScaleKind(kind)
        except ValueError:
            return None, None, None, "неизвестный тип весов"
        port, port_ok = _parse_opt_int(legacy_port)
        autoscale, autoscale_ok = _parse_opt_int(legacy_autoscale)
        if not port_ok or not autoscale_ok:
            return None, None, None, "legacy-маршрут АИС: порт и autoscale — числа"
        return parsed_kind, port, autoscale, None

    @router.post("/refs/scales/create")
    async def refs_scale_create(
        request: Request,
        admin: PanelAdmin,
        site_id: int = Form(),
        name: Annotated[str, Form()] = "",
        kind: Annotated[str, Form()] = ScaleKind.STATIC.value,
        driver: Annotated[str, Form()] = "cas22",
        legacy_ip: Annotated[str, Form()] = "",
        legacy_port: Annotated[str, Form()] = "",
        legacy_autoscale: Annotated[str, Form()] = "",
        ais_object: Annotated[str, Form()] = "",
        ais_scale_no: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        parsed_kind, port, autoscale, error = _parse_scale_form(kind, legacy_port, legacy_autoscale)
        ais_no, ais_no_ok = _parse_opt_int(ais_scale_no)
        if error is None and not ais_no_ok:
            error = "привязка АИС: № весов — число"
        if error is None:
            assert parsed_kind is not None
            error = await asyncio.to_thread(
                _db,
                lambda s: refs_admin.create_scale(
                    s,
                    site_id=site_id,
                    name=name,
                    kind=parsed_kind,
                    driver=driver,
                    legacy_ip=legacy_ip,
                    legacy_port=port,
                    legacy_autoscale=autoscale,
                    ais_object=ais_object,
                    ais_scale_no=ais_no,
                ),
            )
        if error is None:
            logger.info("панель (%s): созданы весы «%s»", admin, name.strip())
            return _refs_redirect(f"весы {name.strip()} созданы")
        return _refs_redirect(error)

    @router.post("/refs/scales/{scale_id}/edit")
    async def refs_scale_edit(
        request: Request,
        admin: PanelAdmin,
        scale_id: int,
        name: Annotated[str, Form()] = "",
        kind: Annotated[str, Form()] = ScaleKind.STATIC.value,
        driver: Annotated[str, Form()] = "cas22",
        legacy_ip: Annotated[str, Form()] = "",
        legacy_port: Annotated[str, Form()] = "",
        legacy_autoscale: Annotated[str, Form()] = "",
        ais_object: Annotated[str, Form()] = "",
        ais_scale_no: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        parsed_kind, port, autoscale, error = _parse_scale_form(kind, legacy_port, legacy_autoscale)
        ais_no, ais_no_ok = _parse_opt_int(ais_scale_no)
        if error is None and not ais_no_ok:
            error = "привязка АИС: № весов — число"
        if error is None:
            assert parsed_kind is not None
            error = await asyncio.to_thread(
                _db,
                lambda s: refs_admin.update_scale(
                    s,
                    scale_id,
                    name=name,
                    kind=parsed_kind,
                    driver=driver,
                    legacy_ip=legacy_ip,
                    legacy_port=port,
                    legacy_autoscale=autoscale,
                    ais_object=ais_object,
                    ais_scale_no=ais_no,
                ),
            )
        if error is None:
            logger.info("панель (%s): весы id=%d обновлены", admin, scale_id)
            return _refs_redirect("весы обновлены")
        return _refs_redirect(error)

    async def _push_scale_config(scale_id: int) -> str:
        """Доставить агенту свежие настройки; текст-хвост для note."""
        settings = await asyncio.to_thread(_db, lambda s: repo.load_scale_settings(s, scale_id))
        if settings is None:
            return ""
        if not hub.connected(scale_id):
            return " — агент офлайн, применятся при следующем подключении"
        versions = await asyncio.to_thread(_db, repo.agent_versions)
        if not supports_secure_sync(versions.get(scale_id)):
            # старому агенту секреты не шлём (правило №7) — он получит
            # снимок при первом hello после автообновления
            return (
                " — агент старой версии: обновите его из панели, "
                "настройки применятся после обновления"
            )
        sent = await hub.send_scale_config(scale_id, ScaleConfigUpdate(settings=settings))
        if sent:
            return " — отправлены агенту (отчёт о применении в логах центра)"
        return " — агент офлайн, применятся при следующем подключении"

    @router.get("/refs/scales/{scale_id}/settings", response_class=HTMLResponse)
    async def scale_settings_page(
        request: Request, user: PanelUser, admin: PanelAdmin, scale_id: int
    ) -> HTMLResponse:
        row = await asyncio.to_thread(
            _db,
            lambda s: (
                None
                if (scale := s.get(Scale, scale_id)) is None
                else (scale, s.get(Site, scale.site_id))
            ),
        )
        if row is None:
            raise HTTPException(status_code=404)
        scale, site = row
        cycle = refs_admin.DEFAULT_CYCLE
        if scale.thresholds:
            with contextlib.suppress(ValueError):
                cycle = CycleSettings.model_validate(scale.thresholds)
        port_cfg = scale.port_cfg or {}
        return render(
            "scale_settings.html",
            request,
            user=user,
            scale=scale,
            site=site,
            cycle=cycle,
            has_center_cycle=bool(scale.thresholds),
            port=port_cfg.get("port") or "",
            baudrate=port_cfg.get("baudrate") or 9600,
            verif_number=scale.verif_number or "",
            verif_date=scale.verif_date.isoformat() if scale.verif_date else "",
            verif_until=scale.verif_until.isoformat() if scale.verif_until else "",
            note=request.query_params.get("note"),
        )

    @router.post("/refs/scales/{scale_id}/settings")
    async def scale_settings_save(
        request: Request,
        admin: PanelAdmin,
        scale_id: int,
        zero_threshold_kg: Annotated[float, Form()],
        vehicle_threshold_kg: Annotated[float, Form()],
        zero_timeout_s: Annotated[float, Form()],
        vehicle_timeout_s: Annotated[float, Form()],
        stable_duration_s: Annotated[float, Form()],
        stable_timeout_s: Annotated[float, Form()],
        no_data_timeout_s: Annotated[float, Form()],
        port: Annotated[str, Form()] = "",
        baudrate: Annotated[str, Form()] = "",
        verif_number: Annotated[str, Form()] = "",
        verif_date: Annotated[str, Form()] = "",
        verif_until: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        def back(note: str) -> RedirectResponse:
            return RedirectResponse(
                f"/panel/refs/scales/{scale_id}/settings?note={quote(note)}",
                status_code=303,
            )

        parsed_baudrate, baudrate_ok = _parse_opt_int(baudrate)
        if not baudrate_ok:
            return back("скорость порта — число")
        cycle = CycleSettings(
            zero_threshold_kg=zero_threshold_kg,
            vehicle_threshold_kg=vehicle_threshold_kg,
            zero_timeout_s=zero_timeout_s,
            vehicle_timeout_s=vehicle_timeout_s,
            stable_duration_s=stable_duration_s,
            stable_timeout_s=stable_timeout_s,
            no_data_timeout_s=no_data_timeout_s,
        )
        error = await asyncio.to_thread(
            _db,
            lambda s: refs_admin.save_scale_settings(
                s, scale_id, cycle=cycle, port=port, baudrate=parsed_baudrate
            ),
        )
        if error is not None:
            return back(error)
        verification_error = await asyncio.to_thread(
            _db,
            lambda s: refs_admin.save_scale_verification(
                s,
                scale_id,
                number=verif_number,
                verified_on=verif_date,
                valid_until=verif_until,
            ),
        )
        logger.info("панель (%s): настройки весов id=%d сохранены", admin, scale_id)
        # цикл и порт уже в БД — доставляем их агенту даже при ошибке в поверке,
        # а note честно говорит, что сохранилось, а что нет (замечание ревью)
        tail = await _push_scale_config(scale_id)
        if verification_error is not None:
            return back(
                f"параметры цикла и порт сохранены{tail}; "
                f"свидетельство о поверке НЕ сохранено: {verification_error}"
            )
        return back(f"настройки сохранены{tail}")

    @router.post("/refs/scales/{scale_id}/camera")
    async def refs_camera_upsert(
        request: Request,
        admin: PanelAdmin,
        scale_id: int,
        role: Annotated[str, Form()],
        snapshot_url: Annotated[str, Form()] = "",
        rtsp_url: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        try:
            parsed_role = CameraRole(role)
        except ValueError:
            return _refs_redirect("неизвестная роль камеры")
        error = await asyncio.to_thread(
            _db,
            lambda s: refs_admin.upsert_camera(
                s,
                scale_id=scale_id,
                role=parsed_role,
                snapshot_url=snapshot_url,
                rtsp_url=rtsp_url,
            ),
        )
        if error is None:
            logger.info("панель (%s): камера %s весов id=%d обновлена", admin, role, scale_id)
            # смена URL камер — часть настроек весов: доставляем агенту сразу
            tail = await _push_scale_config(scale_id)
            return _refs_redirect(f"камера сохранена{tail}")
        return _refs_redirect(error)

    def _parse_channel(raw: str) -> ReleaseChannel | None:
        try:
            return ReleaseChannel(raw)
        except ValueError:
            return None

    @router.post("/refs/agents/create")
    async def refs_agent_create(
        request: Request,
        admin: PanelAdmin,
        scale_id: int = Form(),
        channel: Annotated[str, Form()] = ReleaseChannel.PILOT.value,
    ) -> RedirectResponse:
        parsed_channel = _parse_channel(channel)
        if parsed_channel is None:
            return _refs_redirect("неизвестный канал")
        error, token = await asyncio.to_thread(
            _db,
            lambda s: refs_admin.create_agent(s, scale_id=scale_id, channel=parsed_channel),
        )
        if error is not None:
            return _refs_redirect(error)
        request.session["refs_agent_token"] = token
        logger.info("панель (%s): создан агент весов id=%d", admin, scale_id)
        return _refs_redirect("агент создан — токен показан один раз ниже")

    @router.post("/refs/agents/{agent_id}/reissue-token")
    async def refs_agent_reissue(
        request: Request, admin: PanelAdmin, agent_id: int
    ) -> RedirectResponse:
        error, token = await asyncio.to_thread(
            _db, lambda s: refs_admin.reissue_agent_token(s, agent_id)
        )
        if error is not None:
            return _refs_redirect(error)
        request.session["refs_agent_token"] = token
        logger.info("панель (%s): перевыпущен токен агента id=%d", admin, agent_id)
        return _refs_redirect(
            "токен перевыпущен — старый больше не действует, новый показан один раз ниже"
        )

    @router.post("/refs/agents/{agent_id}/channel")
    async def refs_agent_channel(
        request: Request,
        admin: PanelAdmin,
        agent_id: int,
        channel: Annotated[str, Form()],
    ) -> RedirectResponse:
        parsed_channel = _parse_channel(channel)
        if parsed_channel is None:
            return _refs_redirect("неизвестный канал")
        error = await asyncio.to_thread(
            _db, lambda s: refs_admin.set_agent_channel(s, agent_id, parsed_channel)
        )
        if error is None:
            logger.info("панель (%s): канал агента id=%d изменён", admin, agent_id)
            return _refs_redirect("канал агента изменён")
        return _refs_redirect(error)

    # --- пользователи (только администратор; права сверяются с БД) ---

    def _users_redirect(note: str) -> RedirectResponse:
        return RedirectResponse(f"/panel/users?note={quote(note)}", status_code=303)

    async def _push_operators() -> None:
        """Разослать подключённым агентам свежие снимки их операторов —
        сразу после изменения учёток (блокировка/пароль долетают мгновенно;
        офлайн-агент получит актуальный снимок при следующем hello).
        Старым агентам снимки с секретами не шлются (правило №7)."""
        versions = await asyncio.to_thread(_db, repo.agent_versions)
        for scale_id in hub.connected_scale_ids():
            if not supports_secure_sync(versions.get(scale_id)):
                continue
            records = await asyncio.to_thread(
                _db, lambda s, sid=scale_id: repo.load_operators_for_scale(s, sid)
            )
            await hub.send_operators(scale_id, OperatorsRegistryUpdate(records=records))

    def _parse_role(raw: str) -> UserRole | None:
        try:
            return UserRole(raw)
        except ValueError:
            return None

    def _parse_site_id(raw: str) -> tuple[int | None, bool]:
        """(site_id, ok): пустое поле — «все объекты», мусор — ошибка,
        а не тихая отвязка от объекта."""
        if not raw.strip():
            return None, True
        parsed = _parse_id(raw)
        return parsed, parsed is not None

    @router.get("/users", response_class=HTMLResponse)
    async def users_page(
        request: Request,
        user: PanelUser,
        admin: PanelAdmin,
        search: str = "",
        role: str = "",
        site: str = "",
        status: str = "",
    ) -> HTMLResponse:
        # фильтры списка (запрос Игоря 11.08.2026); кривые значения из URL
        # молча трактуются как «без фильтра»
        role_filter = _parse_role(role) if role else None
        site_filter, site_ok = _parse_site_id(site) if site not in ("", "none") else (None, True)
        rows = await asyncio.to_thread(
            _db,
            lambda s: users_admin.users_list(
                s,
                search=search,
                role=role_filter,
                site_id=site_filter if site_ok else None,
                without_site=site == "none",
                active={"active": True, "disabled": False}.get(status),
            ),
        )
        sites = await asyncio.to_thread(_db, lambda s: queries.refs_data(s).sites)
        # фильтры экрана действуют и на блок «Учётки на агентах» (запрос
        # Игоря 14.08.2026); роль и «— все —» к учёткам агентов неприменимы
        agent_active = {"active": True, "disabled": False}.get(status)
        agent_site = site_filter if site_ok else None
        agent_ops = await asyncio.to_thread(
            _db,
            lambda s: queries.agent_operators(
                s, search=search, site_id=agent_site, active=agent_active
            ),
        )
        return render(
            "users.html",
            request,
            user=user,
            admin_login=admin,
            rows=rows,
            sites=sites,
            agent_ops=agent_ops,
            agent_ops_filtered=bool(search.strip())
            or agent_site is not None
            or agent_active is not None,
            roles=list(UserRole),
            min_password=users_admin.MIN_PASSWORD_LEN,
            filters={"search": search, "role": role, "site": site, "status": status},
            note=request.query_params.get("note"),
        )

    @router.post("/users/agent-block")
    async def users_agent_block(
        request: Request,
        admin: PanelAdmin,
        scale_id: Annotated[int, Form()],
        login: Annotated[str, Form()],
    ) -> RedirectResponse:
        """Перехват местной учётки весового ПК (кнопка блока «Учётки на агентах»).

        Создаёт отключённого оператора-двойника (users_admin) и рассылает
        снимки операторов — реплика «центр главнее» гасит местную учётку.
        """
        error = await asyncio.to_thread(
            _db, lambda s: users_admin.block_agent_operator(s, scale_id, login)
        )
        if error:
            return _users_redirect(f"перехват не выполнен: {error}")
        logger.info("панель (%s): перехват местной учётки весов %d", admin, scale_id)
        await _push_operators()
        # заметка не должна обещать доставку офлайн-агенту (замечание ревью):
        # сценарий кнопки — недобросовестный оператор — может совпасть
        # с выдернутым кабелем; снимок тогда доедет при следующем hello
        if hub.connected(scale_id):
            return _users_redirect(
                "готово: агенту отправлен снимок операторов — местная учётка перекрыта центром"
            )
        return _users_redirect(
            "готово: учётка перекрыта в центре; агент сейчас не на связи — "
            "заблокированная реплика доедет при его подключении"
        )

    @router.post("/users/create")
    async def users_create(
        request: Request,
        admin: PanelAdmin,
        login: Annotated[str, Form()],
        password: Annotated[str, Form()],
        full_name: Annotated[str, Form()] = "",
        role: Annotated[str, Form()] = UserRole.DISPATCHER.value,
        site_id: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        parsed_role = _parse_role(role)
        if parsed_role is None:
            return _users_redirect("неизвестная роль")
        parsed_site, site_ok = _parse_site_id(site_id)
        if not site_ok:
            return _users_redirect("объект не найден")
        error = await asyncio.to_thread(
            _db,
            lambda s: users_admin.create_user(
                s,
                login=login,
                password=password,
                full_name=full_name,
                role=parsed_role,
                site_id=parsed_site,
            ),
        )
        if error is None:
            logger.info("панель (%s): создан пользователь %s", admin, login.strip())
            await _push_operators()
            return _users_redirect(f"пользователь {login.strip()} создан")
        return _users_redirect(error)

    @router.post("/users/{user_id}/edit")
    async def users_edit(
        request: Request,
        admin: PanelAdmin,
        user_id: int,
        full_name: Annotated[str, Form()] = "",
        role: Annotated[str, Form()] = UserRole.DISPATCHER.value,
        site_id: Annotated[str, Form()] = "",
    ) -> RedirectResponse:
        parsed_role = _parse_role(role)
        if parsed_role is None:
            return _users_redirect("неизвестная роль")
        parsed_site, site_ok = _parse_site_id(site_id)
        if not site_ok:
            return _users_redirect("объект не найден")
        error = await asyncio.to_thread(
            _db,
            lambda s: users_admin.update_user(
                s,
                user_id,
                full_name=full_name,
                role=parsed_role,
                site_id=parsed_site,
            ),
        )
        if error is None:
            logger.info("панель (%s): пользователь id=%d обновлён", admin, user_id)
            await _push_operators()
            return _users_redirect("изменения сохранены")
        return _users_redirect(error)

    @router.post("/users/{user_id}/password")
    async def users_password(
        request: Request,
        admin: PanelAdmin,
        user_id: int,
        password: Annotated[str, Form()],
    ) -> RedirectResponse:
        error = await asyncio.to_thread(
            _db, lambda s: users_admin.set_password(s, user_id, password)
        )
        if error is None:
            logger.info("панель (%s): пароль пользователя id=%d сброшен", admin, user_id)
            await _push_operators()
            return _users_redirect("пароль изменён")
        return _users_redirect(error)

    @router.post("/users/{user_id}/toggle")
    async def users_toggle(request: Request, admin: PanelAdmin, user_id: int) -> RedirectResponse:
        error = await asyncio.to_thread(
            _db, lambda s: users_admin.toggle_active(s, user_id, actor_login=admin)
        )
        if error is None:
            logger.info("панель (%s): пользователь id=%d включён/отключён", admin, user_id)
            await _push_operators()
            return _users_redirect("статус учётки изменён")
        return _users_redirect(error)

    # --- фото для пользователей панели (по сессии, не по сервисному токену) ---

    @router.get("/photos/{file_path:path}")
    async def panel_photo(file_path: str, user: PanelUser, scope: PanelScope) -> Response:
        """Снимок записи для экранов панели.

        Ограничение по объекту действует и здесь: путь содержит uuid
        записи и сам по себе неугадываем, но держать доступ только на
        неугадываемости нельзя (замечание ревью 11.08.2026).
        """
        full = (photos_dir / file_path.lstrip("/")).resolve()
        if not full.is_relative_to(photos_dir.resolve()) or not full.is_file():
            raise HTTPException(status_code=404)
        if scope is not None:
            allowed = await asyncio.to_thread(
                _db, lambda s: _photo_belongs_to_site(s, file_path, scope)
            )
            if not allowed:
                raise HTTPException(status_code=404)
        return FileResponse(full, media_type="image/jpeg")

    return router
