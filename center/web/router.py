"""Веб-панель диспетчера (FastAPI + Jinja2 + HTMX, architecture §4.3).

Экраны этапа 1: вход, дашборд объектов, журнал с фильтрами и карточкой
записи, реестр тарирований, справочники. Всё — только чтение
(правило №2: записанные операции неизменны; управление справочниками —
CLI ``tools/center_admin.py``, экраны редактирования — позже).

Доступ по учёткам users (роли admin/dispatcher/operator видят одно и
то же на этапе 1 — разграничение по объектам появится с ролями операторов).
Фото пользователям панели отдаются через ``/panel/photos/...`` со своей
сессией (сервисные токены /vesy/... — только для интеграторов).
"""

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Form, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentHubError
from center.releases import AgentRelease, latest_release
from center.web import queries
from shared.enums import WeighingSource
from shared.messages import EquipmentStatus, UpdateCommand

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]

TEMPLATES_DIR = Path(__file__).parent / "templates"
BISHKEK = ZoneInfo("Asia/Bishkek")
PAGE_SIZE = 50


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


def create_panel_router(
    session_factory: SessionFactory,
    hub: AgentHub,
    *,
    photos_dir: Path,
    releases_dir: Path | None = None,
) -> APIRouter:
    """Маршруты панели; сессии — общий SessionMiddleware приложения центра."""
    router = APIRouter(prefix="/panel")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.filters["fmt_dt"] = _fmt_dt
    templates.env.filters["fmt_time"] = _fmt_time
    templates.env.filters["fmt_kg"] = _fmt_kg
    templates.env.globals["expires"] = queries.tare_expires_at
    # метка старта процесса — сброс браузерного кэша статики при деплое
    templates.env.globals["static_v"] = str(int(datetime.now(UTC).timestamp()))

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    def current_user(request: Request) -> str:
        user = request.session.get("panel_user")
        if not user:
            raise HTTPException(status_code=303, headers={"Location": "/panel/login"})
        return str(user)

    PanelUser = Annotated[str, Depends(current_user)]

    def render(template: str, request: Request, **extra: Any) -> HTMLResponse:
        context: dict[str, Any] = {"request": request, **extra}
        return templates.TemplateResponse(request, template, context)

    # --- вход/выход ---

    @router.get("/login", response_class=HTMLResponse)
    def login_page(request: Request) -> HTMLResponse:
        return render("login.html", request, error=None)

    @router.post("/login", response_class=HTMLResponse)
    async def login_submit(
        request: Request,
        login: Annotated[str, Form()],
        password: Annotated[str, Form()],
    ) -> Response:
        user = await asyncio.to_thread(
            _db, lambda s: queries.verify_user(s, login.strip(), password)
        )
        if user is None:
            logger.warning("панель: неудачный вход %s", login.strip())
            return render("login.html", request, error="Неверный логин или пароль")
        request.session["panel_user"] = user.full_name or user.login
        request.session["panel_role"] = user.role.value
        logger.info("панель: вход %s (%s)", user.login, user.role.value)
        return RedirectResponse("/panel/", status_code=303)

    @router.post("/logout")
    def logout(request: Request) -> RedirectResponse:
        request.session.pop("panel_user", None)
        request.session.pop("panel_role", None)
        return RedirectResponse("/panel/login", status_code=303)

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

    @router.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request, user: PanelUser) -> HTMLResponse:
        scales = await asyncio.to_thread(_db, queries.dashboard_scales)
        return render(
            "dashboard.html",
            request,
            user=user,
            scales=scales,
            equipment=_equipment_map(scales),
            release=await _latest_release(),
            update_note=request.query_params.get("update_note"),
        )

    @router.get("/fragments/dashboard", response_class=HTMLResponse)
    async def dashboard_fragment(request: Request, user: PanelUser) -> HTMLResponse:
        scales = await asyncio.to_thread(_db, queries.dashboard_scales)
        return render(
            "fragments/dashboard_grid.html",
            request,
            user=user,
            scales=scales,
            equipment=_equipment_map(scales),
            release=await _latest_release(),
        )

    @router.post("/scales/{scale_id}/update-agent")
    async def update_agent(request: Request, user: PanelUser, scale_id: int) -> RedirectResponse:
        """Разослать агенту команду автообновления до актуального релиза."""
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
                logger.info("панель (%s): весы %d — %s", user, scale_id, note)
            except AgentHubError as exc:
                note = str(exc)
        return RedirectResponse(f"/panel/?update_note={quote(note)}", status_code=303)

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
        site_id: int | None = None,
        scale_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        vehicle: str | None = None,
        source: str | None = None,
        page: int = 1,
    ) -> HTMLResponse:
        filters = _parse_filters(site_id, scale_id, date_from, date_to, vehicle, source)
        page = max(1, page)
        rows, total = await asyncio.to_thread(
            _db,
            lambda s: queries.journal_page(
                s, filters, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE
            ),
        )
        photos = await asyncio.to_thread(
            _db, lambda s: queries.photos_for_weighings(s, [w.id for w, _, _ in rows])
        )
        refs = await asyncio.to_thread(_db, queries.refs_data)
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
                "site_id": site_id,
                "scale_id": scale_id,
                "date_from": date_from or "",
                "date_to": date_to or "",
                "vehicle": vehicle or "",
                "source": source or "",
            },
        )

    @router.get("/journal/{weighing_id}", response_class=HTMLResponse)
    async def journal_card(request: Request, user: PanelUser, weighing_id: int) -> HTMLResponse:
        card = await asyncio.to_thread(_db, lambda s: queries.weighing_card(s, weighing_id))
        if card is None:
            raise HTTPException(status_code=404)
        return render("record.html", request, user=user, card=card)

    @router.get("/tares", response_class=HTMLResponse)
    async def tares(
        request: Request, user: PanelUser, search: str | None = None, page: int = 1
    ) -> HTMLResponse:
        page = max(1, page)
        rows, total = await asyncio.to_thread(
            _db,
            lambda s: queries.tare_list(
                s, search=search, limit=PAGE_SIZE, offset=(page - 1) * PAGE_SIZE
            ),
        )
        photos = await asyncio.to_thread(
            _db, lambda s: queries.photos_for_weighings(s, [w.id for _, w, _, _ in rows])
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
        )

    @router.get("/refs", response_class=HTMLResponse)
    async def refs(request: Request, user: PanelUser) -> HTMLResponse:
        data = await asyncio.to_thread(_db, queries.refs_data)
        return render("refs.html", request, user=user, refs=data)

    # --- фото для пользователей панели (по сессии, не по сервисному токену) ---

    @router.get("/photos/{file_path:path}")
    def panel_photo(file_path: str, user: PanelUser) -> Response:
        full = (photos_dir / file_path.lstrip("/")).resolve()
        if not full.is_relative_to(photos_dir.resolve()) or not full.is_file():
            raise HTTPException(status_code=404)
        return FileResponse(full, media_type="image/jpeg")

    return router
