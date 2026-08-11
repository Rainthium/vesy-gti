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
from center.db import repo
from center.db.models import ReleaseChannel, Scale, ScaleKind, Site, UserRole
from center.releases import AgentRelease, latest_release
from center.web import queries, refs_admin, users_admin
from shared.enums import CameraRole, WeighingSource
from shared.messages import (
    CycleSettings,
    EquipmentStatus,
    OperatorsRegistryUpdate,
    ScaleConfigUpdate,
    UpdateCommand,
    supports_secure_sync,
)

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
        request.session["panel_login"] = user.login
        request.session["panel_role"] = user.role.value
        logger.info("панель: вход %s (%s)", user.login, user.role.value)
        return RedirectResponse("/panel/", status_code=303)

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
            raise HTTPException(status_code=303, headers={"Location": "/panel/login"})
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
        # токен агента из flash-сессии: показывается ОДИН раз после выпуска
        # (в URL секретам не место, поэтому не query-параметром)
        agent_token = request.session.pop("refs_agent_token", None)
        response = render(
            "refs.html",
            request,
            user=user,
            refs=data,
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
    ) -> RedirectResponse:
        parsed_kind, port, autoscale, error = _parse_scale_form(kind, legacy_port, legacy_autoscale)
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
    ) -> RedirectResponse:
        parsed_kind, port, autoscale, error = _parse_scale_form(kind, legacy_port, legacy_autoscale)
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
        logger.info("панель (%s): настройки весов id=%d сохранены", admin, scale_id)
        tail = await _push_scale_config(scale_id)
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
        raw = raw.strip()
        if not raw:
            return None, True
        # isascii: юникодные «цифры» (³, ٢) проходят isdigit, но роняют int
        if not (raw.isascii() and raw.isdigit()):
            return None, False
        return int(raw), True

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
        return render(
            "users.html",
            request,
            user=user,
            admin_login=admin,
            rows=rows,
            sites=sites,
            roles=list(UserRole),
            min_password=users_admin.MIN_PASSWORD_LEN,
            filters={"search": search, "role": role, "site": site, "status": status},
            note=request.query_params.get("note"),
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
    def panel_photo(file_path: str, user: PanelUser) -> Response:
        full = (photos_dir / file_path.lstrip("/")).resolve()
        if not full.is_relative_to(photos_dir.resolve()) or not full.is_file():
            raise HTTPException(status_code=404)
        return FileResponse(full, media_type="image/jpeg")

    return router
