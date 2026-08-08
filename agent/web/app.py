"""Локальный веб-интерфейс оператора (FastAPI + Jinja2 + HTMX, architecture §3.4).

Экраны: вход, главный (вес, камеры, журнал, режимы), «Оборудование».
Живой вес — WebSocket ``/ws/state``; журнал и пилюли статуса — HTMX-опрос
фрагментов; кадры камер — периодически обновляемые снимки.

Правило режимов (№3): при связи с центром ручные операции заблокированы —
флаг ``manual_allowed`` вычисляется на сервере из ``center_connected()``
и уходит в шаблоны; серверные обработчики ручного режима (следующая
задача) обязаны проверять его сами, а не доверять кнопкам.

Доступ — только после входа оператора; сессия в подписанной cookie.
"""

import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Form, HTTPException, Request, Response, WebSocket
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.websockets import WebSocketDisconnect

from agent.web.services import AgentServices
from agent.weighing.manual import ManualFlowError
from shared.enums import CameraRole, Operation, ScaleStatus

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"
STATE_PUSH_INTERVAL_S = 0.3  # период отправки веса в браузер


def _fmt_time(value: datetime | None) -> str:
    """ЧЧ:ММ в местном времени весового ПК (для журнала и шапки)."""
    if value is None:
        return "—"
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone().strftime("%H:%M")


def _fmt_kg(value: float | None) -> str:
    """Вес с разделителем тысяч: 43310 → «43 310», None → «—»."""
    if value is None:
        return "—"
    # разделитель тысяч — узкий неразрывный пробел (U+202F), как в макетах
    return f"{value:,.0f}".replace(",", "\u202f")


def create_app(services: AgentServices, *, session_secret: str) -> FastAPI:
    """Собрать приложение локального интерфейса поверх слоя сервисов."""
    app = FastAPI(title="Весовая система — интерфейс оператора", docs_url=None, redoc_url=None)
    app.add_middleware(
        SessionMiddleware,
        secret_key=session_secret,
        session_cookie="ves_session",
        same_site="strict",  # доступ только из сети объекта, межсайтовых переходов нет
    )
    # статика локальная (htmx, стили): весовой ПК может быть без интернета
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.filters["fmt_time"] = _fmt_time
    templates.env.filters["fmt_kg"] = _fmt_kg

    # --- общие помощники ---

    def current_operator(request: Request) -> str:
        """Имя вошедшего оператора; без входа — редирект на /login."""
        operator = request.session.get("operator")
        if not operator:
            raise HTTPException(status_code=303, headers={"Location": "/login"})
        return str(operator)

    Operator = Annotated[str, Depends(current_operator)]

    def status_context() -> dict[str, Any]:
        """Данные пилюль шапки и режима — нужны каждому экрану."""
        scale = services.scale_state()
        center_online = services.center_connected()
        return {
            "info": services.info,
            "scale": scale,
            "scale_ok": scale.status is ScaleStatus.OK,
            "center_online": center_online,
            "manual_allowed": not center_online,  # правило режимов №3
            "pending_count": services.pending_count(),
            "now": datetime.now(UTC),
        }

    def journal_context() -> dict[str, Any]:
        """Строки журнала: брутто/тара раскладываются по типу операции."""
        rows = []
        for record, synced in services.recent_weighings(limit=50):
            is_weighing = record.operation is Operation.WEIGHING
            rows.append(
                {
                    "record": record,
                    "gross": record.massa if is_weighing else None,
                    "tare": record.tare_value if is_weighing else record.massa,
                    "netto": record.netto if is_weighing else None,
                    "synced": synced,
                }
            )
        return {"journal_rows": rows}

    def render(template: str, request: Request, **extra: Any) -> HTMLResponse:
        context: dict[str, Any] = {"request": request, **status_context(), **extra}
        return templates.TemplateResponse(request, template, context)

    # --- вход/выход ---

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request) -> HTMLResponse:
        return render("login.html", request, error=None)

    @app.post("/login", response_class=HTMLResponse)
    def login_submit(
        request: Request,
        login: Annotated[str, Form()],
        password: Annotated[str, Form()],
    ) -> Response:
        display_name = services.verify_operator(login.strip(), password)
        if display_name is None:
            logger.warning("неудачный вход оператора: %s", login.strip())
            return render("login.html", request, error="Неверный логин или пароль")
        request.session["operator"] = display_name
        logger.info("вход оператора: %s", display_name)
        return RedirectResponse("/", status_code=303)

    @app.post("/logout")
    def logout(request: Request) -> RedirectResponse:
        request.session.clear()
        return RedirectResponse("/login", status_code=303)

    # --- экраны ---

    @app.get("/", response_class=HTMLResponse)
    def main_page(request: Request, operator: Operator) -> HTMLResponse:
        return render("main.html", request, operator=operator, **journal_context())

    @app.get("/equipment", response_class=HTMLResponse)
    def equipment_page(request: Request, operator: Operator) -> HTMLResponse:
        return render(
            "equipment.html",
            request,
            operator=operator,
            camera_roles=services.camera_roles(),
            tare_registry_size=services.tare_registry_size(),
        )

    # --- HTMX-фрагменты ---

    @app.get("/fragments/status", response_class=HTMLResponse)
    def fragment_status(request: Request, operator: Operator) -> HTMLResponse:
        return render("fragments/status.html", request, operator=operator)

    @app.get("/fragments/journal", response_class=HTMLResponse)
    def fragment_journal(request: Request, operator: Operator) -> HTMLResponse:
        return render("fragments/journal.html", request, operator=operator, **journal_context())

    # --- камеры ---

    @app.get("/cameras/{role}.jpg")
    def camera_snapshot(role: str, operator: Operator) -> Response:
        try:
            camera_role = CameraRole(role)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail="нет такой камеры") from exc
        if camera_role not in services.camera_roles():
            raise HTTPException(status_code=404, detail="камера не настроена")
        shot = services.camera_snapshot(camera_role)
        if not shot.ok or shot.jpeg is None:
            raise HTTPException(status_code=502, detail=shot.error or "камера недоступна")
        return Response(
            content=shot.jpeg,
            media_type="image/jpeg",
            headers={"Cache-Control": "no-store"},
        )

    # --- оборудование: действия ---

    @app.post("/equipment/reopen-port", response_class=HTMLResponse)
    def reopen_port(request: Request, operator: Operator) -> HTMLResponse:
        logger.info("оператор %s переоткрывает порт индикатора", operator)
        services.reopen_port()
        return render("fragments/status.html", request, operator=operator)

    # --- ручной режим (автономный) ---

    _MANUAL_OPERATIONS = {"weighing": Operation.WEIGHING, "taring": Operation.TARING}

    def manual_operation_or_404(op: str) -> Operation:
        operation = _MANUAL_OPERATIONS.get(op)
        if operation is None:
            raise HTTPException(status_code=404, detail="нет такой операции")
        return operation

    def require_manual_mode() -> None:
        """Правило №3: при живой связи с центром ручной режим запрещён.

        Проверка серверная — кнопкам в браузере не доверяем.
        """
        if services.center_connected():
            raise HTTPException(status_code=303, headers={"Location": "/"})

    @app.get("/manual/{op}", response_class=HTMLResponse)
    def manual_form(op: str, request: Request, operator: Operator) -> HTMLResponse:
        operation = manual_operation_or_404(op)
        require_manual_mode()
        return render(
            "manual.html",
            request,
            operator=operator,
            operation=operation,
            error=None,
            vehicle_number="",
            trailer_number="",
        )

    @app.post("/manual/{op}", response_class=HTMLResponse)
    def manual_capture(
        op: str,
        request: Request,
        operator: Operator,
        vehicle_number: Annotated[str, Form()],
        trailer_number: Annotated[str, Form()] = "",
    ) -> HTMLResponse:
        operation = manual_operation_or_404(op)
        require_manual_mode()
        try:
            # одношагово (как в ВесыСофт): нажатие кнопки = фиксация + запись
            preview = services.manual_capture(
                operation,
                vehicle_number=vehicle_number,
                trailer_number=trailer_number or None,
                operator=operator,
            )
        except ManualFlowError as exc:
            return render(
                "manual.html",
                request,
                operator=operator,
                operation=operation,
                error=str(exc),
                vehicle_number=vehicle_number,
                trailer_number=trailer_number,
            )
        logger.info("оператор %s записал операцию %s", operator, preview.record.uuid)
        return render("manual_result.html", request, operator=operator, preview=preview)

    @app.get("/manual-fragments/tare-hint", response_class=HTMLResponse)
    def tare_hint(
        request: Request,
        operator: Operator,
        vehicle_number: str = "",
        trailer_number: str = "",
    ) -> HTMLResponse:
        """Подсказка «по сцепке найдена тара …» (HTMX по вводу номеров).

        Тара ищется по ПАРЕ голова+прицеп (решение 09.08.2026) — смена
        прицепа в форме сразу убирает подсказку чужой тары.
        """
        tare = None
        number = vehicle_number.strip().upper()
        if number:
            tare = services.find_active_tare(number, trailer_number.strip().upper() or None)
        return render("fragments/tare_hint.html", request, operator=operator, tare=tare)

    # --- живой вес ---

    @app.websocket("/ws/state")
    async def ws_state(websocket: WebSocket) -> None:
        if not websocket.session.get("operator"):
            await websocket.close(code=4401)  # не вошёл — соединение не для него
            return
        await websocket.accept()
        try:
            while True:
                scale = services.scale_state()
                await websocket.send_json(
                    {
                        "status": scale.status.value,
                        "weight_kg": scale.weight_kg,
                        "weight_text": _fmt_kg(scale.weight_kg),
                        "stable": scale.stable,
                        "overload": scale.overload,
                        "center_online": services.center_connected(),
                        "manual_allowed": not services.center_connected(),
                        "manual_ready": services.manual_ready(),
                        "pending_count": services.pending_count(),
                    }
                )
                await asyncio.sleep(STATE_PUSH_INTERVAL_S)
        except WebSocketDisconnect:
            pass

    return app
