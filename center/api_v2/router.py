"""Нативный API v2 для АИС «СВХ» — контракт docs/contracts/ais-api-v2.md (1.0,
согласован 17.08.2026).

- ``POST /api/v2/weighings`` — синхронная команда взвешивания/тарирования:
  маршрутизация по паре «Специальный идентификатор СВХ + № весов»
  (справочник центра), команда агенту через хаб, ответ — документ операции
  (раздел 5). Исход операции — всегда HTTP 200 с ``code``; не-200 только
  для неверного запроса (401/403/404/422) — раздел 4.3.
- Идемпотентность по номеру документа АИС (``ais_ref``, раздел 4.5): один
  документ АИС = одна операция. Повтор по состоявшейся → тот же документ с
  ``repeated: true``; повтор во время выполнения → ждём исход первой и
  отвечаем им же; после отказа (ничего не записано) — новая попытка.
- ``GET /api/v2/weighings/{id}``, ``?ais_ref=…``, список за период — сверка
  (раздел 8); ``POST /api/v2/weighings/{id}/ais_ref`` — обратная связь по
  офлайн-операциям (7.5).

Авторизация — сервисный Bearer-токен интегратора + IP-allowlist (те же,
что для фото); правило №7 — значения только из окружения.
"""

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentHubError
from center.api_v1.schemas import BISHKEK_TZ
from center.api_v2.documents import build_document
from center.api_v2.schemas import (
    AisRefLink,
    WeighV2Request,
    check_ais_ref,
    validation_details,
)
from center.db import repo
from center.db.models import AuditLog, Scale, Weighing, WeighingAisRef
from shared.enums import ErrorCode, Operation, WeighingSource
from shared.messages import WeighRequest

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]

DEFAULT_WEIGH_TIMEOUT_S = 120.0
MAX_PER_PAGE = 200


@dataclass(frozen=True)
class ApiV2Config:
    """Настройки API v2 (значения — из окружения, правило №7)."""

    service_tokens: Mapping[str, str]  # токен → имя интегратора («ais-svh»)
    photos_dir: Path
    allowed_ips: frozenset[str] | None = None  # None — без ограничения по адресу
    weigh_timeout_s: float = DEFAULT_WEIGH_TIMEOUT_S


def _error(status: int, code: str, message: str, **extra: Any) -> JSONResponse:
    body: dict[str, Any] = {"code": code, "message": message}
    body.update(extra)
    return JSONResponse(body, status_code=status)


def _parse_moment(value: str | None) -> datetime | None:
    """ISO 8601 с поясом → datetime; naive считаем бишкекским временем."""
    if not value:
        return None
    # незакодированный «+» пояса в query приходит пробелом — восстанавливаем
    value = value.strip().replace(" ", "+")
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=BISHKEK_TZ)
    return moment


def create_api_v2_router(
    hub: AgentHub, session_factory: SessionFactory, config: ApiV2Config
) -> APIRouter:
    """Собрать маршруты v2 поверх хаба агентов и фабрики сессий БД."""
    router = APIRouter()
    # ais_ref → future исхода команды, которая сейчас выполняется: повтор с тем
    # же номером не запускает второе взвешивание, а ждёт этот же исход (4.5)
    inflight: dict[str, asyncio.Future[dict[str, Any]]] = {}

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    # --- авторизация интегратора ---

    def _authorize(request: Request) -> str | JSONResponse:
        token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        integrator = config.service_tokens.get(token) if token else None
        if integrator is None:
            return _error(401, "ERR_UNAUTHORIZED", "нет или неверный сервисный токен")
        client_ip = request.client.host if request.client else ""
        if config.allowed_ips is not None and client_ip not in config.allowed_ips:
            logger.warning("v2: запрос с непозволенного IP %s (%s)", client_ip, integrator)
            return _error(403, "ERR_FORBIDDEN", "адрес не в списке разрешённых")
        return integrator

    def _audit(actor: str, action: str, details: dict[str, Any]) -> None:
        def write(session: Session) -> None:
            session.add(AuditLog(actor=actor, action=action, details=details))
            session.commit()

        _db(write)

    def _document(session: Session, weighing: Weighing) -> dict[str, Any]:
        return build_document(session, weighing, photos_dir=config.photos_dir)

    def _document_by_uuid(record_uuid: UUID) -> dict[str, Any] | None:
        def load(session: Session) -> dict[str, Any] | None:
            weighing = session.execute(
                select(Weighing).where(Weighing.uuid == record_uuid)
            ).scalar_one_or_none()
            if weighing is None or weighing.code is not ErrorCode.OK:
                return None
            return _document(session, weighing)

        return _db(load)

    def _document_by_ais_ref(ais_ref: str) -> dict[str, Any] | None:
        def load(session: Session) -> dict[str, Any] | None:
            weighing = repo.weighing_by_ais_ref(session, ais_ref)
            return _document(session, weighing) if weighing is not None else None

        return _db(load)

    # --- команда ---

    async def _run_command(
        integrator: str, command: WeighV2Request, scale: Scale
    ) -> dict[str, Any]:
        """Выполнить команду на весах и собрать тело ответа (без repeated)."""
        request_id = uuid4()
        # действующая тара по авторитетному реестру центра — в команду агенту
        # (агент 0.4.17 применяет её вместо своей реплики; старые агенты поле
        # не знают и подставляют по реплике)
        tare_hint = await asyncio.to_thread(
            _db,
            lambda s: repo.resolve_tare_hint(
                s, command.operation, command.vehicle, command.trailer
            ),
        )
        weigh_request = WeighRequest(
            request_id=request_id,
            operation=command.operation,
            vehicle_number=command.vehicle,
            trailer_number=command.trailer,
            operator=command.operator,
            ais_ref=command.ais_ref,
            tare=tare_hint.tare,
            tare_resolved=tare_hint.resolved,
        )
        # номер документа АИС уедет в транзакцию сохранения записи (WS-сервер):
        # агент 0.4.17 вернёт его в самой записи, старым агентам его помнит хаб
        hub.remember_ais_ref(request_id, command.ais_ref)
        audit_details: dict[str, Any] = {
            "request": command.model_dump(mode="json"),
            "scale_id": scale.id,
        }
        try:
            result = await hub.send_weigh_request(
                scale.id, weigh_request, timeout_s=config.weigh_timeout_s
            )
        except AgentHubError as exc:
            if exc.code is ErrorCode.ERR_AGENT_OFFLINE:
                hub.take_ais_ref(request_id)  # команда не ушла — результата не будет
            audit_details["code"] = exc.code.value
            await asyncio.to_thread(_audit, f"ais:{integrator}", "weigh_request_v2", audit_details)
            return {"code": exc.code.value, "message": str(exc)}

        code = result.record.code
        audit_details["code"] = code.value
        audit_details["record_uuid"] = str(result.record.uuid)
        await asyncio.to_thread(_audit, f"ais:{integrator}", "weigh_request_v2", audit_details)
        if code is not ErrorCode.OK:
            # отказ — только {code, message}: ничего не записано (решение 10.08.2026)
            return {"code": code.value, "message": result.record.message or code.value}

        document = await asyncio.to_thread(_document_by_uuid, result.record.uuid)
        if document is None:
            # WS-сервер сохраняет запись ДО того, как разбудить команду; сюда
            # можно попасть только при сбое сохранения — честный код
            logger.error("v2: запись %s не найдена в журнале после OK", result.record.uuid)
            return {
                "code": ErrorCode.ERR_INTERNAL.value,
                "message": "операция выполнена, но запись не найдена в журнале центра",
            }
        return {"code": ErrorCode.OK.value, "weighing": document}

    @router.post("/api/v2/weighings")
    async def weigh_v2(request: Request) -> JSONResponse:
        integrator = _authorize(request)
        if isinstance(integrator, JSONResponse):
            return integrator
        try:
            payload = await request.json()
        except Exception:
            return _error(422, "ERR_VALIDATION", "тело запроса не является JSON")
        try:
            command = WeighV2Request.model_validate(payload)
        except ValidationError as exc:
            return _error(
                422,
                "ERR_VALIDATION",
                "запрос не проходит проверку",
                details=validation_details(exc),
            )

        scale = await asyncio.to_thread(
            _db, lambda s: repo.find_scale_by_ais_route(s, command.ais_object, command.scale_no)
        )
        if scale is None:
            logger.warning(
                "v2: весы не найдены: объект АИС %s, № весов %s",
                command.ais_object,
                command.scale_no,
            )
            await asyncio.to_thread(
                _audit,
                f"ais:{integrator}",
                "weigh_request_v2",
                {
                    "request": command.model_dump(mode="json"),
                    "code": "ERR_UNKNOWN_SCALE",
                },
            )
            return _error(
                404,
                "ERR_UNKNOWN_SCALE",
                "объект АИС и номер весов не привязаны к весам в справочнике центра",
            )

        # команда с этим номером сейчас выполняется — ждём её исход, второе
        # взвешивание не запускаем (4.5)
        pending = inflight.get(command.ais_ref)
        if pending is not None:
            body = dict(await asyncio.shield(pending))
            body["repeated"] = True
            return JSONResponse(body)

        # регистрируемся в inflight ДО похода в БД: параллельный повтор с тем же
        # номером не проскочит между проверкой «уже состоялась» и запуском команды
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        inflight[command.ais_ref] = future
        try:
            # идемпотентность (4.5): по этому номеру операция уже состоялась?
            existing = await asyncio.to_thread(_document_by_ais_ref, command.ais_ref)
            if existing is not None:
                await asyncio.to_thread(
                    _audit,
                    f"ais:{integrator}",
                    "weigh_request_v2",
                    {
                        "request": command.model_dump(mode="json"),
                        "code": "OK",
                        "repeated": True,
                        "record_uuid": existing["id"],
                    },
                )
                body = {"code": "OK", "weighing": existing}
                future.set_result(body)
                return JSONResponse({**body, "repeated": True})
            body = await _run_command(integrator, command, scale)
        except BaseException:
            # ждущие повторы получают честный код, а не 500; первый — своё исключение
            if not future.done():
                future.set_result(
                    {
                        "code": ErrorCode.ERR_INTERNAL.value,
                        "message": "команда прервана на стороне центра",
                    }
                )
            raise
        else:
            future.set_result(body)
        finally:
            inflight.pop(command.ais_ref, None)
        return JSONResponse(body)

    # --- сверка (раздел 8) ---

    @router.get("/api/v2/weighings/{weighing_id}")
    async def get_weighing(weighing_id: str, request: Request) -> JSONResponse:
        integrator = _authorize(request)
        if isinstance(integrator, JSONResponse):
            return integrator
        try:
            record_uuid = UUID(weighing_id)
        except ValueError:
            return _error(404, "ERR_NOT_FOUND", "нет операции с таким id")
        document = await asyncio.to_thread(_document_by_uuid, record_uuid)
        if document is None:
            return _error(404, "ERR_NOT_FOUND", "нет операции с таким id")
        return JSONResponse({"weighing": document})

    @router.get("/api/v2/weighings")
    async def list_weighings(request: Request) -> JSONResponse:
        integrator = _authorize(request)
        if isinstance(integrator, JSONResponse):
            return integrator
        params = request.query_params
        try:
            moment_from = _parse_moment(params.get("from"))
            moment_to = _parse_moment(params.get("to"))
            page = max(1, int(params.get("page", "1")))
            per_page = min(MAX_PER_PAGE, max(1, int(params.get("per_page", str(MAX_PER_PAGE)))))
            scale_no = int(params["scale_no"]) if params.get("scale_no") else None
        except ValueError:
            return _error(422, "ERR_VALIDATION", "неверные параметры запроса")
        operation_raw = params.get("operation")
        source_raw = params.get("source")
        try:
            operation = Operation(operation_raw) if operation_raw else None
            source = WeighingSource(source_raw) if source_raw else None
        except ValueError:
            return _error(422, "ERR_VALIDATION", "неверные параметры запроса")
        ais_ref = (params.get("ais_ref") or "").strip() or None
        ais_object = (params.get("ais_object") or "").strip() or None
        unlinked = params.get("unlinked") in {"1", "true", "yes"}

        def load(session: Session) -> dict[str, Any]:
            query = select(Weighing).where(Weighing.code == ErrorCode.OK)
            if ais_ref is not None:
                query = query.join(WeighingAisRef, WeighingAisRef.weighing_id == Weighing.id).where(
                    WeighingAisRef.ais_ref == ais_ref
                )
            if moment_from is not None:
                query = query.where(Weighing.weighed_at >= moment_from)
            if moment_to is not None:
                query = query.where(Weighing.weighed_at < moment_to)
            if ais_object is not None or scale_no is not None:
                query = query.join(Scale, Scale.id == Weighing.scale_id)
                if ais_object is not None:
                    query = query.where(Scale.ais_object == ais_object)
                if scale_no is not None:
                    query = query.where(Scale.ais_scale_no == scale_no)
            if operation is not None:
                query = query.where(Weighing.operation == operation)
            if source is not None:
                query = query.where(Weighing.source == source)
            if unlinked:
                linked_ids = select(WeighingAisRef.weighing_id)
                query = query.where(Weighing.id.not_in(linked_ids))
            total = session.execute(
                select(func.count()).select_from(query.order_by(None).subquery())
            ).scalar_one()
            rows = (
                session.execute(
                    query.order_by(Weighing.weighed_at.asc(), Weighing.id.asc())
                    .offset((page - 1) * per_page)
                    .limit(per_page)
                )
                .scalars()
                .all()
            )
            return {
                "weighings": [_document(session, w) for w in rows],
                "page": page,
                "per_page": per_page,
                "total": total,
            }

        return JSONResponse(await asyncio.to_thread(_db, load))

    # --- обратная связь по офлайн-операциям (7.5) ---

    @router.post("/api/v2/weighings/{weighing_id}/ais_ref")
    async def link_ais_ref(weighing_id: str, request: Request) -> JSONResponse:
        integrator = _authorize(request)
        if isinstance(integrator, JSONResponse):
            return integrator
        try:
            record_uuid = UUID(weighing_id)
        except ValueError:
            return _error(404, "ERR_NOT_FOUND", "нет операции с таким id")
        try:
            payload = await request.json()
            link = AisRefLink.model_validate(payload)
        except (ValidationError, Exception) as exc:
            details = validation_details(exc) if isinstance(exc, ValidationError) else None
            return _error(422, "ERR_VALIDATION", "запрос не проходит проверку", details=details)

        def apply(session: Session) -> tuple[int, dict[str, Any]]:
            weighing = session.execute(
                select(Weighing).where(Weighing.uuid == record_uuid)
            ).scalar_one_or_none()
            if weighing is None or weighing.code is not ErrorCode.OK:
                return 404, {"code": "ERR_NOT_FOUND", "message": "нет операции с таким id"}
            error = check_ais_ref(link.ais_ref, weighing.operation)
            if error is not None:
                return 422, {"code": "ERR_VALIDATION", "message": error}
            outcome = repo.link_ais_ref(session, weighing, link.ais_ref, origin="callback")
            if outcome == "conflict":
                return 409, {
                    "code": "ERR_ALREADY_LINKED",
                    "message": "у операции уже другой номер АИС либо номер занят другой операцией",
                }
            session.add(
                AuditLog(
                    actor=f"ais:{integrator}",
                    action="ais_ref_link",
                    details={
                        "record_uuid": str(record_uuid),
                        "ais_ref": link.ais_ref,
                        "outcome": outcome,
                    },
                )
            )
            session.commit()
            return 200, {"weighing": _document(session, weighing)}

        status, body = await asyncio.to_thread(_db, apply)
        return JSONResponse(body, status_code=status)

    return router
