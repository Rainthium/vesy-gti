"""Совместимый API v1 для АИС «СВХ» (docs/contracts/ais-api-v1.md).

`POST /api/v1/weigh` — синхронная команда взвешивания/тарирования:
маршрутизация по legacy-адресу UniServer → команда агенту через хаб →
ответ в формате текущего контракта (+ согласованные новые поля).

Совместимость: ЛЮБОЙ исход возвращается HTTP 200 с полем ``code`` —
старый клиент разбирает тело, а не статусы (битый JSON — исключение,
FastAPI ответит 422). Ошибки авторизации и маршрутизации отдаются кодом
``ERR_INTERNAL`` с русским ``message``.

Правило №3 соблюдается конструкцией: команда уходит агенту, у агента
онлайн-режим блокирует ручные операции.
"""

import asyncio
import hmac
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from center.agents_ws.hub import AgentHub, AgentHubError
from center.api_v1.schemas import WeighV1Request, bishkek_iso
from center.db import repo
from center.db.models import AuditLog, Scale, Weighing, WeighingPhoto
from shared.enums import CameraRole, ErrorCode, Operation
from shared.messages import WeighRequest, WeighResult
from shared.tare import tare_below_gross

logger = logging.getLogger(__name__)

SessionFactory = Callable[[], Session]

DEFAULT_WEIGH_TIMEOUT_S = 120.0


@dataclass(frozen=True)
class ApiV1Config:
    """Настройки совместимого приёма (правило №7: значения из env, не из кода).

    admin/admin — единственное место, где он разрешён: совместимый приём
    старых запросов АИС (правило проекта №7).
    """

    legacy_username: str = "admin"
    legacy_password: str = "admin"
    weigh_timeout_s: float = DEFAULT_WEIGH_TIMEOUT_S


def create_api_v1_router(
    hub: AgentHub, session_factory: SessionFactory, config: ApiV1Config | None = None
) -> APIRouter:
    """Собрать маршруты v1 поверх хаба агентов и фабрики сессий БД."""
    router = APIRouter()
    cfg = config or ApiV1Config()

    def _db[T](fn: Callable[[Session], T]) -> T:
        with session_factory() as session:
            return fn(session)

    def _error(code: ErrorCode, message: str) -> JSONResponse:
        return JSONResponse({"code": code.value, "message": message})

    def _find_scale(session: Session, request: WeighV1Request) -> Scale | None:
        """Маршрутизация по legacy-адресу UniServer (ip + autoscale [+ port])."""
        rows = session.execute(
            select(Scale)
            .where(Scale.legacy_ip == request.ip_address)
            .where(Scale.legacy_autoscale == request.autoscale)
        ).scalars()
        candidates = [s for s in rows if s.legacy_port is None or s.legacy_port == request.port]
        return candidates[0] if candidates else None

    def _photo_paths(session: Session, record_uuid: UUID) -> dict[str, str | None]:
        """Пути фото записи по ролям камер (front → photo1, rear → photo2)."""
        row = session.execute(
            select(Weighing.id).where(Weighing.uuid == record_uuid)
        ).scalar_one_or_none()
        paths: dict[str, str | None] = {"front": None, "rear": None}
        if row is None:
            return paths
        for photo in session.execute(
            select(WeighingPhoto).where(WeighingPhoto.weighing_id == row)
        ).scalars():
            if photo.role is CameraRole.FRONT:
                paths["front"] = photo.path
            elif photo.role is CameraRole.REAR:
                paths["rear"] = photo.path
        return paths

    def _audit(request: WeighV1Request, code: ErrorCode, record_uuid: UUID | None) -> None:
        """Журналирование команд АИС (architecture §7); пароль не пишем."""
        payload = request.model_dump(mode="json", exclude={"password"})

        def write(session: Session) -> None:
            session.add(
                AuditLog(
                    actor=f"ais:{request.username}",
                    action="weigh_request_v1",
                    details={
                        "request": payload,
                        "code": code.value,
                        "record_uuid": str(record_uuid) if record_uuid else None,
                    },
                )
            )
            session.commit()

        _db(write)

    def _build_success_response(request: WeighV1Request, result: WeighResult) -> dict[str, Any]:
        record = result.record
        response: dict[str, Any] = {
            "code": record.code.value,
            "massa": record.massa,
            "weighing_datetime": bishkek_iso(record.weighed_at),
            "unit_meas": record.unit,
        }
        if record.message:
            response["message"] = record.message
        paths = _db(lambda s: _photo_paths(s, record.uuid))
        response["front_image"] = paths["front"]
        response["rear_image"] = paths["rear"]

        if request.operation is Operation.WEIGHING:
            # тара/нетто: доверяем расчёту агента (реплика реестра);
            # если агент не заполнил, а номер известен — считает центр
            tare_value = record.tare_value
            netto = record.netto
            tare_datetime: str | None = None
            if tare_value is None and record.vehicle_number:
                vehicle = record.vehicle_number
                trailer = record.trailer_number
                tare = _db(lambda s: repo.find_active_tare(s, vehicle, trailer))
                # тара не меньше брутто не подставляется (decisions 04.09.2026) —
                # та же проверка, что у агента 0.4.29; иначе АИС получила бы
                # отрицательное нетто, которого нет в журнале
                if (
                    tare is not None
                    and record.massa is not None
                    and tare_below_gross(tare.tare_value, record.massa)
                ):
                    tare_value = tare.tare_value
                    netto = record.massa - tare.tare_value
                    tare_datetime = bishkek_iso(tare.tared_at)
            elif tare_value is not None and record.tare_weighing_uuid is not None:
                tare_row = _db(
                    lambda s: s.execute(
                        select(Weighing.weighed_at).where(
                            Weighing.uuid == record.tare_weighing_uuid
                        )
                    ).scalar_one_or_none()
                )
                tare_datetime = bishkek_iso(tare_row)
            response["tare"] = tare_value
            response["tare_datetime"] = tare_datetime
            response["netto"] = netto
            if tare_value is None:
                response["no_valid_tare"] = True
        return response

    @router.post("/api/v1/weigh")
    async def weigh_v1(request: WeighV1Request) -> JSONResponse:
        # авторизация по сервисной учётке совместимого приёма
        # сравнение байтами: compare_digest не принимает не-ASCII строки
        credentials_ok = hmac.compare_digest(
            request.username.encode(), cfg.legacy_username.encode()
        ) and hmac.compare_digest(request.password.encode(), cfg.legacy_password.encode())
        if not credentials_ok:
            logger.warning("v1: неверные учётные данные (пользователь %s)", request.username)
            await asyncio.to_thread(_audit, request, ErrorCode.ERR_INTERNAL, None)
            return _error(ErrorCode.ERR_INTERNAL, "Неверные учётные данные")

        scale = await asyncio.to_thread(_db, lambda s: _find_scale(s, request))
        if scale is None:
            logger.warning(
                "v1: весы не найдены по адресу %s:%s autoscale=%s",
                request.ip_address,
                request.port,
                request.autoscale,
            )
            await asyncio.to_thread(_audit, request, ErrorCode.ERR_INTERNAL, None)
            return _error(
                ErrorCode.ERR_INTERNAL,
                "Весы не найдены по указанному адресу — проверьте справочник маршрутизации",
            )

        vehicle_number = (request.vehicle_number or "").strip().upper() or None
        trailer_number = (request.trailer_number or "").strip().upper() or None
        # действующая тара по авторитетному реестру центра — в команду агенту
        # (17.08.2026): реплика на весовом ПК могла отстать
        tare_hint = await asyncio.to_thread(
            _db,
            lambda s: repo.resolve_tare_hint(s, request.operation, vehicle_number, trailer_number),
        )
        command = WeighRequest(
            request_id=uuid4(),
            operation=request.operation,
            vehicle_number=vehicle_number,
            trailer_number=trailer_number,
            # ФИО как прислали, без upper; потолок — ширина колонки в БД
            operator=" ".join((request.operator or "").split())[:200] or None,
            tare=tare_hint.tare,
            tare_resolved=tare_hint.resolved,
        )
        try:
            result = await hub.send_weigh_request(scale.id, command, timeout_s=cfg.weigh_timeout_s)
        except AgentHubError as exc:
            await asyncio.to_thread(_audit, request, exc.code, None)
            return _error(exc.code, str(exc))

        code = result.record.code
        await asyncio.to_thread(_audit, request, code, result.record.uuid)

        # любая ошибка цикла — только {code, message}; ERR_CAMERA тоже:
        # без снимков обеих камер операция не проводится (решение 09.08.2026)
        if code is not ErrorCode.OK:
            if code is ErrorCode.ERR_TARE_TOO_HEAVY:
                # гружёная машина как тарирование (04.09.2026) — событие мониторинга
                alert = (
                    f"тарирование отклонено — {result.record.message or code.value} "
                    f"(ТС {vehicle_number}" + (f"/{trailer_number}" if trailer_number else "") + ")"
                )
                await asyncio.to_thread(
                    _db, lambda s: repo.record_scale_alert(s, scale.id, alert, kind="tare_rejected")
                )
            return _error(code, result.record.message or code.value)

        response = await asyncio.to_thread(_build_success_response, request, result)
        return JSONResponse(response)

    return router
