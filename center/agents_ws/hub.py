"""Реестр живых соединений с агентами и ожидание результатов команд.

Хаб не знает о FastAPI и БД — только о соединениях (протокол ``AgentLink``)
и сообщениях shared.messages. Это позволяет тестировать его без сети.

Команда взвешивания (для API v1): ``send_weigh_request`` отправляет
``weigh_request`` агенту весов и ждёт ``weigh_result`` с тем же
``request_id`` (его доставляет ``resolve_result`` из цикла приёма).
"""

import asyncio
import logging
from typing import Protocol
from uuid import UUID

from shared.enums import ErrorCode
from shared.messages import TareRegistryUpdate, WeighRequest, WeighResult

logger = logging.getLogger(__name__)

DEFAULT_WEIGH_TIMEOUT_S = 120.0  # тайм-аут всей операции взвешивания


class AgentLink(Protocol):
    """Минимум, который хабу нужен от соединения (реализация — WebSocket)."""

    async def send_text(self, data: str) -> None: ...


class AgentHubError(Exception):
    """Ошибка команды с кодом для ответа АИС."""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class AgentHub:
    """Соединения агентов: по одному на весы (scale_id)."""

    def __init__(self) -> None:
        self._links: dict[int, AgentLink] = {}
        # request_id -> (scale_id, future результата)
        self._pending: dict[UUID, tuple[int, asyncio.Future[WeighResult]]] = {}

    # --- жизненный цикл соединений (вызывает WS-маршрут) ---

    def attach(self, scale_id: int, link: AgentLink) -> AgentLink | None:
        """Зарегистрировать соединение; вернуть вытесненное старое (если было).

        Новое соединение агента вытесняет старое: после рестарта агента
        полуживой TCP старого соединения не должен блокировать работу.
        """
        old = self._links.get(scale_id)
        self._links[scale_id] = link
        return old

    def detach(self, scale_id: int, link: AgentLink) -> bool:
        """Снять соединение, если оно всё ещё текущее; вернуть, было ли текущим.

        False означает, что умерло уже вытесненное соединение — статус
        агента и его команды трогать нельзя (он жив через новое).
        """
        if self._links.get(scale_id) is link:
            del self._links[scale_id]
            return True
        return False

    def connected(self, scale_id: int) -> bool:
        return scale_id in self._links

    def connected_scale_ids(self) -> list[int]:
        return list(self._links)

    # --- команды взвешивания ---

    async def send_weigh_request(
        self,
        scale_id: int,
        request: WeighRequest,
        *,
        timeout_s: float = DEFAULT_WEIGH_TIMEOUT_S,
    ) -> WeighResult:
        """Отправить команду агенту и дождаться результата.

        AgentHubError(ERR_AGENT_OFFLINE) — агент не подключён;
        AgentHubError(ERR_INTERNAL) — тайм-аут или разрыв во время операции.
        """
        link = self._links.get(scale_id)
        if link is None:
            raise AgentHubError(ErrorCode.ERR_AGENT_OFFLINE, "нет связи с агентом объекта")
        if request.request_id in self._pending:
            raise AgentHubError(ErrorCode.ERR_BUSY, "команда с таким request_id уже выполняется")

        future: asyncio.Future[WeighResult] = asyncio.get_running_loop().create_future()
        self._pending[request.request_id] = (scale_id, future)
        try:
            try:
                await link.send_text(request.model_dump_json())
            except Exception as exc:  # полуживой TCP: наружу — кодированная ошибка
                raise AgentHubError(
                    ErrorCode.ERR_AGENT_OFFLINE, "соединение с агентом оборвалось"
                ) from exc
            return await asyncio.wait_for(future, timeout=timeout_s)
        except TimeoutError as exc:
            raise AgentHubError(
                ErrorCode.ERR_INTERNAL, "агент не ответил за отведённое время"
            ) from exc
        finally:
            self._pending.pop(request.request_id, None)

    def resolve_result(self, result: WeighResult, *, scale_id: int | None = None) -> bool:
        """Доставить weigh_result ожидающей команде; False — никто не ждал.

        «Ничей» результат — это late-ответ после тайм-аута либо результат
        после рестарта центра: запись всё равно сохраняется вызывающим кодом.
        ``scale_id`` — весы, приславшие результат: чужой request_id
        (не их команда) не резолвится.
        """
        entry = self._pending.get(result.request_id)
        if entry is None:
            return False
        pending_scale_id, future = entry
        if scale_id is not None and pending_scale_id != scale_id:
            logger.warning(
                "весы %d прислали результат чужой команды %s — игнорируем",
                scale_id,
                result.request_id,
            )
            return False
        if future.done():
            return False
        future.set_result(result)
        return True

    def fail_pending_for_scale(self, scale_id: int, reason: str) -> None:
        """Разрыв соединения: команды ЭТИХ весов завершаются ошибкой.

        Команды других весов не трогаем — их соединения живы.
        """
        for request_id, (pending_scale_id, future) in list(self._pending.items()):
            if pending_scale_id != scale_id:
                continue
            if not future.done():
                future.set_exception(AgentHubError(ErrorCode.ERR_AGENT_OFFLINE, reason))
            self._pending.pop(request_id, None)

    # --- рассылка реестра тарирований ---

    async def broadcast_tare_registry(self, update: TareRegistryUpdate) -> int:
        """Разослать снимок реестра всем подключённым агентам; вернуть число."""
        payload = update.model_dump_json()
        sent = 0
        for scale_id, link in list(self._links.items()):
            try:
                await link.send_text(payload)
                sent += 1
            except Exception:
                logger.warning("не удалось отправить реестр агенту весов %d", scale_id)
        return sent
