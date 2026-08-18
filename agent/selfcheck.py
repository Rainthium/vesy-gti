"""Самопроверка после автообновления и доклады об исходе (агент 0.4.19).

Протокол с self-update.bat — файлы в каталоге установки (agent/updater.py):
``update-context.json`` пишет обновляющийся агент перед запуском bat;
новая версия при старте видит контекст со своей версией и проверяет себя:

1. веб-интерфейс оператора поднялся (порт слушает);
2. связь с центром установлена (hello ушёл);
3. индикатор шлёт данные — только если он шёл до обновления
   (``expect_indicator``): молчащий индикатор при выключенных весах или
   занятом UniServer'ом порту не повод откатывать релиз.

Итог — маркер ``update-check.ok`` (плюс доклад центру ``installed``) или
``update-check.fail`` с причиной первой строкой; дальше решает bat: по
``fail`` или по молчанию (6 минут — новая версия не поднялась вовсе) он
возвращает прежнюю версию и пишет ``update-rollback.txt``. Прежняя версия
при старте видит rollback-файл и докладывает центру ``rolled_back`` —
событие «Обновление» в панели и Telegram. Контекст с чужой ``to_version``
при старте означает, что bat не смог подменить папку и перезапустил
прежнюю версию — доклад «обновление не состоялось».

Задача живёт вне списка «любая упавшая задача останавливает агента»
(она заканчивается за минуты) и любую свою ошибку только логирует:
самопроверка не должна уронить работающего агента.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

import agent
from agent.updater import (
    CHECK_FAIL_FILE,
    CHECK_OK_FILE,
    UPDATE_CONTEXT_FILE,
    UpdateContext,
    clear_update_markers,
    read_rollback,
    read_update_context,
    safe_reason,
)
from shared.messages import UpdateStatus

logger = logging.getLogger(__name__)

WEB_WAIT_S = 30.0  # uvicorn на HDD весового ПК стартует за секунды
CENTER_WAIT_S = 120.0  # реконнект с backoff до 30 с — двух минут хватает
INDICATOR_WAIT_S = 90.0  # порт открывается сразу, поток cas22 непрерывный
POLL_S = 1.0
# после ok-маркера bat ещё дочитывает его (опрос раз в 5 с) и убирает задачу
# планировщика — новое обновление не должно стартовать в это окно
SETTLE_S = 30.0


class UpdateSelfCheck:
    """Проверить себя после обновления и доложить центру об исходе."""

    def __init__(
        self,
        base: Path | None,
        *,
        agent_id: str,
        web_ready: Callable[[], bool],
        center_connected: Callable[[], bool],
        indicator_ok: Callable[[], bool],
        notify: Callable[[UpdateStatus], None],
        version: str | None = None,
        web_wait_s: float = WEB_WAIT_S,
        center_wait_s: float = CENTER_WAIT_S,
        indicator_wait_s: float = INDICATOR_WAIT_S,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._base = base
        self._agent_id = agent_id
        self._version = version or agent.__version__
        # публичный: main.py подставляет «веб-сервер поднялся» после создания
        # uvicorn.Server (объект появляется позже сборки кирпичей)
        self.web_ready = web_ready
        self._center_connected = center_connected
        self._indicator_ok = indicator_ok
        self._notify = notify
        self._web_wait_s = web_wait_s
        self._center_wait_s = center_wait_s
        self._indicator_wait_s = indicator_wait_s
        self._sleep = sleep
        self._monotonic = monotonic
        # состояние для обновляющего кода: пока идёт самопроверка (или только
        # что прошла, или провалилась и bat вот-вот откатит), новое обновление
        # стартовать нельзя — два bat перепутали бы маркеры и папки
        self._state = "idle"  # idle | pending | ok | failed
        self._finished_at: float | None = None

    def hold_reason(self) -> tuple[str, bool] | None:
        """Почему сейчас нельзя начинать новое обновление: (причина, навсегда).

        None — можно. ``навсегда=True`` — ждать бессмысленно (самопроверка
        провалена, bat откатывает — процесс скоро остановят).
        """
        if self._state == "pending":
            return "идёт самопроверка после предыдущего обновления", False
        if self._state == "failed":
            return "новая версия не прошла самопроверку — идёт откат", True
        if (
            self._state == "ok"
            and self._finished_at is not None
            and self._monotonic() - self._finished_at < SETTLE_S
        ):
            return "предыдущее обновление ещё завершается", False
        return None

    async def run(self) -> None:
        """Точка входа фоновой задачи: dev-запуск (base=None) — ничего не делает."""
        if self._base is None:
            return
        try:
            await self._run(self._base)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("самопроверка после обновления упала — агент продолжает работу")
        finally:
            if self._state == "pending":
                # упавшая проверка не должна вечно держать обновления
                self._state = "idle"

    async def _run(self, base: Path) -> None:
        # 1. Нас вернули откатом: доложить и убрать следы
        rollback = read_rollback(base)
        if rollback is not None:
            failed_version, reason = rollback
            message = f"откат на {self._version}: {reason}"
            logger.error("автообновление до %s не удалось — %s", failed_version, message)
            self._notify(
                UpdateStatus(
                    agent_id=self._agent_id,
                    version=failed_version or "?",
                    ok=False,
                    error=message,
                    stage="rolled_back",
                    running_version=self._version,
                )
            )
            clear_update_markers(base, context=True)
            return

        context = read_update_context(base)
        if context is None:
            return  # обычный старт

        # 2. Контекст чужой: подмена папки не состоялась, служба перезапущена
        #    прежней версией (bat не смог убрать app — Проводник, антивирус);
        #    версия ни from, ни to — в архиве лежала не та сборка, что в имени
        if context.to_version != self._version:
            if context.from_version == self._version:
                message = (
                    f"обновление не состоялось: служба перезапущена прежней версией "
                    f"{self._version} (подробности в logs/update.log)"
                )
            else:
                message = (
                    f"обновление не состоялось: после подмены папки запустилась версия "
                    f"{self._version}, а не {context.to_version} — в архиве не та сборка? "
                    "(подробности в logs/update.log)"
                )
            logger.error("автообновление до %s: %s", context.to_version, message)
            self._notify(
                UpdateStatus(
                    agent_id=self._agent_id,
                    version=context.to_version,
                    ok=False,
                    error=message,
                    stage="started",
                    running_version=self._version,
                )
            )
            clear_update_markers(base, context=True)
            return

        # 3. Мы — новая версия: самопроверка
        self._state = "pending"
        logger.info(
            "самопроверка после обновления %s → %s (индикатор до обновления: %s)",
            context.from_version,
            context.to_version,
            "шёл" if context.expect_indicator else "молчал",
        )
        failure = await self._check(context)
        if failure is None:
            (base / CHECK_OK_FILE).write_text("ok\n", encoding="utf-8")
            (base / UPDATE_CONTEXT_FILE).unlink(missing_ok=True)
            self._state = "ok"
            self._finished_at = self._monotonic()
            logger.info("самопроверка после обновления пройдена: версия %s в работе", self._version)
            self._notify(
                UpdateStatus(
                    agent_id=self._agent_id,
                    version=self._version,
                    ok=True,
                    stage="installed",
                    running_version=self._version,
                )
            )
            return
        # причину читает прежняя версия после отката (bat её не трогает)
        (base / CHECK_FAIL_FILE).write_text(safe_reason(failure) + "\n", encoding="utf-8")
        self._state = "failed"
        self._finished_at = self._monotonic()
        logger.error(
            "самопроверка после обновления НЕ пройдена: %s — self-update.bat выполнит откат на %s",
            failure,
            context.from_version,
        )

    async def _check(self, context: UpdateContext) -> str | None:
        """None — всё в порядке, иначе причина отказа."""
        if not await self._wait(lambda: self.web_ready(), self._web_wait_s):
            return f"веб-интерфейс оператора не поднялся за {int(self._web_wait_s)} с"
        if not await self._wait(self._center_connected, self._center_wait_s):
            return f"нет связи с центром за {int(self._center_wait_s)} с"
        if context.expect_indicator and not await self._wait(
            self._indicator_ok, self._indicator_wait_s
        ):
            return (
                f"индикатор не шлёт данные за {int(self._indicator_wait_s)} с, а до обновления шёл"
            )
        return None

    async def _wait(self, condition: Callable[[], bool], timeout_s: float) -> bool:
        deadline = self._monotonic() + timeout_s
        while True:
            try:
                if condition():
                    return True
            except Exception:  # условие не должно ронять проверку
                pass
            if self._monotonic() >= deadline:
                return False
            await self._sleep(POLL_S)
