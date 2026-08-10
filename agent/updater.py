"""Автообновление агента по команде центра (запрос Игоря 10.08.2026).

Схема (см. docs/decisions.md):
- центр присылает по WS команду ``update_command`` (версия, путь
  скачивания, sha256, размер);
- агент скачивает архив релиза с центра (HTTP, токен агента), сверяет
  sha256 и размер — повреждённый/подменённый архив отвергается;
- содержимое ``app/`` из архива раскладывается в ``app_new`` рядом
  с текущей ``app`` (стандартная раскладка C:/vesy-agent, см.
  docs/install-agent-windows.md);
- пишется и запускается отдельным процессом скрипт ``self-update.bat``:
  он останавливает службу ves-agent (этим корректно завершая агент),
  подменяет папку (старая остаётся в ``app_old`` для отката) и запускает
  службу заново. Логика повторяет комплектный update.bat, но скрипт
  всегда пишется заново — не зависим от того, что лежит на весовом ПК.

Обновляется только замороженная сборка (PyInstaller): в dev-запуске
``python -m agent.main`` команда логируется и отклоняется.

Занятые весы уважаем: пока идёт операция (runner busy), обновление
откладывается коротким ожиданием — перезапуск службы посреди
взвешивания недопустим.
"""

import asyncio
import hashlib
import logging
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from pathlib import Path

import agent
from shared.messages import UpdateCommand, UpdateStatus

logger = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT_S = 300.0
BUSY_WAIT_S = 600.0  # ждём окончания операции не дольше 10 минут
BUSY_POLL_S = 2.0
SPAWN_DELAY_S = 2.0  # пауза перед запуском self-update.bat: успеть отправить статус

UPDATE_BAT = r"""@echo off
chcp 65001 >nul
rem Скрипт автообновления ves-agent: сгенерирован агентом, запускается
rem отдельным процессом. Лог — logs\update.log
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"
echo [%date% %time%] остановка службы >> "%BASE%\logs\update.log"
"%BASE%\nssm.exe" stop ves-agent >> "%BASE%\logs\update.log" 2>&1
ping -n 6 127.0.0.1 >nul
rmdir /s /q "%BASE%\app_old" 2>nul
move "%BASE%\app" "%BASE%\app_old" >> "%BASE%\logs\update.log" 2>&1
if errorlevel 1 (
    echo [%date% %time%] ОШИБКА: app занята, обновление прервано >> "%BASE%\logs\update.log"
    "%BASE%\nssm.exe" start ves-agent >> "%BASE%\logs\update.log" 2>&1
    exit /b 1
)
move "%BASE%\app_new" "%BASE%\app" >> "%BASE%\logs\update.log" 2>&1
if not exist "%BASE%\app\ves-agent.exe" (
    echo [%date% %time%] ОШИБКА: новая версия не встала, откат >> "%BASE%\logs\update.log"
    move "%BASE%\app_old" "%BASE%\app" >> "%BASE%\logs\update.log" 2>&1
)
"%BASE%\nssm.exe" start ves-agent >> "%BASE%\logs\update.log" 2>&1
echo [%date% %time%] служба запущена >> "%BASE%\logs\update.log"
"""


class UpdateError(Exception):
    """Обновление невозможно или не прошло проверку."""


class AgentUpdater:
    """Обработчик команды автообновления."""

    def __init__(
        self,
        *,
        agent_id: str,
        base_url: str,
        token: str,
        busy: Callable[[], bool],
        install_dir: Path | None = None,
    ) -> None:
        self._agent_id = agent_id
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._busy = busy
        # каталог установки (C:/vesy-agent): родитель папки app с exe
        self._install_dir = install_dir
        self._lock = asyncio.Lock()

    async def handle(self, command: UpdateCommand) -> UpdateStatus:
        """Выполнить команду обновления; вернуть отчёт для центра."""
        if self._lock.locked():
            return UpdateStatus(
                agent_id=self._agent_id,
                version=command.version,
                ok=False,
                error="обновление уже выполняется",
            )
        try:
            async with self._lock:
                await self._run(command)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("автообновление до %s не выполнено: %s", command.version, exc)
            return UpdateStatus(
                agent_id=self._agent_id, version=command.version, ok=False, error=str(exc)
            )
        logger.info("автообновление до %s запущено: служба будет перезапущена", command.version)
        return UpdateStatus(agent_id=self._agent_id, version=command.version, ok=True)

    async def _run(self, command: UpdateCommand) -> None:
        if command.version == agent.__version__:
            raise UpdateError(f"версия {command.version} уже установлена")
        if not getattr(sys, "frozen", False) and self._install_dir is None:
            raise UpdateError("не замороженная сборка (dev-запуск) — обновление вручную")

        base = self._install_dir or Path(sys.executable).resolve().parent.parent
        if not (base / "nssm.exe").exists():
            raise UpdateError(f"nssm.exe не найден в {base} — раскладка не стандартная")
        (base / "logs").mkdir(exist_ok=True)

        # сначала вся подготовка (скачивание длится минуты), и только потом
        # ожидание свободных весов — окно между проверкой busy и рестартом
        # службы должно быть минимальным (замечание ревью 10.08.2026)
        archive = base / "update-download.zip"
        await asyncio.to_thread(self._download, command, archive)
        try:
            await asyncio.to_thread(self._verify, command, archive)
            await asyncio.to_thread(self._extract, archive, base)
        finally:
            archive.unlink(missing_ok=True)

        # дождаться окончания текущей операции (перезапуск посреди
        # взвешивания недопустим)
        waited = 0.0
        while self._busy() and waited < BUSY_WAIT_S:
            await asyncio.sleep(BUSY_POLL_S)
            waited += BUSY_POLL_S
        if self._busy():
            raise UpdateError("весы заняты операцией дольше 10 минут — повторите позже")

        bat = base / "self-update.bat"
        # cmd требует CRLF
        bat.write_bytes(UPDATE_BAT.replace("\n", "\r\n").encode("utf-8"))
        # spawn — отложенно: у ws_client есть 2 секунды отправить update_status
        # центру ДО того, как nssm stop убьёт агента (иначе успешные
        # обновления выглядели бы в логах центра «немыми»)
        asyncio.get_running_loop().call_later(SPAWN_DELAY_S, self._spawn_updater, bat, base)

    def _download(self, command: UpdateCommand, target: Path) -> None:
        url = self._base_url + urllib.parse.quote(command.url_path)
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self._token}"})
        with (
            urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_S) as response,
            target.open("wb") as out,
        ):
            while chunk := response.read(1 << 20):
                out.write(chunk)

    @staticmethod
    def _verify(command: UpdateCommand, archive: Path) -> None:
        size = archive.stat().st_size
        if size != command.size_bytes:
            raise UpdateError(f"размер архива {size} != заявленных {command.size_bytes}")
        digest = hashlib.sha256()
        with archive.open("rb") as fh:
            while chunk := fh.read(1 << 20):
                digest.update(chunk)
        if digest.hexdigest() != command.sha256:
            raise UpdateError("sha256 архива не совпал — обновление отвергнуто")

    @staticmethod
    def _extract(archive: Path, base: Path) -> None:
        """Распаковать app/ из архива релиза в app_new (со сбросом старой)."""
        app_new = base / "app_new"
        if app_new.exists():
            shutil.rmtree(app_new)
        with zipfile.ZipFile(archive) as bundle:
            members = [m for m in bundle.namelist() if m.startswith("app/") and m != "app/"]
            if not any(m.endswith("ves-agent.exe") for m in members):
                raise UpdateError("в архиве нет app/ves-agent.exe — это не релиз агента")
            for member in members:
                relative = Path(member).relative_to("app")
                # защита от zip-slip: члены с .. или абсолютным путём могли бы
                # записать файл МИМО app_new (например, поверх nssm.exe)
                if relative.is_absolute() or ".." in relative.parts:
                    raise UpdateError(f"подозрительный путь в архиве: {member}")
                target = (app_new / relative).resolve()
                if not target.is_relative_to(app_new.resolve()):
                    raise UpdateError(f"путь выходит за пределы app_new: {member}")
                if member.endswith("/"):
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.open(member) as src, target.open("wb") as dst:
                    dst.write(src.read())

    @staticmethod
    def _spawn_updater(bat: Path, base: Path) -> None:
        creationflags = 0
        if sys.platform == "win32":
            creationflags = (
                subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
                | subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
            )
        subprocess.Popen(
            ["cmd", "/c", str(bat)] if sys.platform == "win32" else ["sh", str(bat)],
            cwd=base,
            creationflags=creationflags,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
