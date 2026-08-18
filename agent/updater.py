"""Автообновление агента по команде центра (запрос Игоря 10.08.2026).

Схема (см. docs/decisions.md):
- центр присылает по WS команду ``update_command`` (версия, путь
  скачивания, sha256, размер);
- агент скачивает архив релиза с центра (HTTP, токен агента), сверяет
  sha256 и размер — повреждённый/подменённый архив отвергается — и
  проверяет ОГЛАВЛЕНИЕ архива (есть app/ves-agent.exe, пути без
  zip-slip); это только чтение, файлы агент не пишет;
- проверенный архив остаётся лежать как ``update.zip``, а раскладку
  ``app_new`` делает self-update.bat ДОВЕРЕННЫМИ инструментами Windows
  (tar, при неудаче PowerShell Expand-Archive): боевой урок 12.08.2026 —
  360 Total Security на Джалал-Абаде молча блокировал запись системных
  DLL неподписанным ves-agent.exe (6 отказов Permission denied на
  ucrtbase.dll), а системным tar/cmd антивирусы доверяют. На ПК без
  tar и без Expand-Archive (старые Windows) — прежний путь: агент
  распаковывает сам (может требовать белый список exe в антивирусе,
  см. docs/install-agent-windows.md «Антивирус»);
- пишется скрипт ``self-update.bat``: он распаковывает релиз (см. выше),
  останавливает службу агента (этим корректно завершая его), подменяет
  папку (старая остаётся в ``app_old`` для отката) и запускает службу
  заново. Логика повторяет комплектный update.bat, но скрипт всегда
  пишется заново — не зависим от того, что лежит на весовом ПК;
- скрипт запускается ОДНОРАЗОВОЙ ЗАДАЧЕЙ ПЛАНИРОВЩИКА Windows
  (schtasks), а не дочерним процессом агента: боевой урок 11.08.2026 —
  nssm при остановке службы убивает всё дерево её процессов, и запущенный
  агентом bat умирал сразу после ``nssm stop``, не дойдя до подмены
  папки. Задача планировщика живёт в дереве самого планировщика и
  остановку службы переживает; в конце bat сам удаляет задачу.

Обновляется только замороженная сборка (PyInstaller): в dev-запуске
``python -m agent.main`` команда логируется и отклоняется.

Занятые весы уважаем: пока идёт операция (runner busy), обновление
откладывается коротким ожиданием — перезапуск службы посреди
взвешивания недопустим. С 0.4.13 распаковка происходит ДО этого
ожидания: агент запускает системный tar (фолбэк Expand-Archive)
ДОЧЕРНИМ процессом — писатель файлов доверенный, урок 360 соблюдён, —
и bat остаётся только подмена папки, так что окно между проверкой busy
и остановкой службы снова секундное (идея ревью 12.08.2026). Если
антивирус не даст инструменту работать из-под неподписанного родителя,
агент уходит на прежний путь: проверенный архив остаётся лежать, и
распаковку делает сам bat задачи планировщика (боевой путь 0.4.8+) —
окно шире на время распаковки, кнопку жмёт человек в свободное окно.

Если bat не смог распаковать или подменить папку, службу он не трогает —
агент продолжает работать на прежней версии. Такой исход центру никто
не рапортует (bat с центром не разговаривает), поэтому агент оставляет
себе сторожок: спустя UPDATE_WATCHDOG_S после запуска bat живой процесс
пишет ошибку в свой лог — его видно со страницы «Журнал агента» панели.

Несколько агентов на одном ПК (объект с двумя весами, решение
11.08.2026): имя службы не зашито — install-service.bat записывает его
в ``service.txt`` рядом с конфигом, отсюда же берётся имя одноразовой
задачи планировщика. Иначе обновление одного экземпляра остановило бы
службу соседнего, а две задачи с общим именем затёрли бы друг друга.
Файла нет (агент установлен до 11.08.2026) — работаем с исторической
службой ``ves-agent``; файл есть, но испорчен — обновление отменяется:
молчаливый откат к ``ves-agent`` остановил бы соседний агент.
"""

import asyncio
import hashlib
import logging
import re
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Callable
from pathlib import Path, PureWindowsPath

import agent
from shared.messages import UpdateCommand, UpdateStatus

logger = logging.getLogger(__name__)

DOWNLOAD_TIMEOUT_S = 300.0
UNPACK_TIMEOUT_S = 300.0  # tar/Expand-Archive на HDD весового ПК — минуты
BUSY_WAIT_S = 600.0  # ждём окончания операции не дольше 10 минут
BUSY_POLL_S = 2.0
SPAWN_DELAY_S = 2.0  # пауза перед запуском self-update.bat: успеть отправить статус
SCHTASKS_TIMEOUT_S = 30.0
UPDATE_WATCHDOG_S = 900.0  # живой процесс спустя 15 мин после bat = обновление не прошло
UPDATE_ARCHIVE_NAME = "update.zip"  # проверенный архив, который распакует bat
DEFAULT_SERVICE_NAME = "ves-agent"  # раскладка до 11.08.2026 (один агент на ПК)
SERVICE_NAME_FILE = "service.txt"  # пишет install-service.bat при установке
# имя службы попадает в командную строку bat и в аргументы schtasks —
# пускаем только безопасный набор (ни пробелов, ни кавычек, ни %&|)
SERVICE_NAME_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")

UPDATE_BAT_HEADER = r"""@echo off
chcp 65001 >nul
rem Скрипт автообновления агента __SERVICE__: сгенерирован агентом,
rem запускается задачей планировщика. Лог — logs\update.log
set "BASE=%~dp0"
set "BASE=%BASE:~0,-1%"
"""

# Распаковка релиза доверенными инструментами Windows, а не агентом
# (антивирусы блокируют запись системных DLL неподписанным exe, урок 360).
# Архив распаковывается целиком во временную app_unpack, оттуда в app_new
# забирается только app/ (в корне архива ещё батники и инструкция).
# Успех решают КОДЫ ВОЗВРАТА инструментов, а не наличие exe (ревью:
# упавший на середине tar мог успеть положить exe — неполная сборка не
# должна дойти до подмены); проверка exe перед подменой — контрольная.
# Неубираемые хвосты прежней попытки (rmdir не смог) — отказ сразу:
# распаковка поверх устаревших файлов смешала бы две версии.
# ВАЖНО для cmd: внутри блоков if (...) в текстах echo нельзя использовать
# скобки — «)» преждевременно закрыла бы блок; «if errorlevel 1» внутри
# блоков корректен (проверяет errorlevel в момент исполнения, в отличие
# от %errorlevel%). Команда PowerShell склеена из кусков только в
# исходнике Python — в bat это одна строка.
_POWERSHELL_UNPACK = (
    "powershell -NoProfile -ExecutionPolicy Bypass -Command "
    "\"Expand-Archive -LiteralPath '%BASE%\\update.zip' "
    "-DestinationPath '%BASE%\\app_unpack' -Force\""
)
UPDATE_BAT_UNPACK = (
    r"""echo [%date% %time%] распаковка релиза >> "%BASE%\logs\update.log"
rmdir /s /q "%BASE%\app_unpack" 2>nul
rmdir /s /q "%BASE%\app_new" 2>nul
if exist "%BASE%\app_unpack" (
  echo [%date% %time%] ОШИБКА: app_unpack занята, обновление прервано >> "%BASE%\logs\update.log"
  del "%BASE%\update.zip" 2>nul
  schtasks /Delete /TN __TASK__ /F >nul 2>&1
  exit /b 1
)
if exist "%BASE%\app_new" (
  echo [%date% %time%] ОШИБКА: app_new занята, обновление прервано >> "%BASE%\logs\update.log"
  del "%BASE%\update.zip" 2>nul
  schtasks /Delete /TN __TASK__ /F >nul 2>&1
  exit /b 1
)
mkdir "%BASE%\app_unpack"
tar -xf "%BASE%\update.zip" -C "%BASE%\app_unpack" >> "%BASE%\logs\update.log" 2>&1
if errorlevel 1 (
  echo [%date% %time%] tar не справился, пробуем PowerShell >> "%BASE%\logs\update.log"
  rmdir /s /q "%BASE%\app_unpack" 2>nul
  mkdir "%BASE%\app_unpack"
"""
    + f'  {_POWERSHELL_UNPACK} >> "%BASE%\\logs\\update.log" 2>&1\n'
    + r"""  if errorlevel 1 (
    echo [%date% %time%] ОШИБКА: распаковка не прошла, служба не тронута >> "%BASE%\logs\update.log"
    rmdir /s /q "%BASE%\app_unpack" 2>nul
    del "%BASE%\update.zip" 2>nul
    schtasks /Delete /TN __TASK__ /F >nul 2>&1
    exit /b 1
  )
)
move "%BASE%\app_unpack\app" "%BASE%\app_new" >> "%BASE%\logs\update.log" 2>&1
rmdir /s /q "%BASE%\app_unpack" 2>nul
if not exist "%BASE%\app_new\ves-agent.exe" (
  echo [%date% %time%] ОШИБКА: распаковка не удалась, служба не тронута >> "%BASE%\logs\update.log"
  rmdir /s /q "%BASE%\app_new" 2>nul
  del "%BASE%\update.zip" 2>nul
  schtasks /Delete /TN __TASK__ /F >nul 2>&1
  exit /b 1
)
del "%BASE%\update.zip" 2>nul
"""
)

# Подмена папки — с повторами: переименование app срывается, пока кто-то
# держит в ней файл (боевой урок Джалал-Абада 18.08.2026: открытая в
# Проводнике папка агента при AnyDesk-сессии, антивирус, дочитывающий
# распакованные файлы, или процесс, не успевший выйти за 5 с). Шесть попыток
# по 5 с — полминуты; только потом откат к прежней версии.
UPDATE_BAT_SWAP = r"""echo [%date% %time%] остановка службы __SERVICE__ >> "%BASE%\logs\update.log"
"%BASE%\nssm.exe" stop __SERVICE__ >> "%BASE%\logs\update.log" 2>&1
ping -n 6 127.0.0.1 >nul
rmdir /s /q "%BASE%\app_old" 2>nul
set SWAP_TRIES=0
:swap_retry
move "%BASE%\app" "%BASE%\app_old" >> "%BASE%\logs\update.log" 2>&1
if not errorlevel 1 goto swapped
set /a SWAP_TRIES+=1
if %SWAP_TRIES% geq 6 (
    echo [%date% %time%] ОШИБКА: app занята, обновление прервано >> "%BASE%\logs\update.log"
    "%BASE%\nssm.exe" start __SERVICE__ >> "%BASE%\logs\update.log" 2>&1
    schtasks /Delete /TN __TASK__ /F >nul 2>&1
    exit /b 1
)
echo [%date% %time%] app занята, повтор %SWAP_TRIES% из 6 через 5 с >> "%BASE%\logs\update.log"
ping -n 6 127.0.0.1 >nul
goto swap_retry
:swapped
move "%BASE%\app_new" "%BASE%\app" >> "%BASE%\logs\update.log" 2>&1
if not exist "%BASE%\app\ves-agent.exe" (
    echo [%date% %time%] ОШИБКА: новая версия не встала, откат >> "%BASE%\logs\update.log"
    move "%BASE%\app_old" "%BASE%\app" >> "%BASE%\logs\update.log" 2>&1
)
"%BASE%\nssm.exe" start __SERVICE__ >> "%BASE%\logs\update.log" 2>&1
echo [%date% %time%] служба запущена >> "%BASE%\logs\update.log"
rem уборка одноразовой задачи планировщика, которой запущен этот скрипт
schtasks /Delete /TN __TASK__ /F >nul 2>&1
"""


class UpdateError(Exception):
    """Обновление невозможно или не прошло проверку."""


def read_service_name(base: Path) -> str:
    """Имя службы nssm ЭТОГО экземпляра агента (см. шапку модуля).

    Источник — ``service.txt`` каталога установки. Файла нет — установка
    сделана до 11.08.2026, служба историческая ``ves-agent``. А вот файл
    испорченный (обрезан, сохранён в UTF-16, мусор) — это отказ: на ПК с
    двумя агентами молчаливый откат к ``ves-agent`` остановил бы СОСЕДА
    посреди его работы, поэтому лучше не обновиться и сказать об этом.
    """
    path = base / SERVICE_NAME_FILE
    if not path.exists():
        return DEFAULT_SERVICE_NAME
    try:
        raw = path.read_text(encoding="utf-8-sig", errors="replace")
    except OSError as exc:
        raise UpdateError(f"{SERVICE_NAME_FILE} не читается: {exc}") from exc
    name = raw.strip().splitlines()[0].strip() if raw.strip() else ""
    if not SERVICE_NAME_RE.fullmatch(name):
        raise UpdateError(
            f"{SERVICE_NAME_FILE} повреждён (имя службы {name!r}) — "
            "обновление отменено; впишите в файл имя своей службы"
        )
    return name


def task_name(service: str) -> str:
    """Имя одноразовой задачи планировщика — своё у каждого экземпляра."""
    return f"{service}-update"


def update_bat(service: str, *, unpack: bool = True) -> str:
    """Текст self-update.bat для службы ``service``.

    ``unpack=True`` (обычный путь) — bat сам распаковывает update.zip
    доверенными инструментами; ``unpack=False`` — раскладку app_new уже
    сделал агент (на ПК нет ни tar, ни Expand-Archive).
    """
    template = UPDATE_BAT_HEADER + (UPDATE_BAT_UNPACK if unpack else "") + UPDATE_BAT_SWAP
    return template.replace("__SERVICE__", service).replace("__TASK__", task_name(service))


def unpack_via_bat_available() -> bool:
    """Есть ли на этом ПК доверенный инструмент распаковки для bat.

    Windows 10 1803+ имеет системный tar (bsdtar, zip читает). На более
    старых Windows PowerShell есть всегда, но Expand-Archive появился
    только в 5.0 — проверяем именно командлет, иначе обновление вечно
    падало бы в bat, хотя агент умеет распаковать сам (прежний путь).
    Вне Windows (dev, тесты) — True: bat там не исполняется.
    """
    if sys.platform != "win32":
        return True
    if shutil.which("tar"):
        return True
    if not shutil.which("powershell"):
        return False
    try:
        probe = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "Get-Command Expand-Archive"],
            capture_output=True,
            timeout=SCHTASKS_TIMEOUT_S,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


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
        self._watchdog_handle: asyncio.TimerHandle | None = None
        # доклад центру вне ответа на команду (сторожок): main.py подставляет
        # CenterClient.post_message; None — только лог (тесты, dev)
        self.notify: Callable[[UpdateStatus], None] | None = None

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
        # имя своей службы выясняем до скачивания: отказ должен быть дешёвым
        service = read_service_name(base)
        (base / "logs").mkdir(exist_ok=True)

        # сначала вся подготовка (скачивание и распаковка длятся минуты), и
        # только потом ожидание свободных весов — окно между проверкой busy
        # и рестартом службы должно быть минимальным (замечание ревью
        # 10.08.2026, возвращено к секундам 13.08.2026)
        archive = base / "update-download.zip"
        staged = base / UPDATE_ARCHIVE_NAME
        try:
            await asyncio.to_thread(self._download, command, archive)
            await asyncio.to_thread(self._verify, command, archive)
            await asyncio.to_thread(self._validate_archive, archive)
            unpack_in_bat = False
            if await asyncio.to_thread(self._extract_via_tools, archive, base):
                # app_new готова заранее — bat сделает только подмену папки
                logger.info("релиз распакован доверенным инструментом до остановки службы")
            elif await asyncio.to_thread(unpack_via_bat_available):
                # инструмент из-под агента не сработал (антивирус?) —
                # проверенный архив остаётся лежать под bat: распакует
                # задача планировщика, как в 0.4.8 (окно шире, но путь боевой)
                unpack_in_bat = True
                await asyncio.to_thread(archive.replace, staged)
                logger.warning(
                    "распаковка инструментом из агента не удалась — распакует bat планировщика"
                )
            else:
                logger.warning(
                    "на ПК нет ни tar, ни Expand-Archive — распаковка процессом агента; "
                    "сторонний антивирус может её блокировать (см. install-agent-windows.md)"
                )
                await asyncio.to_thread(self._extract, archive, base)

            # дождаться окончания текущей операции (перезапуск посреди
            # взвешивания недопустим)
            waited = 0.0
            while self._busy() and waited < BUSY_WAIT_S:
                await asyncio.sleep(BUSY_POLL_S)
                waited += BUSY_POLL_S
            if self._busy():
                raise UpdateError("весы заняты операцией дольше 10 минут — повторите позже")

            logger.info("обновление до %s: служба %s", command.version, service)
            bat = base / "self-update.bat"
            # cmd требует CRLF
            bat.write_bytes(
                update_bat(service, unpack=unpack_in_bat).replace("\n", "\r\n").encode("utf-8")
            )
            # spawn — отложенно: у ws_client есть 2 секунды отправить update_status
            # центру ДО того, как nssm stop убьёт агента (иначе успешные
            # обновления выглядели бы в логах центра «немыми»)
            loop = asyncio.get_running_loop()
            loop.call_later(SPAWN_DELAY_S, self._spawn_updater, bat, base, task_name(service))
            # сторожок: об исходе bat центру никто не рапортует (см. шапку модуля)
            if self._watchdog_handle is not None:
                self._watchdog_handle.cancel()
            self._watchdog_handle = loop.call_later(
                UPDATE_WATCHDOG_S, self._watchdog, command.version
            )
        except BaseException:
            # не захламлять весовой ПК: отменённое обновление убирает архив,
            # подготовленный для bat, и заранее распакованную app_new
            # (следующая команда скачает и распакует заново)
            staged.unlink(missing_ok=True)
            shutil.rmtree(base / "app_new", ignore_errors=True)
            raise
        finally:
            archive.unlink(missing_ok=True)

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
    def _validate_archive(archive: Path) -> None:
        """Проверить ОГЛАВЛЕНИЕ релиза без распаковки (раскладку сделает bat).

        Правила те же, что у распаковки агентом: в архиве обязан быть
        app/ves-agent.exe, а пути членов не должны вырываться наружу
        (zip-slip). Проверяются ВСЕ члены, не только app/ — bat
        распаковывает архив целиком во временную app_unpack. Пути
        разбираются по правилам Windows (PureWindowsPath: и «..», и
        абсолютные, и с буквой диска, и с обратными слэшами) — распаковка
        происходит на весовом ПК. Здесь только чтение: файлы на диск не
        пишутся, антивирусу не на что реагировать.
        """
        with zipfile.ZipFile(archive) as bundle:
            members = bundle.namelist()
            if not any(m.startswith("app/") and m.endswith("ves-agent.exe") for m in members):
                raise UpdateError("в архиве нет app/ves-agent.exe — это не релиз агента")
            for member in members:
                path = PureWindowsPath(member)
                # root ловит «\evil.txt»: без буквы диска is_absolute() == False,
                # но при распаковке такой путь указывает в корень диска
                if path.is_absolute() or path.drive or path.root or ".." in path.parts:
                    raise UpdateError(f"подозрительный путь в архиве: {member}")

    def _watchdog(self, version: str) -> None:
        """Агент жив спустя UPDATE_WATCHDOG_S после запуска bat — не обновились.

        Успешное обновление за это время остановило бы службу (и этот
        процесс вместе с ней). Раз живы — bat не справился (распаковка,
        занятая папка, антивирус) и службу не тронул. bat с центром не
        разговаривает, поэтому докладывает сторожок: в свой лог (виден со
        страницы «Журнал агента») и центру через ``notify`` — событием в
        «Событиях» панели и в Telegram (боевой урок Джалал-Абада 18.08.2026:
        центр видел только «уже выполняется» и молчание).
        """
        message = (
            f"автообновление до {version}: спустя {int(UPDATE_WATCHDOG_S // 60)} мин служба "
            "так и не была перезапущена — обновление не прошло; подробности в logs/update.log"
        )
        logger.error(message)
        if self.notify is not None:
            self.notify(
                UpdateStatus(agent_id=self._agent_id, version=version, ok=False, error=message)
            )

    @staticmethod
    def _extract_via_tools(archive: Path, base: Path) -> bool:
        """Распаковать релиз в app_new доверенным инструментом ИЗ агента.

        Писатель файлов — системный tar (подписан Microsoft; фолбэк
        Expand-Archive), запущенный дочерним процессом: урок 360 соблюдён
        (антивирус решает по процессу-писателю), а распаковка происходит
        ДО остановки службы — окно busy→stop снова секундное (идея ревью
        12.08.2026). Любая неудача — False без исключения: вызывающий код
        уходит на распаковку bat'ом планировщика (боевой путь 0.4.8+),
        который антивирусу знаком по другому дереву процессов.
        """
        app_unpack = base / "app_unpack"
        app_new = base / "app_new"
        try:
            if app_unpack.exists():
                shutil.rmtree(app_unpack)
            if app_new.exists():
                shutil.rmtree(app_new)
            app_unpack.mkdir()
        except OSError as exc:
            logger.warning("распаковка инструментом: хвосты прежней попытки не убрать: %s", exc)
            return False
        commands: list[list[str]] = [["tar", "-xf", str(archive), "-C", str(app_unpack)]]
        if sys.platform == "win32":
            # одинарная кавычка в literal-строке PowerShell экранируется удвоением
            ps_archive = str(archive).replace("'", "''")
            ps_target = str(app_unpack).replace("'", "''")
            commands.append(
                [
                    "powershell",
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    f"Expand-Archive -LiteralPath '{ps_archive}' "
                    f"-DestinationPath '{ps_target}' -Force",
                ]
            )
        encoding = "cp866" if sys.platform == "win32" else "utf-8"
        unpacked = False
        for command in commands:
            try:
                result = subprocess.run(command, capture_output=True, timeout=UNPACK_TIMEOUT_S)
            except (OSError, subprocess.SubprocessError) as exc:
                logger.warning("распаковка инструментом: %s не запустился: %s", command[0], exc)
                continue
            if result.returncode == 0:
                unpacked = True
                break
            logger.warning(
                "распаковка инструментом: %s вернул %d: %.200s",
                command[0],
                result.returncode,
                result.stderr.decode(encoding, errors="replace").strip(),
            )
        if not unpacked:
            shutil.rmtree(app_unpack, ignore_errors=True)
            return False
        try:
            # в корне архива кроме app/ лежат батники и инструкция —
            # в app_new забирается только app/ (как в bat-распаковке)
            (app_unpack / "app").replace(app_new)
        except OSError as exc:
            logger.warning("распаковка инструментом: app из архива не переносится: %s", exc)
            shutil.rmtree(app_unpack, ignore_errors=True)
            return False
        shutil.rmtree(app_unpack, ignore_errors=True)
        if not (app_new / "ves-agent.exe").exists():
            logger.warning("распаковка инструментом: в app_new нет ves-agent.exe")
            shutil.rmtree(app_new, ignore_errors=True)
            return False
        return True

    @staticmethod
    def _extract(archive: Path, base: Path) -> None:
        """Распаковать app/ из архива в app_new процессом агента.

        С 0.4.8 это ЗАПАСНОЙ путь для ПК без tar и Expand-Archive: обычно
        распаковывает self-update.bat (см. шапку модуля). Неподписанному
        агенту антивирус может запрещать запись системных DLL — на таких
        машинах нужен белый список exe (урок 360, install-agent-windows.md).
        """
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
    def scheduler_commands(bat: Path, task: str) -> list[list[str]]:
        """Команды одноразовой задачи планировщика (вынесено для тестов).

        /RU SYSTEM — служба агента работает от LocalSystem, права есть;
        /ST обязателен для /SC ONCE, но реальный запуск делает /Run —
        задачу в конце удаляет сам bat (плюс /F перезаписывает хвост
        от прежнего неудачного обновления, если он остался). Имя задачи
        своё у каждого экземпляра агента на ПК (см. шапку модуля).
        """
        return [
            [
                "schtasks",
                "/Create",
                "/TN",
                task,
                "/TR",
                f'cmd /c ""{bat}""',
                "/SC",
                "ONCE",
                "/ST",
                "00:00",
                "/RU",
                "SYSTEM",
                "/F",
            ],
            ["schtasks", "/Run", "/TN", task],
        ]

    @staticmethod
    def _spawn_via_scheduler(bat: Path, base: Path, task: str) -> bool:
        """Запуск bat задачей планировщика — ВНЕ дерева процессов службы.

        Дочерний процесс агента nssm убивает вместе со службой при stop
        (боевой урок 11.08.2026) — задачу планировщика он не достаёт.
        """
        for command in AgentUpdater.scheduler_commands(bat, task):
            try:
                result = subprocess.run(
                    command, cwd=base, capture_output=True, timeout=SCHTASKS_TIMEOUT_S
                )
            except (OSError, subprocess.SubprocessError) as exc:
                # schtasks отсутствует/завис — честный провал вместо
                # исключения в callback цикла (fallback должен сработать)
                logger.error("schtasks %s недоступен: %s", command[1], exc)
                return False
            if result.returncode != 0:
                # schtasks на русской Windows пишет в OEM-кодировке cp866
                logger.error(
                    "schtasks %s не сработал: %s",
                    command[1],
                    result.stderr.decode("cp866", errors="replace").strip()
                    or result.stdout.decode("cp866", errors="replace").strip(),
                )
                return False
        return True

    @staticmethod
    def _spawn_updater(bat: Path, base: Path, task: str) -> None:
        if sys.platform == "win32":
            if AgentUpdater._spawn_via_scheduler(bat, base, task):
                logger.info("скрипт обновления запущен задачей планировщика")
                return
            # запасной путь: отдельным процессом. nssm stop убьёт его вместе
            # с деревом службы — обновление, скорее всего, замрёт после
            # остановки, но лог всё объяснит; лучше попытка, чем ничего
            logger.warning("планировщик недоступен — запасной запуск отдельным процессом")
            creationflags = (
                subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
                | subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
            )
            subprocess.Popen(
                ["cmd", "/c", str(bat)],
                cwd=base,
                creationflags=creationflags,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
            )
            return
        # не-Windows: dev/тесты — обычный отдельный процесс
        subprocess.Popen(
            ["sh", str(bat)],
            cwd=base,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
        )
