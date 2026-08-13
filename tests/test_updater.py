"""Тесты автообновления агента (agent/updater.py, запрос Игоря 10.08.2026).

Железо и реальный центр не используются: скачивание релиза проверяется
через локальный ``http.server`` в фоновом потоке (как в test_cameras.py),
раскладка C:/vesy-agent эмулируется в tmp_path (nssm.exe + app/),
запуск self-update.bat подменяется monkeypatch'ем — subprocess.Popen
в тестах не вызывается никогда.

Ключевые инварианты:
- версия, равная текущей, и dev-запуск отклоняются без сетевых обращений;
- скачивание идёт с токеном агента (сервер без Bearer отвечает 401 —
  успешный сценарий это доказывает);
- неверный sha256/размер/zip-slip → архив отвергнут, ничего не появляется;
- распаковка (с 0.4.13) — доверенным инструментом (tar/Expand-Archive)
  дочерним процессом агента ДО ожидания весов: писатель файлов подписан
  (урок 360), окно busy→stop секундное; не вышло — фолбэк на распаковку
  bat'ом планировщика (боевой путь 0.4.8+), update.zip остаётся лежать;
  на ПК совсем без инструментов — прежняя распаковка агентом;
- self-update.bat пишется с CRLF и содержит остановку/запуск службы;
- занятые весы (busy) откладывают и в итоге отменяют обновление;
- сторожок: живой процесс спустя UPDATE_WATCHDOG_S после запуска bat
  пишет ошибку в лог (bat не рапортует центру о своих провалах).

Пути распаковки в сценарных тестах фиксируются monkeypatch'ем
(_extract_via_tools), иначе исход зависел бы от платформы: bsdtar мака
читает zip, GNU tar на CI — нет. Сам _extract_via_tools проверяется
юнитами TestExtractViaTools (реальный tar — со skipif по bsdtar).
"""

import asyncio
import hashlib
import http.server
import io
import shutil
import subprocess
import sys
import threading
import zipfile
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

import agent
import agent.updater as updater_module
from agent.updater import AgentUpdater
from shared.messages import UpdateCommand, UpdateStatus

TOKEN = "agent-token-42"
NEW_VERSION = "9.9.9"
RELEASE_PATH = f"/agents/releases/ves-agent-{NEW_VERSION}-win64.zip"

NEW_EXE = b"MZ new-agent-exe"
INTERNAL_FILE = b"internal-data-x"

# Записанный сервером запрос: (путь, заголовки)
RecordedRequest = tuple[str, dict[str, str]]


# ---------------------------------------------------------------------------
# Хелперы и фикстуры
# ---------------------------------------------------------------------------


def _make_release_zip(*, with_exe: bool = True) -> bytes:
    """Собрать архив релиза: app/ves-agent.exe + вложенный app/_internal/x."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as bundle:
        if with_exe:
            bundle.writestr("app/ves-agent.exe", NEW_EXE)
        bundle.writestr("app/_internal/x", INTERNAL_FILE)
    return buffer.getvalue()


def _make_handler(
    payload: bytes, requests: list[RecordedRequest]
) -> type[http.server.BaseHTTPRequestHandler]:
    """Обработчик тестового сервера релизов: Bearer-токен обязателен."""

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append((self.path, dict(self.headers.items())))
            if self.headers.get("Authorization") != f"Bearer {TOKEN}":
                self._send(401, b"Unauthorized")
            elif self.path == RELEASE_PATH:
                self._send(200, payload)
            else:
                self._send(404, b"Not Found")

        def _send(self, code: int, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            # не засорять вывод pytest логами сервера
            pass

    return Handler


@pytest.fixture
def release_server() -> Iterator[Callable[[bytes], tuple[str, list[RecordedRequest]]]]:
    """Фабрика локального сервера релизов: отдаёт (base_url, журнал запросов)."""
    servers: list[http.server.ThreadingHTTPServer] = []

    def start(payload: bytes) -> tuple[str, list[RecordedRequest]]:
        requests: list[RecordedRequest] = []
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(payload, requests))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}", requests

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


@pytest.fixture
def install_dir(tmp_path: Path) -> Path:
    """Эмуляция стандартной раскладки C:/vesy-agent: nssm.exe + app/."""
    (tmp_path / "nssm.exe").write_bytes(b"")
    app = tmp_path / "app"
    app.mkdir()
    (app / "ves-agent.exe").write_bytes(b"MZ old-agent-exe")
    return tmp_path


@pytest.fixture
def spawn_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Path, Path, str]]:
    """Подменить запуск self-update.bat: Popen не вызывается, вызовы копятся."""
    calls: list[tuple[Path, Path, str]] = []

    def fake_spawn(bat: Path, base: Path, task: str) -> None:
        calls.append((bat, base, task))

    monkeypatch.setattr(AgentUpdater, "_spawn_updater", staticmethod(fake_spawn))
    # отложенный spawn в тестах доигрывается быстро (см. _handle)
    monkeypatch.setattr(updater_module, "SPAWN_DELAY_S", 0.01)
    return calls


def _make_updater(
    base_url: str,
    install_dir: Path | None,
    *,
    token: str = TOKEN,
    busy: Callable[[], bool] = lambda: False,
) -> AgentUpdater:
    return AgentUpdater(
        agent_id="agent-1", base_url=base_url, token=token, busy=busy, install_dir=install_dir
    )


def _make_command(payload: bytes, **overrides: object) -> UpdateCommand:
    """Команда обновления с корректными sha256/size по содержимому архива."""
    fields: dict[str, object] = {
        "version": NEW_VERSION,
        "url_path": RELEASE_PATH,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }
    fields.update(overrides)
    return UpdateCommand.model_validate(fields)


def _handle(updater: AgentUpdater, command: UpdateCommand) -> UpdateStatus:
    async def run() -> UpdateStatus:
        status = await updater.handle(command)
        # spawn отложен на SPAWN_DELAY_S (статус должен успеть уйти в WS
        # до nssm stop) — доигрываем таймер в этом же event loop
        await asyncio.sleep(updater_module.SPAWN_DELAY_S + 0.05)
        return status

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# Отказы до сети: версия и dev-запуск
# ---------------------------------------------------------------------------


class TestUpdaterPreconditions:
    def test_same_version_rejected(self, install_dir: Path) -> None:
        """Команда с текущей версией агента → ok=False «уже установлена»."""
        updater = _make_updater("http://127.0.0.1:1", install_dir)
        payload = _make_release_zip()
        status = _handle(updater, _make_command(payload, version=agent.__version__))
        assert status.ok is False
        assert status.error is not None and "уже установлена" in status.error
        assert status.version == agent.__version__
        assert status.agent_id == "agent-1"

    def test_dev_run_rejected(self) -> None:
        """Не frozen и install_dir=None (dev-запуск) → ok=False, без обновления."""
        updater = _make_updater("http://127.0.0.1:1", None)
        status = _handle(updater, _make_command(_make_release_zip()))
        assert status.ok is False
        assert status.error is not None and "dev-запуск" in status.error


# ---------------------------------------------------------------------------
# Полный успешный путь
# ---------------------------------------------------------------------------


class TestUpdaterSuccess:
    def test_tool_unpack_flow(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Обычный путь 0.4.13: распаковка инструментом из агента ДО ожидания
        весов — app_new готова заранее, update.zip не остаётся, bat только
        подменяет папку (без блока распаковки).

        Успех инструмента эмулируется фейком с той же раскладкой (реальный
        tar платформозависим — см. TestExtractViaTools).
        """

        def fake_tools(archive: Path, base: Path) -> bool:
            app_new = base / "app_new"
            with zipfile.ZipFile(archive) as bundle:
                for member in bundle.namelist():
                    if not member.startswith("app/") or member.endswith("/"):
                        continue
                    target = app_new / Path(member).relative_to("app")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(bundle.read(member))
            return True

        monkeypatch.setattr(AgentUpdater, "_extract_via_tools", staticmethod(fake_tools))
        payload = _make_release_zip()
        base_url, _requests = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload))
        assert status.ok is True, f"обновление не прошло: {status.error}"

        # app_new разложена заранее, архива не осталось
        app_new = install_dir / "app_new"
        assert (app_new / "ves-agent.exe").read_bytes() == NEW_EXE
        assert not (install_dir / "update.zip").exists()
        assert not (install_dir / "update-download.zip").exists()

        # bat — только подмена папки, распаковки в нём нет
        text = (install_dir / "self-update.bat").read_bytes().decode("utf-8")
        assert "tar -xf" not in text and "Expand-Archive" not in text
        assert 'nssm.exe" stop ves-agent' in text
        assert 'nssm.exe" start ves-agent' in text
        assert spawn_calls != []

    def test_full_update_flow(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Фолбэк-путь: инструмент из агента не сработал (антивирус) →
        скачивание с токеном → проверки → update.zip → self-update.bat
        с распаковкой → запуск.

        Сервер отвечает 401 без Bearer-токена, так что ok=True заодно
        доказывает, что агент шлёт Authorization. Распаковку агент НЕ
        делает — проверенный архив остаётся лежать update.zip, раскладку
        app_new выполнит bat доверенными инструментами (урок 360).
        """
        monkeypatch.setattr(
            AgentUpdater, "_extract_via_tools", staticmethod(lambda archive, base: False)
        )
        payload = _make_release_zip()
        base_url, requests = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload))
        assert status.ok is True, f"обновление не прошло: {status.error}"
        assert status.error is None
        assert status.version == NEW_VERSION

        # скачивание было ровно одно, по пути релиза и с токеном агента
        assert [path for path, _ in requests] == [RELEASE_PATH]
        assert requests[0][1].get("Authorization") == f"Bearer {TOKEN}"

        # агент файлы сборки НЕ раскладывает (это делает bat) — архив
        # проверен и оставлен под именем update.zip байт-в-байт
        assert not (install_dir / "app_new").exists(), "app_new распаковал агент, а должен bat"
        assert (install_dir / "update.zip").read_bytes() == payload
        # старая app не тронута (её подменяет только bat)
        assert (install_dir / "app" / "ves-agent.exe").read_bytes() == b"MZ old-agent-exe"

        # self-update.bat записан с CRLF и содержит ключевые шаги
        bat = install_dir / "self-update.bat"
        raw = bat.read_bytes()
        assert raw.count(b"\n") == raw.count(b"\r\n"), "в bat есть строки без CRLF"
        text = raw.decode("utf-8")
        # распаковка в bat: tar с фолбэком на PowerShell, из update.zip
        assert 'tar -xf "%BASE%\\update.zip"' in text
        assert "Expand-Archive" in text
        assert "app_unpack" in text
        assert 'nssm.exe" stop ves-agent' in text
        assert 'nssm.exe" start ves-agent' in text
        assert "app_new" in text
        # bat запускается задачей планировщика и сам её удаляет в конце
        assert "schtasks /Delete /TN ves-agent-update /F" in text
        # распаковка идёт ДО остановки службы: простой службы минимален,
        # а неудачная распаковка оставляет агента работать
        assert text.index("tar -xf") < text.index('nssm.exe" stop')

        # временный архив скачивания удалён (остался только update.zip)
        assert not (install_dir / "update-download.zip").exists()

        # запуск скрипта: ровно один вызов с путями bat и базового каталога
        assert spawn_calls == [(bat, install_dir, "ves-agent-update")]


# ---------------------------------------------------------------------------
# Отказы проверки архива
# ---------------------------------------------------------------------------


class TestUpdaterVerification:
    def test_wrong_sha256_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Неверный sha256 → отказ, app_new не создана, bat не записан."""
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload, sha256="f" * 64))
        assert status.ok is False
        assert status.error is not None and "sha256" in status.error
        assert "отвергнуто" in status.error
        assert not (install_dir / "app_new").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert not (install_dir / "update-download.zip").exists(), "архив не удалён"
        assert not (install_dir / "update.zip").exists(), "отвергнутый архив оставлен под bat"
        assert spawn_calls == []

    def test_wrong_size_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Неверный size_bytes → отказ без распаковки и без bat."""
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload, size_bytes=len(payload) + 1))
        assert status.ok is False
        assert status.error is not None and "размер" in status.error
        assert not (install_dir / "app_new").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert not (install_dir / "update.zip").exists()
        assert spawn_calls == []

    def test_zip_without_agent_exe_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Архив без app/ves-agent.exe → «не релиз агента», отказ."""
        payload = _make_release_zip(with_exe=False)
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload))
        assert status.ok is False
        assert status.error is not None and "не релиз агента" in status.error
        assert not (install_dir / "app_new").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert not (install_dir / "update.zip").exists()
        assert spawn_calls == []

    @pytest.mark.parametrize(
        "member",
        [
            "app/../evil.txt",  # классический zip-slip
            "../evil.txt",  # вне app/ (bat распаковывает архив целиком)
            "C:/evil.txt",  # абсолютный путь с буквой диска
            "\\evil.txt",  # корневой путь Windows (без буквы диска)
            "app\\..\\evil.txt",  # обратные слэши — для Windows те же разделители
        ],
    )
    def test_zip_slip_member_rejects_whole_archive(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        member: str,
    ) -> None:
        """Подозрительный путь в оглавлении → архив отвергнут ЦЕЛИКОМ.

        Распаковку делает bat без всяких проверок путей, поэтому агент
        обязан отбраковать архив заранее — по одному лишь оглавлению.
        """
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as bundle:
            bundle.writestr("app/ves-agent.exe", NEW_EXE)
            bundle.writestr(member, b"escaped")
        payload = buffer.getvalue()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload))
        assert status.ok is False
        assert status.error is not None and "подозрительный путь" in status.error
        assert not (install_dir / "evil.txt").exists(), "файл из архива вырвался наружу"
        assert not (install_dir / "update.zip").exists(), "опасный архив оставлен под bat"
        assert not (install_dir / "self-update.bat").exists()
        assert spawn_calls == []


# ---------------------------------------------------------------------------
# Авторизация скачивания и занятые весы
# ---------------------------------------------------------------------------


class TestUpdaterDownloadAuth:
    def test_unauthorized_download_fails(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Сервер отвечает 401 (токен не подошёл) → ok=False, ничего не создано."""
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir, token="wrong-token")

        status = _handle(updater, _make_command(payload))
        assert status.ok is False
        assert status.error is not None and "401" in status.error
        assert not (install_dir / "app_new").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert spawn_calls == []


class TestUpdaterBusy:
    def test_busy_forever_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """busy=True всё окно ожидания → отказ «заняты», рестарт не запущен.

        Скачивание и распаковка при этом ПРОХОДЯТ — подготовка намеренно
        идёт до ожидания весов (замечание ревью 10.08.2026: окно между
        проверкой busy и рестартом службы должно быть минимальным).
        Окно ожидания ужато monkeypatch'ем, чтобы не ждать 10 минут.
        """
        monkeypatch.setattr(
            AgentUpdater, "_extract_via_tools", staticmethod(lambda archive, base: False)
        )
        monkeypatch.setattr(updater_module, "BUSY_WAIT_S", 0.2)
        monkeypatch.setattr(updater_module, "BUSY_POLL_S", 0.05)
        polls = 0

        def busy() -> bool:
            nonlocal polls
            polls += 1
            return True

        payload = _make_release_zip()
        base_url, _requests = release_server(payload)
        updater = _make_updater(base_url, install_dir, busy=busy)
        status = _handle(updater, _make_command(payload))
        assert status.ok is False
        assert status.error is not None and "заняты" in status.error
        assert polls > 1, "busy опрашивался меньше двух раз — ожидания не было"
        # подготовка прошла, но рестарт службы не запускался; подготовленные
        # для bat update.zip и app_new тоже убраны — отменённое обновление
        # не оставляет следов
        assert not (install_dir / "update-download.zip").exists()
        assert not (install_dir / "update.zip").exists()
        assert not (install_dir / "app_new").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert spawn_calls == []


# ---------------------------------------------------------------------------
# Распаковка: обычно её делает bat (урок 360, 12.08.2026), а на ПК без
# tar и Expand-Archive — прежний путь: агент распаковывает сам
# ---------------------------------------------------------------------------


class TestUnpackFallback:
    def test_no_tools_agent_extracts_itself(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Нет ни tar, ни Expand-Archive → агент раскладывает app_new сам,
        bat без блока распаковки, update.zip не остаётся."""
        monkeypatch.setattr(
            AgentUpdater, "_extract_via_tools", staticmethod(lambda archive, base: False)
        )
        monkeypatch.setattr(updater_module, "unpack_via_bat_available", lambda: False)
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        status = _handle(updater, _make_command(payload))
        assert status.ok is True, f"обновление не прошло: {status.error}"

        # app_new создана агентом, содержимое байт-в-байт
        app_new = install_dir / "app_new"
        assert (app_new / "ves-agent.exe").read_bytes() == NEW_EXE
        assert (app_new / "_internal" / "x").read_bytes() == INTERNAL_FILE
        assert not (install_dir / "update.zip").exists()

        text = (install_dir / "self-update.bat").read_bytes().decode("utf-8")
        assert "tar -xf" not in text and "Expand-Archive" not in text
        assert 'nssm.exe" stop ves-agent' in text
        assert spawn_calls != []

    def test_update_bat_variants(self) -> None:
        """unpack=True несёт блок распаковки до остановки службы, False — нет;
        подстановка службы и задачи работает в обоих."""
        with_unpack = updater_module.update_bat("ves-agent-2", unpack=True)
        without = updater_module.update_bat("ves-agent-2", unpack=False)
        assert "__SERVICE__" not in with_unpack and "__TASK__" not in with_unpack
        assert "__SERVICE__" not in without and "__TASK__" not in without
        assert "tar -xf" in with_unpack and "Expand-Archive" in with_unpack
        assert with_unpack.index("tar -xf") < with_unpack.index('nssm.exe" stop')
        assert "tar -xf" not in without and "app_unpack" not in without
        # успех распаковки решают коды возврата инструментов (ревью: tar,
        # упавший на середине, мог успеть положить exe — считать по exist
        # нельзя); плюс контрольная проверка exe перед подменой
        assert with_unpack.count("if errorlevel 1 (") >= 3, (
            "нет проверок кодов возврата tar/PowerShell"
        )
        assert 'if not exist "%BASE%\\app_new\\ves-agent.exe"' in with_unpack
        # неубираемый хвост прежней попытки (rmdir не смог) — отказ сразу,
        # а не распаковка поверх устаревших файлов
        assert 'if exist "%BASE%\\app_unpack"' in with_unpack
        assert 'if exist "%BASE%\\app_new"' in with_unpack
        for text in (with_unpack, without):
            assert 'nssm.exe" stop ves-agent-2' in text
            assert "schtasks /Delete /TN ves-agent-2-update /F" in text

    def test_unpack_block_has_no_parens_in_if_echo(self) -> None:
        """Внутри блоков if (...) в bat нельзя использовать скобки в echo —
        «)» преждевременно закрыла бы блок. Проверяем построчно."""
        depth = 0
        for line in updater_module.update_bat("ves-agent").splitlines():
            stripped = line.strip()
            if depth > 0 and stripped.startswith("echo"):
                assert "(" not in stripped and ")" not in stripped, (
                    f"скобки в echo внутри блока if: {stripped!r}"
                )
            depth += stripped.count("(") - stripped.count(")")
        assert depth == 0, "непарные скобки в bat"


def _tar_reads_zip() -> bool:
    """Читает ли системный tar zip-архивы (bsdtar мака/Windows — да, GNU — нет)."""
    try:
        probe = subprocess.run(["tar", "--version"], capture_output=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return False
    return b"bsdtar" in probe.stdout


class TestExtractViaTools:
    """_extract_via_tools: распаковка доверенным инструментом из агента (0.4.13)."""

    @pytest.mark.skipif(not _tar_reads_zip(), reason="системный tar не читает zip (GNU tar)")
    def test_real_tar_unpacks(self, tmp_path: Path) -> None:
        """Настоящий tar раскладывает app_new; app_unpack и архив не остаются."""
        archive = tmp_path / "update-download.zip"
        archive.write_bytes(_make_release_zip())
        assert AgentUpdater._extract_via_tools(archive, tmp_path) is True
        assert (tmp_path / "app_new" / "ves-agent.exe").read_bytes() == NEW_EXE
        assert (tmp_path / "app_new" / "_internal" / "x").read_bytes() == INTERNAL_FILE
        assert not (tmp_path / "app_unpack").exists()

    @pytest.mark.skipif(not _tar_reads_zip(), reason="системный tar не читает zip (GNU tar)")
    def test_archive_without_exe_is_refused(self, tmp_path: Path) -> None:
        """Распаковалось, но exe в app нет → False и никакой app_new."""
        archive = tmp_path / "update-download.zip"
        archive.write_bytes(_make_release_zip(with_exe=False))
        assert AgentUpdater._extract_via_tools(archive, tmp_path) is False
        assert not (tmp_path / "app_new").exists()
        assert not (tmp_path / "app_unpack").exists()

    def test_tool_failure_returns_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Инструмент вернул ненулевой код (антивирус, битый tar) → False,
        временные каталоги убраны, исключение не летит."""
        calls: list[list[str]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            calls.append(command)
            return subprocess.CompletedProcess(command, returncode=1, stdout=b"", stderr=b"boom")

        monkeypatch.setattr(subprocess, "run", fake_run)
        archive = tmp_path / "update-download.zip"
        archive.write_bytes(_make_release_zip())
        assert AgentUpdater._extract_via_tools(archive, tmp_path) is False
        assert calls, "инструмент распаковки даже не запускался"
        assert not (tmp_path / "app_unpack").exists()
        assert not (tmp_path / "app_new").exists()

    def test_tool_missing_returns_false(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """tar в системе нет (OSError при запуске) → False без исключения."""

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            raise FileNotFoundError("tar not found")

        monkeypatch.setattr(subprocess, "run", fake_run)
        archive = tmp_path / "update-download.zip"
        archive.write_bytes(_make_release_zip())
        assert AgentUpdater._extract_via_tools(archive, tmp_path) is False

    def test_stale_dirs_removed_before_unpack(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Хвосты прежней попытки (app_unpack/app_new со старьём) убираются
        до распаковки — версии не смешиваются даже при неудаче."""
        stale_unpack = tmp_path / "app_unpack" / "old"
        stale_unpack.mkdir(parents=True)
        stale_new = tmp_path / "app_new"
        stale_new.mkdir()
        (stale_new / "ves-agent.exe").write_bytes(b"MZ stale")

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            return subprocess.CompletedProcess(command, returncode=1, stdout=b"", stderr=b"")

        monkeypatch.setattr(subprocess, "run", fake_run)
        archive = tmp_path / "update-download.zip"
        archive.write_bytes(_make_release_zip())
        assert AgentUpdater._extract_via_tools(archive, tmp_path) is False
        assert not (tmp_path / "app_new").exists(), "старая app_new не убрана"
        assert not (tmp_path / "app_unpack").exists()


class TestUnpackDetection:
    """unpack_via_bat_available: выбор инструмента распаковки на весовом ПК."""

    def _win(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "platform", "win32")

    def test_non_windows_always_true(self) -> None:
        """Вне Windows (dev/тесты) bat не исполняется — детекция не мешает."""
        assert updater_module.unpack_via_bat_available() is True

    def test_tar_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Есть tar (Windows 10 1803+) → True, PowerShell даже не пробуем."""
        self._win(monkeypatch)
        monkeypatch.setattr(shutil, "which", lambda name: "tar.exe" if name == "tar" else None)

        def no_run(*args: object, **kwargs: object) -> None:
            raise AssertionError("PowerShell не должен запускаться, если есть tar")

        monkeypatch.setattr(subprocess, "run", no_run)
        assert updater_module.unpack_via_bat_available() is True

    def test_no_tools_at_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Нет ни tar, ни powershell → False (распаковка агентом)."""
        self._win(monkeypatch)
        monkeypatch.setattr(shutil, "which", lambda name: None)
        assert updater_module.unpack_via_bat_available() is False

    @pytest.mark.parametrize(("returncode", "expected"), [(0, True), (1, False)])
    def test_powershell_probe(
        self, monkeypatch: pytest.MonkeyPatch, returncode: int, expected: bool
    ) -> None:
        """Без tar решает наличие командлета Expand-Archive (PowerShell ≥5):
        на Windows 7 powershell есть, а командлета нет — там фолбэк."""
        self._win(monkeypatch)
        monkeypatch.setattr(
            shutil,
            "which",
            lambda name: "powershell.exe" if name == "powershell" else None,
        )
        monkeypatch.setattr(
            subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(command, returncode, b"", b""),
        )
        assert updater_module.unpack_via_bat_available() is expected

    def test_powershell_probe_failure_means_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Проба PowerShell упала/зависла → False, а не исключение."""
        self._win(monkeypatch)
        monkeypatch.setattr(
            shutil,
            "which",
            lambda name: "powershell.exe" if name == "powershell" else None,
        )

        def raising_run(*args: object, **kwargs: object) -> None:
            raise subprocess.TimeoutExpired(cmd="powershell", timeout=1)

        monkeypatch.setattr(subprocess, "run", raising_run)
        assert updater_module.unpack_via_bat_available() is False


class TestUpdateWatchdog:
    def test_alive_after_deadline_logs_error(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Процесс жив спустя UPDATE_WATCHDOG_S после запуска bat → ошибка
        в логе агента: bat о своих провалах центру не рапортует, а лог
        агента виден со страницы «Журнал агента» панели."""
        monkeypatch.setattr(updater_module, "UPDATE_WATCHDOG_S", 0.03)
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        async def run() -> UpdateStatus:
            status = await updater.handle(_make_command(payload))
            # доигрываем оба таймера: spawn (0.01) и сторожок (0.03)
            await asyncio.sleep(0.1)
            return status

        with caplog.at_level("ERROR", logger="agent.updater"):
            status = asyncio.run(run())
        assert status.ok is True
        assert any("так и не была перезапущена" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Запуск скрипта задачей планировщика (боевой урок 11.08.2026: nssm при
# остановке службы убивает дерево её процессов — дочерний bat умирал
# после nssm stop, не дойдя до подмены папки)
# ---------------------------------------------------------------------------


class TestSchedulerSpawn:
    def test_scheduler_commands_shape(self, tmp_path: Path) -> None:
        """Одноразовая задача: создание с /F (перезапись хвоста прежнего
        обновления), запуск /Run; имя совпадает с удалением в bat."""
        bat = tmp_path / "self-update.bat"
        task = updater_module.task_name(updater_module.DEFAULT_SERVICE_NAME)
        create, run = AgentUpdater.scheduler_commands(bat, task)
        assert create[0] == "schtasks" and "/Create" in create
        assert "/RU" in create and create[create.index("/RU") + 1] == "SYSTEM"
        assert "/F" in create
        assert str(bat) in create[create.index("/TR") + 1]
        assert run == ["schtasks", "/Run", "/TN", task]
        assert f"/TN {task} /F" in updater_module.update_bat(updater_module.DEFAULT_SERVICE_NAME)

    def test_spawn_via_scheduler_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Оба вызова schtasks успешны → True, команды в правильном порядке."""
        calls: list[list[str]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, b"", b"")

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert (
            AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path, "ves-agent-update")
            is True
        )
        assert [c[1] for c in calls] == ["/Create", "/Run"]

    def test_spawn_via_scheduler_create_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Провал /Create → False и /Run не вызывается (для запасного пути)."""
        calls: list[list[str]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 1, b"", "нет прав".encode())

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert (
            AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path, "ves-agent-update")
            is False
        )
        assert [c[1] for c in calls] == ["/Create"]

    def test_spawn_via_scheduler_missing_schtasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """schtasks отсутствует/завис (OSError/TimeoutExpired) → False,
        а не исключение: fallback-запуск должен получить шанс сработать."""

        def raising_run(command: list[str], **kwargs: object) -> None:
            raise FileNotFoundError("schtasks не найден")

        monkeypatch.setattr(subprocess, "run", raising_run)
        assert (
            AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path, "ves-agent-update")
            is False
        )


# ---------------------------------------------------------------------------
# Имя службы: на объекте с двумя весами агенты стоят на одном ПК двумя
# службами (решение 11.08.2026) — обновление каждого должно трогать
# СВОЮ службу и свою задачу планировщика
# ---------------------------------------------------------------------------


class TestServiceName:
    def test_missing_file_keeps_historical_name(self, tmp_path: Path) -> None:
        """Агент установлен до 11.08.2026 (service.txt нет) → ves-agent."""
        assert updater_module.read_service_name(tmp_path) == "ves-agent"

    def test_name_from_file(self, tmp_path: Path) -> None:
        """Имя второго экземпляра читается как есть (CRLF от echo не мешает)."""
        (tmp_path / "service.txt").write_bytes(b"ves-agent-2\r\n")
        assert updater_module.read_service_name(tmp_path) == "ves-agent-2"

    def test_multiline_file_takes_first_line(self, tmp_path: Path) -> None:
        """Хвост после имени игнорируется — берётся первая строка."""
        (tmp_path / "service.txt").write_bytes("ves-agent-2\r\nмусор\r\n".encode())
        assert updater_module.read_service_name(tmp_path) == "ves-agent-2"

    @pytest.mark.parametrize(
        "content",
        [
            b"",
            b"\r\n",
            b"ves agent\r\n",  # пробел: в bat распалось бы на аргументы
            b'ves-agent" & schtasks /Delete /TN x /F\r\n',  # инъекция в команду
            b"\xff\xfe not-utf8\r\n",
            "ves-agent-2\r\n".encode("utf-16"),  # «Сохранить как → Юникод»
            ("a" * 65 + "\r\n").encode(),
        ],
    )
    def test_broken_file_cancels_update(self, tmp_path: Path, content: bytes) -> None:
        """Файл ЕСТЬ, но испорчен → отказ, а не тихий откат к ves-agent:
        на ПК с двумя агентами такой откат остановил бы соседа (ревью)."""
        (tmp_path / "service.txt").write_bytes(content)
        with pytest.raises(updater_module.UpdateError, match=r"service\.txt"):
            updater_module.read_service_name(tmp_path)

    def test_broken_file_reported_to_center(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Отказ виден центру, скачивания и подмены службы не было."""
        (install_dir / "service.txt").write_bytes(b"ves agent\r\n")
        payload = _make_release_zip()
        base_url, requests = release_server(payload)
        status = _handle(_make_updater(base_url, install_dir), _make_command(payload))
        assert status.ok is False
        assert status.error is not None and "service.txt" in status.error
        assert requests == [], "архив скачивался, хотя имя службы неизвестно"
        assert not (install_dir / "self-update.bat").exists()
        assert spawn_calls == []

    def test_update_bat_uses_own_service_and_task(self) -> None:
        """В bat второго экземпляра нет ни одного упоминания чужой службы."""
        text = updater_module.update_bat("ves-agent-2")
        assert "__SERVICE__" not in text and "__TASK__" not in text
        assert 'nssm.exe" stop ves-agent-2' in text
        assert 'nssm.exe" start ves-agent-2' in text
        assert "schtasks /Delete /TN ves-agent-2-update /F" in text
        # соседняя служба ves-agent не должна фигурировать ни в одной команде
        # (app\ves-agent.exe — это файл сборки, к службе он не относится)
        for line in text.splitlines():
            command = line.strip()
            assert not command.endswith("stop ves-agent")
            assert not command.startswith("schtasks /Delete /TN ves-agent-update")
            assert "stop ves-agent " not in command and "start ves-agent " not in command

    def test_second_agent_updates_own_service(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path, str]],
    ) -> None:
        """Сквозной путь второго агента: bat и задача планировщика — свои."""
        (install_dir / "service.txt").write_bytes(b"ves-agent-2\r\n")
        payload = _make_release_zip()
        base_url, _ = release_server(payload)
        status = _handle(_make_updater(base_url, install_dir), _make_command(payload))
        assert status.ok is True
        text = (install_dir / "self-update.bat").read_bytes().decode("utf-8")
        assert 'nssm.exe" stop ves-agent-2' in text
        assert spawn_calls == [(install_dir / "self-update.bat", install_dir, "ves-agent-2-update")]
