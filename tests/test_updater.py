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
- неверный sha256/размер → архив отвергнут, app_new и bat не появляются;
- self-update.bat пишется с CRLF и содержит остановку/запуск службы;
- занятые весы (busy) откладывают и в итоге отменяют обновление.
"""

import asyncio
import hashlib
import http.server
import io
import subprocess
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
def spawn_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Path, Path]]:
    """Подменить запуск self-update.bat: Popen не вызывается, вызовы копятся."""
    calls: list[tuple[Path, Path]] = []

    def fake_spawn(bat: Path, base: Path) -> None:
        calls.append((bat, base))

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
    def test_full_update_flow(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
    ) -> None:
        """Скачивание с токеном → проверка → app_new → self-update.bat → запуск.

        Сервер отвечает 401 без Bearer-токена, так что ok=True заодно
        доказывает, что агент шлёт Authorization.
        """
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

        # app_new создана с exe и вложенным файлом, содержимое байт-в-байт
        app_new = install_dir / "app_new"
        assert (app_new / "ves-agent.exe").read_bytes() == NEW_EXE
        assert (app_new / "_internal" / "x").read_bytes() == INTERNAL_FILE
        # старая app не тронута (её подменяет только bat)
        assert (install_dir / "app" / "ves-agent.exe").read_bytes() == b"MZ old-agent-exe"

        # self-update.bat записан с CRLF и содержит ключевые шаги
        bat = install_dir / "self-update.bat"
        raw = bat.read_bytes()
        assert raw.count(b"\n") == raw.count(b"\r\n"), "в bat есть строки без CRLF"
        text = raw.decode("utf-8")
        assert 'nssm.exe" stop ves-agent' in text
        assert 'nssm.exe" start ves-agent' in text
        assert "app_new" in text
        # bat запускается задачей планировщика и сам её удаляет в конце
        assert "schtasks /Delete /TN ves-agent-update /F" in text

        # временный архив удалён
        assert not (install_dir / "update-download.zip").exists()

        # запуск скрипта: ровно один вызов с путями bat и базового каталога
        assert spawn_calls == [(bat, install_dir)]


# ---------------------------------------------------------------------------
# Отказы проверки архива
# ---------------------------------------------------------------------------


class TestUpdaterVerification:
    def test_wrong_sha256_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
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
        assert spawn_calls == []

    def test_wrong_size_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
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
        assert spawn_calls == []

    def test_zip_without_agent_exe_rejected(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
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
        assert spawn_calls == []

    def test_zip_slip_member_stays_inside_app_new(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
    ) -> None:
        """Член архива с app/../ не должен вырваться из app_new."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as bundle:
            bundle.writestr("app/ves-agent.exe", NEW_EXE)
            bundle.writestr("app/../evil.txt", b"escaped")
        payload = buffer.getvalue()
        base_url, _ = release_server(payload)
        updater = _make_updater(base_url, install_dir)

        _handle(updater, _make_command(payload))
        assert not (install_dir / "evil.txt").exists(), "файл из архива вырвался из app_new"


# ---------------------------------------------------------------------------
# Авторизация скачивания и занятые весы
# ---------------------------------------------------------------------------


class TestUpdaterDownloadAuth:
    def test_unauthorized_download_fails(
        self,
        install_dir: Path,
        release_server: Callable[[bytes], tuple[str, list[RecordedRequest]]],
        spawn_calls: list[tuple[Path, Path]],
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
        spawn_calls: list[tuple[Path, Path]],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """busy=True всё окно ожидания → отказ «заняты», рестарт не запущен.

        Скачивание и распаковка при этом ПРОХОДЯТ — подготовка намеренно
        идёт до ожидания весов (замечание ревью 10.08.2026: окно между
        проверкой busy и рестартом службы должно быть минимальным).
        Окно ожидания ужато monkeypatch'ем, чтобы не ждать 10 минут.
        """
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
        # подготовка прошла, но рестарт службы не запускался
        assert not (install_dir / "update-download.zip").exists()
        assert not (install_dir / "self-update.bat").exists()
        assert spawn_calls == []


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
        create, run = AgentUpdater.scheduler_commands(bat)
        assert create[0] == "schtasks" and "/Create" in create
        assert "/RU" in create and create[create.index("/RU") + 1] == "SYSTEM"
        assert "/F" in create
        assert str(bat) in create[create.index("/TR") + 1]
        assert run == ["schtasks", "/Run", "/TN", updater_module.TASK_NAME]
        assert f"/TN {updater_module.TASK_NAME} /F" in updater_module.UPDATE_BAT

    def test_spawn_via_scheduler_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Оба вызова schtasks успешны → True, команды в правильном порядке."""
        calls: list[list[str]] = []

        def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, b"", b"")

        monkeypatch.setattr(subprocess, "run", fake_run)
        assert AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path) is True
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
        assert AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path) is False
        assert [c[1] for c in calls] == ["/Create"]

    def test_spawn_via_scheduler_missing_schtasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """schtasks отсутствует/завис (OSError/TimeoutExpired) → False,
        а не исключение: fallback-запуск должен получить шанс сработать."""

        def raising_run(command: list[str], **kwargs: object) -> None:
            raise FileNotFoundError("schtasks не найден")

        monkeypatch.setattr(subprocess, "run", raising_run)
        assert AgentUpdater._spawn_via_scheduler(tmp_path / "u.bat", tmp_path) is False
