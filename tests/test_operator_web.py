"""Тесты локального веб-интерфейса оператора (agent/web/app.py + services.py).

Покрытие:
- аутентификация: редиректы без входа, ошибка входа, вход/выход,
  WebSocket без сессии закрывается кодом 4401;
- правило режимов №3: при связи с центром ручные операции заблокированы,
  в автономном режиме доступны; manual_allowed в /ws/state инвертирует
  center_online;
- экраны: главная (данные объекта, журнал, «нет тары», источник «Вручную
  (офлайн)»), «Оборудование», HTMX-фрагменты, баннер «НЕТ ДАННЫХ»;
- камеры: отдача JPEG с Cache-Control: no-store, 502 при сбое камеры,
  404 для неизвестной и ненастроенной роли;
- действия: переоткрытие порта вызывает сервис и возвращает фрагмент;
- живой вес: формат weight_text с узким пробелом, отражение смены
  состояния фейка в следующих кадрах;
- ручной режим (/manual/*): серверная блокировка при связи с центром,
  форма и страница результата, ошибки prepare, commit/discard,
  подсказка тары, manual_ready в /ws/state;
- пароли операторов (agent/sync/storage.py): upsert/verify, замена пароля,
  соль в hash_password, устойчивость verify_password к мусору, отсутствие
  открытого пароля в файле БД.

Железо не используется: сервисы — управляемый фейк FakeServices.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from agent.cameras.capture import CameraShot
from agent.drivers.base import ScaleState
from agent.sync.storage import AgentStorage, hash_password, verify_password
from agent.web.app import create_app
from agent.web.services import AgentInfo
from agent.weighing.manual import ManualFlowError, ManualPreview
from shared.enums import CameraRole, ErrorCode, Operation, ScaleStatus, WeighingSource
from shared.messages import TareRecord, WeighingRecord

# узкий неразрывный пробел — разделитель тысяч в весе (как в макетах)
NNBSP = " "

# минимальный валидный заголовок JPEG — достаточно для проверки отдачи байтов
FAKE_JPEG = b"\xff\xd8\xff\xe0" + b"fake-camera-frame" + b"\xff\xd9"

OPERATOR_LOGIN = "osmonov"
OPERATOR_PASSWORD = "secret"
OPERATOR_NAME = "А. Осмонов"


def make_record(**overrides: Any) -> WeighingRecord:
    """Типичная запись журнала; overrides — точечные замены полей."""
    fields: dict[str, Any] = {
        "uuid": uuid4(),
        "operation": Operation.WEIGHING,
        "code": ErrorCode.OK,
        "massa": 43310.0,
        "unit": "kg",
        "stable": True,
        "weighed_at": datetime(2026, 8, 7, 10, 30, tzinfo=UTC),
        "vehicle_number": "01KG777AAA",
        "trailer_number": None,
        "tare_value": 15300.0,
        "tare_weighing_uuid": None,
        "netto": 28010.0,
        "source": WeighingSource.AIS,
        "operator": None,
        "message": None,
    }
    fields.update(overrides)
    return WeighingRecord(**fields)


def make_preview(**record_overrides: Any) -> ManualPreview:
    """Готовое превью ручной операции для заглушки manual_prepare."""
    record = make_record(
        source=WeighingSource.LOCAL_OFFLINE,
        operator=OPERATOR_NAME,
        **record_overrides,
    )
    return ManualPreview(preview_id="pv-test-1", record=record, photos=[], tare=None)


class FakeServices:
    """Управляемый фейк AgentServices: переключатели online/scale/snapshot."""

    info: AgentInfo

    def __init__(self) -> None:
        self.online = True  # связь с центром (правило режимов №3)
        self.scale = ScaleState(status=ScaleStatus.OK, weight_kg=1460.0, stable=True)
        self.snapshot_ok = True
        self.pending = 0
        self.registry_size = 1812
        self.roles = [CameraRole.FRONT, CameraRole.REAR]
        self.journal: list[WeighingRecord] = []
        self.journal_synced = True  # флаг «дослано» для всех записей фейка
        self.manual_ready_flag = False
        self.manual_error: str | None = None
        self.manual_preview: ManualPreview | None = None
        self.manual_capture_args: tuple[Operation, str, str | None, str] | None = None
        self.tare_hint: TareRecord | None = None
        self.reopen_called = False
        self.info = AgentInfo(
            site_name="СВХ «Тест-Терминал»",
            scale_name="Весы SCS-80",
            indicator_model="CAS CI-201A",
            driver_name="cas22 · пакет 22 байта",
            port_label="COM3 · 9600 · 8-N-1",
            agent_version="0.1.0-test",
            center_url="wss://ves.gti.kg",
        )

    def scale_state(self) -> ScaleState:
        return self.scale

    def center_connected(self) -> bool:
        return self.online

    def pending_count(self) -> int:
        return self.pending

    def tare_registry_size(self) -> int:
        return self.registry_size

    def recent_weighings(self, limit: int = 50) -> list[tuple[WeighingRecord, bool]]:
        # по умолчанию все записи «досланы»; тест может подменить journal_synced
        return [(r, self.journal_synced) for r in self.journal[:limit]]

    def camera_roles(self) -> list[CameraRole]:
        return self.roles

    def camera_snapshot(self, role: CameraRole) -> CameraShot:
        if not self.snapshot_ok:
            return CameraShot(
                role=role, jpeg=None, captured_at=datetime.now(UTC), error="таймаут камеры"
            )
        return CameraShot(role=role, jpeg=FAKE_JPEG, captured_at=datetime.now(UTC))

    def verify_operator(self, login: str, password: str) -> str | None:
        if (login, password) == (OPERATOR_LOGIN, OPERATOR_PASSWORD):
            return OPERATOR_NAME
        return None

    def reopen_port(self) -> None:
        self.reopen_called = True

    # --- ручной режим: управляемые заглушки ---

    def manual_ready(self) -> bool:
        return self.manual_ready_flag

    def manual_capture(
        self,
        operation: Operation,
        *,
        vehicle_number: str,
        trailer_number: str | None,
        operator: str,
    ) -> ManualPreview:
        # одношагово: успешный вызов означает, что операция уже «записана»
        if self.manual_error is not None:
            raise ManualFlowError(self.manual_error)
        assert self.manual_preview is not None, "тест не задал manual_preview"
        self.manual_capture_args = (operation, vehicle_number, trailer_number, operator)
        return self.manual_preview

    def find_active_tare(self, vehicle_number: str) -> TareRecord | None:
        return self.tare_hint


@pytest.fixture
def services() -> FakeServices:
    return FakeServices()


@pytest.fixture
def client(services: FakeServices) -> TestClient:
    app = create_app(services, session_secret="test-secret")
    return TestClient(app)


def do_login(client: TestClient) -> None:
    """Войти оператором фейка (сессия остаётся в cookie клиента)."""
    response = client.post(
        "/login",
        data={"login": OPERATOR_LOGIN, "password": OPERATOR_PASSWORD},
        follow_redirects=False,
    )
    assert response.status_code == 303


@pytest.fixture
def operator_client(client: TestClient) -> TestClient:
    do_login(client)
    return client


# --- аутентификация ---


class TestAuth:
    @pytest.mark.parametrize(
        "path",
        ["/", "/equipment", "/fragments/status", "/fragments/journal", "/cameras/front.jpg"],
    )
    def test_protected_paths_redirect_to_login(self, client: TestClient, path: str) -> None:
        """Без входа все экраны, фрагменты и камеры отправляют на /login."""
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["Location"] == "/login"

    def test_login_page_open_without_session(self, client: TestClient) -> None:
        """Страница входа доступна анониму и рендерится по реальному шаблону."""
        response = client.get("/login")
        assert response.status_code == 200
        assert "Вход в систему" in response.text

    def test_wrong_credentials_show_error_without_session(self, client: TestClient) -> None:
        """Неверный пароль: страница входа с текстом ошибки, сессия не создана."""
        response = client.post(
            "/login",
            data={"login": OPERATOR_LOGIN, "password": "wrong"},
            follow_redirects=False,
        )
        assert response.status_code == 200
        assert "Неверный логин или пароль" in response.text
        # сессии нет — главная по-прежнему требует входа
        assert client.get("/", follow_redirects=False).status_code == 303

    def test_successful_login_shows_operator_name(self, client: TestClient) -> None:
        """Верный вход: 303 на главную, в шапке — имя оператора."""
        response = client.post(
            "/login",
            data={"login": OPERATOR_LOGIN, "password": OPERATOR_PASSWORD},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["Location"] == "/"
        page = client.get("/")
        assert page.status_code == 200
        assert OPERATOR_NAME in page.text

    def test_login_strips_whitespace_in_login(self, client: TestClient) -> None:
        """Логин с пробелами по краям принимается (оператор набирает с клавиатуры)."""
        response = client.post(
            "/login",
            data={"login": f"  {OPERATOR_LOGIN}  ", "password": OPERATOR_PASSWORD},
            follow_redirects=False,
        )
        assert response.status_code == 303

    def test_logout_clears_session(self, operator_client: TestClient) -> None:
        """После /logout главная снова редиректит на вход."""
        response = operator_client.post("/logout", follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["Location"] == "/login"
        assert operator_client.get("/", follow_redirects=False).status_code == 303

    def test_websocket_without_session_closed_4401(self, client: TestClient) -> None:
        """WebSocket живого веса без входа закрывается кодом 4401."""
        with pytest.raises(WebSocketDisconnect) as exc_info, client.websocket_connect("/ws/state"):
            pass
        assert exc_info.value.code == 4401


# --- правило режимов №3 ---


class TestModeRule:
    def test_online_manual_blocked(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Центр на связи: взвешивание только через АИС, ручных ссылок нет."""
        services.online = True
        page = operator_client.get("/")
        assert page.status_code == 200
        assert "АИС «СВХ»" in page.text
        assert "/manual/weighing" not in page.text
        assert "/manual/taring" not in page.text
        # кнопка присутствует, но неактивна
        assert "disabled" in page.text
        assert "АВТОНОМНЫЙ РЕЖИМ" not in page.text

    def test_offline_manual_allowed(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Связи нет: баннер автономного режима и ссылки ручных операций."""
        services.online = False
        page = operator_client.get("/")
        assert page.status_code == 200
        assert "АВТОНОМНЫЙ РЕЖИМ" in page.text
        assert 'href="/manual/weighing"' in page.text
        assert 'href="/manual/taring"' in page.text

    def test_ws_manual_allowed_inverts_center_online(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """В кадрах /ws/state manual_allowed всегда противоположен center_online."""
        services.online = True
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["center_online"] is True
        assert frame["manual_allowed"] is False

        services.online = False
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["center_online"] is False
        assert frame["manual_allowed"] is True


# --- экраны и фрагменты ---


class TestScreens:
    def test_main_page_shows_info_and_journal(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Главная: данные объекта из info и журнал с раскладкой полей."""
        base_time = datetime(2026, 8, 7, 9, 0, tzinfo=UTC)
        services.journal = [
            # обычное взвешивание с тарой: брутто выводится с узким пробелом
            make_record(weighed_at=base_time),
            # взвешивание без действующей тары: нетто не считается
            make_record(
                vehicle_number="05KG254AEA",
                massa=27480.0,
                tare_value=None,
                netto=None,
                weighed_at=base_time - timedelta(minutes=19),
            ),
            # ручная офлайн-запись
            make_record(
                vehicle_number="07KG090ABB",
                source=WeighingSource.LOCAL_OFFLINE,
                operator=OPERATOR_LOGIN,
                weighed_at=base_time - timedelta(minutes=40),
            ),
        ]
        page = operator_client.get("/")
        assert page.status_code == 200
        # данные объекта берутся из info, а не захардкожены
        assert services.info.site_name in page.text
        assert services.info.scale_name in page.text
        # журнал: номер ТС, брутто с узким пробелом, «нет тары», источник
        assert "01KG777AAA" in page.text
        assert f"43{NNBSP}310" in page.text
        assert "нет тары" in page.text
        assert "Вручную (офлайн)" in page.text

    def test_equipment_page(self, services: FakeServices, operator_client: TestClient) -> None:
        """«Оборудование»: индикатор, порт, версия агента, реестр, обе камеры."""
        page = operator_client.get("/equipment")
        assert page.status_code == 200
        assert services.info.indicator_model in page.text
        assert services.info.port_label in page.text
        assert services.info.agent_version in page.text
        assert str(services.registry_size) in page.text
        assert "Камера · ПЕРЕД" in page.text
        assert "Камера · ЗАД" in page.text

    def test_fragments_are_partials(self, operator_client: TestClient) -> None:
        """HTMX-фрагменты — не полные страницы: без <html>, с якорями для swap."""
        status = operator_client.get("/fragments/status")
        assert status.status_code == 200
        assert "<html" not in status.text
        assert 'id="header-status"' in status.text

        journal = operator_client.get("/fragments/journal")
        assert journal.status_code == 200
        assert "<html" not in journal.text
        assert 'id="journal"' in journal.text

    def test_no_data_banner_and_ws_status(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Нет пакетов с индикатора: красный баннер и status=no_data в /ws/state."""
        services.scale = ScaleState(status=ScaleStatus.NO_DATA, error="поток отсутствует 5 с")
        page = operator_client.get("/")
        assert page.status_code == 200
        assert "НЕТ ДАННЫХ" in page.text
        assert "banner-error" in page.text
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["status"] == "no_data"
        assert frame["weight_kg"] is None
        assert frame["weight_text"] == "—"


# --- камеры ---


class TestCameras:
    def test_snapshot_ok(self, operator_client: TestClient) -> None:
        """Снимок отдаётся как есть: image/jpeg, байты фейка, без кэширования."""
        response = operator_client.get("/cameras/front.jpg")
        assert response.status_code == 200
        assert response.headers["Content-Type"] == "image/jpeg"
        assert response.content == FAKE_JPEG
        assert response.headers["Cache-Control"] == "no-store"

    def test_snapshot_error_returns_502(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Камера не отдала кадр — 502, взвешивание это не касается."""
        services.snapshot_ok = False
        response = operator_client.get("/cameras/front.jpg")
        assert response.status_code == 502

    def test_unknown_role_returns_404(self, operator_client: TestClient) -> None:
        """Роль вне перечисления CameraRole — 404."""
        assert operator_client.get("/cameras/xxx.jpg").status_code == 404

    def test_unconfigured_role_returns_404(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Валидная роль, не настроенная на объекте, — тоже 404."""
        services.roles = [CameraRole.FRONT]
        assert operator_client.get("/cameras/rear.jpg").status_code == 404


# --- действия на «Оборудовании» ---


class TestActions:
    def test_reopen_port_calls_service(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Кнопка переоткрытия порта дёргает сервис и возвращает фрагмент статуса."""
        response = operator_client.post("/equipment/reopen-port")
        assert response.status_code == 200
        assert services.reopen_called is True
        assert 'id="header-status"' in response.text
        assert "<html" not in response.text

    def test_reopen_port_requires_login(self, client: TestClient) -> None:
        """Без входа действие недоступно (редирект на /login)."""
        response = client.post("/equipment/reopen-port", follow_redirects=False)
        assert response.status_code == 303


# --- живой вес ---


class TestWebSocketState:
    def test_state_frame_format(self, services: FakeServices, operator_client: TestClient) -> None:
        """Кадр /ws/state: weight_text с узким пробелом и полный набор полей."""
        services.pending = 3
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["status"] == "ok"
        assert frame["weight_kg"] == 1460.0
        assert frame["weight_text"] == f"1{NNBSP}460"
        assert frame["stable"] is True
        assert frame["overload"] is False
        assert frame["pending_count"] == 3

    def test_state_change_reflected_in_next_frames(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Смена состояния фейка видна в последующих кадрах трансляции."""
        with operator_client.websocket_connect("/ws/state") as ws:
            first = ws.receive_json()
            assert first["weight_text"] == f"1{NNBSP}460"
            services.scale = ScaleState(status=ScaleStatus.OK, weight_kg=43310.0, stable=False)
            # кадры идут раз в 0.3 с; ждём обновления не дольше нескольких кадров
            frame = first
            for _ in range(10):
                frame = ws.receive_json()
                if frame["weight_kg"] == 43310.0:
                    break
            assert frame["weight_kg"] == 43310.0
            assert frame["weight_text"] == f"43{NNBSP}310"
            assert frame["stable"] is False


# --- ручной режим: маршруты /manual/* ---


class TestManualRoutes:
    @pytest.mark.parametrize("path", ["/manual/weighing", "/manual/taring"])
    def test_online_get_redirects_to_main(
        self, services: FakeServices, operator_client: TestClient, path: str
    ) -> None:
        """Правило №3 серверно: при связи с центром форма недоступна — 303 на /."""
        services.online = True
        response = operator_client.get(path, follow_redirects=False)
        assert response.status_code == 303
        assert response.headers["Location"] == "/"

    @pytest.mark.parametrize("path", ["/manual/weighing", "/manual/taring"])
    def test_online_post_redirects_without_prepare(
        self, services: FakeServices, operator_client: TestClient, path: str
    ) -> None:
        """POST при связи с центром тоже блокируется: кнопкам не доверяем,
        manual_capture не вызывается."""
        services.online = True
        services.manual_preview = make_preview()
        response = operator_client.post(
            path,
            data={"vehicle_number": "01KG777AAA"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["Location"] == "/"
        assert services.manual_capture_args is None

    def test_unknown_operation_404(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Операция вне weighing/taring — 404 даже в автономном режиме."""
        services.online = False
        assert operator_client.get("/manual/unknown", follow_redirects=False).status_code == 404

    def test_offline_form_renders(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """В офлайне форма открывается: переключатель операций и поле номера."""
        services.online = False
        page = operator_client.get("/manual/weighing")
        assert page.status_code == 200
        assert 'href="/manual/weighing"' in page.text
        assert 'href="/manual/taring"' in page.text
        assert 'name="vehicle_number"' in page.text
        assert "Взвесить" in page.text

        taring = operator_client.get("/manual/taring")
        assert taring.status_code == 200
        assert "Тарировать" in taring.text

    @pytest.mark.parametrize(
        ("path", "operation"),
        [("/manual/weighing", Operation.WEIGHING), ("/manual/taring", Operation.TARING)],
    )
    def test_post_success_shows_result_page(
        self,
        services: FakeServices,
        operator_client: TestClient,
        path: str,
        operation: Operation,
    ) -> None:
        """Успешная фиксация (одношаговая, как в ВесыСофт): операция уже
        записана, карточка результата информационная — без кнопок подтверждения."""
        services.online = False
        services.manual_preview = make_preview(operation=operation)
        response = operator_client.post(
            path,
            data={"vehicle_number": "01kg777aaa", "trailer_number": "BD123"},
        )
        assert response.status_code == 200
        assert "Записано в журнал" in response.text
        assert "Новая операция" in response.text
        assert "На главную" in response.text
        # кнопок подтверждения больше нет — запись уже сделана
        assert "/manual/actions/" not in response.text
        # нормализация номеров — дело слоя логики, веб передаёт как введено
        assert services.manual_capture_args == (operation, "01kg777aaa", "BD123", OPERATOR_NAME)

    def test_post_empty_trailer_becomes_none(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Пустое поле прицепа уходит в сервис как None, а не пустая строка."""
        services.online = False
        services.manual_preview = make_preview()
        operator_client.post(
            "/manual/weighing",
            data={"vehicle_number": "01KG777AAA", "trailer_number": ""},
        )
        assert services.manual_capture_args is not None
        assert services.manual_capture_args[2] is None

    def test_post_error_returns_form_with_message(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Отказ prepare: снова форма с текстом ошибки и введённым номером."""
        services.online = False
        services.manual_error = "АТС не на весах — дождитесь заезда"
        response = operator_client.post(
            "/manual/weighing",
            data={"vehicle_number": "01KG777AAA"},
        )
        assert response.status_code == 200
        assert "АТС не на весах — дождитесь заезда" in response.text
        assert 'name="vehicle_number"' in response.text
        assert 'value="01KG777AAA"' in response.text  # введённое не пропало

    def test_tare_hint_found(self, services: FakeServices, operator_client: TestClient) -> None:
        """По известному номеру фрагмент показывает найденную тару."""
        services.tare_hint = TareRecord(
            vehicle_number="01KG777AAA",
            tare_value=15300.0,
            tared_at=datetime(2026, 7, 1, tzinfo=UTC),
            weighing_uuid=uuid4(),
        )
        response = operator_client.get(
            "/manual-fragments/tare-hint", params={"vehicle_number": "01KG777AAA"}
        )
        assert response.status_code == 200
        assert "найдена тара" in response.text
        assert f"15{NNBSP}300" in response.text

    def test_tare_hint_empty_number_is_empty_fragment(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Пустой номер: реестр не опрашивается, фрагмент пустой."""
        services.tare_hint = TareRecord(  # даже если бы реестр что-то вернул
            vehicle_number="01KG777AAA",
            tare_value=15300.0,
            tared_at=datetime(2026, 7, 1, tzinfo=UTC),
            weighing_uuid=uuid4(),
        )
        response = operator_client.get(
            "/manual-fragments/tare-hint", params={"vehicle_number": "   "}
        )
        assert response.status_code == 200
        assert 'id="tare-hint"' in response.text
        assert "найдена тара" not in response.text

    def test_tare_hint_requires_login(self, client: TestClient) -> None:
        """Без сессии фрагмент недоступен — 303 на /login."""
        response = client.get(
            "/manual-fragments/tare-hint",
            params={"vehicle_number": "01KG777AAA"},
            follow_redirects=False,
        )
        assert response.status_code == 303

    def test_ws_state_has_manual_ready(
        self, services: FakeServices, operator_client: TestClient
    ) -> None:
        """Кадр /ws/state содержит manual_ready и отражает флаг сервиса."""
        services.manual_ready_flag = False
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["manual_ready"] is False

        services.manual_ready_flag = True
        with operator_client.websocket_connect("/ws/state") as ws:
            frame = ws.receive_json()
        assert frame["manual_ready"] is True


# --- пароли операторов (agent/sync/storage.py) ---


@pytest.fixture
def storage(tmp_path: Path) -> Iterator[AgentStorage]:
    store = AgentStorage(tmp_path / "agent.db")
    yield store
    store.close()


class TestOperatorStorage:
    def test_verify_returns_full_name(self, storage: AgentStorage) -> None:
        """Верный пароль → отображаемое имя (full_name)."""
        storage.upsert_operator(OPERATOR_LOGIN, OPERATOR_PASSWORD, full_name=OPERATOR_NAME)
        assert storage.verify_operator(OPERATOR_LOGIN, OPERATOR_PASSWORD) == OPERATOR_NAME

    def test_verify_falls_back_to_login_without_full_name(self, storage: AgentStorage) -> None:
        """Без full_name отображаемым именем становится сам логин."""
        storage.upsert_operator("duty1", "pass1")
        assert storage.verify_operator("duty1", "pass1") == "duty1"

    def test_verify_wrong_password_and_unknown_login(self, storage: AgentStorage) -> None:
        """Неверный пароль и несуществующий логин → None."""
        storage.upsert_operator(OPERATOR_LOGIN, OPERATOR_PASSWORD)
        assert storage.verify_operator(OPERATOR_LOGIN, "wrong") is None
        assert storage.verify_operator("nobody", OPERATOR_PASSWORD) is None

    def test_upsert_replaces_password(self, storage: AgentStorage) -> None:
        """Повторный upsert меняет пароль: старый перестаёт подходить."""
        storage.upsert_operator(OPERATOR_LOGIN, "old-password", full_name=OPERATOR_NAME)
        storage.upsert_operator(OPERATOR_LOGIN, "new-password", full_name=OPERATOR_NAME)
        assert storage.verify_operator(OPERATOR_LOGIN, "old-password") is None
        assert storage.verify_operator(OPERATOR_LOGIN, "new-password") == OPERATOR_NAME

    def test_hash_password_uses_salt(self) -> None:
        """Два хеша одного пароля различаются (соль), но оба проверяются."""
        first = hash_password("same-password")
        second = hash_password("same-password")
        assert first != second
        assert verify_password("same-password", first)
        assert verify_password("same-password", second)

    @pytest.mark.parametrize(
        "stored",
        [
            "abc",  # не формат вовсе
            "",  # пустая строка
            "md5$1$aa$bb",  # чужая схема
            "pbkdf2$notanumber$aa$bb",  # итерации не число
            "pbkdf2$1000$xyz$abc",  # соль не hex
            "pbkdf2$1000$aabb",  # не хватает частей
        ],
    )
    def test_verify_password_garbage_stored(self, stored: str) -> None:
        """Мусорный сохранённый хеш → False без исключений."""
        assert verify_password("any-password", stored) is False

    def test_password_not_stored_plaintext(self, tmp_path: Path) -> None:
        """В файле БД нет открытого пароля — только формат pbkdf2$."""
        secret = "SverhSekretno42!"
        db_path = tmp_path / "agent.db"
        store = AgentStorage(db_path)
        store.upsert_operator(OPERATOR_LOGIN, secret, full_name=OPERATOR_NAME)
        store.close()
        # дамп основного файла и возможных WAL/SHM
        dump = b"".join(p.read_bytes() for p in tmp_path.glob("agent.db*"))
        assert secret.encode() not in dump
        assert b"pbkdf2$" in dump
