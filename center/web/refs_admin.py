"""Редактирование справочников из панели (вкладка «Справочники», только admin).

Мутации возвращают текст ошибки по-русски или None при успехе — как в
``users_admin``. Правила:

- объекты и весы не удаляются (на них ссылаются записи журнала) — только
  правятся; код объекта после создания не меняется (входит в канонические
  пути фото);
- legacy-маршрут АИС задаётся целиком (ip + port + autoscale) или никак:
  частично заполненный маршрут не находился бы v1-маршрутизацией;
- камеры — upsert по (весы, роль): у весов не больше одной камеры ПЕРЕД
  и одной ЗАД (ограничение БД);
- агент — один на весы; токен генерируется здесь, наружу отдаётся ОДИН раз,
  в БД хранится только sha256 (правило №7). Перевыпуск токена сразу
  обрывает связь со старым агентом — только по явному действию админа.
"""

import logging
import re
import secrets
from datetime import date

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from center.api_v2.schemas import AIS_OBJECT_RE
from center.db import repo
from center.db.models import (
    Agent,
    Camera,
    ReleaseChannel,
    Scale,
    ScaleKind,
    Site,
)
from shared.enums import CameraRole
from shared.messages import CycleSettings
from shared.tare import DEFAULT_MAX_TARE_KG

logger = logging.getLogger(__name__)

# дефолты цикла = дефолты agent/config.py (выгрузка Кызыл-Кыи 07.08.2026);
# agent в образ центра не входит, поэтому значения продублированы
DEFAULT_CYCLE = CycleSettings(
    zero_threshold_kg=200.0,
    vehicle_threshold_kg=500.0,
    zero_timeout_s=10.0,
    vehicle_timeout_s=90.0,
    stable_duration_s=5.0,
    stable_timeout_s=30.0,
    no_data_timeout_s=5.0,
    max_tare_kg=DEFAULT_MAX_TARE_KG,
)

# код объекта попадает в пути фото и конфиги — только слаг
SITE_CODE_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")
# драйвер — имя модуля agent/drivers/*
DRIVER_RE = re.compile(r"[a-z0-9_]{1,32}")


def _valid_ip(ip: str) -> bool:
    # isascii + isdigit: юникодные «цифры» проходят isdigit, но либо роняют
    # int(), либо дают мусорный маршрут, который АИС никогда не пришлёт
    parts = ip.split(".")
    return len(parts) == 4 and all(
        p.isascii() and p.isdigit() and len(p) <= 3 and int(p) <= 255 for p in parts
    )


def create_site(session: Session, *, code: str, name: str) -> str | None:
    code = code.strip().lower()
    name = name.strip()
    if not SITE_CODE_RE.fullmatch(code):
        return "код объекта: латиница/цифры/дефис, до 32 символов"
    if not name:
        return "название объекта пустое"
    if session.execute(select(Site).where(Site.code == code)).scalar_one_or_none():
        return f"код {code} уже занят"
    session.add(Site(code=code, name=name))
    try:
        session.commit()
    except IntegrityError:  # гонка двух админов: unique sites.code
        session.rollback()
        return f"код {code} уже занят"
    logger.info("справочники: создан объект %s (%s)", name, code)
    return None


def update_site(session: Session, site_id: int, *, name: str) -> str | None:
    """Правится только название: код входит в канонические пути фото."""
    site = session.get(Site, site_id)
    if site is None:
        return "объект не найден"
    name = name.strip()
    if not name:
        return "название объекта пустое"
    site.name = name
    session.commit()
    logger.info("справочники: объект %s переименован в «%s»", site.code, name)
    return None


def _check_legacy(
    ip: str, port: int | None, autoscale: int | None
) -> tuple[str | None, int | None, int | None, str | None]:
    """Нормализация legacy-маршрута: всё или ничего; (ip, port, autoscale, ошибка)."""
    ip = ip.strip()
    if not ip and port is None and autoscale is None:
        return None, None, None, None
    if not ip or port is None or autoscale is None:
        return None, None, None, "legacy-маршрут АИС: заполните ip, порт и autoscale вместе"
    if not _valid_ip(ip):
        return None, None, None, "legacy-маршрут АИС: некорректный IP"
    if not 1 <= port <= 65535:
        return None, None, None, "legacy-маршрут АИС: некорректный порт"
    if not 1 <= autoscale <= 999:
        return None, None, None, "legacy-маршрут АИС: autoscale от 1 до 999"
    return ip, port, autoscale, None


def _check_ais_route(
    ais_object: str, ais_scale_no: int | None
) -> tuple[str | None, int | None, str | None]:
    """Нормализация привязки АИС v2: (объект АИС, № весов, ошибка).

    «Специальный идентификатор СВХ» — строка как в справочнике АИС («0014»,
    ведущие нули значимы); номер весов на объекте по умолчанию 1.
    """
    ais_object = ais_object.strip()
    if not ais_object:
        if ais_scale_no is not None:
            return None, None, "привязка АИС: укажите идентификатор СВХ"
        return None, None, None
    if not AIS_OBJECT_RE.fullmatch(ais_object):
        return None, None, "привязка АИС: идентификатор СВХ — цифры/латиница до 16 символов"
    scale_no = 1 if ais_scale_no is None else ais_scale_no
    if not 1 <= scale_no <= 999:
        return None, None, "привязка АИС: № весов от 1 до 999"
    return ais_object, scale_no, None


def _integrity_message(exc: IntegrityError) -> str:
    """Какой уникальный индекс сработал: legacy-маршрут или привязка АИС."""
    if "uq_scales_ais_route" in str(exc.orig):
        return "такая привязка АИС (объект + № весов) уже назначена другим весам"
    return "такой legacy-маршрут уже назначен другим весам"


def create_scale(
    session: Session,
    *,
    site_id: int,
    name: str,
    kind: ScaleKind,
    driver: str,
    legacy_ip: str = "",
    legacy_port: int | None = None,
    legacy_autoscale: int | None = None,
    ais_object: str = "",
    ais_scale_no: int | None = None,
) -> str | None:
    if session.get(Site, site_id) is None:
        return "объект не найден"
    name = name.strip()
    if not name:
        return "название весов пустое"
    driver = driver.strip().lower()
    if not DRIVER_RE.fullmatch(driver):
        return "драйвер: латиница/цифры/подчёркивание, до 32 символов"
    ip, port, autoscale, error = _check_legacy(legacy_ip, legacy_port, legacy_autoscale)
    if error:
        return error
    ais_obj, ais_no, error = _check_ais_route(ais_object, ais_scale_no)
    if error:
        return error
    session.add(
        Scale(
            site_id=site_id,
            name=name,
            kind=kind,
            driver=driver,
            legacy_ip=ip,
            legacy_port=port,
            legacy_autoscale=autoscale,
            ais_object=ais_obj,
            ais_scale_no=ais_no,
        )
    )
    try:
        session.commit()
    except IntegrityError as exc:  # уникальные индексы маршрутов
        session.rollback()
        return _integrity_message(exc)
    logger.info("справочники: созданы весы «%s» (driver %s)", name, driver)
    return None


def update_scale(
    session: Session,
    scale_id: int,
    *,
    name: str,
    kind: ScaleKind,
    driver: str,
    legacy_ip: str = "",
    legacy_port: int | None = None,
    legacy_autoscale: int | None = None,
    ais_object: str = "",
    ais_scale_no: int | None = None,
) -> str | None:
    scale = session.get(Scale, scale_id)
    if scale is None:
        return "весы не найдены"
    name = name.strip()
    if not name:
        return "название весов пустое"
    driver = driver.strip().lower()
    if not DRIVER_RE.fullmatch(driver):
        return "драйвер: латиница/цифры/подчёркивание, до 32 символов"
    ip, port, autoscale, error = _check_legacy(legacy_ip, legacy_port, legacy_autoscale)
    if error:
        return error
    ais_obj, ais_no, error = _check_ais_route(ais_object, ais_scale_no)
    if error:
        return error
    scale.name = name
    scale.kind = kind
    scale.driver = driver
    scale.legacy_ip = ip
    scale.legacy_port = port
    scale.legacy_autoscale = autoscale
    scale.ais_object = ais_obj
    scale.ais_scale_no = ais_no
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        return _integrity_message(exc)
    logger.info("справочники: весы id=%d обновлены («%s»)", scale_id, name)
    return None


def upsert_camera(
    session: Session,
    *,
    scale_id: int,
    role: CameraRole,
    snapshot_url: str,
    rtsp_url: str,
    preview_url: str = "",
) -> str | None:
    """Создать/обновить камеру весов по роли; пустые URL допустимы
    (камера остаётся в справочнике, но пути к ней не заданы).

    preview_url — лёгкий кадр только для превью оператора (суб-поток,
    channels/102/picture): превью на агенте учащается до раза в секунду;
    фото операций всегда идут с основного URL.
    """
    if session.get(Scale, scale_id) is None:
        return "весы не найдены"
    snapshot_url = snapshot_url.strip()
    rtsp_url = rtsp_url.strip()
    preview_url = preview_url.strip()
    if snapshot_url and not snapshot_url.startswith(("http://", "https://")):
        return "snapshot-URL должен начинаться с http:// или https://"
    if rtsp_url and not rtsp_url.startswith("rtsp://"):
        return "RTSP-URL должен начинаться с rtsp://"
    if preview_url and not preview_url.startswith(("http://", "https://")):
        return "URL превью должен начинаться с http:// или https://"
    if preview_url and not snapshot_url and not rtsp_url:
        # снимок настроек не включает камеру без основного URL — превью
        # до агента не доехало бы, а пилюля «настроен» вводила бы в заблуждение
        return "URL превью задаётся только вместе с основным URL камеры"
    camera = session.execute(
        select(Camera).where(Camera.scale_id == scale_id, Camera.role == role)
    ).scalar_one_or_none()
    if camera is None:
        camera = Camera(scale_id=scale_id, role=role)
        session.add(camera)
    camera.snapshot_url = snapshot_url or None
    camera.rtsp_url = rtsp_url or None
    camera.preview_url = preview_url or None
    session.commit()
    # URL камер содержат пароли — в лог только факт изменения (правило №7)
    logger.info("справочники: камера %s весов id=%d обновлена", role.value, scale_id)
    return None


def save_scale_settings(
    session: Session,
    scale_id: int,
    *,
    cycle: CycleSettings,
    port: str,
    baudrate: int | None,
    indicator_model: str = "",
    photo_retention_days: int | None = None,
    manual_allowed: bool | None = None,
) -> str | None:
    """Сохранить настройки весов (страница настроек, решение Игоря 10.08.2026).

    manual_allowed — ручной режим оператору при живой связи с центром
    (03.09.2026, объект без АИС «СВХ»; агент 0.4.28+): None — не трогать
    (скрипты заведения и старые вызовы не снимут разрешение молча). Аудит
    переключения пишет маршрут — у него есть имя пользователя.

    Цикл — полный набор в scales.thresholds; COM-порт/скорость —
    в scales.port_cfg (пустой порт = порт остаётся локальным на весовом ПК);
    indicator_model — подпись в интерфейсе агента (пустая = подписью
    управляет локальный конфиг, 20.08.2026); photo_retention_days — через
    сколько дней агент убирает локальные ФАЙЛЫ снимков, принятых центром
    (None = локальный конфиг, 0 = не убирать; 02.09.2026 — записи журнала
    не удаляются никогда, правило №2).
    Доставку агенту делает маршрут (push + при каждом hello).
    """
    scale = session.get(Scale, scale_id)
    if scale is None:
        return "весы не найдены"
    values = cycle.model_dump()
    # лимит тары — единственный параметр цикла, где 0 законен (лимит выключен)
    if any(v <= 0 for key, v in values.items() if key != "max_tare_kg"):
        return "все параметры цикла должны быть больше нуля"
    if cycle.vehicle_threshold_kg <= cycle.zero_threshold_kg:
        return "порог заезда должен быть больше порога пустых весов"
    if cycle.stable_duration_s > cycle.stable_timeout_s:
        return "время стабильности не может превышать её таймаут"
    port = port.strip()
    if len(port) > 64:
        return "COM-порт: не длиннее 64 символов"
    if baudrate is not None and not 300 <= baudrate <= 921600:
        return "скорость порта вне разумного диапазона"
    indicator_model = indicator_model.strip()
    if len(indicator_model) > 120:
        return "модель индикатора: не длиннее 120 символов"
    if photo_retention_days is not None and not 0 <= photo_retention_days <= 3650:
        return "срок хранения локальных фото: от 0 до 3650 дней"
    scale.thresholds = values
    scale.port_cfg = {"port": port, "baudrate": baudrate or 9600} if port else None
    scale.indicator_model = indicator_model or None
    scale.photo_retention_days = photo_retention_days
    if manual_allowed is not None:
        scale.manual_allowed = manual_allowed
    session.commit()
    logger.info("справочники: настройки весов id=%d сохранены", scale_id)
    return None


def _parse_form_date(raw: str) -> tuple[date | None, bool]:
    """(дата, ok): пустая строка — None, мусор — ошибка."""
    raw = raw.strip()
    if not raw:
        return None, True
    try:
        return date.fromisoformat(raw), True
    except ValueError:
        return None, False


def save_scale_verification(
    session: Session, scale_id: int, *, number: str, verified_on: str, valid_until: str
) -> str | None:
    """Свидетельство о поверке весов (печатается на весовой карточке).

    Пустой номер — свидетельство не указано: поля очищаются, на карточке
    будет прочерк. Даты необязательны; заполненные проверяются на порядок.
    """
    scale = session.get(Scale, scale_id)
    if scale is None:
        return "весы не найдены"
    number = " ".join(number.split())
    if len(number) > 64:
        return "номер свидетельства: не длиннее 64 символов"
    date_on, ok_on = _parse_form_date(verified_on)
    date_until, ok_until = _parse_form_date(valid_until)
    if not ok_on or not ok_until:
        return "даты поверки — в формате ГГГГ-ММ-ДД"
    if not number and (date_on or date_until):
        return "у свидетельства о поверке нет номера — укажите его"
    if date_on and date_until and date_until < date_on:
        return "срок действия поверки раньше её даты"
    scale.verif_number = number or None
    scale.verif_date = date_on if number else None
    scale.verif_until = date_until if number else None
    session.commit()
    logger.info("справочники: поверка весов id=%d сохранена", scale_id)
    return None


def create_agent(
    session: Session, *, scale_id: int, channel: ReleaseChannel
) -> tuple[str | None, str | None]:
    """Создать агента и выпустить токен; (ошибка, токен) — токен показывается
    один раз, в БД остаётся только хеш."""
    if session.get(Scale, scale_id) is None:
        return "весы не найдены", None
    exists = session.execute(select(Agent).where(Agent.scale_id == scale_id)).scalar_one_or_none()
    if exists is not None:
        return "у этих весов уже есть агент (токен можно перевыпустить)", None
    token = secrets.token_urlsafe(32)
    session.add(Agent(scale_id=scale_id, token_hash=repo.hash_agent_token(token), channel=channel))
    try:
        session.commit()
    except IntegrityError:  # гонка двух админов: unique agents.scale_id
        session.rollback()
        return "у этих весов уже есть агент (токен можно перевыпустить)", None
    logger.info("справочники: создан агент весов id=%d (канал %s)", scale_id, channel.value)
    return None, token


def reissue_agent_token(session: Session, agent_id: int) -> tuple[str | None, str | None]:
    """Перевыпустить токен агента; старый токен перестаёт действовать сразу."""
    agent = session.get(Agent, agent_id)
    if agent is None:
        return "агент не найден", None
    token = secrets.token_urlsafe(32)
    agent.token_hash = repo.hash_agent_token(token)
    session.commit()
    logger.info("справочники: перевыпущен токен агента id=%d", agent_id)
    return None, token


def set_agent_channel(session: Session, agent_id: int, channel: ReleaseChannel) -> str | None:
    agent = session.get(Agent, agent_id)
    if agent is None:
        return "агент не найден"
    agent.channel = channel
    session.commit()
    logger.info("справочники: агент id=%d переведён на канал %s", agent_id, channel.value)
    return None
