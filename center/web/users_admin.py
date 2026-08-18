"""Администрирование пользователей панели (экран «Пользователи», только admin).

Мутации возвращают текст ошибки по-русски или None при успехе — маршрут
показывает его флеш-заметкой. Правила:

- пароль не короче 8 символов, admin/admin запрещён (правило проекта №7);
- логин уникален и без пробелов, после создания не меняется;
- нельзя отключить самого себя и нельзя оставить систему без единого
  активного администратора (защита от локаута);
- пользователи не удаляются — только отключаются (учётка остаётся
  в истории входов и в будущей репликации операторов на агентов).
"""

import hashlib
import logging
import re
import secrets
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from center.db.models import Scale, Site, User, UserRole
from shared.passwords import hash_password

logger = logging.getLogger(__name__)

MIN_PASSWORD_LEN = 8
# белый список символов: логин попадает в шаблоны и журналы — никакой
# экзотики (кавычек, скобок), чтобы исключить инъекции в разметку
LOGIN_RE = re.compile(r"[a-zA-Z0-9._-]{1,64}")


def users_list(
    session: Session,
    *,
    search: str = "",
    role: UserRole | None = None,
    site_id: int | None = None,
    without_site: bool = False,
    active: bool | None = None,
) -> list[tuple[User, Site | None]]:
    """Пользователи с их объектами (активные сверху, затем по логину).

    Фильтры (запрос Игоря 11.08.2026): подстрока логина/ФИО без учёта
    регистра, роль, объект (``without_site`` — учётки без привязки,
    т.е. «все объекты»), статус. None/пусто — фильтр не применяется.
    """
    query = select(User, Site).outerjoin(Site, Site.id == User.site_id)
    if search.strip():
        # LIKE-знаки экранируются: '_' — допустимый символ логина и должен
        # искаться литерально, а '%' не должен возвращать всех
        escaped = search.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        needle = f"%{escaped}%"
        query = query.where(
            User.login.ilike(needle, escape="\\") | User.full_name.ilike(needle, escape="\\")
        )
    if role is not None:
        query = query.where(User.role == role)
    if without_site:
        query = query.where(User.site_id.is_(None))
    elif site_id is not None:
        query = query.where(User.site_id == site_id)
    if active is not None:
        query = query.where(User.is_active == active)
    rows = session.execute(query.order_by(User.is_active.desc(), User.login)).all()
    return [(user, site) for user, site in rows]


def _check_password(login: str, password: str) -> str | None:
    if (login, password) == ("admin", "admin"):
        return "admin/admin запрещён (правило проекта №7)"
    if len(password) < MIN_PASSWORD_LEN:
        return f"пароль короче {MIN_PASSWORD_LEN} символов"
    return None


def _resolve_site(session: Session, site_id: int | None) -> tuple[int | None, str | None]:
    """Проверка, что объект существует; (site_id, ошибка)."""
    if site_id is None:
        return None, None
    if session.get(Site, site_id) is None:
        return None, "объект не найден"
    return site_id, None


def _active_admins_besides(session: Session, user_id: int | None) -> int:
    """Сколько активных администраторов, не считая данного пользователя."""
    query = select(func.count()).where(User.role == UserRole.ADMIN, User.is_active)
    if user_id is not None:
        query = query.where(User.id != user_id)
    return int(session.execute(query).scalar_one())


def create_user(
    session: Session,
    *,
    login: str,
    password: str,
    full_name: str,
    role: UserRole,
    site_id: int | None,
) -> str | None:
    """Создать пользователя; логин нормализуется (strip) и проверяется."""
    login = login.strip()
    if not LOGIN_RE.fullmatch(login):
        return "логин: латинские буквы, цифры и . _ - (от 1 до 64 символов)"
    if error := _check_password(login, password):
        return error
    site_id, error = _resolve_site(session, site_id)
    if error:
        return error
    exists = session.execute(select(User).where(User.login == login)).scalar_one_or_none()
    if exists is not None:
        return f"логин {login} уже занят"
    session.add(
        User(
            login=login,
            pw_hash=hash_password(password),
            full_name=full_name.strip(),
            role=role,
            site_id=site_id,
        )
    )
    session.commit()
    logger.info("пользователи: создан %s (%s)", login, role.value)
    return None


def update_user(
    session: Session,
    user_id: int,
    *,
    full_name: str,
    role: UserRole,
    site_id: int | None,
) -> str | None:
    """Сменить ФИО/роль/объект; последнего активного админа не разжаловать."""
    user = session.get(User, user_id)
    if user is None:
        return "пользователь не найден"
    site_id, error = _resolve_site(session, site_id)
    if error:
        return error
    if (
        user.role is UserRole.ADMIN
        and role is not UserRole.ADMIN
        and user.is_active
        and _active_admins_besides(session, user.id) == 0
    ):
        return "нельзя снять роль у последнего активного администратора"
    user.full_name = full_name.strip()
    user.role = role
    user.site_id = site_id
    session.commit()
    logger.info("пользователи: %s обновлён (роль %s)", user.login, role.value)
    return None


def set_password(session: Session, user_id: int, password: str) -> str | None:
    """Сбросить пароль (новый вводит администратор, старый не нужен)."""
    user = session.get(User, user_id)
    if user is None:
        return "пользователь не найден"
    if error := _check_password(user.login, password):
        return error
    user.pw_hash = hash_password(password)
    session.commit()
    logger.info("пользователи: пароль %s сброшен", user.login)
    return None


def toggle_active(session: Session, user_id: int, *, actor_login: str) -> str | None:
    """Отключить/включить учётку; себя и последнего админа не отключить."""
    user = session.get(User, user_id)
    if user is None:
        return "пользователь не найден"
    if user.is_active:
        if user.login == actor_login:
            return "нельзя отключить самого себя"
        if user.role is UserRole.ADMIN and _active_admins_besides(session, user.id) == 0:
            return "нельзя отключить последнего активного администратора"
    user.is_active = not user.is_active
    session.commit()
    logger.info("пользователи: %s %s", user.login, "включён" if user.is_active else "отключён")
    return None


def block_agent_operator(session: Session, scale_id: int, login: str) -> str | None:
    """Перехватить логином центра учётку, заведённую на весовом ПК вручную.

    Кнопка «Заблокировать» блока «Учётки на агентах» (запрос Игоря
    14.08.2026). Механика «центр главнее»: в центре появляется ОТКЛЮЧЁННЫЙ
    оператор-двойник с тем же логином, случайным паролем и привязкой
    к объекту весов — реплика перезапишет местную учётку заблокированной,
    вход погаснет и офлайн. Снимок агенту рассылает маршрут после вызова.

    Если логин уже занят оператором этого объекта, двойник не нужен —
    свежий снимок сам перекроет местную копию (активным оператором или
    уже существующей блокировкой). Чужие логины не трогаем.
    """
    scale = session.get(Scale, scale_id)
    if scale is None:
        return "весы не найдены"
    login = login.strip()
    if not LOGIN_RE.fullmatch(login):
        return (
            "логин не проходит правила центра (латиница, цифры, . _ -) — "
            "такую учётку можно убрать только на весовом ПК"
        )
    existing = session.execute(select(User).where(User.login == login)).scalar_one_or_none()
    if existing is not None:
        if existing.role is not UserRole.OPERATOR:
            return f"логин {login} занят пользователем панели — перехват невозможен"
        if existing.site_id is not None and existing.site_id != scale.site_id:
            return (
                f"логин {login} занят оператором другого объекта — "
                "заблокируйте учётку на весовом ПК"
            )
        # оператор этого объекта (или без привязки) уже есть в центре:
        # достаточно повторного снимка, он перекроет местную копию
        return None
    session.add(
        User(
            login=login,
            # пароль никому не известен и не нужен: двойник существует
            # только чтобы занять логин заблокированной репликой
            pw_hash=hash_password(secrets.token_urlsafe(24)),
            full_name="Блокировка местной учётки",
            role=UserRole.OPERATOR,
            site_id=scale.site_id,
            is_active=False,
        )
    )
    session.commit()
    logger.info("пользователи: перехват местной учётки %s (весы %d)", login, scale_id)
    return None


def session_stamp(pw_hash: str) -> str:
    """Штамп пароля для сессии: производная от хеша (не сам хеш — cookie сессии
    подписана, но читаема). Меняется вместе с паролем: живые сессии с прежним
    штампом центр выбивает — смена пароля разлогинивает всех, кто им пользовался
    (вопрос Игоря 18.08.2026: учётку давали другому человеку)."""
    return hashlib.sha256(pw_hash.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class SessionState:
    """Что панель проверяет по БД на КАЖДЫЙ запрос живой сессии."""

    active: bool
    is_admin: bool
    site_id: int | None  # None — видно всё (админ или пользователь без привязки)
    stamp: str | None  # штамп текущего пароля; None — пользователя нет/отключён


def session_state(session: Session, login: str) -> SessionState:
    user = session.execute(
        select(User).where(User.login == login, User.is_active)
    ).scalar_one_or_none()
    if user is None:
        return SessionState(active=False, is_admin=False, site_id=None, stamp=None)
    is_admin = user.role is UserRole.ADMIN
    return SessionState(
        active=True,
        is_admin=is_admin,
        site_id=None if is_admin else user.site_id,
        stamp=session_stamp(user.pw_hash),
    )


def is_active_admin(session: Session, login: str) -> bool:
    """Актуальная проверка прав по БД (сессия могла пережить разжалование)."""
    return session_state(session, login).is_admin


def visible_site_id(session: Session, login: str) -> tuple[bool, int | None]:
    """(активен ли пользователь, объект-ограничение); None — видно всё.

    Решение 11.08.2026 (перед тиражом на 13 объектов): администратор
    видит систему целиком, остальные — только объект своей привязки.
    Пользователь без привязки (site_id NULL) видит всё: так заведены
    диспетчеры головного офиса. Права читаются по БД на каждый запрос —
    смена привязки применяется сразу, без перевхода.

    Первый элемент отделяет «нет ограничений» от «пользователя нет или
    он отключён»: иначе отключение УВЕЛИЧИВАЛО бы видимость живой
    сессии — уволенный сотрудник с открытой вкладкой видел бы все
    объекты (находка ревью 11.08.2026).
    """
    state = session_state(session, login)
    return state.active, state.site_id
