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

import logging
import re

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from center.db.models import Site, User, UserRole
from shared.passwords import hash_password

logger = logging.getLogger(__name__)

MIN_PASSWORD_LEN = 8
# белый список символов: логин попадает в шаблоны и журналы — никакой
# экзотики (кавычек, скобок), чтобы исключить инъекции в разметку
LOGIN_RE = re.compile(r"[a-zA-Z0-9._-]{1,64}")


def users_list(session: Session) -> list[tuple[User, Site | None]]:
    """Все пользователи с их объектами (активные сверху, затем по логину)."""
    rows = session.execute(
        select(User, Site)
        .outerjoin(Site, Site.id == User.site_id)
        .order_by(User.is_active.desc(), User.login)
    ).all()
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


def is_active_admin(session: Session, login: str) -> bool:
    """Актуальная проверка прав по БД (сессия могла пережить разжалование)."""
    user = session.execute(
        select(User).where(User.login == login, User.is_active)
    ).scalar_one_or_none()
    return user is not None and user.role is UserRole.ADMIN
