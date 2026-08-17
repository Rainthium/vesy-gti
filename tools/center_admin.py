"""Администрирование центра из консоли: пользователи, объекты, весы, агенты.

Пользователей панели теперь удобнее вести на экране «Пользователи»
(/panel/users, только admin); create-user остаётся для первого админа
и аварийного доступа. Экраны редактирования справочников появятся
в панели позже; до тех пор — эти команды (запускать на сервере центра,
DATABASE_URL из окружения):

    uv run python -m tools.center_admin create-user --login d.ivanov --role dispatcher
    uv run python -m tools.center_admin create-site --code kyzyl-kyia --name 'СВХ «Кызыл-Кыя»'
    uv run python -m tools.center_admin create-scale --site kyzyl-kyia --name 'Весы SCS-80' \
        --driver cas22 --legacy-ip 192.168.150.185 --legacy-port 8087 --legacy-autoscale 2
    uv run python -m tools.center_admin create-agent --scale-id 1
    uv run python -m tools.center_admin list

Пароль пользователя запрашивается интерактивно (не светится в истории
шелла); токен агента печатается ОДИН раз при создании — в БД только хеш.
"""

import argparse
import getpass
import secrets
import sys

from sqlalchemy import select

from center.db import repo
from center.db.models import Agent, Scale, ScaleKind, Site, User, UserRole
from center.db.session import make_engine, make_session_factory
from shared.passwords import hash_password


def main() -> None:
    parser = argparse.ArgumentParser(description="Администрирование центра весовой системы")
    sub = parser.add_subparsers(dest="command", required=True)

    p_user = sub.add_parser("create-user", help="создать/обновить пользователя панели")
    p_user.add_argument("--login", required=True)
    p_user.add_argument("--full-name", default="")
    p_user.add_argument("--role", choices=[r.value for r in UserRole], default="dispatcher")

    p_site = sub.add_parser("create-site", help="создать объект")
    p_site.add_argument("--code", required=True)
    p_site.add_argument("--name", required=True)

    p_scale = sub.add_parser("create-scale", help="создать весы")
    p_scale.add_argument("--site", required=True, help="код объекта")
    p_scale.add_argument("--name", required=True)
    p_scale.add_argument("--driver", default="cas22")
    p_scale.add_argument("--kind", choices=[k.value for k in ScaleKind], default="static")
    p_scale.add_argument("--legacy-ip", default=None)
    p_scale.add_argument("--legacy-port", type=int, default=None)
    p_scale.add_argument("--legacy-autoscale", type=int, default=None)
    # привязка контракта v2 (17.08.2026): «Специальный идентификатор СВХ» из
    # справочника АИС (строка с ведущими нулями) + № весов на объекте
    p_scale.add_argument("--ais-object", default=None, help="напр. 0014 (Кызыл-Кыя)")
    p_scale.add_argument(
        "--ais-scale-no", type=int, default=None, help="№ весов в АИС, по умолчанию 1"
    )

    p_agent = sub.add_parser("create-agent", help="создать агента и выпустить токен")
    p_agent.add_argument("--scale-id", type=int, required=True)

    sub.add_parser("list", help="показать объекты, весы, агентов, пользователей")

    args = parser.parse_args()
    engine = make_engine()
    session_factory = make_session_factory(engine)

    with session_factory() as session:
        if args.command == "create-user":
            password = getpass.getpass("Пароль: ")
            if (args.login, password) == ("admin", "admin"):
                sys.exit("admin/admin запрещён (правило проекта №7).")
            if len(password) < 8:
                sys.exit("Пароль короче 8 символов — откажемся.")
            existing = session.execute(
                select(User).where(User.login == args.login)
            ).scalar_one_or_none()
            if existing is not None:
                existing.pw_hash = hash_password(password)
                existing.full_name = args.full_name or existing.full_name
                existing.role = UserRole(args.role)
                print(f"Пользователь {args.login} обновлён.")
            else:
                session.add(
                    User(
                        login=args.login,
                        pw_hash=hash_password(password),
                        full_name=args.full_name,
                        role=UserRole(args.role),
                    )
                )
                print(f"Пользователь {args.login} создан ({args.role}).")
            session.commit()

        elif args.command == "create-site":
            session.add(Site(code=args.code, name=args.name))
            session.commit()
            print(f"Объект {args.name} создан.")

        elif args.command == "create-scale":
            site = session.execute(select(Site).where(Site.code == args.site)).scalar_one_or_none()
            if site is None:
                sys.exit(f"Объект с кодом {args.site} не найден (create-site сначала).")
            if args.ais_scale_no is not None and not args.ais_object:
                sys.exit(
                    "--ais-scale-no без --ais-object не имеет смысла (укажите идентификатор СВХ)."
                )
            scale = Scale(
                site_id=site.id,
                name=args.name,
                kind=ScaleKind(args.kind),
                driver=args.driver,
                legacy_ip=args.legacy_ip,
                legacy_port=args.legacy_port,
                legacy_autoscale=args.legacy_autoscale,
                ais_object=args.ais_object,
                ais_scale_no=((args.ais_scale_no or 1) if args.ais_object else args.ais_scale_no),
            )
            session.add(scale)
            session.commit()
            print(f"Весы «{args.name}» созданы (id={scale.id}).")

        elif args.command == "create-agent":
            token = secrets.token_urlsafe(32)
            session.add(Agent(scale_id=args.scale_id, token_hash=repo.hash_agent_token(token)))
            session.commit()
            print("Агент создан. Токен (показывается ОДИН раз, в БД только хеш):")
            print(f"  {token}")
            print("Впишите его в конфиг агента на весовом ПК.")

        elif args.command == "list":
            for site in session.execute(select(Site)).scalars():
                print(f"[{site.id}] {site.name} ({site.code})")
                for scale in session.execute(
                    select(Scale).where(Scale.site_id == site.id)
                ).scalars():
                    print(f"  весы [{scale.id}] {scale.name} · {scale.driver}")
                    for agent in session.execute(
                        select(Agent).where(Agent.scale_id == scale.id)
                    ).scalars():
                        print(
                            f"    агент [{agent.id}] {agent.status.value} v{agent.version or '—'}"
                        )
            print("Пользователи:")
            for user in session.execute(select(User)).scalars():
                print(f"  {user.login} · {user.role.value} · {user.full_name or '—'}")


if __name__ == "__main__":
    main()
