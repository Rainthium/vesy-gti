"""Подключение к БД центра (PostgreSQL).

Адрес берётся из переменной окружения ``DATABASE_URL``; значение
по умолчанию — локальный dev-postgres из docker-compose (порт 5443,
учётка ves/ves — только для разработки; боевые реквизиты задаются
через .env вне git, правило №7).
"""

import os

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

DEV_DATABASE_URL = "postgresql+psycopg://ves:ves@localhost:5443/ves"


def database_url() -> str:
    return os.environ.get("DATABASE_URL", DEV_DATABASE_URL)


def make_engine(url: str | None = None) -> Engine:
    return create_engine(url or database_url(), pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
