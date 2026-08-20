"""Модель индикатора/весов в справочнике (запрос Игоря 20.08.2026).

Подпись «CAS CI-201A (весы SCS-80, 80 т)» в интерфейсе агента жила только
в локальном config.toml весового ПК — правка требовала AnyDesk. Теперь
поле в справочнике весов: агенту едет в снимке настроек (страница
«Настройки» весов), как поверка и камеры. NULL — подписью продолжает
управлять локальный конфиг; downgrade убирает колонку.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "f2a3b4c5d6e7"
down_revision: str | None = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scales", sa.Column("indicator_model", sa.String(120), nullable=True))


def downgrade() -> None:
    op.drop_column("scales", "indicator_model")
