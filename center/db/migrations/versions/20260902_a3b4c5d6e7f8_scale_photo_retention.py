"""Срок хранения локальных фото весового ПК — в справочнике весов (02.09.2026).

Ретеншн локальных снимков (30 дней после подтверждения центром) жил только
в config.toml весового ПК — правка требовала AnyDesk. Теперь поле в
справочнике весов: агенту едет в снимке настроек (страница «Настройки»
весов) и применяется на лету. NULL — сроком продолжает управлять локальный
конфиг; 0 — не убирать никогда. Записи журнала при этом не удаляются
никогда (правило №2) — управляется только хранение ФАЙЛОВ снимков.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: str | None = "f2a3b4c5d6e7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scales", sa.Column("photo_retention_days", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("scales", "photo_retention_days")
