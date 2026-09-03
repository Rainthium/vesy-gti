"""Ручной режим при живой связи с центром — флаг в справочнике весов (03.09.2026).

Правило №3 запрещает оператору ручные операции, пока есть связь с центром:
взвешивания идут по команде АИС «СВХ». На объекте, к которому АИС ещё не
подключена (СВХ «Кокчо-Коз» — штатное ПО выключено, агент на COM-порту),
взвешивать было бы нечем. Решение Игоря 03.09.2026: явное разрешение в
настройках весов, только админ, каждое переключение в аудите; агенту 0.4.28+
едет в снимке настроек и применяется на лету. Записи при этом обычные
ручные (local_offline) и досылаются сразу. По умолчанию — выключено.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "b4c5d6e7f8a9"
down_revision: str | None = "a3b4c5d6e7f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scales",
        sa.Column("manual_allowed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("scales", "manual_allowed")
