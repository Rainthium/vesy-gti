"""Снимки учёток весовых ПК (обратный канал operators_report, 14.08.2026).

Агент 0.4.14 присылает полный список своих учёток (включая заведённые
на месте CLI add-operator) — блок «Учётки на агентах» экрана
«Пользователи». Хешей паролей в таблице нет (правило №7).
"""

import sqlalchemy as sa
from alembic import op

revision: str = "e5f6a7b8c9d0"
down_revision: str | None = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "agent_operators",
        sa.Column(
            "scale_id", sa.Integer(), sa.ForeignKey("scales.id"), primary_key=True, nullable=False
        ),
        sa.Column("login", sa.String(128), primary_key=True, nullable=False),
        sa.Column("full_name", sa.String(200), nullable=False, server_default=""),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("from_center", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("reported_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("agent_operators")
