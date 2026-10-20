"""Журнал изменений реестра тар — инкрементальная репликация агентам (20.10.2026).

Revision ID: d6e7f8a9b0c1
Revises: c5d6e7f8a9b0
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "d6e7f8a9b0c1"
down_revision: str | None = "c5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "tare_registry_changes",
        sa.Column("seq", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("vehicle_number", sa.String(length=32), nullable=False),
        sa.Column("trailer_number", sa.String(length=32), nullable=False, server_default=""),
        sa.Column("weighing_id", sa.BigInteger(), sa.ForeignKey("weighings.id"), nullable=True),
        sa.Column("tare_value", sa.Float(), nullable=True),
        sa.Column("tared_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("tare_registry_changes")
