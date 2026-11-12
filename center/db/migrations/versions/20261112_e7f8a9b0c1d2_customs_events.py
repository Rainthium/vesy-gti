"""Таможенная интеграция: код органа у объекта, отказы тарирования, outbox (12.11.2026).

Контракт customs-events-v1, версия 1.0. Поток АИС «СВХ» не затрагивается:
у событий таможни своя таблица ``customs_events``.

Revision ID: e7f8a9b0c1d2
Revises: d6e7f8a9b0c1
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "e7f8a9b0c1d2"
down_revision: str | None = "d6e7f8a9b0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("sites", sa.Column("customs_code", sa.String(length=32), nullable=True))
    op.create_table(
        "tare_rejections",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("uuid", sa.Uuid(), nullable=False),
        sa.Column("scale_id", sa.Integer(), sa.ForeignKey("scales.id"), nullable=False),
        sa.Column("rejected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("vehicle_number", sa.String(length=32), nullable=True),
        sa.Column("trailer_number", sa.String(length=32), nullable=True),
        sa.Column("operator", sa.String(length=200), nullable=True),
        sa.Column("ais_ref", sa.String(length=32), nullable=True),
        sa.Column("code", sa.String(length=32), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("tare_limit_kg", sa.Float(), nullable=False),
        sa.Column("massa_kg", sa.Float(), nullable=True),
        sa.UniqueConstraint("uuid", name="uq_tare_rejections_uuid"),
    )
    op.create_table(
        "customs_events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("weighing_id", sa.BigInteger(), sa.ForeignKey("weighings.id"), nullable=True),
        sa.Column(
            "rejection_id", sa.BigInteger(), sa.ForeignKey("tare_rejections.id"), nullable=True
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "(weighing_id IS NOT NULL) <> (rejection_id IS NOT NULL)",
            name="ck_customs_events_subject",
        ),
    )
    op.create_index(
        "ix_customs_events_pending",
        "customs_events",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_customs_events_weighing", "customs_events", ["weighing_id"])
    # отказ — свидетельство для таможни: как и журнал, не редактируется и не
    # удаляется (функция триггера — из миграции схемы центра)
    op.execute(
        """
        CREATE TRIGGER tare_rejections_immutable
            BEFORE UPDATE OR DELETE ON tare_rejections
            FOR EACH ROW EXECUTE FUNCTION forbid_weighing_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS tare_rejections_immutable ON tare_rejections")
    op.drop_index("ix_customs_events_weighing", table_name="customs_events")
    op.drop_index("ix_customs_events_pending", table_name="customs_events")
    op.drop_table("customs_events")
    op.drop_table("tare_rejections")
    op.drop_column("sites", "customs_code")
