"""Журнал событий мониторинга (этап 2, 13.08.2026).

Переходы детекторов (агент офлайн/онлайн, индикатор молчит, камера
недоступна, очереди растут, мало места на диске): экран «События»
панели и доставка в Telegram (notified_at — отметка отправки).
"""

import sqlalchemy as sa
from alembic import op

revision: str = "d4e5f6a7b8c9"
down_revision: str | None = "c9d0e1f2a3b4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "monitoring_events",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("scale_id", sa.Integer(), sa.ForeignKey("scales.id"), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column(
            "severity",
            sa.Enum(
                "danger",
                "warning",
                "ok",
                name="monitoring_severity",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_monitoring_events_created", "monitoring_events", ["created_at"])
    op.create_index(
        "ix_monitoring_events_unnotified",
        "monitoring_events",
        ["id"],
        postgresql_where=sa.text("notified_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_monitoring_events_unnotified", table_name="monitoring_events")
    op.drop_index("ix_monitoring_events_created", table_name="monitoring_events")
    op.drop_table("monitoring_events")
