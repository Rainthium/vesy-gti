"""Outbox событий в АИС «СВХ» (контракт v2, раздел 7; согласован 17.08.2026).

``weighing_events`` — очередь публикации в RabbitMQ: строка появляется в
той же транзакции, что и запись офлайн-операции (``source=local_offline``),
фоновый публикатор отправляет ``weighing.completed`` с подтверждением брокера
и ставит ``published_at``. Неотправленное видно панели и мониторингу; повторная
публикация по кнопке — новая строка (event_id детерминирован по операции).
"""

import sqlalchemy as sa
from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str | None = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "weighing_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("weighing_id", sa.BigInteger(), sa.ForeignKey("weighings.id"), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_weighing_events_pending",
        "weighing_events",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_weighing_events_weighing", "weighing_events", ["weighing_id"])


def downgrade() -> None:
    op.drop_index("ix_weighing_events_weighing", table_name="weighing_events")
    op.drop_index("ix_weighing_events_pending", table_name="weighing_events")
    op.drop_table("weighing_events")
