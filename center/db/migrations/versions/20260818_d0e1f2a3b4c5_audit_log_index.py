"""Индекс audit_log (action, at) — отчёты читают отказы команд АИС (этап 4, 18.08.2026).

Экран «Отчёты» считает отказы команд АИС по журналу аудита
(weigh_request_v1/v2 с code ≠ OK за период): без индекса каждый показ
отчёта — полный скан таблицы, которая растёт с каждой командой АИС.
Данные не меняются; downgrade снимает индекс.
"""

from alembic import op

revision: str = "d0e1f2a3b4c5"
down_revision: str | None = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_audit_log_action_at", "audit_log", ["action", "at"])


def downgrade() -> None:
    op.drop_index("ix_audit_log_action_at", table_name="audit_log")
