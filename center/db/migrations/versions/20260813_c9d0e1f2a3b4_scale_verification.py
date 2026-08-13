"""Свидетельство о поверке весов (задача весовой карточки, 13.08.2026).

Три колонки в scales: номер свидетельства, дата поверки, срок действия.
Печатаются на весовой карточке строкой «№3961 от 26.02.2026 (срок до
26.02.2027)» и реплицируются агенту в снимке настроек весов.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "c9d0e1f2a3b4"
down_revision: str | None = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scales", sa.Column("verif_number", sa.String(64), nullable=True))
    op.add_column("scales", sa.Column("verif_date", sa.Date(), nullable=True))
    op.add_column("scales", sa.Column("verif_until", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("scales", "verif_until")
    op.drop_column("scales", "verif_date")
    op.drop_column("scales", "verif_number")
