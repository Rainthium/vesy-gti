"""Одно сторно на запись — частичный уникальный индекс по storno_of (04.09.2026).

Сторнирование (decisions 04.09.2026) — новой записью со ссылкой storno_of на
исходную; исходная не меняется (правило №2). Проверка «запись уже
аннулирована» в repo.storno_weighing идёт до вставки, но две одновременные
попытки прошли бы её обе — индекс закрепляет инвариант в БД, а повторная
вставка превращается в честный отказ StornoError.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "c5d6e7f8a9b0"
down_revision: str | None = "b4c5d6e7f8a9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_weighings_storno_of",
        "weighings",
        ["storno_of"],
        unique=True,
        postgresql_where=sa.text("storno_of IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_weighings_storno_of", table_name="weighings")
