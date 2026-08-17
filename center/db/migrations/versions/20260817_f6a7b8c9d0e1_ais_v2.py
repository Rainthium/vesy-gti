"""Контракт v2 с АИС «СВХ» (согласован 17.08.2026): привязка объектов АИС и
номера документов АИС.

- ``scales.ais_object`` / ``ais_scale_no`` — «Специальный идентификатор СВХ»
  из справочника АИС (строка с ведущими нулями, «0014») + номер весов на
  объекте («Авто весы 1/2»): ключ маршрутизации команд v2, уникален парой;
- ``weighing_ais_refs`` — номер документа АИС (``WEI…``/``TAR…``) операции:
  ключ идемпотентности команд и обратная связь по офлайн-операциям.
  Отдельная таблица: запись ``weighings`` неизменяема (правило №2), а номер
  у офлайн-операции появляется позже неё.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "f6a7b8c9d0e1"
down_revision: str | None = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("scales", sa.Column("ais_object", sa.String(16), nullable=True))
    op.add_column("scales", sa.Column("ais_scale_no", sa.Integer(), nullable=True))
    op.create_index(
        "uq_scales_ais_route",
        "scales",
        ["ais_object", "ais_scale_no"],
        unique=True,
        postgresql_where=sa.text("ais_object IS NOT NULL"),
    )
    op.create_table(
        "weighing_ais_refs",
        sa.Column(
            "weighing_id",
            sa.BigInteger(),
            sa.ForeignKey("weighings.id"),
            primary_key=True,
            nullable=False,
        ),
        sa.Column("ais_ref", sa.String(32), nullable=False),
        sa.Column("origin", sa.String(16), nullable=False),
        sa.Column("linked_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("uq_weighing_ais_refs_ais_ref", "weighing_ais_refs", ["ais_ref"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_weighing_ais_refs_ais_ref", table_name="weighing_ais_refs")
    op.drop_table("weighing_ais_refs")
    op.drop_index("uq_scales_ais_route", table_name="scales")
    op.drop_column("scales", "ais_scale_no")
    op.drop_column("scales", "ais_object")
