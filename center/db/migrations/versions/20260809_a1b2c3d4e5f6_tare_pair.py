"""Тара по паре голова+прицеп (решение Игоря 09.08.2026).

Реестр тар получает колонку trailer_number ('' = без прицепа) и составной
первичный ключ (vehicle_number, trailer_number): смена прицепа больше
не подставляет тару старой сцепки. Существующие записи дозаполняются
номером прицепа из исходного тарирования (weighings.trailer_number).
"""

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "00579b2541c6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tare_registry",
        sa.Column("trailer_number", sa.String(32), nullable=False, server_default=""),
    )
    # дозаполнить прицеп из исходного тарирования
    op.execute(
        """
        UPDATE tare_registry AS t
        SET trailer_number = COALESCE(w.trailer_number, '')
        FROM weighings AS w
        WHERE w.id = t.weighing_id
        """
    )
    op.drop_constraint("tare_registry_pkey", "tare_registry", type_="primary")
    op.create_primary_key(
        "tare_registry_pkey", "tare_registry", ["vehicle_number", "trailer_number"]
    )


def downgrade() -> None:
    # при откате возможны дубли по голове — оставляем самое свежее тарирование
    op.execute(
        """
        DELETE FROM tare_registry AS t
        USING tare_registry AS newer
        WHERE newer.vehicle_number = t.vehicle_number
          AND newer.tared_at > t.tared_at
        """
    )
    op.drop_constraint("tare_registry_pkey", "tare_registry", type_="primary")
    op.create_primary_key("tare_registry_pkey", "tare_registry", ["vehicle_number"])
    op.drop_column("tare_registry", "trailer_number")
