"""URL лёгкого превью у камеры (запрос Игоря 20.08.2026, Аламедин).

Камеры 6 МП отдают полный кадр медленно — превью оператора дёргается.
Новое необязательное поле preview_url в справочнике камер: HTTP-снапшот
суб-потока (channels/102/picture) только для превью, раз в секунду;
фото операций по-прежнему снимаются с основного URL (правило №2).
Пустое поле — поведение прежнее; downgrade просто убирает колонку.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "e1f2a3b4c5d6"
down_revision: str | None = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cameras", sa.Column("preview_url", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("cameras", "preview_url")
