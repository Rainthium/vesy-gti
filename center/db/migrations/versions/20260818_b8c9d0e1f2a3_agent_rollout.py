"""Автовыкат агентов по каналам (architecture §7а, 18.08.2026).

``agent_releases`` (таблица была в схеме с 07.08, не использовалась):
канал становится необязательным — релиз без канала «не назначен»/«архив»
(перевод нового релиза в канал снимает канал с прежнего), плюс размер,
описание, кто и когда назначил. Артефакт по-прежнему файл в
AGENT_RELEASES_DIR: строка описывает его (file_path = имя файла).

``agent_updates`` — журнал раскатки: одна строка на пару (агент, версия) —
статус (commanded → started → installed | failed | rolled_back), число
попыток, ошибка, откуда команда (auto — движок каналов, manual — кнопка).
По нему панель показывает «Раскатку», а движок не шлёт одну и ту же
команду по кругу (откат — терминален до ручного повтора).

downgrade возвращает NOT NULL каналу и упадёт, если есть релизы без канала
(«не назначен»/архив) — перед откатом миграции их надо удалить или
назначить; журнал раскатки при откате теряется.
"""

import sqlalchemy as sa
from alembic import op

revision: str = "b8c9d0e1f2a3"
down_revision: str | None = "a7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("agent_releases", "channel", existing_type=sa.String(32), nullable=True)
    op.add_column("agent_releases", sa.Column("size_bytes", sa.BigInteger(), nullable=True))
    op.add_column(
        "agent_releases", sa.Column("notes", sa.Text(), nullable=False, server_default="")
    )
    op.add_column("agent_releases", sa.Column("published_by", sa.String(64), nullable=True))
    op.add_column(
        "agent_releases", sa.Column("channel_changed_at", sa.DateTime(timezone=True), nullable=True)
    )

    op.create_table(
        "agent_updates",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("agent_id", sa.Integer(), sa.ForeignKey("agents.id"), nullable=False),
        sa.Column("version", sa.String(32), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "commanded",
                "started",
                "installed",
                "failed",
                "rolled_back",
                name="agent_update_status",
                native_enum=False,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("origin", sa.String(16), nullable=False, server_default="auto"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("commanded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("running_version", sa.String(32), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.UniqueConstraint("agent_id", "version", name="uq_agent_updates_agent_version"),
    )
    op.create_index("ix_agent_updates_version", "agent_updates", ["version"])


def downgrade() -> None:
    op.drop_index("ix_agent_updates_version", table_name="agent_updates")
    op.drop_table("agent_updates")
    op.drop_column("agent_releases", "channel_changed_at")
    op.drop_column("agent_releases", "published_by")
    op.drop_column("agent_releases", "notes")
    op.drop_column("agent_releases", "size_bytes")
    op.alter_column("agent_releases", "channel", existing_type=sa.String(32), nullable=False)
