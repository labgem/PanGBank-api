"""add_has_multiple_species_to_pangenome

Revision ID: 9c4d9d5c3e2a
Revises: 2e0c8d6a1f3b
Create Date: 2026-06-05 16:05:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "9c4d9d5c3e2a"
down_revision: Union[str, None] = "2e0c8d6a1f3b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pangenome",
        sa.Column(
            "has_multiple_species",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("pangenome", "has_multiple_species")
