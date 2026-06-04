"""move_strain_and_organism_name_to_genome

Revision ID: 2e0c8d6a1f3b
Revises: 765d642f1d8c
Create Date: 2026-06-04 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2e0c8d6a1f3b"
down_revision: Union[str, None] = "765d642f1d8c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("genome", sa.Column("strain", sa.String(), nullable=True))
    op.add_column("genome", sa.Column("organism_name", sa.String(), nullable=True))

    # Backfill genome-level fields from existing per-link values.
    op.execute(
        sa.text(
            """
            UPDATE genome
            SET strain = (
                SELECT gpl.strain
                FROM genomepangenomelink AS gpl
                WHERE gpl.genome_id = genome.id
                  AND gpl.strain IS NOT NULL
                LIMIT 1
            )
            WHERE strain IS NULL
            """
        )
    )

    op.execute(
        sa.text(
            """
            UPDATE genome
            SET organism_name = (
                SELECT gpl.organism_name
                FROM genomepangenomelink AS gpl
                WHERE gpl.genome_id = genome.id
                  AND gpl.organism_name IS NOT NULL
                LIMIT 1
            )
            WHERE organism_name IS NULL
            """
        )
    )

    op.drop_column("genomepangenomelink", "strain")
    op.drop_column("genomepangenomelink", "organism_name")


def downgrade() -> None:
    op.add_column("genomepangenomelink", sa.Column("strain", sa.String(), nullable=True))
    op.add_column(
        "genomepangenomelink", sa.Column("organism_name", sa.String(), nullable=True)
    )

    # Restore link-level values from genome-level fields.
    op.execute(
        sa.text(
            """
            UPDATE genomepangenomelink
            SET strain = (
                SELECT genome.strain
                FROM genome
                WHERE genome.id = genomepangenomelink.genome_id
            )
            """
        )
    )

    op.execute(
        sa.text(
            """
            UPDATE genomepangenomelink
            SET organism_name = (
                SELECT genome.organism_name
                FROM genome
                WHERE genome.id = genomepangenomelink.genome_id
            )
            """
        )
    )

    op.drop_column("genome", "organism_name")
    op.drop_column("genome", "strain")
