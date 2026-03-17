"""add_genome_count_and_pangenome_count_to_collectionrelease

Revision ID: d656ecb3cb3f
Revises: fd73e89e68ea
Create Date: 2026-03-16 16:20:41.502635

"""
from typing import Sequence, Union

from alembic import op  # type: ignore
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd656ecb3cb3f'
down_revision: Union[str, None] = 'fd73e89e68ea'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Step 1: Add the columns as nullable
    op.add_column('collectionrelease', sa.Column('pangenome_count', sa.Integer(), nullable=True))
    op.add_column('collectionrelease', sa.Column('genome_count', sa.Integer(), nullable=True))
    
    # Step 2: Populate pangenome_count for existing records
    # Count the number of pangenomes linked to each collection release
    op.execute("""
        UPDATE collectionrelease
        SET pangenome_count = (
            SELECT COUNT(*)
            FROM pangenome
            WHERE pangenome.collection_release_id = collectionrelease.id
        )
    """)
    
    # Step 3: Populate genome_count for existing records
    # Count the number of distinct genomes across all pangenomes in each collection release
    op.execute("""
        UPDATE collectionrelease
        SET genome_count = (
            SELECT COUNT(DISTINCT gpl.genome_id)
            FROM pangenome p
            JOIN genomepangenomelink gpl ON gpl.pangenome_id = p.id
            WHERE p.collection_release_id = collectionrelease.id
        )
    """)


def downgrade() -> None:
    # Remove the columns
    op.drop_column('collectionrelease', 'genome_count')
    op.drop_column('collectionrelease', 'pangenome_count')
