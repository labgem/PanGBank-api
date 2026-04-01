"""add_genome_status_table

Revision ID: 1b2d64350ce4
Revises: d656ecb3cb3f
Create Date: 2026-04-01 15:09:41.778411

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1b2d64350ce4'
down_revision: Union[str, None] = 'd656ecb3cb3f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create the genomestatus table
    op.create_table(
        'genomestatus',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('genome_id', sa.Integer(), nullable=False),
        sa.Column('collection_release_id', sa.Integer(), nullable=False),
        sa.Column('status_type', sa.String(), nullable=False),
        sa.Column('origin', sa.String(), nullable=False),
        sa.ForeignKeyConstraint(['collection_release_id'], ['collectionrelease.id'], ),
        sa.ForeignKeyConstraint(['genome_id'], ['genome.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('genome_id', 'collection_release_id', 'status_type', 'origin')
    )
    
    # Create indexes for better query performance
    op.create_index(op.f('ix_genomestatus_genome_id'), 'genomestatus', ['genome_id'], unique=False)
    op.create_index(op.f('ix_genomestatus_collection_release_id'), 'genomestatus', ['collection_release_id'], unique=False)
    op.create_index(op.f('ix_genomestatus_status_type'), 'genomestatus', ['status_type'], unique=False)
    op.create_index(op.f('ix_genomestatus_origin'), 'genomestatus', ['origin'], unique=False)


def downgrade() -> None:
    # Drop indexes first
    op.drop_index(op.f('ix_genomestatus_origin'), table_name='genomestatus')
    op.drop_index(op.f('ix_genomestatus_status_type'), table_name='genomestatus')
    op.drop_index(op.f('ix_genomestatus_collection_release_id'), table_name='genomestatus')
    op.drop_index(op.f('ix_genomestatus_genome_id'), table_name='genomestatus')
    
    # Drop the table
    op.drop_table('genomestatus')
