"""remove raw_json and add asset field to closed_position

Revision ID: 001
Revises: 
Create Date: 2025-11-07

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('positions_closed', sa.Column('asset', sa.String(length=128), nullable=True))
    op.create_index(op.f('ix_positions_closed_asset'), 'positions_closed', ['asset'], unique=False)
    
    op.drop_column('positions_closed', 'raw_json')
    op.drop_column('positions_active', 'raw_json')
    op.drop_column('activities', 'raw_json')


def downgrade() -> None:
    op.add_column('activities', sa.Column('raw_json', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('positions_active', sa.Column('raw_json', sa.TEXT(), autoincrement=False, nullable=True))
    op.add_column('positions_closed', sa.Column('raw_json', sa.TEXT(), autoincrement=False, nullable=True))
    
    op.drop_index(op.f('ix_positions_closed_asset'), table_name='positions_closed')
    op.drop_column('positions_closed', 'asset')




