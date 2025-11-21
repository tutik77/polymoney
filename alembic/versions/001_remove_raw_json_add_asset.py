"""remove raw_json and add asset field to closed_position

Revision ID: 001
Revises:
Create Date: 2025-11-07

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(
    insp: sa.engine.reflection.Inspector, table: str, column: str
) -> bool:
    try:
        cols = [c["name"] for c in insp.get_columns(table)]
        return column in cols
    except Exception:
        return False


def _index_exists(
    insp: sa.engine.reflection.Inspector, table: str, index_name: str
) -> bool:
    try:
        idx = [i.get("name") for i in insp.get_indexes(table)]
        return index_name in idx
    except Exception:
        return False


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Add positions_closed.asset if missing
    if not _column_exists(insp, "positions_closed", "asset"):
        op.add_column(
            "positions_closed", sa.Column("asset", sa.String(length=128), nullable=True)
        )

    # Create index on positions_closed.asset if missing
    idx_name = op.f("ix_positions_closed_asset")
    if not _index_exists(insp, "positions_closed", idx_name):
        op.create_index(idx_name, "positions_closed", ["asset"], unique=False)

    # Drop legacy raw_json columns if they exist
    if _column_exists(insp, "positions_closed", "raw_json"):
        op.drop_column("positions_closed", "raw_json")
    if _column_exists(insp, "positions_active", "raw_json"):
        op.drop_column("positions_active", "raw_json")
    if _column_exists(insp, "activities", "raw_json"):
        op.drop_column("activities", "raw_json")


def downgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # Recreate raw_json columns
    if not _column_exists(insp, "activities", "raw_json"):
        op.add_column(
            "activities",
            sa.Column("raw_json", sa.TEXT(), autoincrement=False, nullable=True),
        )
    if not _column_exists(insp, "positions_active", "raw_json"):
        op.add_column(
            "positions_active",
            sa.Column("raw_json", sa.TEXT(), autoincrement=False, nullable=True),
        )
    if not _column_exists(insp, "positions_closed", "raw_json"):
        op.add_column(
            "positions_closed",
            sa.Column("raw_json", sa.TEXT(), autoincrement=False, nullable=True),
        )

    # Drop index and column asset (if present)
    idx_name = op.f("ix_positions_closed_asset")
    if _index_exists(insp, "positions_closed", idx_name):
        op.drop_index(idx_name, table_name="positions_closed")
    if _column_exists(insp, "positions_closed", "asset"):
        op.drop_column("positions_closed", "asset")
