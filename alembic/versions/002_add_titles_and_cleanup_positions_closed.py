"""add titles and cleanup positions_closed columns

Revision ID: 002_add_titles_and_cleanup
Revises: 001
Create Date: 2025-11-12
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "002_add_titles_and_cleanup"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add title columns
    op.add_column("activities", sa.Column("title", sa.String(length=1024), nullable=True))
    op.add_column("sim_trades", sa.Column("title", sa.String(length=1024), nullable=True))
    op.add_column("sim_positions_closed", sa.Column("title", sa.String(length=1024), nullable=True))
    op.add_column("positions_closed", sa.Column("title", sa.String(length=1024), nullable=True))

    # Drop obsolete columns from positions_closed
    with op.batch_alter_table("positions_closed") as batch_op:
        # Drop unique constraint that references tx_hash
        batch_op.drop_constraint("uq_positions_closed_dedupe", type_="unique")
        # Drop columns
        for col in ("fees_total", "opened_at", "close_reason", "tx_hash"):
            try:
                batch_op.drop_column(col)
            except Exception:
                # In case the column was already absent
                pass
        # Recreate unique constraint without tx_hash, using closed_at instead
        batch_op.create_unique_constraint(
            "uq_positions_closed_dedupe",
            ["user_pk", "market_pk", "side", "closed_at"],
        )


def downgrade() -> None:
    # Revert unique constraint change first
    with op.batch_alter_table("positions_closed") as batch_op:
        batch_op.drop_constraint("uq_positions_closed_dedupe", type_="unique")
        # Recreate the old columns
        batch_op.add_column(sa.Column("tx_hash", sa.String(length=128), nullable=True))
        batch_op.add_column(sa.Column("close_reason", sa.String(length=32), nullable=True))
        batch_op.add_column(sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("fees_total", sa.Numeric(38, 8), nullable=True))
        # Restore original unique constraint
        batch_op.create_unique_constraint(
            "uq_positions_closed_dedupe",
            ["user_pk", "market_pk", "side", "tx_hash"],
        )

    # Drop title columns
    op.drop_column("positions_closed", "title")
    op.drop_column("sim_positions_closed", "title")
    op.drop_column("sim_trades", "title")
    op.drop_column("activities", "title")


