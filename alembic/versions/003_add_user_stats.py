"""add user stats: closed_positions_count and win_rate

Revision ID: 003_add_user_stats
Revises: 002_add_titles_and_cleanup
Create Date: 2025-11-13
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "003_add_user_stats"
down_revision = "002_add_titles_and_cleanup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add aggregated stats columns to users
    op.add_column(
        "users",
        sa.Column(
            "closed_positions_count", sa.Integer(), nullable=False, server_default="0"
        ),
    )
    op.add_column(
        "users",
        sa.Column("win_rate", sa.Numeric(5, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "win_rate")
    op.drop_column("users", "closed_positions_count")
