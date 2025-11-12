from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    display_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)

    positions: Mapped[list["ClosedPosition"]] = relationship(back_populates="user")


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    market_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    slug: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    resolution: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    positions: Mapped[list["ClosedPosition"]] = relationship(back_populates="market")


class ClosedPosition(Base):
    __tablename__ = "positions_closed"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_pk: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    market_pk: Mapped[int] = mapped_column(ForeignKey("markets.id", ondelete="CASCADE"), index=True)

    side: Mapped[str] = mapped_column(String(8))
    asset: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    quantity: Mapped[Optional[float]] = mapped_column(Numeric(38, 8))
    entry_avg_price: Mapped[Optional[float]] = mapped_column(Numeric(38, 8))
    exit_avg_price: Mapped[Optional[float]] = mapped_column(Numeric(38, 8))
    realized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(38, 8))
    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)

    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)

    user: Mapped[User] = relationship(back_populates="positions")
    market: Mapped[Market] = relationship(back_populates="positions")

    __table_args__ = (
        UniqueConstraint(
            "user_pk",
            "market_pk",
            "side",
            "closed_at",
            name="uq_positions_closed_dedupe",
        ),
    )


class ActivePosition(Base):
    __tablename__ = "positions_active"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_pk: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)

    asset: Mapped[str] = mapped_column(String(128), index=True)
    condition_id: Mapped[str] = mapped_column(String(128), index=True)

    size: Mapped[float] = mapped_column(Numeric(38, 8))
    avg_price: Mapped[float] = mapped_column(Numeric(38, 8))
    initial_value: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    current_value: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    cash_pnl: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    percent_pnl: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    total_bought: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    realized_pnl: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    current_price: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    redeemable: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    mergeable: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    slug: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    icon: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    event_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    event_slug: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    outcome: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    outcome_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    negative_risk: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True, nullable=True)

    user: Mapped[User] = relationship()

    __table_args__ = (
        UniqueConstraint("user_pk", "asset", name="uq_positions_active_user_asset"),
    )


class Activity(Base):
    __tablename__ = "activities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_pk: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    type: Mapped[str] = mapped_column(String(64), index=True)
    side: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)

    asset: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)
    condition_id: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)

    price: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    size: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)
    fee: Mapped[Optional[float]] = mapped_column(Numeric(38, 8), nullable=True)

    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    tx_hash: Mapped[Optional[str]] = mapped_column(String(128), index=True, nullable=True)

    user: Mapped[User] = relationship()

    __table_args__ = (
        UniqueConstraint("user_pk", "tx_hash", name="uq_activities_user_tx"),
    )


# --------------------
# Simulation DB models
# --------------------

class SimPortfolioGlobal(Base):
    __tablename__ = "sim_portfolios_global"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sim_user: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    cash: Mapped[float] = mapped_column(Numeric(38, 8))
    realized_pnl: Mapped[float] = mapped_column(Numeric(38, 8))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

class SimPortfolio(Base):
    __tablename__ = "sim_portfolios"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sim_user: Mapped[str] = mapped_column(String(128), index=True)
    leader_address: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    cash: Mapped[float] = mapped_column(Numeric(38, 8))
    realized_pnl: Mapped[float] = mapped_column(Numeric(38, 8))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    __table_args__ = (
        UniqueConstraint("sim_user", "leader_address", name="uq_sim_portfolio_user_leader"),
    )


class SimActivePosition(Base):
    __tablename__ = "sim_positions_active"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sim_user: Mapped[str] = mapped_column(String(128), index=True)
    leader_address: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    asset: Mapped[str] = mapped_column(String(128), index=True)
    quantity: Mapped[float] = mapped_column(Numeric(38, 8))
    avg_cost: Mapped[float] = mapped_column(Numeric(38, 8))
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    condition_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    __table_args__ = (
        UniqueConstraint("sim_user", "leader_address", "asset", name="uq_sim_active_user_leader_asset"),
    )


class SimClosedPosition(Base):
    __tablename__ = "sim_positions_closed"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sim_user: Mapped[str] = mapped_column(String(128), index=True)
    asset: Mapped[str] = mapped_column(String(128), index=True)
    quantity: Mapped[float] = mapped_column(Numeric(38, 8))
    avg_cost: Mapped[float] = mapped_column(Numeric(38, 8))
    payout: Mapped[float] = mapped_column(Numeric(38, 8))
    realized_pnl: Mapped[float] = mapped_column(Numeric(38, 8))
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    leader_address: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)


class SimTrade(Base):
    __tablename__ = "sim_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sim_user: Mapped[str] = mapped_column(String(128), index=True)
    leader_address: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    side: Mapped[str] = mapped_column(String(8))
    asset: Mapped[str] = mapped_column(String(128), index=True)
    price: Mapped[float] = mapped_column(Numeric(38, 8))
    size: Mapped[float] = mapped_column(Numeric(38, 8))
    fee: Mapped[float] = mapped_column(Numeric(38, 8))
    notional: Mapped[float] = mapped_column(Numeric(38, 8))
    exec_type: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    source_tx: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)
    source_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
