from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class UserOut(BaseModel):
    id: int
    user_id: str
    display_name: Optional[str] = None
    closed_positions_count: int
    win_rate: Optional[float] = None


class ClosedPositionOut(BaseModel):
    id: int
    user_pk: int
    market_pk: int
    side: str
    title: Optional[str] = None
    quantity: Optional[float] = None
    entry_avg_price: Optional[float] = None
    exit_avg_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    closed_at: Optional[datetime] = None


class ActivePositionOut(BaseModel):
    id: int
    user_pk: int
    asset: str
    condition_id: str
    size: float
    avg_price: float
    initial_value: Optional[float] = None
    current_value: Optional[float] = None
    cash_pnl: Optional[float] = None
    percent_pnl: Optional[float] = None
    total_bought: Optional[float] = None
    realized_pnl: Optional[float] = None
    current_price: Optional[float] = None
    redeemable: Optional[bool] = None
    mergeable: Optional[bool] = None
    title: Optional[str] = None
    slug: Optional[str] = None
    icon: Optional[str] = None
    event_id: Optional[str] = None
    event_slug: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    end_date: Optional[datetime] = None
    negative_risk: Optional[bool] = None
    updated_at: Optional[datetime] = None


class ActivityOut(BaseModel):
    id: int
    user_pk: int
    ts: datetime
    type: str
    title: Optional[str] = None
    side: Optional[str] = None
    asset: Optional[str] = None
    condition_id: Optional[str] = None
    price: Optional[float] = None
    size: Optional[float] = None
    fee: Optional[float] = None
    tx_hash: Optional[str] = None


class OrderIn(BaseModel):
    outcome_token_id: str = Field(..., alias="token_id")
    side: str
    price: float
    size: float
    time_in_force: str = Field("GTC", alias="tif")


class FollowRequest(BaseModel):
    display_name: Optional[str] = None
    poll_interval: Optional[float] = None
    bootstrap: Optional[bool] = None


class IngestOnceRequest(BaseModel):
    limit: Optional[int] = None
    active_max_total: Optional[int] = None
    closed_max_total: Optional[int] = None


# Simulator schemas


class SimRealtimeStartRequest(BaseModel):
    """Request to start realtime simulation for user(s)"""

    users: list[str] = []
    sim_user: Optional[str] = "default"
    initial_cash: Optional[float] = 10_000_000.0
    poll_interval: Optional[float] = 10.0
    slippage_bps: Optional[float] = 0.0
    sizing_strategy: Optional[str] = "target_profit"
    sizing_value: Optional[float] = 0.005
    # max_trade_size: Optional[float] = None  # Optional cap on trade size in shares


class SimPositionOut(BaseModel):
    """Single position in realtime sim snapshot"""

    asset: str
    quantity: float
    avg_cost: float


class SimPortfolioOut(BaseModel):
    """Realtime sim portfolio snapshot"""

    user: str
    cash: float
    realized_pnl: float
    positions: list[SimPositionOut]
    updated_at: datetime


class SimPortfolioGlobalOut(BaseModel):
    sim_user: str
    cash: float
    realized_pnl: float
    updated_at: datetime


class SimActivePositionDbOut(BaseModel):
    """Single active position in the sim DB (per leader + asset)"""

    sim_user: str
    leader_address: Optional[str]
    asset: str
    quantity: float
    avg_cost: float
    end_date: Optional[datetime] = None
    title: Optional[str] = None
    condition_id: Optional[str] = None


class SimClosedPositionDbOut(BaseModel):
    """Single closed position from sim DB"""

    title: Optional[str] = None
    asset: str
    quantity: float
    avg_cost: float
    payout: float
    realized_pnl: float
    closed_at: datetime
    leader_address: Optional[str] = None


class SimTradeOut(BaseModel):
    id: int
    sim_user: str
    leader_address: Optional[str]
    ts: datetime
    side: str
    title: Optional[str] = None
    asset: str
    price: float
    size: float
    fee: float
    notional: float
    exec_type: Optional[str] = None
    source_tx: Optional[str] = None
    source_ts: Optional[datetime] = None


class SettledPositionOut(BaseModel):
    """Details about a single position that was settled"""

    asset: str
    leader_address: str
    quantity: float
    avg_cost: float
    payout: float
    realized_pnl: float
    settlement_type: str  # "resolved" or "expired"
    end_date: Optional[datetime] = None
    title: Optional[str] = None


class SettlementResultOut(BaseModel):
    """Result of calling the settlement endpoint"""

    sim_user: str
    settled_count: int
    total_pnl: float
    total_cash_change: float
    positions: list[SettledPositionOut]


class SimLeaderStatsOut(BaseModel):
    """Aggregated stats per tracked leader for a sim user"""

    leader_address: str
    active_count: int
    closed_count: int
    realized_pnl: float
    win_rate: float
