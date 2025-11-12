from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple, Any, List

from billiard.exceptions import SoftTimeLimitExceeded
import structlog
import time
from collections import OrderedDict

from ..config import get_settings
from ..polymarket_client import PolymarketClient
from ..utils.datetime import parse_datetime_aware
from .activities import insert_activities
from .users import get_or_create_user


# -------------------------
# In-memory state snapshots
# -------------------------

class LruTtlSet:
    """
    LRU+TTL set for deduplicating processed activity signatures.
    Keeps a bounded number of keys and expires old ones by time.
    """
    def __init__(self, max_size: int = 20000, ttl_seconds: float = 6 * 3600.0) -> None:
        self._store: "OrderedDict[tuple, float]" = OrderedDict()
        self._max_size = max(1, int(max_size))
        self._ttl = max(0.0, float(ttl_seconds))

    def _now(self) -> float:
        return time.monotonic()

    def _evict(self, now_ts: float) -> None:
        # Evict expired entries from the oldest
        if self._ttl > 0.0:
            while self._store:
                oldest_key, ts = next(iter(self._store.items()))
                if now_ts - ts > self._ttl:
                    self._store.pop(oldest_key, None)
                else:
                    break
        # Enforce max size
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)

    def contains(self, key: tuple) -> bool:
        now_ts = self._now()
        ts = self._store.get(key)
        if ts is None:
            return False
        if self._ttl > 0.0 and (now_ts - ts > self._ttl):
            # Expired
            self._store.pop(key, None)
            return False
        # Keep hot
        self._store.move_to_end(key)
        return True

    def add(self, key: tuple) -> bool:
        """
        Add key. Returns True if newly added, False if it already existed (refreshed).
        """
        now_ts = self._now()
        if key in self._store:
            self._store.move_to_end(key)
            self._store[key] = now_ts
            self._evict(now_ts)
            return False
        self._store[key] = now_ts
        self._store.move_to_end(key)
        self._evict(now_ts)
        return True

@dataclass(frozen=True)
class PositionSnapshot:
    asset: str
    quantity: float
    avg_cost: float


@dataclass(frozen=True)
class PortfolioSnapshot:
    user: str
    cash: float
    realized_pnl: float
    positions: List[PositionSnapshot]
    updated_at: datetime


_SIM_STATE: Dict[str, PortfolioSnapshot] = {}

async def _snapshot_from_db(sim_user: str, user: str) -> PortfolioSnapshot:
    """Build snapshot from DB state"""
    from ..db import session_scope
    from .sim_db import get_sim_portfolio, get_sim_active_positions
    
    async with session_scope() as session:
        portfolio_row = await get_sim_portfolio(session, sim_user)
        if not portfolio_row:
            return PortfolioSnapshot(
                user=user,
                cash=0.0,
                realized_pnl=0.0,
                positions=[],
                updated_at=datetime.now(timezone.utc),
            )
        
        active_positions = await get_sim_active_positions(session, sim_user)
        pos_list: List[PositionSnapshot] = []
        for pos in active_positions:
            pos_list.append(PositionSnapshot(
                asset=pos.asset,
                quantity=float(pos.quantity),
                avg_cost=float(pos.avg_cost),
            ))
        
        return PortfolioSnapshot(
            user=user,
            cash=float(portfolio_row["cash"]),
            realized_pnl=float(portfolio_row["realized_pnl"]),
            positions=pos_list,
            updated_at=portfolio_row["updated_at"],
        )


def get_sim_snapshot(address: str) -> Optional[PortfolioSnapshot]:
    return _SIM_STATE.get(address)


def get_all_sim_snapshots() -> Dict[str, PortfolioSnapshot]:
    return dict(_SIM_STATE)


async def _get_live_price_for_asset(client: PolymarketClient, asset: Optional[str], side: str, fallback_price: Optional[float]) -> Optional[float]:
    if not asset:
        return fallback_price
    try:
        quotes = await client.fetch_best_quote_for_asset(str(asset))
        bid = quotes.get("bid")
        ask = quotes.get("ask")
        if side == "buy":
            return ask if ask is not None else (bid if bid is not None else fallback_price)
        else:
            return bid if bid is not None else (ask if ask is not None else fallback_price)
    except Exception:
        return fallback_price


def _vwap_from_orderbook(side: str, size: float, book: Dict[str, List[Dict[str, float]]]) -> Tuple[Optional[float], float]:
    """
    Compute VWAP execution price consuming order book depth for the requested size.
    Returns (avg_price or None, executed_size).
    
    Note: Polymarket API returns order book unsorted or in worst-first order,
    so we must sort explicitly to get best prices first.
    """
    if size <= 0:
        return None, 0.0
    levels = (book.get("asks") if side == "buy" else book.get("bids")) or []
    if not levels:
        return None, 0.0
    
    # CRITICAL: Sort to ensure best prices are consumed first
    # For buy: asks ascending (cheapest first)
    # For sell: bids descending (most expensive first)
    if side == "buy":
        levels = sorted(levels, key=lambda x: float(x.get("price", 0.0)))
    else:
        levels = sorted(levels, key=lambda x: float(x.get("price", 0.0)), reverse=True)
    
    remaining = float(size)
    notional = 0.0
    executed = 0.0
    for lvl in levels:
        lvl_price = float(lvl.get("price", 0.0))
        lvl_size = float(lvl.get("size", 0.0))
        if lvl_price <= 0 or lvl_size <= 0:
            continue
        take = min(remaining, lvl_size)
        if take <= 0:
            break
        notional += lvl_price * take
        executed += take
        remaining -= take
        if remaining <= 1e-12:
            break
    if executed <= 0:
        return None, 0.0
    return notional / executed, executed


async def follow_user_realtime_sim(
    user_address: str,
    display_name: Optional[str],
    *,
    initial_cash: float = 10_000_000.0,
    poll_interval: Optional[float] = None,
    slippage_bps: float = 0.0,
    sim_user_id: str = "default",
    client: Optional[PolymarketClient] = None,
) -> None:
    """
    Follow a single user's activities and simulate copying trades at live quotes.

    - Live quote: approximate via user's active positions `curPrice` for the asset.
    - Execution uses current live price (optionally with a tiny slippage if задано).
    - Размер сделки зеркальный: exec_size = event_size (в акциях/долях outcome).
    - Продажи ограничены текущими остатками по активу.
    """
    log = structlog.get_logger()
    settings = get_settings()
    interval = poll_interval if poll_interval is not None else settings.activities_poll_interval_seconds

    # Establish baseline last_seen
    last_seen_ts = datetime.now(timezone.utc)

    from ..db import session_scope
    from .sim_db import (
        record_sim_trade,
        ensure_sim_global,
        increment_global_portfolio,
        buy_sim_position,
        sell_sim_position,
        get_sim_active_position_by_leader_asset,
    )

    async def _loop(c: PolymarketClient) -> None:
        nonlocal last_seen_ts
        # Explicit start marker (useful operational log)
        log.info(
            "sim_follow_start",
            user=user_address,
            sim_user=sim_user_id,
            interval=interval,
            slippage_bps=slippage_bps,
        )
        # Ensure global portfolio exists for this sim user (only once)
        # Also ensure User row exists for the leader
        async with session_scope() as session:
            await ensure_sim_global(session, sim_user=sim_user_id, initial_cash=initial_cash)
            leader_user = await get_or_create_user(session, user_address, display_name)
        
        # In-memory deduplication with LRU/TTL
        dedupe_max_size = getattr(settings, "sim_dedupe_max_size", 20000)
        dedupe_ttl_seconds = getattr(settings, "sim_dedupe_ttl_seconds", 6 * 3600.0)
        processed_signatures = LruTtlSet(max_size=dedupe_max_size, ttl_seconds=dedupe_ttl_seconds)
        
        # Build metadata cache from leader's active positions (done once at startup)
        metadata_cache: Dict[str, Dict[str, Any]] = {}
        try:
            active_positions_raw = await c.fetch_user_active_positions(user_address, max_total=1000)
            for pos in active_positions_raw:
                asset_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
                if asset_id:
                    end_date_str = pos.get("endDate") or pos.get("end_date")
                    end_date_parsed = parse_datetime_aware(end_date_str) if end_date_str else None
                    metadata_cache[str(asset_id)] = {
                        "end_date": end_date_parsed,
                        "title": pos.get("title"),
                        "condition_id": pos.get("conditionId") or pos.get("condition_id"),
                    }
            log.info("sim_cache_built", user=user_address, assets=len(metadata_cache))
        except Exception as e:
            log.warning("sim_cache_failed", user=user_address, error=str(e)[:200])
        
        async def _refresh_metadata_for_asset(asset_id: str) -> Optional[Dict[str, Any]]:
            """Attempt to fetch metadata for a specific asset by reloading leader's active positions with no size threshold."""
            try:
                fresh = await c.fetch_user_active_positions(user_address, max_total=1000, size_threshold="0")
                found: Optional[Dict[str, Any]] = None
                for pos in fresh:
                    a_id = pos.get("asset") or pos.get("token_id") or pos.get("tokenId")
                    if str(a_id) == str(asset_id):
                        end_date_str = pos.get("endDate") or pos.get("end_date")
                        end_date_parsed = parse_datetime_aware(end_date_str) if end_date_str else None
                        md = {
                            "end_date": end_date_parsed,
                            "title": pos.get("title"),
                            "condition_id": pos.get("conditionId") or pos.get("condition_id"),
                        }
                        metadata_cache[str(asset_id)] = md
                        found = md
                        break
                log.info("sim_meta_refresh", user=user_address, asset=str(asset_id)[:20], found=bool(found))
                return found
            except Exception as e:
                log.warning("sim_meta_refresh_failed", user=user_address, asset=str(asset_id)[:20], error=str(e)[:200])
                return None

        while True:
            try:
                max_pages = max(1, int(settings.activities_max_pages_per_poll))
                raw = await c.fetch_user_activities(
                    user_address,
                    page_size=settings.activities_page_size,
                    max_total=settings.activities_page_size * max_pages,
                )

                # Sort oldest->newest and filter by timestamp > last_seen_ts (with a small grace)
                grace = timedelta(seconds=settings.activities_ts_grace_seconds)
                candidates: List[Dict[str, Any]] = []
                for it in raw:
                    ts = parse_datetime_aware(
                        it.get("timestamp") or it.get("time") or it.get("createdAt") or it.get("blockTime")
                    )
                    if ts and ts > (last_seen_ts - grace):
                        candidates.append(it)
                candidates.sort(key=lambda it: parse_datetime_aware(
                    it.get("timestamp") or it.get("time") or it.get("createdAt") or it.get("blockTime")
                ) or datetime.fromtimestamp(0, tz=timezone.utc))

                if candidates:
                    await insert_activities(leader_user, candidates)
                    # Too noisy for INFO; keep only at DEBUG to avoid clutter
                    log.debug("sim_new_activities", user=user_address, count=len(candidates))

                async with session_scope() as session:
                    # Prefetch order books per unique asset
                    unique_assets = sorted({
                        str(it.get("asset") or it.get("token_id") or it.get("tokenId"))
                        for it in candidates
                        if (it.get("asset") or it.get("token_id") or it.get("tokenId"))
                    })
                    orderbooks: Dict[str, Dict[str, List[Dict[str, float]]]] = {}
                    if unique_assets:
                        ob_tasks = [c.fetch_order_book(asset) for asset in unique_assets]
                        ob_results = await asyncio.gather(*ob_tasks, return_exceptions=True)
                        for asset, res in zip(unique_assets, ob_results):
                            if isinstance(res, Exception):
                                orderbooks[asset] = {"bids": [], "asks": []}
                            else:
                                orderbooks[asset] = res  # type: ignore[assignment]

                    best_quote_cache: Dict[str, Dict[str, Optional[float]]] = {}

                    trades_executed = 0
                    for it in candidates:
                        # Build deduplication signature
                        ts = parse_datetime_aware(
                            it.get("timestamp") or it.get("time") or it.get("createdAt") or it.get("blockTime")
                        )
                        tx_hash = it.get("txHash") or it.get("transactionHash") or it.get("hash")
                        side = (it.get("side") or it.get("action") or "").lower()
                        asset = it.get("asset") or it.get("token_id") or it.get("tokenId")
                        size_v = it.get("size") if it.get("size") is not None else it.get("amount")
                        price_v = it.get("price") if it.get("price") is not None else it.get("avgPrice")
                        
                        # Create signature for deduplication
                        # Use tx_hash if available, otherwise composite key
                        if tx_hash:
                            signature = ("tx", tx_hash)
                        else:
                            signature = ("composite", ts, side, asset, size_v, price_v)
                        
                        # Skip if already processed (within TTL/LRU window)
                        if processed_signatures.contains(signature):
                            continue
                        processed_signatures.add(signature)
                        
                        if side not in {"buy", "sell"}:
                            continue
                        
                        # CRITICAL: Don't use 'or' with numeric values - 0 is falsy!
                        fee_v = it.get("fee") or it.get("fees") or it.get("takerFee") or it.get("makerFee")
                        try:
                            event_price = float(price_v) if price_v is not None else None
                            size = float(size_v) if size_v is not None else None
                            _ = float(fee_v) if fee_v is not None else 0.0
                        except Exception:
                            continue
                        
                        if asset is None or size is None or size <= 0:
                            continue

                        # Prefer depth-based execution (VWAP); fallback to best bid/ask with bps slippage
                        desired_size = float(size)
                        # Try depth from cache
                        book = orderbooks.get(str(asset)) or {"bids": [], "asks": []}
                        exec_price: Optional[float] = None
                        exec_size: float = 0.0
                        exec_type: Optional[str] = None
                        
                        # Ensure we have metadata for BUY trades; otherwise refresh and skip if still missing
                        if side == "buy":
                            meta = metadata_cache.get(str(asset))
                            if not meta or not meta.get("end_date"):
                                meta = await _refresh_metadata_for_asset(str(asset))
                            if not meta or not meta.get("end_date"):
                                log.warning(
                                    "sim_skip_no_metadata",
                                    user=user_address,
                                    asset=str(asset)[:32],
                                    reason="no_end_date",
                                )
                                continue

                        if (book.get("bids") or book.get("asks")):
                            vwap_price, vwap_size = _vwap_from_orderbook(side, desired_size, book)
                            if vwap_price is not None and vwap_size > 0:
                                exec_price = vwap_price
                                exec_size = vwap_size
                                exec_type = "vwap"
                        # Fallback to best quote with simple slippage
                        if exec_price is None or exec_size <= 0:
                            # best quote cached per asset
                            if str(asset) not in best_quote_cache:
                                best_quote_cache[str(asset)] = await c.fetch_best_quote_for_asset(str(asset))
                            quotes = best_quote_cache[str(asset)]
                            bid = quotes.get("bid")
                            ask = quotes.get("ask")
                            live_price = (ask if side == "buy" else bid) if (bid is not None or ask is not None) else event_price
                            
                            if live_price is None:
                                continue
                            slip = max(0.0, slippage_bps) / 10_000.0
                            exec_price = live_price * (1.0 + slip) if side == "buy" else live_price * (1.0 - slip)
                            exec_size = desired_size
                            exec_type = "best"
                        
                        # Validate
                        if exec_price is None or exec_price <= 0 or exec_size <= 0:
                            continue

                        # Apply trade via DB (комиссии отключены)
                        executed_size: float = 0.0
                        trade_value_usdc: float = 0.0
                        realized_delta: float = 0.0
                        cash_delta: float = 0.0
                        
                        if side == "buy":
                            # Buy updates position and portfolio
                            # Get metadata from cache
                            meta = metadata_cache.get(str(asset), {})
                            await buy_sim_position(
                                session,
                                sim_user=sim_user_id,
                                leader_address=user_address,
                                asset=str(asset),
                                price=exec_price,
                                size=exec_size,
                                end_date=meta.get("end_date"),
                                title=meta.get("title"),
                                condition_id=meta.get("condition_id"),
                            )
                            executed_size = exec_size
                            trade_value_usdc = exec_price * executed_size
                            cash_delta = -trade_value_usdc
                            realized_delta = 0.0
                        else:
                            # Sell with safe capping
                            result = await sell_sim_position(
                                session,
                                sim_user=sim_user_id,
                                leader_address=user_address,
                                asset=str(asset),
                                price=exec_price,
                                size=exec_size,
                            )
                            if result is None:
                                continue
                            executed_size, new_quantity, realized_delta = result
                            if executed_size <= 0:
                                continue
                            trade_value_usdc = exec_price * executed_size
                            cash_delta = trade_value_usdc

                        # Update global portfolio (cash and realized PnL)
                        await increment_global_portfolio(
                            session,
                            sim_user=sim_user_id,
                            cash_delta=cash_delta,
                            realized_pnl_delta=realized_delta,
                        )

                        # Record trade
                        src_ts = parse_datetime_aware(
                            it.get("timestamp") or it.get("time") or it.get("createdAt") or it.get("blockTime")
                        )
                        src_tx = it.get("txHash") or it.get("transactionHash") or it.get("hash")
                        await record_sim_trade(
                            session,
                            sim_user=sim_user_id,
                            leader_address=user_address,
                            ts=datetime.now(timezone.utc),
                            side=side,
                            asset=str(asset),
                            price=exec_price,
                            size=executed_size,
                            fee=0.0,
                            notional=trade_value_usdc,
                            exec_type=exec_type,
                            title=(metadata_cache.get(str(asset)) or {}).get("title"),
                            source_tx=src_tx,
                            source_ts=src_ts,
                        )

                        trades_executed += 1
                        log.info(
                            "sim_trade",
                            user=user_address,
                            side=side,
                            asset=str(asset)[:20],
                            size=executed_size,
                            price=exec_price,
                            value=trade_value_usdc,
                            pnl=realized_delta if side == "sell" else 0.0,
                            exec_type=exec_type,
                        )

                        # Advance last_seen_ts
                        ts = parse_datetime_aware(
                            it.get("timestamp") or it.get("time") or it.get("createdAt") or it.get("blockTime")
                        )
                        if ts and ts > last_seen_ts:
                            last_seen_ts = ts

                snapshot = await _snapshot_from_db(sim_user_id, user_address)
                _SIM_STATE[user_address] = snapshot

                # Log snapshot only when a trade was executed to reduce noise
                if trades_executed > 0:
                    log.info(
                        "sim_snapshot",
                        user=user_address,
                        cash=snapshot.cash,
                        pnl=snapshot.realized_pnl,
                        positions=len(snapshot.positions),
                        trades_executed=trades_executed,
                    )
                
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                log.info("sim_cancelled", user=user_address)
                break
            except SoftTimeLimitExceeded:
                # Celery soft timeouts surface as this exception; treat as graceful cancel
                log.info("sim_cancelled", user=user_address)
                break
            except Exception as e:
                log.error("sim_error", user=user_address, error=str(e)[:200])
                await asyncio.sleep(min(60.0, interval))

    if client is not None:
        await _loop(client)
    else:
        async with PolymarketClient() as c:
            await _loop(c)


