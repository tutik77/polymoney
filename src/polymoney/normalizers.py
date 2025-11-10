from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .utils.datetime import parse_datetime_aware


def normalize_activity(raw: Dict[str, Any]) -> Dict[str, Any]:
    ts = parse_datetime_aware(
        raw.get("timestamp") or raw.get("time") or raw.get("createdAt") or raw.get("blockTime")
    )
    side = raw.get("side") or raw.get("action")
    a_type = raw.get("type") or ("trade" if side in {"buy", "sell"} else None)
    token = raw.get("asset") or raw.get("token_id") or raw.get("tokenId")
    cond = raw.get("conditionId") or raw.get("condition_id")
    price = raw.get("price") or raw.get("avgPrice")
    size = raw.get("size") or raw.get("amount")
    fee = raw.get("fee") or raw.get("fees") or raw.get("takerFee") or raw.get("makerFee")
    txh = raw.get("txHash") or raw.get("transactionHash") or raw.get("hash")
    return {
        "ts": ts,
        "type": a_type or "unknown",
        "side": side,
        "asset": token,
        "condition_id": cond,
        "price": price,
        "size": size,
        "fee": fee,
        "tx_hash": txh,
    }


def normalize_closed_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    def _parse_dt(val: Any) -> Optional[datetime]:
        return parse_datetime_aware(val)

    return {
        "market_external_id": raw.get("conditionId") or raw.get("marketId") or raw.get("market_id"),
        "market_slug": raw.get("marketSlug") or raw.get("slug") or raw.get("eventSlug"),
        "market_title": raw.get("marketTitle") or raw.get("title"),
        "side": raw.get("side") or "",
        "asset": raw.get("asset") or raw.get("tokenId") or raw.get("token_id"),
        "quantity": raw.get("quantity") or raw.get("totalBought"),
        "entry_avg_price": raw.get("entryAvg") or raw.get("avgPrice"),
        "exit_avg_price": raw.get("exitAvg") or raw.get("curPrice"),
        "realized_pnl": raw.get("realizedPnl"),
        "fees_total": raw.get("fees"),
        "opened_at": _parse_dt(raw.get("openedAt")),
        "closed_at": _parse_dt(raw.get("closedAt")) or _parse_dt(raw.get("endDate")),
        "close_reason": raw.get("closeReason"),
        "tx_hash": raw.get("txHash"),
    }


def normalize_active_position(raw: Dict[str, Any]) -> Dict[str, Any]:
    end_dt = None
    if isinstance(raw.get("endDate"), str):
        try:
            end_dt = datetime.strptime(raw["endDate"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except Exception:
            end_dt = None

    return {
        "asset": raw.get("asset"),
        "condition_id": raw.get("conditionId"),
        "size": raw.get("size"),
        "avg_price": raw.get("avgPrice"),
        "initial_value": raw.get("initialValue"),
        "current_value": raw.get("currentValue"),
        "cash_pnl": raw.get("cashPnl"),
        "percent_pnl": raw.get("percentPnl"),
        "total_bought": raw.get("totalBought"),
        "realized_pnl": raw.get("realizedPnl"),
        "current_price": raw.get("curPrice"),
        "redeemable": raw.get("redeemable"),
        "mergeable": raw.get("mergeable"),
        "title": raw.get("title"),
        "slug": raw.get("slug"),
        "icon": raw.get("icon"),
        "event_id": raw.get("eventId"),
        "event_slug": raw.get("eventSlug"),
        "outcome": raw.get("outcome"),
        "outcome_index": raw.get("outcomeIndex"),
        "end_date": end_dt,
        "negative_risk": raw.get("negativeRisk"),
    }


__all__ = [
    "normalize_activity",
    "normalize_closed_position",
    "normalize_active_position",
]


