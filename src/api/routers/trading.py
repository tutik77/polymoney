from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ...services.trading import place_order_simple
from ..schemas import OrderIn


router = APIRouter(prefix="/trading", tags=["trading"])


@router.post("/orders")
async def place_order(payload: OrderIn) -> dict:
    try:
        resp = place_order_simple(
            outcome_token_id=payload.outcome_token_id,
            side=payload.side,
            price=payload.price,
            size=payload.size,
            time_in_force=payload.time_in_force,
        )
        # Ensure it's JSON-serializable
        return {"result": resp}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
