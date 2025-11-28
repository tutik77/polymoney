from __future__ import annotations

from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query

from ...polymarket_client import PolymarketClient
from ..schemas import (
    MarketResolutionResponse,
    TokenInfo,
    UserBetStatus,
)

router = APIRouter(prefix="/test", tags=["test"])


@router.get("/market/{condition_id}", response_model=MarketResolutionResponse)
async def get_market_resolution(
    condition_id: str,
    asset_id: Optional[str] = Query(None, description="Token ID to check bet status")
) -> MarketResolutionResponse:
    async with PolymarketClient() as client:
        market = await client.fetch_market_by_condition_id(condition_id)
        
        if not market:
            raise HTTPException(
                status_code=404,
                detail=f"Market with condition_id '{condition_id}' not found"
            )
        
        tokens_info: List[TokenInfo] = []
        raw_tokens = market.get("tokens", [])
        
        user_bet_status = None
        
        for token in raw_tokens:
            token_id = token.get("token_id")
            price = token.get("price")
            outcome = token.get("outcome", "Unknown")
            
            winner = None
            if price is not None:
                price_float = float(price)
                if abs(price_float - 1.0) < 0.01:
                    winner = True
                elif abs(price_float) < 0.01:
                    winner = False
            
            tokens_info.append(
                TokenInfo(
                    token_id=token_id or "",
                    outcome=outcome,
                    price=float(price) if price is not None else None,
                    winner=winner,
                )
            )
            
            if asset_id and token_id == asset_id:
                status = "PENDING"
                payout = float(price) if price is not None else 0.0
                
                if winner is True:
                    status = "WON"
                    payout = 1.0
                elif winner is False:
                    status = "LOST"
                    payout = 0.0
                
                user_bet_status = UserBetStatus(
                    token_id=token_id,
                    outcome_name=outcome,
                    status=status,
                    payout=payout
                )
        
        return MarketResolutionResponse(
            condition_id=market.get("condition_id", condition_id),
            question=market.get("question", "N/A"),
            closed=market.get("closed", False),
            active=market.get("active", True),
            umaResolutionStatus=market.get("umaResolutionStatus"),
            tokens=tokens_info,
            user_bet_status=user_bet_status,
        )
