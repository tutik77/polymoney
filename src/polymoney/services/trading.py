from __future__ import annotations

from typing import Any, Dict

from ..trading import place_order_simple as _place_order_simple


def place_order_simple(outcome_token_id: str, side: str, price: float, size: float, time_in_force: str = "GTC") -> Dict[str, Any]:
    return _place_order_simple(outcome_token_id=outcome_token_id, side=side, price=price, size=size, time_in_force=time_in_force)



