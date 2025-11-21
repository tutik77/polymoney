from __future__ import annotations

from typing import Any, Dict, Optional

from .config import get_settings


class PolymarketTradingClient:
    """
    Thin wrapper over py-clob-client for placing orders via Polymarket CLOB.

    Expects credentials and network settings from environment variables loaded by get_settings().
    Required envs:
      - POLY_PRIVATE_KEY: hex private key for signing orders
    Optional envs:
      - POLYMARKET_CLOB_HOST (default: https://clob.polymarket.com)
      - POLY_CHAIN_ID (default: 137)
      - POLY_PROXY_WALLET (optional funder/proxy wallet address)
    """

    def __init__(self) -> None:
        s = get_settings()
        if not s.private_key or not s.private_key.strip():
            raise ValueError(
                "POLY_PRIVATE_KEY is not set. Provide a private key in env."
            )
        self._host: str = s.polymarket_clob_host
        self._chain_id: int = s.chain_id
        self._private_key: str = s.private_key.strip()
        self._proxy_wallet: Optional[str] = (
            s.proxy_wallet.strip() if s.proxy_wallet else None
        )

    def place_limit_order(
        self,
        outcome_token_id: str,
        side: str,
        price: float,
        size: float,
        time_in_force: str = "GTC",
    ) -> Dict[str, Any]:
        """
        Place a limit order.

        - outcome_token_id: Polymarket outcome token (aka tokenId/asset id)
        - side: "buy" or "sell"
        - price: 0..1
        - size: number of shares
        - time_in_force: one of GTC/IOC/FOK (if supported by SDK)
        """
        if side.lower() not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")
        if not (0.0 < price < 1.0):
            raise ValueError("price must be in (0,1)")
        if size <= 0:
            raise ValueError("size must be > 0")

        try:
            # Import inside method to avoid hard dependency at import time
            from py_clob_client.client import ClobClient  # type: ignore
        except Exception as e:  # pragma: no cover - dependency missing
            raise RuntimeError(
                "py-clob-client is required. Install with: pip install py-clob-client"
            ) from e

        client_kwargs: Dict[str, Any] = {
            "host": self._host,
            "key": self._private_key,
            "chain_id": self._chain_id,
        }
        if self._proxy_wallet:
            client_kwargs["funder"] = self._proxy_wallet

        clob = ClobClient(**client_kwargs)  # type: ignore[arg-type]

        # Resolve constants/types from the SDK if present
        side_value: Any = side
        tif_value: Any = time_in_force.upper()
        try:
            from py_clob_client.order_builder import constants as obc  # type: ignore

            side_value = obc.BUY if side.lower() == "buy" else obc.SELL
        except Exception:
            pass

        try:
            # Preferred typed path
            from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore

            order_args = OrderArgs(
                price=price, size=size, side=side_value, token_id=outcome_token_id
            )
            signed = clob.create_order(order_args)
            tif_value = getattr(OrderType, time_in_force.upper(), OrderType.GTC)
            resp = clob.post_order(signed, tif_value)
            return resp  # type: ignore[return-value]
        except Exception:
            # Fallback to dict-based call signature for broader compatibility
            order_payload: Dict[str, Any] = {
                "price": price,
                "size": size,
                "side": side_value,
                "token_id": outcome_token_id,
                "time_in_force": tif_value,
            }
            resp = clob.post_order(order_payload)
            return resp  # type: ignore[return-value]


def place_order_simple(
    outcome_token_id: str,
    side: str,
    price: float,
    size: float,
    time_in_force: str = "GTC",
) -> Dict[str, Any]:
    """Convenience function for quick use without instantiating the class."""
    client = PolymarketTradingClient()
    return client.place_limit_order(
        outcome_token_id=outcome_token_id,
        side=side,
        price=price,
        size=size,
        time_in_force=time_in_force,
    )
