from __future__ import annotations

import argparse
import json
import sys

from .trading import place_order_simple


def main() -> int:
    parser = argparse.ArgumentParser(description="Place a Polymarket limit order")
    parser.add_argument("id", help="Outcome token id (tokenId/asset)")
    parser.add_argument("side", choices=["buy", "sell"], help="Order side")
    parser.add_argument("price", type=float, help="Limit price in (0,1)")
    parser.add_argument("size", type=float, help="Number of shares")
    parser.add_argument("--tif", default="GTC", choices=["GTC", "IOC", "FOK"], help="Time in force")
    args = parser.parse_args()

    try:
        resp = place_order_simple(outcome_token_id=args.id, side=args.side, price=args.price, size=args.size, time_in_force=args.tif)
        print(json.dumps(resp, ensure_ascii=False, indent=2))
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())




