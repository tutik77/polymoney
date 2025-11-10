from __future__ import annotations

import argparse
import asyncio

from .logging_setup import configure_logging
from .services.sim_realtime import follow_user_realtime_sim
from .polymarket_client import PolymarketClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Realtime dry-run: follow users' trades and simulate at live quotes")
    parser.add_argument("--user", action="append", required=True, help="User address (0x...). Repeat for multiple users.")
    parser.add_argument("--sim-user", default="default", help="Simulation user id (namespace for DB portfolio)")
    parser.add_argument("--initial-cash", type=float, default=10_000_000.0, help="Initial cash for simulation")
    parser.add_argument("--interval", type=float, default=None, help="Poll interval seconds (optional)")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="Optional slippage in bps (default 0)")
    args = parser.parse_args()

    configure_logging()

    async def _amain() -> int:
        users = args.user or []

        async with PolymarketClient() as client:
            tasks = []
            for addr in users:
                tasks.append(
                    asyncio.create_task(
                        follow_user_realtime_sim(
                            user_address=addr,
                            display_name=None,
                            initial_cash=args.initial_cash,
                            poll_interval=args.interval,
                            slippage_bps=args.slippage_bps,
                            sim_user_id=args.sim_user,
                            client=client,
                        )
                    )
                )
            await asyncio.gather(*tasks)
        return 0

    asyncio.run(_amain())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


