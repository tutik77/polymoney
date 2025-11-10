from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict

from .logging_setup import configure_logging
from .services.sim import run_closed_positions_dry_run


async def _amain(top_n: int, initial_cash: float, refresh_ingest: bool) -> int:
    summary = await run_closed_positions_dry_run(top_n=top_n, initial_cash=initial_cash, refresh_ingest=refresh_ingest)
    # Print concise JSON so it can be parsed by tools if needed
    print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Dry-run simulator (closed positions) for top-N users")
    parser.add_argument("--top-n", type=int, default=10, help="Number of leaderboard users to copy (default: 10)")
    parser.add_argument(
        "--initial-cash",
        type=float,
        default=10_000_000.0,
        help="Initial cash balance for the simulation (default: 10,000,000)",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Do not refresh ingest; use existing DB contents",
    )
    args = parser.parse_args()

    configure_logging()
    return asyncio.run(_amain(top_n=args.top_n, initial_cash=args.initial_cash, refresh_ingest=not args.no_refresh))


if __name__ == "__main__":
    raise SystemExit(main())












