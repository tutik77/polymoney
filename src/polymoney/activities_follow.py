from __future__ import annotations

import argparse
import asyncio
from typing import Optional

from .config import get_settings
from .logging_setup import configure_logging
from .services.activities import follow_user_loop


async def ensure_schema() -> None:  # deprecated shim
    from .db import ensure_schema as _ensure
    await _ensure()


async def follow_user(user_address: str, display_name: Optional[str], poll_interval: Optional[float] = None, bootstrap: bool = False) -> None:
    configure_logging()
    settings = get_settings()
    interval = poll_interval if poll_interval is not None else settings.activities_poll_interval_seconds
    await ensure_schema()
    await follow_user_loop(user_address=user_address, display_name=display_name, poll_interval=interval, bootstrap=bootstrap)


def main() -> int:
    parser = argparse.ArgumentParser(description="Follow a user's activities and store new ones")
    parser.add_argument("--user", required=True, help="User address (0x...)")
    parser.add_argument("--name", default=None, help="Display name (optional)")
    parser.add_argument("--interval", type=float, default=None, help="Poll interval seconds (optional)")
    parser.add_argument("--bootstrap", action="store_true", help="Insert latest page on first run before realtime")
    args = parser.parse_args()

    asyncio.run(follow_user(args.user, args.name, args.interval, bootstrap=args.bootstrap))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


