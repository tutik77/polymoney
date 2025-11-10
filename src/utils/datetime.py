from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def parse_datetime_aware(value: Any) -> Optional[datetime]:
    """Best-effort parse of various datetime representations to an aware UTC datetime.

    Supports:
    - datetime (naive -> UTC assumed)
    - unix timestamp (int/float)
    - ISO strings, with optional trailing 'Z'
    - Date-only strings 'YYYY-MM-DD' (assumed UTC midnight)
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            if len(value) == 10 and value[4] == "-" and value[7] == "-":
                return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if value.endswith("Z"):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


__all__ = ["parse_datetime_aware"]


