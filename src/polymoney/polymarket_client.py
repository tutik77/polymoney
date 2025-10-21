from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import aiohttp
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential
import orjson

from .config import get_settings


@dataclass
class LeaderboardEntry:
    user_id: str
    display_name: Optional[str]


class PolymarketClient:
    def __init__(self) -> None:
        settings = get_settings()
        self._base_url = settings.polymarket_base_url.rstrip("/")
        self._data_api = "https://data-api.polymarket.com"
        self._timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)
        self._limiter = AsyncLimiter(settings.requests_per_second, 1)

    async def __aenter__(self) -> "PolymarketClient":  # noqa: D401
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._session.close()

    @retry(wait=wait_exponential(min=0.5, max=8), stop=stop_after_attempt(5))
    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        async with self._limiter:
            async with self._session.get(url, params=params, headers={"accept": "application/json"}) as resp:
                resp.raise_for_status()
                return await resp.json(loads=orjson.loads)

    async def fetch_leaderboard_top(
        self,
        limit: int = 500,
        time_period: str = "month",
        order_by: str = "PNL",
        category: str = "overall",
    ) -> List[LeaderboardEntry]:
        entries: List[LeaderboardEntry] = []
        page_size = 100
        offset = 0
        while len(entries) < limit:
            params = {
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": min(page_size, limit - len(entries)),
                "offset": offset,
                "category": category,
            }
            url = f"{self._data_api}/v1/leaderboard"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            for item in data:
                user_addr = item.get("proxyWallet") or item.get("user")
                name = item.get("userName") or item.get("name")
                if user_addr:
                    entries.append(LeaderboardEntry(user_id=user_addr, display_name=name))
            if len(data) < params["limit"]:
                break
            offset += params["limit"]
        return entries

    async def fetch_user_closed_positions(self, user_id: str, page_size: int = 25, max_total: Optional[int] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        offset = 0
        while True:
            if max_total is not None and len(results) >= max_total:
                break
            effective_limit = page_size if max_total is None else max(1, min(page_size, max_total - len(results)))
            params = {
                "user": user_id,
                "sortBy": "realizedpnl",
                "sortDirection": "DESC",
                "limit": effective_limit,
                "offset": offset,
            }
            url = f"{self._data_api}/closed-positions"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            results.extend(data)
            if len(data) < effective_limit:
                break
            offset += effective_limit
        return results

    async def fetch_user_active_positions(self, user_id: str, page_size: int = 50, max_total: Optional[int] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        offset = 0
        while True:
            if max_total is not None and len(results) >= max_total:
                break
            effective_limit = page_size if max_total is None else max(1, min(page_size, max_total - len(results)))
            params = {
                "user": user_id,
                "sortBy": "CURRENT",
                "sortDirection": "DESC",
                "sizeThreshold": ".1",
                "limit": effective_limit,
                "offset": offset,
            }
            url = f"{self._data_api}/positions"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            results.extend(data)
            if len(data) < effective_limit:
                break
            offset += effective_limit
        return results



