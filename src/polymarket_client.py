from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import random
import aiohttp
from aiolimiter import AsyncLimiter
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
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
        self._clob_api = settings.polymarket_clob_host.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=settings.request_timeout_seconds)
        self._log = structlog.get_logger()
        # Adaptive RPS
        self._current_rps: float = float(settings.requests_per_second)
        # Never allow limiter capacity < 1; clamp min RPS to 1.0
        self._min_rps: float = max(1.0, float(settings.min_requests_per_second))
        self._max_rps: float = max(
            self._current_rps, float(settings.max_requests_per_second)
        )
        self._adapt_up_successes: int = max(1, int(settings.adapt_up_successes))
        self._adapt_down_factor: float = max(0.1, float(settings.adapt_down_factor))
        self._adapt_up_factor: float = max(1.0, float(settings.adapt_up_factor))
        self._success_streak: int = 0
        # Build limiter with integer capacity, at least 1 token/period
        self._rebuild_limiter(self._current_rps)

    def _rebuild_limiter(self, new_rps: float) -> None:
        self._current_rps = max(self._min_rps, min(new_rps, self._max_rps))
        permits = max(1, int(round(self._current_rps)))
        self._limiter = AsyncLimiter(permits, 1)
        # self._log.info("rps_update", rps=self._current_rps)  # Too spammy

    def _adjust_rate(self, *, down: bool = False, up: bool = False) -> None:
        if down:
            target = max(self._min_rps, self._current_rps * self._adapt_down_factor)
            if target < self._current_rps - 1e-6:
                self._rebuild_limiter(target)
            self._success_streak = 0
        elif up:
            target = min(self._max_rps, self._current_rps * self._adapt_up_factor)
            if target > self._current_rps + 1e-6:
                self._rebuild_limiter(target)

    async def __aenter__(self) -> "PolymarketClient":  # noqa: D401
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._session.close()

    @retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(8))
    async def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        async with self._limiter:
            async with self._session.get(
                url, params=params, headers={"accept": "application/json"}
            ) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        wait_s = float(retry_after) if retry_after is not None else 3.0
                    except Exception:
                        wait_s = 3.0
                    wait_s = min(max(wait_s, 1.0), 60.0) + random.uniform(0, 0.5)
                    self._log.warning("rate_limited", url=str(resp.url), wait_s=wait_s)
                    self._adjust_rate(down=True)
                    await asyncio.sleep(wait_s)
                    resp.raise_for_status()
                resp.raise_for_status()
                data = await resp.json(loads=orjson.loads)
                self._success_streak += 1
                if self._success_streak >= self._adapt_up_successes:
                    self._adjust_rate(up=True)
                    self._success_streak = 0
                return data

    async def fetch_leaderboard_top(
        self,
        limit: int = 500,
        time_period: str = "month",
        order_by: str = "PNL",
        category: str = "overall",
    ) -> List[LeaderboardEntry]:
        entries: List[LeaderboardEntry] = []
        page_size = get_settings().leaderboard_page_size
        if page_size <= 0:
            page_size = 50
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
                    entries.append(
                        LeaderboardEntry(user_id=user_addr, display_name=name)
                    )
            offset += len(data)
        return entries

    async def fetch_user_closed_positions(
        self,
        user_id: str,
        page_size: Optional[int] = None,
        max_total: Optional[int] = None,
        *,
        sort_by: Optional[str] = None,
        sort_direction: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        offset = 0
        if page_size is None:
            page_size = get_settings().closed_positions_page_size
        # progress_step = 500
        # next_log_at = progress_step
        while True:
            if max_total is not None and len(results) >= max_total:
                break
            effective_limit = (
                page_size
                if max_total is None
                else max(1, min(page_size, max_total - len(results)))
            )
            params = {
                "user": user_id,
                "sortBy": (sort_by if sort_by is not None else "realizedpnl"),
                "sortDirection": (
                    sort_direction if sort_direction is not None else "DESC"
                ),
                "limit": effective_limit,
                "offset": offset,
            }
            url = f"{self._data_api}/closed-positions"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            results.extend(data)
            # Progress logging removed - too spammy, final count logged at end of fetch
            # while len(results) >= next_log_at:
            #     self._log.info("fetch_progress", user=user_id, count=len(results))
            #     next_log_at += progress_step
            if max_total is not None and len(results) >= max_total:
                break
            offset += len(data)
        return results

    async def fetch_user_active_positions(
        self,
        user_id: str,
        page_size: Optional[int] = None,
        max_total: Optional[int] = None,
        size_threshold: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        offset = 0
        if page_size is None:
            page_size = get_settings().active_positions_page_size
        while True:
            if max_total is not None and len(results) >= max_total:
                break
            effective_limit = (
                page_size
                if max_total is None
                else max(1, min(page_size, max_total - len(results)))
            )
            params = {
                "user": user_id,
                "sortBy": "CURRENT",
                "sortDirection": "DESC",
                "sizeThreshold": size_threshold if size_threshold is not None else ".1",
                "limit": effective_limit,
                "offset": offset,
            }
            url = f"{self._data_api}/positions"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            results.extend(data)
            if max_total is not None and len(results) >= max_total:
                break
            offset += len(data)
        return results

    async def fetch_user_activities(
        self,
        user_id: str,
        page_size: Optional[int] = None,
        max_total: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        offset = 0
        if page_size is None:
            page_size = get_settings().activities_page_size
        while True:
            if max_total is not None and len(results) >= max_total:
                break
            effective_limit = (
                page_size
                if max_total is None
                else max(1, min(page_size, max_total - len(results)))
            )
            params = {
                "user": user_id,
                "limit": effective_limit,
                "offset": offset,
            }
            url = f"{self._data_api}/activity"
            data = await self._get_json(url, params=params)
            if not isinstance(data, list) or not data:
                break
            results.extend(data)
            offset += len(data)
            # Progress logging removed - too spammy
            # if offset % (effective_limit * 5) == 0:
            #     self._log.info("fetch_progress", user=user_id, count=len(results))
        return results

    async def fetch_order_book(
        self, token_id: str
    ) -> Dict[str, List[Dict[str, float]]]:
        """Fetch order book (bids/asks) for a token."""
        url = f"{self._clob_api}/book"
        params = {"token_id": token_id}
        try:
            data = await self._get_json(url, params=params)
            # Expected format: {"bids": [...], "asks": [...]}
            if not isinstance(data, dict):
                return {"bids": [], "asks": []}
            return {
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
            }
        except Exception as e:
            real_e = e
            if isinstance(e, RetryError):
                real_e = e.last_attempt.exception()

            err_msg = str(real_e)
            status = None
            if isinstance(real_e, aiohttp.ClientResponseError):
                status = real_e.status
                err_msg = f"Http {status}: {real_e.message}"

            # 404 is expected for closed/removed tokens - log as debug
            log_level = self._log.debug if status == 404 else self._log.warning
            log_level(
                "orderbook_error",
                token=token_id[:20],
                error=err_msg,
                status=status,
            )
            return {"bids": [], "asks": []}

    async def fetch_best_quote_for_asset(
        self, token_id: str
    ) -> Dict[str, Optional[float]]:
        url = f"{self._clob_api}/midpoint"
        params = {"token_id": token_id}
        try:
            data = await self._get_json(url, params=params)
            if not isinstance(data, dict):
                return {"bid": None, "ask": None}
            bid = data.get("bid")
            ask = data.get("ask")
            mid = data.get("mid")
            if bid is None and ask is None and mid is not None:
                bid = ask = float(mid)
            return {
                "bid": float(bid) if bid is not None else None,
                "ask": float(ask) if ask is not None else None,
            }
        except Exception as e:
            real_e = e
            if isinstance(e, RetryError):
                real_e = e.last_attempt.exception()

            err_msg = str(real_e)
            status = None
            if isinstance(real_e, aiohttp.ClientResponseError):
                status = real_e.status
                err_msg = f"Http {status}: {real_e.message}"

            # 404 is expected for closed/removed tokens - log as debug
            # Other errors are unexpected - log as warning
            log_level = self._log.debug if status == 404 else self._log.warning
            log_level(
                "quote_error",
                token=token_id[:20],
                error=err_msg,
                status=status,
            )
            return {"bid": None, "ask": None}
