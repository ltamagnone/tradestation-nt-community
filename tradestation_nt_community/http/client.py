"""
TradeStation async HTTP API client.

Uses ``httpx.AsyncClient`` for all API calls so callers can ``await`` them
directly without ``asyncio.to_thread`` wrappers.
"""

import asyncio
import logging
import os
import time
import uuid
from collections.abc import Callable
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from urllib.parse import urlparse

import httpx

_log = logging.getLogger(__name__)


class DuplicateOrderConfirmIdException(Exception):
    """
    Raised when TS acknowledges a duplicate OrderConfirmId but the original
    order can no longer be found in the /orders listing (e.g. already filled
    and aged out of the query window).

    Attributes
    ----------
    confirm_id : str
        The OrderConfirmId that was re-submitted.
    message : str
        The body-level error message returned by TradeStation.
    """

    def __init__(self, message: str, confirm_id: str) -> None:
        super().__init__(
            f"Duplicate OrderConfirmId '{confirm_id}' acknowledged by TS but "
            f"original order not found in /orders listing. "
            f"TS message: {message}"
        )
        self.confirm_id = confirm_id
        self.message = message


class OrderRejectedException(Exception):
    """
    Raised when TradeStation returns HTTP 200 but the response body contains
    a FAILED error that is NOT a duplicate-confirm dedup acknowledgement.

    This covers real rejections: invalid symbol, insufficient margin,
    quantity limits, session restrictions, etc.

    Attributes
    ----------
    ts_message : str
        The rejection message returned by TradeStation.
    """

    def __init__(self, ts_message: str) -> None:
        super().__init__(f"TradeStation order rejected: {ts_message}")
        self.ts_message = ts_message

from tradestation_nt_community.common.enums import TradeStationBarUnit

_ALLOWED_HOSTS = frozenset({
    "api.tradestation.com",
    "sim-api.tradestation.com",
})


class TradeStationHttpClient:
    """
    Async HTTP client for TradeStation REST APIs.

    All public methods are coroutines — call them with ``await``.

    Parameters
    ----------
    client_id : str, optional
        TradeStation API client ID (falls back to ``TRADESTATION_CLIENT_ID`` env var).
    client_secret : str, optional
        TradeStation API client secret (falls back to ``TRADESTATION_CLIENT_SECRET``).
    refresh_token : str, optional
        OAuth refresh token (falls back to ``TRADESTATION_REFRESH_TOKEN``).
    use_sandbox : bool, default False
        If ``True``, use the TradeStation sandbox/simulation API.
    base_url : str, optional
        Override the default API base URL. Must be HTTPS to a known
        TradeStation domain unless ``allow_custom_base_url`` is set.
    allow_custom_base_url : bool, default False
        If ``True``, skip hostname validation on ``base_url``. Use for
        local proxies or mock servers only.

    """

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        use_sandbox: bool = False,
        base_url: str | None = None,
        allow_custom_base_url: bool = False,
        max_retries: int = 3,
        retry_delay_initial_ms: int = 1000,
        retry_delay_max_ms: int = 60_000,
    ) -> None:
        self.client_id = client_id or os.getenv("TRADESTATION_CLIENT_ID")
        self._client_secret = client_secret or os.getenv("TRADESTATION_CLIENT_SECRET")
        self._refresh_token = refresh_token or os.getenv("TRADESTATION_REFRESH_TOKEN")

        if not all([self.client_id, self._client_secret, self._refresh_token]):
            raise ValueError(
                "TradeStation credentials required. "
                "Provide via parameters or set environment variables: "
                "TRADESTATION_CLIENT_ID, TRADESTATION_CLIENT_SECRET, TRADESTATION_REFRESH_TOKEN",
            )

        self.auth_url = "https://signin.tradestation.com/oauth/token"
        if base_url:
            if not allow_custom_base_url:
                parsed = urlparse(base_url)
                if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_HOSTS:
                    raise ValueError(
                        f"base_url must be HTTPS to a TradeStation domain "
                        f"({', '.join(sorted(_ALLOWED_HOSTS))}). "
                        f"Got: {base_url}  "
                        f"Set allow_custom_base_url=True to override.",
                    )
            self.base_url = base_url
        elif use_sandbox:
            self.base_url = "https://sim-api.tradestation.com/v3"
        else:
            self.base_url = "https://api.tradestation.com/v3"

        self._access_token: str | None = None
        self.token_expiry: datetime | None = None
        self._on_token_rotated: Callable[[str], None] | None = None
        self._auth_lock = asyncio.Lock()

        self._max_retries: int = max(1, max_retries)
        self._retry_delay_initial_s: float = retry_delay_initial_ms / 1000.0
        self._retry_delay_max_s: float = retry_delay_max_ms / 1000.0

        # Persistent async HTTP client — reuses TCP connections across requests.
        # Closed in close(); callers should not share instances across event loops.
        # pool_keepalive=1200 keeps connections warm for 20 min (matches token
        # refresh cycle), so orders never pay a TCP+TLS cold-start penalty.
        self._httpx = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(keepalive_expiry=1200),
        )

    @property
    def access_token(self) -> str | None:
        """Current OAuth access token (read-only)."""
        return self._access_token

    def set_token_rotation_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked whenever the OAuth refresh token rotates.

        The callback receives the new refresh token as its only argument. Use
        this to persist the rotated token to disk so process restarts succeed
        without manual credential renewal.
        """
        self._on_token_rotated = callback

    async def _ensure_authenticated(self) -> None:
        async with self._auth_lock:
            if not self._access_token or not self.token_expiry:
                await self._refresh_access_token()
                return
            if datetime.now(tz=timezone.utc) >= self.token_expiry - timedelta(minutes=5):
                await self._refresh_access_token()

    async def _refresh_access_token(self) -> None:
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self._client_secret,
            "refresh_token": self._refresh_token,
        }
        response = None
        for attempt in range(3):
            if attempt > 0:
                await asyncio.sleep(3.0)
                _log.info(f"Retrying token refresh (attempt {attempt + 1}/3)")
            response = await self._httpx.post(self.auth_url, data=data)
            if response.status_code == 200:
                token_data = response.json()
                self._access_token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 1200)
                self.token_expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in)
                if "refresh_token" in token_data:
                    self._refresh_token = token_data["refresh_token"]
                    _log.debug("OAuth refresh token rotated — updated in memory")
                    if self._on_token_rotated:
                        try:
                            self._on_token_rotated(self._refresh_token)
                        except Exception as cb_err:
                            _log.warning(f"Token rotation callback failed: {cb_err}")
                return
            if response.status_code < 500:
                break  # 4xx: credential problem — retrying won't help
            _log.warning(f"Auth server error (HTTP {response.status_code}) — retrying")
        _log.debug(f"Auth failed (HTTP {response.status_code}): {response.text[:500]}")
        raise Exception(f"TradeStation authentication failed: HTTP {response.status_code}")

    async def _get_headers(self) -> dict[str, str]:
        await self._ensure_authenticated()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Send an authenticated request, retrying on HTTP 429.

        Reads Retry-After on 429 and sleeps accordingly. Also proactively
        sleeps when X-RateLimit-Remaining drops below 5 to avoid hitting
        the wall. Retries up to self._max_retries times total.
        """
        httpx_fn = getattr(self._httpx, method.lower())
        resp: httpx.Response | None = None
        for attempt in range(self._max_retries):
            kwargs["headers"] = await self._get_headers()
            resp = await httpx_fn(url, **kwargs)
            if resp.status_code == 429:
                retry_after = float(
                    resp.headers.get("Retry-After", self._retry_delay_initial_s)
                )
                retry_after = min(retry_after, self._retry_delay_max_s)
                _log.warning(
                    f"Rate limited (429) on {url} — sleeping {retry_after:.1f}s "
                    f"(attempt {attempt + 1}/{self._max_retries})"
                )
                await asyncio.sleep(retry_after)
                continue
            remaining = resp.headers.get("X-RateLimit-Remaining")
            if remaining is not None and int(remaining) < 5:
                reset_epoch = int(resp.headers.get("X-RateLimit-Reset", 0))
                sleep_secs = max(0.1, reset_epoch - time.time()) + 0.1
                _log.warning(
                    f"Rate limit nearly exhausted ({remaining} remaining) — "
                    f"sleeping {sleep_secs:.1f}s"
                )
                await asyncio.sleep(sleep_secs)
            return resp
        return resp  # type: ignore[return-value]  # exhausted retries

    async def get_bars(
        self,
        symbol: str,
        interval: str,
        unit: TradeStationBarUnit,
        barsback: int | None = None,
        first_date: str | None = None,
        last_date: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Request historical bar data from TradeStation.

        Parameters
        ----------
        symbol : str
            The symbol to request bars for (e.g., 'GCG25' for Gold Feb 2025).
        interval : str
            The bar interval (e.g., '1', '5', '15', '60').
        unit : TradeStationBarUnit
            The unit type (Minute, Daily, Weekly, Monthly).
        barsback : int, optional
            Number of bars to retrieve (alternative to date range).
        first_date : str, optional
            Start date in format 'MM-DD-YYYY' or 'MM-DD-YYYY HH:MM'.
        last_date : str, optional
            End date in format 'MM-DD-YYYY' or 'MM-DD-YYYY HH:MM'.

        Return
        -------
        list[dict[str, Any]]
            List of bar data dictionaries.

        """
        url = f"{self.base_url}/marketdata/barcharts/{symbol}"
        params: dict[str, str] = {"interval": interval, "unit": unit.value}
        if barsback:
            params["barsback"] = str(barsback)
        else:
            if first_date:
                params["firstdate"] = first_date
            if last_date:
                params["lastdate"] = last_date

        response = await self._request("GET", url, params=params)
        if response.status_code != 200:
            _log.debug(f"Get bars failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"TradeStation get bars failed: HTTP {response.status_code}")
        return response.json().get("Bars", [])

    async def search_symbols(
        self,
        search_text: str,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search for symbols in TradeStation.

        Parameters
        ----------
        search_text : str
            The search query (partial symbol or description).
        category : str, optional
            Filter by category (STOCK, FUTURE, OPTION, etc.).

        Return
        -------
        list[dict[str, Any]]
            List of symbol search results.

        """
        url = f"{self.base_url}/marketdata/symbols/search/{search_text}"
        params = {}
        if category:
            params["category"] = category
        response = await self._request("GET", url, params=params)
        if response.status_code != 200:
            _log.debug(f"Symbol search failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Symbol search failed: HTTP {response.status_code}")
        return response.json()

    async def get_symbol_details(self, symbol: str) -> dict[str, Any]:
        """
        Get detailed information about a symbol.

        Parameters
        ----------
        symbol : str
            The symbol to get details for.

        Return
        -------
        dict[str, Any]
            Symbol details including contract specifications.

        """
        url = f"{self.base_url}/marketdata/symbols/{symbol}"
        response = await self._request("GET", url)
        if response.status_code != 200:
            _log.debug(f"Get symbol details failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get symbol details failed: HTTP {response.status_code}")
        data = response.json()
        if isinstance(data, dict) and "Symbols" in data:
            symbols = data.get("Symbols", [])
            return symbols[0] if symbols else {}
        return data[0] if isinstance(data, list) and data else data

    # =========================================================================
    # Account & Order Execution Methods
    # =========================================================================

    async def get_accounts(self) -> list[dict[str, Any]]:
        """
        Get user accounts from TradeStation.

        Return
        -------
        list[dict[str, Any]]
            List of account information dictionaries.

        """
        url = f"{self.base_url}/brokerage/accounts"
        response = await self._request("GET", url)
        if response.status_code != 200:
            _log.debug(f"Get accounts failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get accounts failed: HTTP {response.status_code}")
        data = response.json()
        return data.get("Accounts", []) if isinstance(data, dict) else data

    async def get_balances(self, account_keys: str) -> dict[str, Any]:
        """
        Get account balances from TradeStation.

        Parameters
        ----------
        account_keys : str
            Account key(s) — single or comma-separated.

        Return
        -------
        dict[str, Any]
            Account balance information.

        """
        url = f"{self.base_url}/brokerage/accounts/{account_keys}/balances"
        response = await self._request("GET", url)
        if response.status_code != 200:
            _log.debug(f"Get balances failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get balances failed: HTTP {response.status_code}")
        data = response.json()
        if isinstance(data, dict) and "Balances" in data:
            balances = data.get("Balances", [])
            return balances[0] if balances else {}
        return data

    async def get_positions(self, account_keys: str) -> list[dict[str, Any]]:
        """
        Get current positions from TradeStation.

        Parameters
        ----------
        account_keys : str
            Account key(s) — single or comma-separated.

        Return
        -------
        list[dict[str, Any]]
            List of position dictionaries.

        """
        url = f"{self.base_url}/brokerage/accounts/{account_keys}/positions"
        response = await self._request("GET", url)
        if response.status_code != 200:
            _log.debug(f"Get positions failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get positions failed: HTTP {response.status_code}")
        data = response.json()
        return data.get("Positions", []) if isinstance(data, dict) else data

    def _generate_order_confirm_id(self) -> str:
        """Generate a unique 22-character order confirm ID (TS max length)."""
        return uuid.uuid4().hex[:22]

    def _is_duplicate_confirm_error(self, err: str | None, msg: str) -> bool:
        """Return True if the body-level error is a TS dedup acknowledgement."""
        return (
            err == "FAILED"
            and "unique" in msg
            and ("orderconfirmid" in msg or "orderconfirm" in msg)
        )

    _CONFIRM_ID_KEYS = ("OrderConfirmId", "OrderConfirmID", "orderConfirmId")

    async def _resolve_duplicates(
        self,
        account_id: str,
        confirm_ids: list[str],
        ts_message: str,
    ) -> dict[str, Any]:
        """
        Resolve one or more duplicate-acknowledged OrderConfirmIds to their
        real OrderIDs via GET /orders.

        Used by both ``place_order`` (single confirm_id, N=1) and
        ``place_order_group`` (multiple legs, N>1). For groups, all legs must
        be found or the whole response is treated as unrecoverable.

        Parameters
        ----------
        account_id : str
            Account to search.
        confirm_ids : list[str]
            One entry per leg; order matters (matched legs are returned in
            the same order).
        ts_message : str
            The rejection message from TS (logged on lookup failure).

        Return
        -------
        dict[str, Any]
            Success-shaped response. For single: ``{"Orders": [...]}``. For
            groups: ``{"OrderGroupId": ..., "Orders": [...]}`` — OrderGroupId
            is sourced from the first matched order's ``OrderGroupId`` field
            if present, otherwise omitted.

        Raises
        ------
        DuplicateOrderConfirmIdException
            If ANY leg's confirm_id is not found in /orders (partial recovery
            is not safe — caller can't tell which legs landed and which didn't).
        """
        _log.info(
            f"[DEDUP] {len(confirm_ids)} duplicate OrderConfirmId(s) acknowledged "
            f"by TS — resolving via /orders"
        )
        orders = await self.get_orders(account_id)
        # Build confirm_id → order index for O(1) lookup
        by_confirm_id: dict[str, dict[str, Any]] = {}
        for order in orders:
            for key in self._CONFIRM_ID_KEYS:
                cid = order.get(key)
                if cid:
                    by_confirm_id[cid] = order
                    break

        resolved: list[dict[str, Any]] = []
        unresolved: list[str] = []
        group_id: str | None = None
        for cid in confirm_ids:
            order = by_confirm_id.get(cid)
            if order is None:
                unresolved.append(cid)
                continue
            real_id = order.get("OrderID", order.get("Id", ""))
            if group_id is None:
                group_id = order.get("OrderGroupId") or order.get("OrderGroupID")
            resolved.append({
                "OrderID": real_id,
                "Message": f"Dedup acknowledged: existing order {real_id}",
            })

        if unresolved:
            _log.warning(
                f"[DEDUP] {len(unresolved)}/{len(confirm_ids)} confirm_id(s) not "
                f"found in /orders: {unresolved}. TS message: {ts_message}"
            )
            raise DuplicateOrderConfirmIdException(
                message=ts_message,
                confirm_id=", ".join(unresolved),
            )

        response: dict[str, Any] = {"Orders": resolved}
        if group_id is not None:
            response["OrderGroupId"] = group_id
        _log.info(
            f"[DEDUP] Resolved {len(resolved)} order(s)"
            + (f" in group {group_id}" if group_id else "")
        )
        return response

    def _check_order_body_error(
        self,
        response_json: dict[str, Any],
    ) -> tuple[bool, bool, str, str]:
        """
        Parse the response body for a body-level error.

        Returns
        -------
        tuple of (has_error, is_duplicate, err, msg)
            has_error : bool — True if ``Error`` field is present and truthy.
            is_duplicate : bool — True if this is a dedup acknowledgement.
            err : str — raw ``Error`` value (empty string if absent).
            msg : str — lowercased ``Message`` value (empty string if absent).
        """
        orders_in_response = response_json.get("Orders") or []
        first = (orders_in_response[0] if orders_in_response else {}) or {}
        err = first.get("Error") or ""
        msg = (first.get("Message") or "").lower()
        has_error = bool(err)
        is_duplicate = self._is_duplicate_confirm_error(err if err else None, msg)
        return has_error, is_duplicate, err, msg

    async def place_order(
        self,
        account_id: str,
        symbol: str,
        quantity: str,
        order_type: str,
        trade_action: str,
        time_in_force: str = "DAY",
        limit_price: str | None = None,
        stop_price: str | None = None,
        order_confirm_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Place an order with TradeStation.

        An ``OrderConfirmId`` is automatically injected into every submission
        to guarantee idempotency: if a transient 5xx causes ``_request()`` to
        retry, TS will reject the duplicate with a body-level error that this
        method detects and resolves to the real OrderID, so the caller always
        receives exactly one order.

        Parameters
        ----------
        account_id : str
            The account ID to place the order for.
        symbol : str
            The symbol to trade (e.g., 'ESH25').
        quantity : str
            Order quantity.
        order_type : str
            Order type: 'Market', 'Limit', 'StopMarket', 'StopLimit'.
        trade_action : str
            'Buy', 'Sell', 'BuyToCover', 'SellShort'.
        time_in_force : str, default 'DAY'
            Time in force: 'DAY', 'GTC', 'GTD', etc.
        limit_price : str, optional
            Limit price for Limit or StopLimit orders.
        stop_price : str, optional
            Stop price for StopMarket or StopLimit orders.
        order_confirm_id : str, optional
            Caller-supplied idempotency key (≤ 22 chars). If omitted, one is
            generated automatically via ``uuid4().hex[:22]``.

        Return
        -------
        dict[str, Any]
            Order confirmation response. On a dedup acknowledgement the
            response is synthesised from a /orders lookup and shaped like a
            normal success (``{"Orders": [{"OrderID": ..., "Message": ...}]}``).

        Raises
        ------
        OrderRejectedException
            If TS returns HTTP 200 with a FAILED error that is not a dedup
            acknowledgement (e.g. invalid symbol, insufficient margin).
        DuplicateOrderConfirmIdException
            If TS acknowledges a duplicate but the original order can no
            longer be found in the /orders listing.
        Exception
            If TS returns a non-200/201 HTTP status.
        """
        confirm_id = order_confirm_id or self._generate_order_confirm_id()

        url = f"{self.base_url}/orderexecution/orders"
        order_data: dict[str, Any] = {
            "AccountID": account_id,
            "Symbol": symbol,
            "Quantity": quantity,
            "OrderType": order_type,
            "TradeAction": trade_action,
            "TimeInForce": {"Duration": time_in_force},
            "OrderConfirmId": confirm_id,
        }
        if order_type in ("Limit", "StopLimit") and limit_price:
            order_data["LimitPrice"] = limit_price
        if order_type in ("StopMarket", "StopLimit") and stop_price:
            order_data["StopPrice"] = stop_price

        response = await self._request("POST", url, json=order_data)
        if response.status_code not in (200, 201):
            _log.debug(f"Place order failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Place order failed (HTTP {response.status_code}): {response.text[:200]}")

        response_json = response.json()
        has_error, is_duplicate, err, msg = self._check_order_body_error(response_json)

        if not has_error:
            # (a) Normal success — return as-is
            return response_json

        if is_duplicate:
            # (b) Dedup acknowledgement — resolve real OrderID from /orders
            return await self._resolve_duplicates(account_id, [confirm_id], msg)

        # (c) Real rejection — raise so the caller sees it
        orders_in_response = response_json.get("Orders") or []
        first = (orders_in_response[0] if orders_in_response else {}) or {}
        raw_msg = first.get("Message") or err
        _log.debug(f"Place order rejected by TS (body-level): {raw_msg}")
        raise OrderRejectedException(raw_msg)

    async def replace_order(
        self,
        order_id: str,
        account_id: str,
        symbol: str,
        quantity: str,
        order_type: str,
        trade_action: str,
        time_in_force: str = "DAY",
        limit_price: str | None = None,
        stop_price: str | None = None,
    ) -> dict[str, Any]:
        """
        Replace (modify) an existing open order.

        TradeStation replaces the order atomically — the original order is
        updated in-place with no cancel/resubmit race window.

        Parameters
        ----------
        order_id : str
            The TradeStation order ID to replace.
        account_id, symbol, quantity, order_type, trade_action, time_in_force,
        limit_price, stop_price : same semantics as place_order.

        Return
        -------
        dict[str, Any]
            Replacement confirmation response.

        """
        url = f"{self.base_url}/orderexecution/orders/{order_id}"
        order_data: dict[str, Any] = {
            "AccountID": account_id,
            "Symbol": symbol,
            "Quantity": quantity,
            "OrderType": order_type,
            "TradeAction": trade_action,
            "TimeInForce": {"Duration": time_in_force},
        }
        if order_type in ("Limit", "StopLimit") and limit_price:
            order_data["LimitPrice"] = limit_price
        if order_type in ("StopMarket", "StopLimit") and stop_price:
            order_data["StopPrice"] = stop_price

        response = await self._request("PUT", url, json=order_data)
        if response.status_code not in (200, 201):
            _log.debug(f"Replace order failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Replace order failed (HTTP {response.status_code}): {response.text[:200]}")
        return response.json()

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        """
        Cancel an open order.

        Parameters
        ----------
        order_id : str
            The order ID to cancel.

        Return
        -------
        dict[str, Any]
            Cancellation confirmation response.

        """
        url = f"{self.base_url}/orderexecution/orders/{order_id}"
        response = await self._request("DELETE", url)
        if response.status_code not in (200, 204):
            _log.debug(f"Cancel order failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Cancel order failed (HTTP {response.status_code}): {response.text[:200]}")
        try:
            return response.json()
        except Exception:
            return {"OrderID": order_id, "Status": "Cancelled"}

    async def get_orders(
        self,
        account_keys: str,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get orders for account(s).

        Parameters
        ----------
        account_keys : str
            Account key(s) — single or comma-separated.
        since : str, optional
            Return orders since this date (format: 'MM-DD-YYYY').

        Return
        -------
        list[dict[str, Any]]
            List of order dictionaries.

        """
        url = f"{self.base_url}/brokerage/accounts/{account_keys}/orders"
        params: dict[str, str] = {}
        if since:
            params["since"] = since
        response = await self._request("GET", url, params=params)
        if response.status_code != 200:
            _log.debug(f"Get orders failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get orders failed: HTTP {response.status_code}")
        data = response.json()
        return data.get("Orders", []) if isinstance(data, dict) else data

    async def place_order_group(
        self,
        group_type: str,
        orders: list[dict[str, Any]],
        order_confirm_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Submit a group of orders (OCO or bracket) atomically.

        TradeStation group orders are submitted to
        ``POST /v3/orderexecution/ordergroups`` and link orders so that
        fills or cancellations on one leg affect the others.

        An ``OrderConfirmId`` is injected into each leg to guarantee
        idempotency across retries (same protocol as ``place_order``).

        Parameters
        ----------
        group_type : str
            ``"OCO"`` (Order Cancels Order) or ``"BRK"`` (Bracket/OSO).
        orders : list[dict[str, Any]]
            List of individual order dicts, each in the same format as
            ``place_order`` — i.e. with ``AccountID``, ``Symbol``,
            ``Quantity``, ``OrderType``, ``TradeAction``, ``TimeInForce``
            (and optional ``LimitPrice`` / ``StopPrice``). Each leg must
            contain ``AccountID`` so that dedup resolution can query /orders.
        order_confirm_ids : list[str], optional
            Caller-supplied idempotency keys, one per leg (each ≤ 22 chars).
            If omitted (or shorter than ``orders``), missing entries are
            generated automatically.

        Return
        -------
        dict[str, Any]
            Group order confirmation response containing ``OrderGroupId``
            and a ``Orders`` list with individual ``OrderID`` values.
            On a dedup acknowledgement the response is synthesised from a
            /orders lookup for the first leg's ``AccountID``.

        Raises
        ------
        OrderRejectedException
            If TS returns HTTP 200 with a FAILED error that is not a dedup
            acknowledgement.
        DuplicateOrderConfirmIdException
            If TS acknowledges a duplicate but the original order can no
            longer be found in the /orders listing.
        Exception
            If TS returns a non-200/201 HTTP status.
        """
        # Inject OrderConfirmId into each leg
        confirm_ids: list[str] = list(order_confirm_ids or [])
        stamped_orders: list[dict[str, Any]] = []
        for i, leg in enumerate(orders):
            if i < len(confirm_ids):
                cid = confirm_ids[i]
            else:
                cid = self._generate_order_confirm_id()
                confirm_ids.append(cid)
            stamped_orders.append({**leg, "OrderConfirmId": cid})

        url = f"{self.base_url}/orderexecution/ordergroups"
        payload = {"Type": group_type, "Orders": stamped_orders}
        response = await self._request("POST", url, json=payload)
        if response.status_code not in (200, 201):
            _log.debug(f"Place order group failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Place order group failed (HTTP {response.status_code}): {response.text[:200]}")

        response_json = response.json()

        has_error, is_duplicate, err, msg = self._check_order_body_error(response_json)

        if not has_error:
            # (a) Normal success — return as-is
            return response_json

        if is_duplicate:
            # (b) Dedup acknowledgement — resolve ALL legs from /orders so the
            # caller sees every real OrderID, not just the first leg. Uses the
            # first leg's AccountID since all legs in a group share one account.
            account_id = stamped_orders[0].get("AccountID", "") if stamped_orders else ""
            if not account_id:
                raise OrderRejectedException(
                    "Group dedup response received but legs carry no AccountID — "
                    "cannot resolve via /orders. TS message: " + msg
                )
            return await self._resolve_duplicates(account_id, confirm_ids, msg)

        # (c) Real rejection — raise so the caller sees it
        orders_in_response = response_json.get("Orders") or []
        first = (orders_in_response[0] if orders_in_response else {}) or {}
        raw_msg = first.get("Message") or err
        _log.debug(f"Place order group rejected by TS (body-level): {raw_msg}")
        raise OrderRejectedException(raw_msg)

    async def get_quotes(self, symbols: str) -> list[dict[str, Any]]:
        """
        Get real-time quotes for one or more symbols.

        Parameters
        ----------
        symbols : str
            Symbol or comma-separated list (e.g. 'GCJ26' or 'GCJ26,ESM26').

        Return
        -------
        list[dict[str, Any]]
            List of quote dicts with Bid, Ask, Last, BidSize, AskSize,
            LastSize, Volume, Symbol, and TimeStamp.

        """
        url = f"{self.base_url}/marketdata/quotes/{symbols}"
        response = await self._request("GET", url)
        if response.status_code != 200:
            _log.debug(f"Get quotes failed (HTTP {response.status_code}): {response.text[:500]}")
            raise Exception(f"Get quotes failed: HTTP {response.status_code}")
        data = response.json()
        return data.get("Quotes", []) if isinstance(data, dict) else data

    async def close(self) -> None:
        """Close the underlying HTTP client and clear credentials from memory."""
        self._access_token = None
        self._client_secret = None
        self._refresh_token = None
        self.token_expiry = None
        await self._httpx.aclose()
