# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
TradeStation async HTTP API client.

Uses ``httpx.AsyncClient`` for all API calls so callers can ``await`` them
directly without ``asyncio.to_thread`` wrappers.
"""

import os
from datetime import datetime
from datetime import timedelta
from typing import Any

import httpx

from nautilus_tradestation.common.enums import TradeStationBarUnit


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
        Override the default API base URL.

    """

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        use_sandbox: bool = False,
        base_url: str | None = None,
    ) -> None:
        self.client_id = client_id or os.getenv("TRADESTATION_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("TRADESTATION_CLIENT_SECRET")
        self.refresh_token = refresh_token or os.getenv("TRADESTATION_REFRESH_TOKEN")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError(
                "TradeStation credentials required. "
                "Provide via parameters or set environment variables: "
                "TRADESTATION_CLIENT_ID, TRADESTATION_CLIENT_SECRET, TRADESTATION_REFRESH_TOKEN",
            )

        self.auth_url = "https://signin.tradestation.com/oauth/token"
        if base_url:
            self.base_url = base_url
        elif use_sandbox:
            self.base_url = "https://sim-api.tradestation.com/v3"
        else:
            self.base_url = "https://api.tradestation.com/v3"

        self.access_token: str | None = None
        self.token_expiry: datetime | None = None

        # Persistent async HTTP client — reuses TCP connections across requests.
        # Closed in close(); callers should not share instances across event loops.
        self._httpx = httpx.AsyncClient(timeout=30.0)

    async def _ensure_authenticated(self) -> None:
        if not self.access_token or not self.token_expiry:
            await self._refresh_access_token()
            return
        if datetime.utcnow() >= self.token_expiry - timedelta(minutes=5):
            await self._refresh_access_token()

    async def _refresh_access_token(self) -> None:
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        response = await self._httpx.post(self.auth_url, data=data)
        if response.status_code != 200:
            raise Exception(f"TradeStation authentication failed: {response.text}")
        token_data = response.json()
        self.access_token = token_data["access_token"]
        expires_in = token_data.get("expires_in", 1200)
        self.token_expiry = datetime.utcnow() + timedelta(seconds=expires_in)

    async def _get_headers(self) -> dict[str, str]:
        await self._ensure_authenticated()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

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

        response = await self._httpx.get(url, headers=await self._get_headers(), params=params)
        if response.status_code != 200:
            raise Exception(f"TradeStation API request failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers(), params=params)
        if response.status_code != 200:
            raise Exception(f"Symbol search failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers())
        if response.status_code != 200:
            raise Exception(f"Get symbol details failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers())
        if response.status_code != 200:
            raise Exception(f"Get accounts failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers())
        if response.status_code != 200:
            raise Exception(f"Get balances failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers())
        if response.status_code != 200:
            raise Exception(f"Get positions failed: {response.text}")
        data = response.json()
        return data.get("Positions", []) if isinstance(data, dict) else data

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
    ) -> dict[str, Any]:
        """
        Place an order with TradeStation.

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

        Return
        -------
        dict[str, Any]
            Order confirmation response.

        """
        url = f"{self.base_url}/orderexecution/orders"
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

        response = await self._httpx.post(
            url, headers=await self._get_headers(), json=order_data
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Place order failed: {response.text}")
        return response.json()

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

        response = await self._httpx.put(
            url, headers=await self._get_headers(), json=order_data
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Replace order failed: {response.text}")
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
        response = await self._httpx.delete(url, headers=await self._get_headers())
        if response.status_code not in (200, 204):
            raise Exception(f"Cancel order failed: {response.text}")
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
        response = await self._httpx.get(url, headers=await self._get_headers(), params=params)
        if response.status_code != 200:
            raise Exception(f"Get orders failed: {response.text}")
        data = response.json()
        return data.get("Orders", []) if isinstance(data, dict) else data

    async def place_order_group(
        self,
        group_type: str,
        orders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Submit a group of orders (OCO or bracket) atomically.

        TradeStation group orders are submitted to
        ``POST /v3/orderexecution/ordergroups`` and link orders so that
        fills or cancellations on one leg affect the others.

        Parameters
        ----------
        group_type : str
            ``"OCO"`` (Order Cancels Order) or ``"BRK"`` (Bracket/OSO).
        orders : list[dict[str, Any]]
            List of individual order dicts, each in the same format as
            ``place_order`` — i.e. with ``AccountID``, ``Symbol``,
            ``Quantity``, ``OrderType``, ``TradeAction``, ``TimeInForce``
            (and optional ``LimitPrice`` / ``StopPrice``).

        Return
        -------
        dict[str, Any]
            Group order confirmation response containing ``OrderGroupId``
            and a ``Orders`` list with individual ``OrderID`` values.

        """
        url = f"{self.base_url}/orderexecution/ordergroups"
        payload = {"Type": group_type, "Orders": orders}
        response = await self._httpx.post(
            url, headers=await self._get_headers(), json=payload
        )
        if response.status_code not in (200, 201):
            raise Exception(f"Place order group failed: {response.text}")
        return response.json()

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
        response = await self._httpx.get(url, headers=await self._get_headers())
        if response.status_code != 200:
            raise Exception(f"Get quotes failed: {response.text}")
        data = response.json()
        return data.get("Quotes", []) if isinstance(data, dict) else data

    async def close(self) -> None:
        """Close the underlying HTTP client and release connections."""
        await self._httpx.aclose()
