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
Async mock TradeStation HTTP client for adapter tests — no network calls.

All public methods are ``async def`` matching the Phase 6 async client interface.
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from nautilus_tradestation.common.enums import TradeStationBarUnit
from nautilus_tradestation.http.client import TradeStationHttpClient


_RESOURCES = Path(__file__).parent / "resources"


class MockTradeStationHttpClient(TradeStationHttpClient):
    """
    Async mock HTTP client that returns stub data without network calls.

    Bypasses authentication and credential checks for use in unit tests.
    All public methods are coroutines matching the real client interface.
    """

    def __new__(cls):
        return object.__new__(cls)

    def __init__(self) -> None:
        self.client_id = "mock_client_id"
        self.client_secret = "mock_secret"
        self.refresh_token = "mock_refresh"
        self.auth_url = "https://mock.tradestation.com/oauth/token"
        self.base_url = "https://mock.tradestation.com/v3"
        self.access_token = "mock_access_token"
        self.token_expiry = datetime.max
        # No real httpx client — tests never touch the network
        self._httpx = None  # type: ignore[assignment]

    async def _ensure_authenticated(self) -> None:
        pass

    async def get_bars(
        self,
        symbol: str,
        interval: str,
        unit: TradeStationBarUnit,
        barsback: int | None = None,
        first_date: str | None = None,
        last_date: str | None = None,
    ) -> list[dict[str, Any]]:
        data = json.loads((_RESOURCES / "bars_response.json").read_text())
        return data.get("Bars", [])

    async def get_quotes(self, symbols: str) -> list[dict[str, Any]]:
        return [json.loads((_RESOURCES / "quote_response.json").read_text())]

    async def get_symbol_details(self, symbol: str) -> dict[str, Any]:
        if symbol in ("AAPL", "MSFT", "TSLA"):
            return json.loads((_RESOURCES / "symbol_detail_equity.json").read_text())
        # Option symbols contain a space (OCC format: "AAPL 250321C00175000")
        if " " in symbol:
            return json.loads((_RESOURCES / "symbol_detail_option.json").read_text())
        return json.loads((_RESOURCES / "symbol_detail_future.json").read_text())

    async def get_accounts(self) -> list[dict[str, Any]]:
        data = json.loads((_RESOURCES / "accounts_response.json").read_text())
        return data.get("Accounts", [])

    async def get_balances(self, account_keys: str) -> dict[str, Any]:
        return json.loads((_RESOURCES / "balances_response.json").read_text())

    async def get_positions(self, account_keys: str) -> list[dict[str, Any]]:
        return json.loads((_RESOURCES / "positions_response.json").read_text())

    async def get_orders(self, account_keys: str, since: str | None = None) -> list[dict[str, Any]]:
        return [
            json.loads((_RESOURCES / "order_market_filled.json").read_text()),
            json.loads((_RESOURCES / "order_limit_open.json").read_text()),
            json.loads((_RESOURCES / "order_stop_filled.json").read_text()),
            json.loads((_RESOURCES / "order_canceled.json").read_text()),
        ]

    async def place_order_group(
        self, group_type: str, orders: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return json.loads((_RESOURCES / "place_order_group_response.json").read_text())

    async def place_order(
        self, account_id, symbol, quantity, order_type, trade_action,
        time_in_force="DAY", limit_price=None, stop_price=None,
    ) -> dict[str, Any]:
        data = json.loads((_RESOURCES / "place_order_response.json").read_text())
        return data.get("Orders", [{}])[0]

    async def replace_order(self, order_id: str, *args, **kwargs) -> dict[str, Any]:
        return {"OrderID": order_id, "Status": "OPN"}

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return {"OrderID": order_id, "Status": "Cancelled"}

    async def search_symbols(
        self, search_text: str, category: str | None = None,
    ) -> list[dict[str, Any]]:
        return []

    async def close(self) -> None:
        pass
