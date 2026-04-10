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
Parsing functions for TradeStation instrument definitions.
"""
import logging

import pandas as pd

from nautilus_trader.model.enums import AssetClass, OptionKind
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import Equity, FuturesContract, OptionContract
from nautilus_trader.model.objects import Currency, Price, Quantity


_log = logging.getLogger(__name__)


def parse_instrument(
    symbol: str,
    data: dict,
    venue: Venue,
) -> Equity | FuturesContract | OptionContract | None:
    """Parse a TradeStation symbol-details dict into a NautilusTrader instrument.

    Parameters
    ----------
    symbol : str
        The symbol string (e.g. 'GCJ26', 'AAPL', 'AAPL 250321C00175000').
    data : dict
        Raw symbol details from the TradeStation API.
    venue : Venue
        The venue to assign (typically TRADESTATION_VENUE).

    Returns
    -------
    Equity | FuturesContract | OptionContract | None
        Parsed instrument, or None if the asset type is unsupported or parsing fails.
    """
    try:
        asset_type = data.get("AssetType", data.get("Category", "")).upper()
        instrument_id = InstrumentId(symbol=Symbol(symbol), venue=venue)

        price_format = data.get("PriceFormat", {})
        qty_format = data.get("QuantityFormat", {})

        price_increment = Price.from_str(price_format.get("Increment", "0.01"))
        # price_precision must match price_increment.precision (NautilusTrader enforces this)
        price_precision = price_increment.precision
        lot_size = Quantity.from_str(qty_format.get("MinimumTradeQuantity", "1"))

        ts_event = pd.Timestamp.utcnow().value
        ts_init = ts_event

        if asset_type == "STOCK":
            return Equity(
                instrument_id=instrument_id,
                raw_symbol=Symbol(data.get("Symbol", symbol)),
                currency=Currency.from_str(data.get("Currency", "USD")),
                price_precision=price_precision,
                price_increment=price_increment,
                lot_size=lot_size,
                isin=data.get("ISIN"),
                ts_event=ts_event,
                ts_init=ts_init,
            )

        elif asset_type == "FUTURE":
            point_value = float(price_format.get("PointValue", 100))

            expiry_str = data.get("ExpirationDate", "") or data.get("ContractExpireDate", "")
            if expiry_str:
                expiration_ns = pd.Timestamp(expiry_str, tz="UTC").value
            else:
                expiration_ns = (pd.Timestamp.utcnow() + pd.DateOffset(years=1)).value

            activation_ns = ts_event
            underlying = data.get("Root", symbol[:2])

            return FuturesContract(
                instrument_id=instrument_id,
                raw_symbol=Symbol(data.get("Symbol", symbol)),
                asset_class=AssetClass.COMMODITY,
                currency=Currency.from_str(data.get("Currency", "USD")),
                price_precision=price_precision,
                price_increment=price_increment,
                multiplier=Quantity.from_str(str(point_value)),
                lot_size=lot_size,
                underlying=underlying,
                activation_ns=activation_ns,
                expiration_ns=expiration_ns,
                ts_event=ts_event,
                ts_init=ts_init,
            )

        elif asset_type == "OPTION":
            return _parse_option(symbol, data, instrument_id, price_increment,
                                 price_precision, lot_size, ts_event)

        else:
            _log.warning(f"Unsupported asset type '{asset_type}' for symbol {symbol}")
            return None

    except Exception as e:
        _log.error(f"Error parsing instrument {symbol}: {e}")
        return None


def _parse_option_kind(option_type_str: str) -> OptionKind:
    """Parse TradeStation OptionType string to NautilusTrader OptionKind.

    TradeStation returns ``"Call"`` or ``"Put"`` (also ``"C"`` / ``"P"``
    when embedded in the symbol string).  Default is CALL for unknown values.
    """
    t = option_type_str.strip().upper()
    if t in ("PUT", "P"):
        return OptionKind.PUT
    return OptionKind.CALL


def _parse_option(
    symbol: str,
    data: dict,
    instrument_id: InstrumentId,
    price_increment: Price,
    price_precision: int,
    lot_size: Quantity,
    ts_event: int,
) -> OptionContract | None:
    """Parse an OPTION asset type from a TradeStation symbol-details dict.

    TradeStation option symbols follow the OCC format:
    ``<underlying> <YYMMDD><C|P><8-digit-strike>``
    e.g. ``"AAPL 250321C00175000"``  → AAPL, 2025-03-21, Call, $175.00

    The API also provides explicit ``OptionType``, ``StrikePrice``, and
    ``ExpirationDate`` fields which are preferred over symbol parsing.
    """
    try:
        price_format = data.get("PriceFormat", {})
        multiplier = float(price_format.get("PointValue", 100))

        # --- Expiration ---
        expiry_str = data.get("ExpirationDate", "")
        if expiry_str:
            expiration_ns = pd.Timestamp(expiry_str, tz="UTC").value
        else:
            # Fall back to parsing from OCC symbol (chars 5–10: YYMMDD)
            raw_sym = data.get("Symbol", symbol)
            # OCC format: "AAPL 250321C00175000" — date starts at char after space+underlying
            try:
                parts = raw_sym.split()
                date_part = parts[1][:6]  # e.g. "250321"
                expiration_ns = pd.Timestamp(
                    f"20{date_part[:2]}-{date_part[2:4]}-{date_part[4:6]}", tz="UTC"
                ).value
            except Exception:
                expiration_ns = (pd.Timestamp.utcnow() + pd.DateOffset(months=1)).value

        # --- Strike price ---
        strike_str = data.get("StrikePrice", "")
        if strike_str:
            strike_price = Price.from_str(str(float(strike_str)))
        else:
            # Parse from OCC symbol: last 8 digits ÷ 1000 = strike
            try:
                raw_sym = data.get("Symbol", symbol)
                occ_right = raw_sym.split()[1]  # e.g. "250321C00175000"
                strike_raw = int(occ_right[7:])  # last 8 digits
                strike_val = strike_raw / 1000.0
                strike_price = Price(round(strike_val, price_precision), price_precision)
            except Exception:
                strike_price = Price(0.0, price_precision)

        # --- Option kind (Call / Put) ---
        option_type_str = data.get("OptionType", "")
        if not option_type_str:
            # Parse from OCC symbol char at position 6: C or P
            try:
                raw_sym = data.get("Symbol", symbol)
                occ_right = raw_sym.split()[1]
                option_type_str = occ_right[6]  # "C" or "P"
            except Exception:
                option_type_str = "C"
        option_kind = _parse_option_kind(option_type_str)

        # --- Underlying ---
        underlying = data.get("Underlying", data.get("Symbol", symbol).split()[0])

        return OptionContract(
            instrument_id=instrument_id,
            raw_symbol=Symbol(data.get("Symbol", symbol)),
            asset_class=AssetClass.EQUITY,
            currency=Currency.from_str(data.get("Currency", "USD")),
            price_precision=price_precision,
            price_increment=price_increment,
            multiplier=Quantity.from_str(str(int(multiplier))),
            lot_size=lot_size,
            underlying=underlying,
            option_kind=option_kind,
            strike_price=strike_price,
            expiration_ns=expiration_ns,
            activation_ns=ts_event,
            ts_event=ts_event,
            ts_init=ts_event,
        )

    except Exception as e:
        _log.error(f"Error parsing option contract {symbol}: {e}")
        return None


def determine_price_precision(data: dict) -> int:
    """Determine price precision from MinMove field in symbol data.

    Parameters
    ----------
    data : dict
        Raw symbol details dict.

    Returns
    -------
    int
        Number of decimal places (defaults to 2 on error).
    """
    min_move = data.get("MinMove", "0.01")
    try:
        if "." in str(min_move):
            return len(str(min_move).split(".")[1])
        return 0
    except Exception:
        return 2
