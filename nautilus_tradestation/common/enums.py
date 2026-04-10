"""
TradeStation adapter enumerations.
"""

from enum import Enum


class TradeStationAssetType(str, Enum):
    """
    Represents a TradeStation asset type.
    """

    STOCK = "STOCK"
    FUTURE = "FUTURE"
    OPTION = "OPTION"
    FOREX = "FOREX"
    INDEX = "INDEX"


class TradeStationBarUnit(str, Enum):
    """
    Represents a TradeStation bar unit type for historical data.
    """

    MINUTE = "Minute"
    DAILY = "Daily"
    WEEKLY = "Weekly"
    MONTHLY = "Monthly"


class TradeStationOrderType(str, Enum):
    """
    Represents a TradeStation order type.
    """

    MARKET = "Market"
    LIMIT = "Limit"
    STOP_MARKET = "StopMarket"
    STOP_LIMIT = "StopLimit"
    MARKET_ON_OPEN = "MarketOnOpen"
    MARKET_ON_CLOSE = "MarketOnClose"
    TRAILING_STOP = "TrailingStop"


class TradeStationTimeInForce(str, Enum):
    """
    Represents a TradeStation time-in-force instruction.
    """

    DAY = "DAY"
    GTC = "GTC"
    GTD = "GTD"
    IOC = "IOC"
    FOK = "FOK"
    OPG = "OPG"
    CLO = "CLO"


class TradeStationOrderSide(str, Enum):
    """
    Represents a TradeStation order side.
    """

    BUY = "Buy"
    SELL = "Sell"
    BUY_TO_COVER = "BuyToCover"
    SELL_SHORT = "SellShort"


class TradeStationOrderStatus(str, Enum):
    """
    Represents a TradeStation order status.
    """

    ACK = "ACK"  # Acknowledged
    FLL = "FLL"  # Filled
    FLP = "FLP"  # Partially Filled
    FPR = "FPR"  # Filled Partial Replaced
    CAN = "CAN"  # Canceled
    EXP = "EXP"  # Expired
    REJ = "REJ"  # Rejected
    BRO = "BRO"  # Broken
    LAT = "LAT"  # Too Late
    DON = "DON"  # Done for Day
    UCN = "UCN"  # User Canceled
    TSC = "TSC"  # Trailing Stop Canceled
    OPN = "OPN"  # Open
    UPD = "UPD"  # Updated
