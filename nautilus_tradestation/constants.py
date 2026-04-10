from typing import Final

from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import Venue


TRADESTATION: Final[str] = "TRADESTATION"
TRADESTATION_VENUE: Final[Venue] = Venue(TRADESTATION)
TRADESTATION_CLIENT_ID: Final[ClientId] = ClientId(TRADESTATION)
