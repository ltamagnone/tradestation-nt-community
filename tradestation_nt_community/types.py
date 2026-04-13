"""
TradeStation adapter type definitions.
"""

import nautilus_pyo3


# Define the union type for TradeStation instruments
TradeStationInstrument = (
    nautilus_pyo3.Equity
    | nautilus_pyo3.FuturesContract
    | nautilus_pyo3.OptionContract
    | nautilus_pyo3.CurrencyPair
)

# Tuple of supported instrument types for type checking
TRADESTATION_INSTRUMENT_TYPES = (
    nautilus_pyo3.Equity,
    nautilus_pyo3.FuturesContract,
    nautilus_pyo3.OptionContract,
    nautilus_pyo3.CurrencyPair,
)
