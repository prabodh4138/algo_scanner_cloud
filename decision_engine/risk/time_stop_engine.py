import pandas as pd


# ==================================================
# TIME-BASED STOP / ZONE EXPIRY ENGINE
# ==================================================
def apply_time_stop(
    trades_df: pd.DataFrame,
    price_df: pd.DataFrame,
    max_bars_alive: int = 10,
    timeframe: str = "D",
) -> pd.DataFrame:
    """
    Invalidates trades that fail to move within a fixed number of bars.

    trades_df must contain:
        - symbol
        - auth_zone_created_at (date/datetime)

    price_df must contain:
        - trade_date
        - symbol

    Adds:
        - bars_alive
        - time_stop_triggered (bool)
    """

    if trades_df.empty or price_df.empty:
        return trades_df

    trades = trades_df.copy()
    prices = price_df.copy()

    prices["trade_date"] = pd.to_datetime(prices["trade_date"])
    trades["auth_zone_created_at"] = pd.to_datetime(
        trades["auth_zone_created_at"]
    )

    bars_alive_list = []
    stop_flags = []

    for _, trade in trades.iterrows():
        symbol = trade["symbol"]
        zone_created = trade["auth_zone_created_at"]

        symbol_prices = prices[
            (prices["symbol"] == symbol)
            & (prices["trade_date"] > zone_created)
        ].sort_values("trade_date")

        bars_alive = len(symbol_prices)

        bars_alive_list.append(bars_alive)

        stop_flags.append(bars_alive >= max_bars_alive)

    trades["bars_alive"] = bars_alive_list
    trades["time_stop_triggered"] = stop_flags

    return trades
