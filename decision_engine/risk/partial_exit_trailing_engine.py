import pandas as pd


# ==================================================
# PARTIAL EXIT + TRAILING STOP ENGINE
# ==================================================
def apply_partial_exit_and_trailing(
    trades_df: pd.DataFrame,
    price_df: pd.DataFrame,
    r1_multiple: float = 1.0,
    r2_multiple: float = 2.0,
    trail_pct: float = 0.5,
) -> pd.DataFrame:
    """
    Applies:
    - Partial exit at 1R
    - Breakeven stop after 1R
    - Trailing stop after 2R

    trades_df must contain:
        symbol, entry, stop, quantity

    price_df must contain:
        trade_date, symbol, high, low
    """

    if trades_df.empty or price_df.empty:
        return trades_df

    trades = trades_df.copy()
    prices = price_df.copy()

    prices["trade_date"] = pd.to_datetime(prices["trade_date"])

    result_rows = []

    for _, trade in trades.iterrows():
        symbol = trade["symbol"]
        entry = trade["entry"]
        stop = trade["stop"]
        qty = trade["quantity"]

        risk_per_share = entry - stop
        r1_price = entry + r1_multiple * risk_per_share
        r2_price = entry + r2_multiple * risk_per_share

        remaining_qty = qty
        pnl = 0.0
        trail_stop = stop
        partial_exit_done = False

        symbol_prices = prices[
            prices["symbol"] == symbol
        ].sort_values("trade_date")

        for _, bar in symbol_prices.iterrows():
            high = bar["high"]
            low = bar["low"]

            # --- STOP HIT ---
            if low <= trail_stop:
                pnl += (trail_stop - entry) * remaining_qty
                remaining_qty = 0
                break

            # --- PARTIAL EXIT @ 1R ---
            if not partial_exit_done and high >= r1_price:
                exit_qty = remaining_qty // 2
                pnl += (r1_price - entry) * exit_qty
                remaining_qty -= exit_qty
                trail_stop = entry  # Breakeven
                partial_exit_done = True

            # --- TRAILING AFTER 2R ---
            if high >= r2_price:
                trail_stop = max(
                    trail_stop,
                    high - trail_pct * risk_per_share
                )

        result = trade.to_dict()
        result["final_quantity"] = remaining_qty
        result["realized_pnl"] = round(pnl, 2)
        result["partial_exit"] = partial_exit_done
        result["final_stop"] = round(trail_stop, 2)

        result_rows.append(result)

    return pd.DataFrame(result_rows)
