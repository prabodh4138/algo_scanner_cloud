import pandas as pd
import math


# ==================================================
# CONFIDENCE → RISK MULTIPLIER
# ==================================================
def confidence_multiplier(conf: float) -> float:
    if conf >= 75:
        return 1.0
    elif conf >= 65:
        return 0.75
    elif conf >= 55:
        return 0.5
    else:
        return 0.25


# ==================================================
# EXECUTION & CAPITAL ALLOCATION ENGINE
# ==================================================
def build_execution_plan(
    confident_df: pd.DataFrame,
    total_capital: float = 1_000_000,
    max_risk_per_trade_pct: float = 0.01,
    max_portfolio_risk_pct: float = 0.06,
) -> pd.DataFrame:
    """
    Builds FINAL trade execution plan.
    LONG-only by design.
    """

    trades = []

    # Highest confidence first
    confident_df = confident_df.sort_values(
        "directional_confidence",
        ascending=False
    )

    portfolio_risk_used = 0.0

    for _, row in confident_df.iterrows():
        entry = row["close"]
        zone_low = row.get("auth_zone_low")

        # Safety checks
        if zone_low is None or entry <= zone_low:
            continue

        stop = zone_low * 0.995
        risk_per_share = entry - stop

        if risk_per_share <= 0:
            continue

        multiplier = confidence_multiplier(
            row["directional_confidence"]
        )

        capital_at_risk = (
            total_capital
            * max_risk_per_trade_pct
            * multiplier
        )

        # Portfolio risk cap
        if portfolio_risk_used + capital_at_risk > (
            total_capital * max_portfolio_risk_pct
        ):
            continue

        qty = math.floor(capital_at_risk / risk_per_share)

        if qty <= 0:
            continue

        target = entry + 2.5 * risk_per_share

        trades.append({
            "symbol": row["symbol"],
            "entry": round(entry, 2),
            "stop": round(stop, 2),
            "target": round(target, 2),
            "quantity": qty,
            "risk_per_trade": round(capital_at_risk, 2),
            "directional_confidence": row["directional_confidence"],
            "auth_zone_timeframe": row["auth_zone_timeframe"],
            "auth_zone_pattern": row["auth_zone_pattern"],
        })

        portfolio_risk_used += capital_at_risk

    return pd.DataFrame(trades)
