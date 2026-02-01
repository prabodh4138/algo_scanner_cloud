import pandas as pd


# ==================================================
# ZONE FRESHNESS / TOUCH COUNT ENGINE (ROBUST)
# ==================================================
def compute_zone_freshness(
    zones_df: pd.DataFrame,
    price_df: pd.DataFrame,
    max_touches: int = 3,
) -> pd.DataFrame:
    """
    Computes zone freshness in a defensive, schema-safe way.

    REQUIRED (minimum):
        zones_df: zone_low, zone_high
        price_df: trade_date, low, high

    OPTIONAL (used if present):
        zone_created_at
        base_end_date
        zone_start_date
    """

    if zones_df.empty or price_df.empty:
        return zones_df

    zones = zones_df.copy()
    prices = price_df.copy()

    prices["trade_date"] = pd.to_datetime(prices["trade_date"])

    # --------------------------------------------------
    # 🔴 SAFE DERIVATION OF zone_created_at
    # --------------------------------------------------
    if "zone_created_at" in zones.columns:
        zones["zone_created_at"] = pd.to_datetime(
            zones["zone_created_at"]
        )

    elif "base_end_date" in zones.columns:
        zones["zone_created_at"] = pd.to_datetime(
            zones["base_end_date"]
        )

    elif "zone_start_date" in zones.columns:
        zones["zone_created_at"] = pd.to_datetime(
            zones["zone_start_date"]
        )

    else:
        # Fallback: use earliest available HTF date (safe, conservative)
        earliest_date = prices["trade_date"].min()
        zones["zone_created_at"] = earliest_date

    # --------------------------------------------------
    # TOUCH COUNT LOGIC
    # --------------------------------------------------
    touch_counts = []
    freshness_scores = []
    exhausted_flags = []

    for _, zone in zones.iterrows():
        zl = zone["zone_low"]
        zh = zone["zone_high"]
        created_at = zone["zone_created_at"]

        touches = prices[
            (prices["trade_date"] > created_at)
            & (prices["low"] <= zh)
            & (prices["high"] >= zl)
        ]

        touch_count = len(touches)

        if touch_count == 0:
            freshness_score = 20
        elif touch_count == 1:
            freshness_score = 10
        elif touch_count == 2:
            freshness_score = 0
        else:
            freshness_score = -20

        exhausted = touch_count >= max_touches

        touch_counts.append(touch_count)
        freshness_scores.append(freshness_score)
        exhausted_flags.append(exhausted)

    zones["zone_touch_count"] = touch_counts
    zones["zone_freshness_score"] = freshness_scores
    zones["zone_exhausted"] = exhausted_flags

    return zones
