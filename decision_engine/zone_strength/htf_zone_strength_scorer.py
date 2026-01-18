import pandas as pd


# ==================================================
# SCORING HELPERS
# ==================================================
def score_timeframe(tf: str) -> int:
    return 25 if tf == "M" else 15


def score_pattern(zone_type: str, pattern: str) -> int:
    if zone_type == "DEMAND":
        return 25 if pattern == "DBR" else 15
    else:  # SUPPLY
        return 25 if pattern == "RBD" else 15


def score_base_quality(base_candles: int) -> int:
    if base_candles <= 2:
        return 20
    elif base_candles <= 4:
        return 12
    elif base_candles <= 6:
        return 5
    else:
        return 0


def score_departure_efficiency(base_candles: int) -> int:
    if base_candles <= 2:
        return 20
    elif base_candles <= 4:
        return 12
    elif base_candles <= 6:
        return 5
    else:
        return 0


def score_freshness() -> int:
    # Touch-count logic added later (non-breaking)
    return 10


# ==================================================
# MASTER SCORER
# ==================================================
def score_htf_zones(zones_df: pd.DataFrame) -> pd.DataFrame:
    if zones_df.empty:
        return zones_df

    df = zones_df.copy()

    df["score_timeframe"] = df["timeframe"].apply(score_timeframe)
    df["score_pattern"] = df.apply(
        lambda r: score_pattern(r["zone_type"], r["pattern"]),
        axis=1,
    )
    df["score_base"] = df["base_candles"].apply(score_base_quality)
    df["score_departure"] = df["base_candles"].apply(score_departure_efficiency)
    df["score_freshness"] = score_freshness()

    df["htf_zone_score"] = (
        df["score_timeframe"]
        + df["score_pattern"]
        + df["score_base"]
        + df["score_departure"]
        + df["score_freshness"]
    )

    # Grade (human readable)
    df["zone_grade"] = pd.cut(
        df["htf_zone_score"],
        bins=[0, 49, 69, 84, 100],
        labels=["REJECT", "C", "B", "A"],
    )

    return df
