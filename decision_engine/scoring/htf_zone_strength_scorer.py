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


# ==================================================
# MASTER SCORER (WITH FRESHNESS)
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

    # Freshness score must exist (added earlier)
    if "zone_freshness_score" not in df.columns:
        df["zone_freshness_score"] = 0

    df["htf_zone_score"] = (
        df["score_timeframe"]
        + df["score_pattern"]
        + df["score_base"]
        + df["score_departure"]
        + df["zone_freshness_score"]
    )

    # Grade
    df["zone_grade"] = pd.cut(
        df["htf_zone_score"],
        bins=[-100, 49, 69, 84, 120],
        labels=["REJECT", "C", "B", "A"],
    )

    # Exhausted zones are auto-rejected
    if "zone_exhausted" in df.columns:
        df.loc[df["zone_exhausted"], "zone_grade"] = "REJECT"

    return df
