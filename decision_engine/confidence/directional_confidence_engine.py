import pandas as pd


def score_htf_strength(htf_score: float) -> int:
    if htf_score >= 90:
        return 40
    elif htf_score >= 80:
        return 32
    elif htf_score >= 70:
        return 24
    elif htf_score >= 60:
        return 16
    else:
        return 8


def score_timeframe(tf: str) -> int:
    return 20 if tf == "M" else 10


def score_location(price: float, zone_low: float) -> int:
    distance_pct = (price - zone_low) / zone_low

    if distance_pct <= 0.03:
        return 25
    elif distance_pct <= 0.06:
        return 18
    elif distance_pct <= 0.10:
        return 10
    else:
        return 0


def supply_penalty(has_supply_overhead: bool) -> int:
    return -15 if has_supply_overhead else 0


def compute_directional_confidence(
    aligned_df: pd.DataFrame,
    htf_zones_df: pd.DataFrame,
) -> pd.DataFrame:
    rows = []

    allowed = aligned_df[aligned_df["alignment_status"] == "ALLOWED"]

    for _, d in allowed.iterrows():
        symbol = d["symbol"]
        price = d["close"]

        zones = htf_zones_df[
            (htf_zones_df["symbol"] == symbol)
            & (htf_zones_df["zone_grade"].isin(["A", "B"]))
        ]

        demand = zones[zones["zone_type"] == "DEMAND"]
        if demand.empty:
            continue

        demand = demand.sort_values(
            by=["timeframe", "htf_zone_score"],
            ascending=False,
        )

        auth_zone = demand.iloc[0]

        supply = zones[zones["zone_type"] == "SUPPLY"]
        has_supply_overhead = any(supply["zone_low"] > price)

        score = 0
        score += score_htf_strength(auth_zone["htf_zone_score"])
        score += score_timeframe(auth_zone["timeframe"])
        score += score_location(price, auth_zone["zone_low"])
        score += supply_penalty(has_supply_overhead)

        rows.append({
            **d,
            "directional_confidence": max(0, min(100, score)),
            "auth_zone_timeframe": auth_zone["timeframe"],
            "auth_zone_pattern": auth_zone["pattern"],
            "auth_zone_score": auth_zone["htf_zone_score"],
            "auth_zone_low": auth_zone["zone_low"],
        })

    return pd.DataFrame(rows)
