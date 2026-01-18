import pandas as pd


# ==================================================
# ALIGNMENT ENGINE
# ==================================================
def apply_daily_htf_alignment(
    daily_df: pd.DataFrame,
    htf_zones_df: pd.DataFrame,
    supply_block_pct: float = 0.10,
):
    """
    Evaluates DAILY setups against HTF zones.

    Returns daily_df with:
    - alignment_status (ALLOWED / BLOCKED)
    - block_reason
    """

    results = []

    for _, d in daily_df.iterrows():
        symbol = d["symbol"]
        price = d["close"]

        zones = htf_zones_df[
            (htf_zones_df["symbol"] == symbol)
            & (htf_zones_df["zone_grade"].isin(["A", "B"]))
        ]

        # -----------------------------
        # RULE-2: SUPPLY BLOCK (HARD)
        # -----------------------------
        supply = zones[zones["zone_type"] == "SUPPLY"]

        for _, z in supply.iterrows():
            if z["zone_low"] > price:
                distance_pct = (z["zone_low"] - price) / price
                if distance_pct <= supply_block_pct:
                    results.append({
                        **d,
                        "alignment_status": "BLOCKED",
                        "block_reason": "HTF_SUPPLY_OVERHEAD",
                    })
                    break
        else:
            # -----------------------------
            # RULE-1: DEMAND AUTHORIZATION
            # -----------------------------
            demand = zones[zones["zone_type"] == "DEMAND"]

            if demand.empty:
                results.append({
                    **d,
                    "alignment_status": "BLOCKED",
                    "block_reason": "NO_HTF_DEMAND",
                })
            else:
                # Monthly priority
                demand = demand.sort_values(
                    by=["timeframe"],
                    ascending=False  # M before W
                )

                authorized = False
                for _, z in demand.iterrows():
                    if price >= z["zone_low"]:
                        authorized = True
                        break

                if authorized:
                    results.append({
                        **d,
                        "alignment_status": "ALLOWED",
                        "block_reason": None,
                    })
                else:
                    results.append({
                        **d,
                        "alignment_status": "BLOCKED",
                        "block_reason": "BELOW_HTF_DEMAND",
                    })

    return pd.DataFrame(results)
