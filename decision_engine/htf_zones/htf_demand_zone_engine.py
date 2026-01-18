import pandas as pd


def detect_htf_demand_zones(weekly_df: pd.DataFrame, monthly_df: pd.DataFrame) -> pd.DataFrame:
    zones = []

    for tf_df in (weekly_df, monthly_df):
        timeframe = tf_df["timeframe"].iloc[0]

        for symbol, g in tf_df.groupby("symbol"):
            g = g.sort_values("trade_date").reset_index(drop=True)

            # Candle features
            g["range"] = g["high"] - g["low"]
            g["body"] = (g["close"] - g["open"]).abs()
            g["body_ratio"] = g["body"] / g["range"]

            g["is_base"] = g["body_ratio"] <= 0.40
            g["is_bull_impulse"] = (g["body_ratio"] >= 0.60) & (g["close"] > g["open"])
            g["is_bear_impulse"] = (g["body_ratio"] >= 0.60) & (g["close"] < g["open"])

            i = 1
            while i < len(g) - 1:

                # ==================================================
                # 🔹 DBR — DROP → BASE → RALLY (PRIMARY DEMAND)
                # ==================================================
                if g.loc[i - 1, "is_bear_impulse"]:

                    base_start = i
                    base_end = i

                    while (
                        base_end + 1 < len(g)
                        and g.loc[base_end + 1, "is_base"]
                        and (base_end - base_start + 1) < 6
                    ):
                        base_end += 1

                    if base_end + 1 < len(g):
                        impulse = g.loc[base_end + 1]

                        base_high = g.loc[base_start:base_end, "high"].max()
                        base_low = g.loc[base_start:base_end, "low"].min()
                        base_range = base_high - base_low

                        if (
                            impulse["is_bull_impulse"]
                            and impulse["close"] > base_high
                            and (impulse["close"] - base_high) >= 1.5 * base_range
                        ):
                            zones.append({
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "zone_low": base_low,
                                "zone_high": g.loc[base_start:base_end, ["open", "close"]].max().max(),
                                "base_candles": base_end - base_start + 1,
                                "impulse_date": impulse["trade_date"],
                                "pattern": "DBR",
                                "zone_type": "DEMAND",
                            })

                            i = base_end + 2
                            continue

                # ==================================================
                # 🔹 RBR — RALLY → BASE → RALLY (CONTINUATION DEMAND)
                # ==================================================
                if g.loc[i - 1, "is_bull_impulse"]:

                    base_start = i
                    base_end = i

                    while (
                        base_end + 1 < len(g)
                        and g.loc[base_end + 1, "is_base"]
                        and (base_end - base_start + 1) < 6
                    ):
                        base_end += 1

                    if base_end + 1 < len(g):
                        impulse = g.loc[base_end + 1]

                        base_high = g.loc[base_start:base_end, "high"].max()
                        base_low = g.loc[base_start:base_end, "low"].min()
                        base_range = base_high - base_low

                        if (
                            impulse["is_bull_impulse"]
                            and impulse["close"] > base_high
                            and (impulse["close"] - base_high) >= 1.5 * base_range
                        ):
                            zones.append({
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "zone_low": base_low,
                                "zone_high": g.loc[base_start:base_end, ["open", "close"]].max().max(),
                                "base_candles": base_end - base_start + 1,
                                "impulse_date": impulse["trade_date"],
                                "pattern": "RBR",
                                "zone_type": "DEMAND",
                            })

                            i = base_end + 2
                            continue

                i += 1

    return pd.DataFrame(zones)
