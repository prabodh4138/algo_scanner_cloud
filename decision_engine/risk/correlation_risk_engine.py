import pandas as pd


# ==================================================
# SECTOR & CORRELATION RISK CONTROL
# ==================================================
def apply_correlation_risk_control(
    trades_df: pd.DataFrame,
    max_trades_per_sector: int = 2,
    max_trades_per_index: int = 3,
    sector_col: str = "sector",
    index_col: str = "index_name",
) -> pd.DataFrame:
    """
    Enforces correlation risk limits.

    Rules:
    - Max N trades per sector
    - Max N trades per index
    - Higher confidence trades kept first
    """

    if trades_df.empty:
        return trades_df

    df = trades_df.copy()

    if "directional_confidence" not in df.columns:
        raise ValueError("directional_confidence column required")

    # Sort strongest ideas first
    df = df.sort_values(
        "directional_confidence",
        ascending=False
    ).reset_index(drop=True)

    accepted_rows = []
    sector_count = {}
    index_count = {}

    for _, row in df.iterrows():
        sector = row.get(sector_col, "UNKNOWN")
        index_name = row.get(index_col, "UNKNOWN")

        # Sector cap
        if sector_count.get(sector, 0) >= max_trades_per_sector:
            continue

        # Index cap
        if index_count.get(index_name, 0) >= max_trades_per_index:
            continue

        # Accept trade
        accepted_rows.append(row)

        sector_count[sector] = sector_count.get(sector, 0) + 1
        index_count[index_name] = index_count.get(index_name, 0) + 1

    return pd.DataFrame(accepted_rows).reset_index(drop=True)
