import pandas as pd


# ==================================================
# HTF TREND REGIME CLASSIFIER
# ==================================================
def classify_htf_trend(
    df: pd.DataFrame,
    lookback: int = 50,
    threshold_pct: float = 0.02,
) -> str:
    """
    Determines HTF trend regime using structure.
    
    Returns:
        'BULL', 'BEAR', or 'RANGE'
    """

    if df.empty or len(df) < lookback:
        return "RANGE"

    recent = df.tail(lookback)

    high_max = recent["high"].max()
    low_min = recent["low"].min()
    last_close = recent.iloc[-1]["close"]

    range_pct = (high_max - low_min) / low_min

    if range_pct < threshold_pct:
        return "RANGE"

    mid = (high_max + low_min) / 2

    if last_close > mid:
        return "BULL"
    else:
        return "BEAR"
