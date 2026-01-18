import pandas as pd


# ==================================================
# LOAD DAILY DATA (SINGLE SOURCE OF TRUTH)
# ==================================================
def load_daily_stock_data(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    required_cols = [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    df = df[required_cols].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    return df


# ==================================================
# DAILY → WEEKLY (SAFE & FUTURE-PROOF)
# ==================================================
def resample_weekly(daily_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for symbol, g in daily_df.groupby("symbol"):
        g = g.set_index("trade_date")

        wk = (
            g
            .resample("W-FRI")
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna()
            .reset_index()
        )

        wk["symbol"] = symbol
        wk["timeframe"] = "W"
        rows.append(wk)

    return pd.concat(rows, ignore_index=True)


# ==================================================
# DAILY → MONTHLY (SAFE & FUTURE-PROOF)
# ==================================================
def resample_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for symbol, g in daily_df.groupby("symbol"):
        g = g.set_index("trade_date")

        mo = (
            g
            .resample("ME")   # Month-End (new pandas standard)
            .agg(
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
            )
            .dropna()
            .reset_index()
        )

        mo["symbol"] = symbol
        mo["timeframe"] = "M"
        rows.append(mo)

    return pd.concat(rows, ignore_index=True)


# ==================================================
# MASTER BUILDER
# ==================================================
def build_timeframes(parquet_path: str):
    daily_df = load_daily_stock_data(parquet_path)

    weekly_df = resample_weekly(daily_df)
    monthly_df = resample_monthly(daily_df)

    daily_df = daily_df.copy()
    daily_df["timeframe"] = "D"

    return daily_df, weekly_df, monthly_df
