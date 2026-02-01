import pandas as pd
from utils.supabase_rest_client import supabase_select


def load_daily_stock_data():
    """
    Load daily stock data from Supabase.
    Expected table: equity_daily_raw
    """
    rows = supabase_select(
        table="equity_daily_raw",
        columns="symbol,trade_date,open,high,low,close,volume"
    )

    if not rows:
        raise ValueError("No daily stock data found in Supabase")

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values(["symbol", "trade_date"])
    return df


def resample_ohlc(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    ohlc = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    return (
        df.set_index("trade_date")
          .groupby("symbol")
          .resample(timeframe)
          .agg(ohlc)
          .dropna()
          .reset_index()
    )


def build_timeframes(_parquet_path=None):
    """
    Build Daily / Weekly / Monthly OHLC dataframes.
    parquet_path is ignored for cloud execution.
    """

    daily_df = load_daily_stock_data()
    daily_df["timeframe"] = "D"

    weekly_df = resample_ohlc(daily_df, "W")
    weekly_df["timeframe"] = "W"

    monthly_df = resample_ohlc(daily_df, "M")
    monthly_df["timeframe"] = "M"

    return daily_df, weekly_df, monthly_df
