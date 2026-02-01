import pandas as pd


def clean_index_dataframe(df: pd.DataFrame, index_name: str) -> pd.DataFrame:
    """
    NSE Index CSV cleaner
    Compatible with public.index_daily_raw schema
    """

    if df is None or df.empty:
        raise ValueError("Index CSV is empty")

    # --------------------------------------
    # Normalize column names
    # --------------------------------------
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    rename_map = {
        "date": "trade_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
    }

    df = df.rename(columns=rename_map)

    required = {"trade_date", "open", "high", "low", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # --------------------------------------
    # Inject index_name (as per DB schema)
    # --------------------------------------
    df["index_name"] = index_name.strip().upper()

    # --------------------------------------
    # Type enforcement
    # --------------------------------------
    df["trade_date"] = pd.to_datetime(
        df["trade_date"], dayfirst=True, errors="raise"
    ).dt.date

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # --------------------------------------
    # Deduplicate (PK safe)
    # --------------------------------------
    df = df.drop_duplicates(subset=["trade_date", "index_name"])
    df = df.sort_values(["index_name", "trade_date"]).reset_index(drop=True)

    # --------------------------------------
    # Final column order (matches table)
    # --------------------------------------
    return df[
        ["trade_date", "index_name", "open", "high", "low", "close"]
    ]
