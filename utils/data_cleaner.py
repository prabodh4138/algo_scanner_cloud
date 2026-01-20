"""
data_cleaner.py
----------------
Cloud-safe CSV cleaning utilities.

✔ Reused from ingestion_engine.py
✔ NO parquet I/O
✔ NO filesystem dependency
✔ DataFrame in → DataFrame out
✔ Safe for Streamlit + Supabase
"""

import pandas as pd
from typing import Dict


# ======================================================
# INTERNAL HELPERS (UNCHANGED LOGIC)
# ======================================================
def _norm(col: str) -> str:
    return (
        col.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


def _rename_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]

    for src, tgt in mapping.items():
        src_n = _norm(src)
        if src_n in df.columns and tgt not in df.columns:
            df.rename(columns={src_n: tgt}, inplace=True)

    return df


# ======================================================
# EQUITY (STOCK) CSV CLEANER
# ======================================================
def clean_stock_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        raise ValueError("Stock CSV is empty")

    df = _rename_columns(
        df,
        {
            "date": "trade_date",
            "datetime": "trade_date",
            "symbol": "symbol",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        },
    )

    required = {
        "trade_date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in stock CSV: {missing}")

    df = df[list(required)].copy()

    # --- type enforcement ---
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise").dt.date
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    df["volume"] = pd.to_numeric(df["volume"], errors="raise").astype("int64")

    # --- data sanity ---
    if (df["high"] < df["low"]).any():
        raise ValueError("Invalid OHLC data: high < low")

    # --- deduplicate on PK ---
    df = df.drop_duplicates(subset=["trade_date", "symbol"])

    # --- final ordering ---
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    return df


# ======================================================
# INDEX CSV CLEANER
# ======================================================
def clean_index_df(df: pd.DataFrame, index_name: str | None = None) -> pd.DataFrame:
    if df.empty:
        raise ValueError("Index CSV is empty")

    df = _rename_columns(
        df,
        {
            "date": "trade_date",
            "datetime": "trade_date",
            "index": "index_name",
            "name": "index_name",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
        },
    )

    if "index_name" not in df.columns:
        if not index_name:
            raise ValueError("index_name missing and not provided explicitly")
        df["index_name"] = index_name

    required = {
        "trade_date",
        "index_name",
        "open",
        "high",
        "low",
        "close",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in index CSV: {missing}")

    df = df[list(required)].copy()

    # --- type enforcement ---
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise").dt.date
    df["index_name"] = df["index_name"].astype(str).str.upper().str.strip()

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="raise")

    # --- data sanity ---
    if (df["high"] < df["low"]).any():
        raise ValueError("Invalid OHLC data: high < low")

    # --- deduplicate on PK ---
    df = df.drop_duplicates(subset=["trade_date", "index_name"])

    # --- final ordering ---
    df = df.sort_values(["index_name", "trade_date"]).reset_index(drop=True)

    return df
