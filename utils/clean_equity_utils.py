import pandas as pd


def clean_equity_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Universal NSE Bhavcopy cleaner
    Works for sec_bhavdata_full_*.csv
    """

    if df is None or df.empty:
        raise ValueError("CSV is empty")

    # --------------------------------------------------
    # STEP 1: NORMALIZE COLUMN NAMES (REMOVE SPACES)
    # --------------------------------------------------
    df = df.copy()
    df.columns = [c.strip().upper() for c in df.columns]

    # --------------------------------------------------
    # STEP 2: FILTER ONLY EQ SERIES
    # --------------------------------------------------
    if "SERIES" not in df.columns:
        raise ValueError("SERIES column not found — not a valid NSE bhavcopy")

    df = df[df["SERIES"] == "EQ"]

    if df.empty:
        raise ValueError("No EQ series data found after filtering")

    # --------------------------------------------------
    # STEP 3: FORCE MAP NSE BHAVCOPY COLUMNS
    # --------------------------------------------------
    rename_map = {
        "SYMBOL": "symbol",
        "DATE1": "trade_date",
        "OPEN_PRICE": "open",
        "HIGH_PRICE": "high",
        "LOW_PRICE": "low",
        "CLOSE_PRICE": "close",
        "TTL_TRD_QNTY": "volume",
    }

    df = df.rename(columns=rename_map)

    # --------------------------------------------------
    # STEP 4: VALIDATE REQUIRED COLUMNS
    # --------------------------------------------------
    required_columns = {
        "trade_date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in stock CSV: {missing}")

    # Volume optional (but expected in bhavcopy)
    if "volume" not in df.columns:
        df["volume"] = 0

    # --------------------------------------------------
    # STEP 5: SELECT + TYPE CAST
    # --------------------------------------------------
    df = df[list(required_columns | {"volume"})].copy()

    df["trade_date"] = pd.to_datetime(
        df["trade_date"],
        dayfirst=True,
        errors="raise"
    ).dt.date

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()

    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="raise")

    df["volume"] = (
        pd.to_numeric(df["volume"], errors="coerce")
        .fillna(0)
        .astype("int64")
    )

    # --------------------------------------------------
    # STEP 6: SANITY CHECKS
    # --------------------------------------------------
    if (df["high"] < df["low"]).any():
        raise ValueError("Invalid OHLC data detected (high < low)")

    # Remove duplicates
    df = df.drop_duplicates(subset=["trade_date", "symbol"])

    # Sort for consistency
    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)

    return df
