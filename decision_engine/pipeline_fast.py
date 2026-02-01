import pandas as pd
import io

from decision_engine.pipeline import run_pipeline


def run_pipeline_from_bytes(
    parquet_bytes: bytes,
    total_capital: float,
) -> pd.DataFrame:
    """
    Fast wrapper that avoids filesystem I/O.
    """
    buffer = io.BytesIO(parquet_bytes)
    df = pd.read_parquet(buffer)

    # TEMP parquet in memory
    temp_path = "_in_memory.parquet"
    df.to_parquet(temp_path)

    return run_pipeline(
        parquet_path=temp_path,
        total_capital=total_capital,
    )
