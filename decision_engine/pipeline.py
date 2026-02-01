import pandas as pd

# ==================================================
# CORE DATA
# =================================================

from decision_engine.utils.timeframe_resampler import build_timeframes


# ==================================================
# ZONE ENGINES
# ==================================================
from decision_engine.htf_zones.htf_demand_zone_engine import (
    detect_htf_demand_zones
)
from decision_engine.htf_zones.htf_supply_zone_engine import (
    detect_htf_supply_zones
)

# ==================================================
# SCORING & FILTERING
# ==================================================
from decision_engine.scoring.htf_zone_strength_scorer import score_htf_zones
from decision_engine.scoring.htf_zone_freshness_engine import (
    compute_zone_freshness
)

# ✅ CORRECT IMPORT (MATCHES YOUR FILE)
from decision_engine.alignment.daily_htf_alignment_gate import (
    apply_daily_htf_alignment
)

from decision_engine.confidence.directional_confidence_engine import (
    compute_directional_confidence
)

# ==================================================
# EXECUTION
# ==================================================
from decision_engine.execution.execution_engine import build_execution_plan


# ==================================================
# MASTER PIPELINE
# ==================================================
def run_pipeline(
    parquet_path: str,
    total_capital: float = 1_000_000,
) -> pd.DataFrame:
    """
    FULL institutional-grade decision pipeline.
    """

    # --------------------------------------------------
    # 1. BUILD TIMEFRAMES
    # --------------------------------------------------
    daily_df, weekly_df, monthly_df = build_timeframes(parquet_path)

    # --------------------------------------------------
    # 2. HTF DEMAND & SUPPLY ZONES
    # --------------------------------------------------
    demand_zones = detect_htf_demand_zones(
        weekly_df=weekly_df,
        monthly_df=monthly_df,
    )

    supply_zones = detect_htf_supply_zones(
        weekly_df=weekly_df,
        monthly_df=monthly_df,
    )

    all_zones = pd.concat(
        [demand_zones, supply_zones],
        ignore_index=True
    )

    if all_zones.empty:
        return pd.DataFrame()

    # --------------------------------------------------
    # 3. ZONE FRESHNESS
    # --------------------------------------------------
    all_zones = compute_zone_freshness(
        zones_df=all_zones,
        price_df=daily_df,
    )

    # --------------------------------------------------
    # 4. ZONE STRENGTH SCORING
    # --------------------------------------------------
    all_zones = score_htf_zones(all_zones)

    # --------------------------------------------------
    # 5. DAILY HTF ALIGNMENT (✅ FIXED)
    # --------------------------------------------------
    gated_df = apply_daily_htf_alignment(
        daily_df=daily_df,
        htf_zones_df=all_zones,
    )

    # Keep only ALLOWED setups
    gated_df = gated_df[
        gated_df["alignment_status"] == "ALLOWED"
    ].copy()

    if gated_df.empty:
        return pd.DataFrame()

    # --------------------------------------------------
    # 6. DIRECTIONAL CONFIDENCE
    # --------------------------------------------------
    confident_df = compute_directional_confidence(
        df=gated_df,
        htf_price_df=weekly_df,
    )

    confident_df = confident_df[
        confident_df["directional_confidence"] >= 55
    ].copy()

    if confident_df.empty:
        return pd.DataFrame()

    # --------------------------------------------------
    # 7. FINAL EXECUTION PLAN
    # --------------------------------------------------
    execution_df = build_execution_plan(
        confident_df=confident_df,
        price_df=daily_df,
        total_capital=total_capital,
    )

    return execution_df
