import sys
from pathlib import Path

# ======================================================
# Explicit filesystem path setup (Streamlit Cloud safe)
# ======================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DECISION_UTILS_PATH = PROJECT_ROOT / "decision_engine" / "utils"
APP_UTILS_PATH = PROJECT_ROOT / "utils"

if str(DECISION_UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(DECISION_UTILS_PATH))

if str(APP_UTILS_PATH) not in sys.path:
    sys.path.insert(0, str(APP_UTILS_PATH))

# ======================================================
# Imports (now guaranteed to work)
# ======================================================
import streamlit as st
import pandas as pd

from data_cleaner import clean_stock_df, clean_index_df
from supabase_rest_client import supabase_insert


# ======================================================
# PAGE UI
# ======================================================
st.set_page_config(page_title="Data Upload", layout="wide")

st.title("📤 CSV Data Upload")
st.caption("Clean → Validate → Append to Supabase")


# ======================================================
# STOCK (EQUITY) CSV UPLOAD
# ======================================================
st.subheader("📈 Stock (Equity) Daily Data")

stock_file = st.file_uploader(
    "Upload Stock CSV",
    type=["csv"],
    key="stock_csv"
)

if stock_file is not None:
    raw_df = pd.read_csv(stock_file)

    try:
        clean_df = clean_stock_df(raw_df)
    except Exception as e:
        st.error(f"❌ Cleaning failed: {e}")
    else:
        st.success("✅ Stock CSV cleaned successfully")
        st.info(f"Clean rows ready: {len(clean_df)}")
        st.info(f"Latest trade_date in upload: {clean_df['trade_date'].max()}")

        st.dataframe(clean_df.head(), use_container_width=True)

        if st.button("🚀 Upload Cleaned Stock Data"):
            supabase_insert(
                "equity_daily_raw",
                clean_df.to_dict("records")
            )
            st.success("✅ Stock data uploaded to Supabase")


# ======================================================
# INDEX CSV UPLOAD
# ======================================================
st.divider()
st.subheader("📊 Index Daily Data")

index_name = st.text_input(
    "Index Name (example: NIFTY_50, BANKNIFTY)",
    value=""
)

index_file = st.file_uploader(
    "Upload Index CSV",
    type=["csv"],
    key="index_csv"
)

if index_file is not None:
    if not index_name.strip():
        st.warning("⚠️ Please enter Index Name before upload")
    else:
        raw_df = pd.read_csv(index_file)

        try:
            clean_df = clean_index_df(raw_df, index_name=index_name)
        except Exception as e:
            st.error(f"❌ Cleaning failed: {e}")
        else:
            st.success("✅ Index CSV cleaned successfully")
            st.info(f"Clean rows ready: {len(clean_df)}")
            st.info(f"Latest trade_date in upload: {clean_df['trade_date'].max()}")

            st.dataframe(clean_df.head(), use_container_width=True)

            if st.button("🚀 Upload Cleaned Index Data"):
                supabase_insert(
                    "index_daily_raw",
                    clean_df.to_dict("records")
                )
                st.success("✅ Index data uploaded to Supabase")
