import sys
from pathlib import Path

# ======================================================
# Force filesystem imports (Streamlit Cloud compatible)
# ======================================================
ROOT_DIR = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(ROOT_DIR / "decision_engine" / "utils"))
sys.path.insert(0, str(ROOT_DIR / "utils"))

import streamlit as st
import pandas as pd

from data_cleaner import clean_stock_df, clean_index_df
from supabase_rest_client import supabase_insert
