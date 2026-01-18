"""
Supabase REST Client
--------------------
• Streamlit Cloud safe
• Uses ANON key (never service key)
• Supports INSERT, UPSERT, SELECT
• Sanitizes NaN / inf / bad JSON
• Append-safe and production-ready
"""

import os
import math
import requests
import streamlit as st


# ======================================================
# CONFIG — STREAMLIT + LOCAL SAFE
# ======================================================
SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_ANON_KEY = st.secrets.get("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    raise RuntimeError("❌ Supabase credentials not found")

HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=ignore-duplicates",
}


# ======================================================
# INTERNAL SANITIZER (DO NOT REMOVE)
# ======================================================
def _sanitize_value(v):
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def sanitize_payload(rows):
    safe_rows = []
    for row in rows:
        safe_rows.append({k: _sanitize_value(v) for k, v in row.items()})
    return safe_rows


# ======================================================
# INSERT (APPEND-ONLY TABLES)
# ======================================================
def supabase_insert(table_name, rows, timeout=30):
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    safe_rows = sanitize_payload(rows)

    response = requests.post(
        url,
        headers=HEADERS,
        json=safe_rows,
        timeout=timeout,
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"[SUPABASE INSERT ERROR] {response.status_code} | {response.text}"
        )

    print(f"[SUPABASE] Inserted {len(safe_rows)} rows into {table_name}")


# ======================================================
# UPSERT (FORWARD / PAPER TRADES)
# ======================================================
def supabase_upsert(table_name, rows, on_conflict, timeout=30):
    if not rows:
        return

    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict={on_conflict}"
    safe_rows = sanitize_payload(rows)

    response = requests.post(
        url,
        headers=HEADERS,
        json=safe_rows,
        timeout=timeout,
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(
            f"[SUPABASE UPSERT ERROR] {response.status_code} | {response.text}"
        )

    print(f"[SUPABASE] Upserted {len(safe_rows)} rows into {table_name}")


# ======================================================
# SELECT (STREAMLIT READ-ONLY USE)
# ======================================================
def supabase_select(
    table_name,
    columns="*",
    filters=None,
    order=None,
    limit=None,
    timeout=30,
):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?select={columns}"

    if filters:
        for k, v in filters.items():
            url += f"&{k}=eq.{v}"

    if order:
        url += f"&order={order}"

    if limit:
        url += f"&limit={limit}"

    response = requests.get(
        url,
        headers=HEADERS,
        timeout=timeout,
    )

    if response.status_code != 200:
        raise RuntimeError(
            f"[SUPABASE SELECT ERROR] {response.status_code} | {response.text}"
        )

    return response.json()
