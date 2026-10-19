"""
Streamlit-Dashboard fuer marts.mart_delivery_performance.

Beantwortet die Business-Frage des Projekts visuell: wo sind Verspaetungsraten
am hoechsten, und wie stark haengt das mit dem Review Score zusammen?

Start: streamlit run dashboard/app.py
"""

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "./warehouse/ecom.duckdb")

st.set_page_config(page_title="Delivery Performance", layout="wide")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)
    df = con.execute("SELECT * FROM dev_marts.mart_delivery_performance").fetchdf()
    con.close()
    return df


st.title("📦 Delivery Performance & Customer Impact")
st.caption(
    "Welche Kategorien/Regionen haben die höchste Verspätungsrate, "
    "und wie stark hängt das mit dem Review Score zusammen?"
)

try:
    df = load_data()
except duckdb.Error as e:
    st.error(
        f"Konnte Warehouse nicht lesen ({e}). "
        f"Erst `dbt build` in dbt_project/ ausführen, damit dev_marts.mart_delivery_performance existiert."
    )
    st.stop()

if df.empty:
    st.warning("Keine Daten vorhanden. Pipeline erst einmal durchlaufen lassen (siehe README Quickstart).")
    st.stop()

# --- Filter ---
col_f1, col_f2 = st.columns(2)
with col_f1:
    states = st.multiselect("Bundesstaat", sorted(df["customer_state"].unique()))
with col_f2:
    categories = st.multiselect("Produktkategorie", sorted(df["product_category"].unique()))

filtered = df.copy()
if states:
    filtered = filtered[filtered["customer_state"].isin(states)]
if categories:
    filtered = filtered[filtered["product_category"].isin(categories)]

# --- KPIs ---
k1, k2, k3 = st.columns(3)
k1.metric("Bestellungen gesamt", f"{filtered['total_orders'].sum():,}")
overall_late_rate = filtered["late_orders"].sum() / max(filtered["total_orders"].sum(), 1)
k2.metric("Ø Verspätungsrate", f"{overall_late_rate:.1%}")
review_gap = (
    filtered["avg_review_score_on_time"].mean() - filtered["avg_review_score_late"].mean()
)
k3.metric("Review-Score-Gap (pünktlich vs. spät)", f"{review_gap:.2f} ★")

st.divider()

# --- Verspätungsrate nach Kategorie ---
by_category = (
    filtered.groupby("product_category")
    .agg(total_orders=("total_orders", "sum"), late_orders=("late_orders", "sum"))
    .reset_index()
)
by_category["late_delivery_rate"] = by_category["late_orders"] / by_category["total_orders"]

fig_category = px.bar(
    by_category.sort_values("late_delivery_rate", ascending=False),
    x="product_category",
    y="late_delivery_rate",
    title="Verspätungsrate nach Produktkategorie",
    labels={"late_delivery_rate": "Verspätungsrate", "product_category": "Kategorie"},
)
fig_category.update_yaxes(tickformat=".0%")
st.plotly_chart(fig_category, use_container_width=True)

col_a, col_b = st.columns(2)

with col_a:
    by_state = (
        filtered.groupby("customer_state")
        .agg(total_orders=("total_orders", "sum"), late_orders=("late_orders", "sum"))
        .reset_index()
    )
    by_state["late_delivery_rate"] = by_state["late_orders"] / by_state["total_orders"]
    fig_state = px.bar(
        by_state.sort_values("late_delivery_rate", ascending=False),
        x="customer_state",
        y="late_delivery_rate",
        title="Verspätungsrate nach Bundesstaat",
    )
    fig_state.update_yaxes(tickformat=".0%")
    st.plotly_chart(fig_state, use_container_width=True)

with col_b:
    fig_review = px.bar(
        pd.DataFrame({
            "Lieferung": ["Pünktlich", "Verspätet"],
            "Ø Review Score": [
                filtered["avg_review_score_on_time"].mean(),
                filtered["avg_review_score_late"].mean(),
            ],
        }),
        x="Lieferung", y="Ø Review Score",
        title="Review Score: pünktlich vs. verspätet",
        range_y=[0, 5],
    )
    st.plotly_chart(fig_review, use_container_width=True)

# --- Trend über Zeit ---
trend = (
    filtered.groupby("purchase_month")
    .agg(total_orders=("total_orders", "sum"), late_orders=("late_orders", "sum"))
    .reset_index()
)
trend["late_delivery_rate"] = trend["late_orders"] / trend["total_orders"]
fig_trend = px.line(
    trend.sort_values("purchase_month"),
    x="purchase_month", y="late_delivery_rate",
    title="Verspätungsrate über Zeit",
    markers=True,
)
fig_trend.update_yaxes(tickformat=".0%")
st.plotly_chart(fig_trend, use_container_width=True)

st.divider()
st.dataframe(filtered.sort_values("late_delivery_rate", ascending=False), use_container_width=True)
