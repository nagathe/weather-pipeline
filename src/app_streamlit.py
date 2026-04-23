"""
app_streamlit.py — Dashboard météo interactif.

Lit la table 'weather_raw' depuis SQLite et affiche
un tableau de bord Matplotlib/Seaborn via Streamlit.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "weather.db"


def load_data() -> pd.DataFrame:
    """Charge toutes les lignes de weather_raw depuis SQLite."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM weather_raw", conn)


def make_dashboard(df: pd.DataFrame) -> plt.Figure:
    """
    Construit la figure Matplotlib 2×2.

    Args:
        df : DataFrame issu de load_data().

    Returns:
        Figure Matplotlib prête pour st.pyplot().
    """
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Weather Dashboard", fontsize=18, fontweight="bold")

    sns.barplot(data=df, x="city", y="temperature",   hue="city", legend=False, ax=axes[0, 0], palette="coolwarm", errorbar=None)
    axes[0, 0].set(title="Température (°C)", xlabel="")

    sns.barplot(data=df, x="city", y="humidity",      hue="city", legend=False, ax=axes[0, 1], palette="Blues_d",  errorbar=None)
    axes[0, 1].set(title="Humidité (%)",      xlabel="")

    sns.barplot(data=df, x="city", y="temperature_f", hue="city", legend=False, ax=axes[1, 0], palette="Greens_d", errorbar=None)
    axes[1, 0].set(title="Température (°F)", xlabel="")

    df_melted = df.melt(id_vars="city", value_vars=["temperature", "humidity", "temperature_f"])
    sns.barplot(data=df_melted, x="city", y="value", hue="variable", ax=axes[1, 1], palette="Set2", errorbar=None)
    axes[1, 1].set(title="Comparaison globale", xlabel="")

    plt.tight_layout()
    return fig


# ─── Streamlit ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌤️ Weather Dashboard")

df = load_data()
st.success(f"{len(df)} ligne(s) chargée(s) depuis SQLite")
st.dataframe(df, use_container_width=True)

st.pyplot(make_dashboard(df))
