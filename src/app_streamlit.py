"""
app_streamlit.py — Dashboard météo interactif.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "weather.db"


def load_data() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM weather_raw", conn)
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def make_dashboard(df: pd.DataFrame) -> plt.Figure:
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.suptitle("Weather Dashboard", fontsize=18, fontweight="bold")

    palette = sns.color_palette("tab10", n_colors=df["city"].nunique())

    # ── [0,0] Température moyenne par ville ─────────────────────────────────
    ax = axes[0, 0]
    temp_mean = df.groupby("city")["temperature"].mean().reset_index()
    temp_mean = temp_mean.sort_values("temperature", ascending=False)
    sns.barplot(data=temp_mean, x="city", y="temperature",
                hue="city", palette=palette, legend=False,
                ax=ax, errorbar=None)
    ax.set(title="Température moyenne par ville (°C)", xlabel="", ylabel="°C")
    ax.tick_params(axis="x", rotation=30)

    # ── [0,1] Conditions météo par ville ────────────────────────────────────
    ax = axes[0, 1]
    weather_counts = (
        df.groupby(["city", "weather"])
        .size()
        .reset_index(name="count")
    )
    sns.barplot(data=weather_counts, x="city", y="count", hue="weather",
                ax=ax, palette="Set2")
    ax.set(title="Conditions météo par ville", xlabel="", ylabel="Nb de relevés")
    ax.tick_params(axis="x", rotation=30)
    ax.legend(title="Condition", bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)

    # ── [1,0] Humidité moyenne par ville ────────────────────────────────────
    ax = axes[1, 0]
    hum_mean = df.groupby("city")["humidity"].mean().sort_values(ascending=False).reset_index()
    sns.barplot(data=hum_mean, x="city", y="humidity",
                hue="city", palette=palette, legend=False,
                ax=ax, errorbar=None)
    ax.set(title="Humidité moyenne par ville (%)", xlabel="", ylabel="%")
    ax.tick_params(axis="x", rotation=30)
    ax.set_ylim(0, 100)

    # ── [1,1] Température vs Humidité ───────────────────────────────────────
    ax = axes[1, 1]
    sns.scatterplot(data=df, x="temperature", y="humidity",
                    hue="city", palette="tab10", s=120, ax=ax)
    for _, row in df.iterrows():
        ax.annotate(row["weather"], (row["temperature"], row["humidity"]),
                    fontsize=6, alpha=0.7,
                    xytext=(4, 4), textcoords="offset points")
    ax.set(title="Température vs Humidité", xlabel="°C", ylabel="%")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)

    plt.tight_layout()
    return fig


# ─── Streamlit ───────────────────────────────────────────────────────────────

st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌤️ Weather Dashboard")

df = load_data()
st.success(f"{len(df)} ligne(s) chargée(s) depuis SQLite")
st.dataframe(df, width="stretch")

st.pyplot(make_dashboard(df))
