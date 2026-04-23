import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "weather.db"

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM weather_raw", conn)
    conn.close()
    return df

def make_dashboard(df):
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Weather Dashboard", fontsize=18, fontweight="bold")

    sns.barplot(data=df, x="city", y="temperature", hue="city", legend=False, ax=axes[0,0], palette="coolwarm", errorbar=None)
    axes[0,0].set_title("Température (°C)")
    axes[0,0].set_xlabel("")

    sns.barplot(data=df, x="city", y="humidity", hue="city", legend=False, ax=axes[0,1], palette="Blues_d", errorbar=None)
    axes[0,1].set_title("Humidité (%)")
    axes[0,1].set_xlabel("")

    sns.barplot(data=df, x="city", y="temperature_f", hue="city", legend=False, ax=axes[1,0], palette="Greens_d", errorbar=None)
    axes[1,0].set_title("Température (°F)")
    axes[1,0].set_xlabel("")

    df_melted = df.melt(id_vars="city", value_vars=["temperature", "humidity", "temperature_f"])
    sns.barplot(data=df_melted, x="city", y="value", hue="variable", ax=axes[1,1], palette="Set2", errorbar=None)
    axes[1,1].set_title("Comparaison globale")
    axes[1,1].set_xlabel("")

    plt.tight_layout()
    return fig  # ← c'était ça le bug

st.set_page_config(page_title="Weather Dashboard", layout="wide")
st.title("🌤️ Weather Dashboard")

df = load_data()
st.success(f"{len(df)} ligne(s) chargée(s) depuis SQLite")
st.dataframe(df, width='stretch')

fig = make_dashboard(df)
st.pyplot(fig)
