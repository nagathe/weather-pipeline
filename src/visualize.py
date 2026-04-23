"""
visualize.py — Génération et sauvegarde du dashboard météo.

Lit la table 'weather_raw' depuis SQLite, produit une figure
Matplotlib/Seaborn 2×2 et la sauvegarde en PNG dans /data/.
"""

import logging
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

#
import matplotlib
matplotlib.use("MacOSX")
#
logger = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent.parent / "data" / "weather.db"
OUTPUT_DIR = Path(__file__).parent.parent / "data"


def load_data() -> pd.DataFrame:
    """Charge toutes les lignes de weather_raw depuis SQLite."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM weather_raw", conn)

    # Parsing correct des colonnes temporelles
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True)
    df["date"] = pd.to_datetime(df["date"])          # format YYYY-MM-DD → OK
    return df


def make_dashboard(df: pd.DataFrame) -> None:
    """
    Construit la figure 2×2, la sauvegarde en PNG et l'affiche.

    Graphiques :
        [0,0] Température par ville          — boxplot (snapshot du jour)
        [0,1] Conditions météo               — nb de relevés par condition et par ville
        [1,0] Humidité par ville             — barplot moyen
        [1,1] Température vs Humidité        — scatter coloré par condition météo

    Args:
        df : DataFrame issu de load_data().
    """
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(16, 11))
    fig.suptitle("Weather Dashboard", fontsize=18, fontweight="bold")

    palette = sns.color_palette("tab10", n_colors=df["city"].nunique())

    # ── [0,0] Température par ville ─────────────────────────────────────────
    ax = axes[0, 0]
    order = df.groupby("city")["temperature"].mean().sort_values(ascending=False).index
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
    
    # ── [1,1] Température vs Humidité ────────────────────────────────────────
    ax = axes[1, 1]
    sns.scatterplot(data=df, x="temperature", y="humidity",
                    hue="city",
                    palette="tab10", s=120, ax=ax)
    # Annoter chaque point avec la condition météo
    for _, row in df.iterrows():
        ax.annotate(row["weather"], (row["temperature"], row["humidity"]),
                    fontsize=6, alpha=0.7,
                    xytext=(4, 4), textcoords="offset points")
    ax.set(title="Température vs Humidité", xlabel="°C", ylabel="%")
    ax.legend(bbox_to_anchor=(1.01, 1), loc="upper left", fontsize=8)

    plt.tight_layout()
    output_path = OUTPUT_DIR / "dashboard.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Dashboard sauvegardé → %s", output_path)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    df = load_data()
    logger.info("%d ligne(s) chargée(s) depuis SQLite.", len(df))
    make_dashboard(df)
