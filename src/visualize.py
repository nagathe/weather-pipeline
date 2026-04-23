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

logger = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent.parent / "data" / "weather.db"
OUTPUT_DIR = Path(__file__).parent.parent / "data"


def load_data() -> pd.DataFrame:
    """Charge toutes les lignes de weather_raw depuis SQLite."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql("SELECT * FROM weather_raw", conn)


def make_dashboard(df: pd.DataFrame) -> None:
    """
    Construit la figure 2×2, la sauvegarde en PNG et l'affiche.

    Args:
        df : DataFrame issu de load_data().
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

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / "dashboard.png"
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    logger.info("Dashboard sauvegardé : %s", output_path)
    plt.show()
    plt.close(fig)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    df = load_data()
    logger.info("%d ligne(s) chargée(s) depuis SQLite.", len(df))
    make_dashboard(df)
