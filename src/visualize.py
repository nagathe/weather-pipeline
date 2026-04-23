import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "weather.db"
OUTPUT_DIR = Path(__file__).parent.parent / "data"

def load_data():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM weather_raw", conn)
    conn.close()
    return df

def make_dashboard(df):
    sns.set_theme(style="darkgrid")
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Weather Dashboard", fontsize=18, fontweight="bold")

    # 1 — Température °C
    # errorbar=None : pas de barres d'erreur (une seule mesure par ville)
    sns.barplot(data=df, x="city", y="temperature", hue="city", legend=False, ax=axes[0,0], palette="coolwarm", errorbar=None)
    axes[0,0].set_title("Température (°C)")
    axes[0,0].set_xlabel("")

    # 2 — Humidité
    # errorbar=None : pas de barres d'erreur (une seule mesure par ville)
    sns.barplot(data=df, x="city", y="humidity", hue="city", legend=False, ax=axes[0,1], palette="Blues_d", errorbar=None)
    axes[0,1].set_title("Humidité (%)")
    axes[0,1].set_xlabel("")

    # 3 — Température °F
    # errorbar=None : pas de barres d'erreur (une seule mesure par ville)
    sns.barplot(data=df, x="city", y="temperature_f", hue="city", legend=False, ax=axes[1,0], palette="Greens_d", errorbar=None)
    axes[1,0].set_title("Température (°F)")
    axes[1,0].set_xlabel("")

    # 4 — Comparaison globale : on fusionne les 3 métriques en une seule colonne
    # melt() transforme les colonnes en lignes pour pouvoir les comparer côte à côte
    # errorbar=None : cohérence avec les autres graphiques
    df_melted = df.melt(id_vars="city", value_vars=["temperature", "humidity", "temperature_f"])
    sns.barplot(data=df_melted, x="city", y="value", hue="variable", ax=axes[1,1], palette="Set2", errorbar=None)
    axes[1,1].set_title("Comparaison globale")
    axes[1,1].set_xlabel("")

    plt.tight_layout()
    output_path = OUTPUT_DIR / "dashboard.png"
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"[INFO] Dashboard sauvegardé : {output_path}")
    plt.show()

if __name__ == "__main__":
    df = load_data()
    print(f"[INFO] {len(df)} ligne(s) chargée(s) depuis SQLite")
    make_dashboard(df)
