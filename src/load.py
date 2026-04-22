# load.py
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DATA_DIR   = Path(__file__).parent.parent / "data"
DB_PATH    = DATA_DIR / "weather.db"
TABLE_NAME = "weather_raw"


# ─────────────────────────────────────────────
# 1. CSV
# ─────────────────────────────────────────────

def load_csv(df: pd.DataFrame, prefix: str = "weather") -> Path:
    """
    Sauvegarde le DataFrame en CSV horodaté dans /data.
    Retourne le chemin du fichier créé.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath   = DATA_DIR / f"{prefix}_{timestamp}.csv"

    df.to_csv(filepath, index=False)
    print(f"[INFO] CSV sauvegardé : {filepath}")
    return filepath


# ─────────────────────────────────────────────
# 2. SQLite
# ─────────────────────────────────────────────

def load_sqlite(df: pd.DataFrame, db_path: Path = DB_PATH) -> None:
    """
    Insère le DataFrame dans une table SQLite.
    - Crée la table si elle n'existe pas (CREATE IF NOT EXISTS)
    - Append les nouvelles lignes (pas d'écrasement)
    - Convertit les types pandas non supportés par SQLite
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df_sql = _prepare_for_sqlite(df)

    with sqlite3.connect(db_path) as conn:
        _create_table_if_not_exists(conn)
        df_sql.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

    print(f"[INFO] SQLite : {len(df_sql)} ligne(s) insérée(s) → {db_path}")
    print(f"[INFO] SQLite : total table '{TABLE_NAME}' = {count} ligne(s)")


def _prepare_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """
    SQLite ne supporte pas :
    - pd.StringDtype  → cast en str natif Python
    - pd.Int64Dtype   → cast en float (NaN compatibles)
    - datetime avec tz → cast en string ISO
    - date Python      → cast en string
    """
    df = df.copy()

    # String pandas → str
    for col in df.select_dtypes(include="string").columns:
        df[col] = df[col].astype(object)

    # Int64 nullable → float (SQLite stocke NULL, pas NA)
    for col in df.select_dtypes(include="Int64").columns:
        df[col] = df[col].astype(float)

    # datetime avec timezone → string ISO
    for col in df.select_dtypes(include="datetimetz").columns:
        df[col] = df[col].astype(str)

    # date Python (dtype object contenant des date) → string
    if "date" in df.columns:
        df["date"] = df["date"].astype(str)

    return df


def _create_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """
    Crée la table weather_raw si elle n'existe pas déjà.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            city          TEXT,
            temperature   REAL,
            humidity      REAL,
            weather       TEXT,
            timestamp     REAL,
            datetime_utc  TEXT,
            date          TEXT,
            hour_utc      REAL,
            day_of_week   TEXT,
            temperature_f REAL,
            data_quality  TEXT,
            loaded_at     TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        )
    """)
    conn.commit()


# ─────────────────────────────────────────────
# 3. Orchestration
# ─────────────────────────────────────────────

def load(df: pd.DataFrame) -> None:
    """
    Point d'entrée du module Load.
    Lance CSV + SQLite en séquence.
    """
    if df.empty:
        print("[WARN] DataFrame vide — rien à charger.")
        return

    print("\n" + "=" * 50)
    print("  LOAD — début")
    print("=" * 50)

    load_csv(df)
    load_sqlite(df)

    print("=" * 50)
    print("  LOAD — terminé")
    print("=" * 50 + "\n")


# ─────────────────────────────────────────────
# 4. TEST LOCAL
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from transform import transform

    mock_records = [
        {"city": "Paris",  "temperature": 18.5, "humidity": 72, "weather": "light rain",
         "timestamp": 1717000000, "datetime": "2024-05-29T16:26:40"},
        {"city": "London", "temperature": 14.2, "humidity": 85, "weather": "overcast clouds",
         "timestamp": 1717000100, "datetime": "2024-05-29T16:28:20"},
        {"city": "Berlin", "temperature": None, "humidity": 60, "weather": "clear sky",
         "timestamp": 1717000200, "datetime": "2024-05-29T16:30:00"},
        {"city": "Madrid", "temperature": 999.0,"humidity": 40, "weather": "sunny",
         "timestamp": None,       "datetime": None},
    ]

    df_clean = transform(mock_records)
    load(df_clean)
