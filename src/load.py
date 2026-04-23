"""
load.py — Chargement des données transformées vers CSV et SQLite.

Pipeline : extract → transform → LOAD

Cibles :
    - CSV    : fichier horodaté dans /data/ (archivage)
    - SQLite : table 'weather_raw' en append dans /data/weather.db
"""

import logging
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR   = Path(__file__).parent.parent / "data"
DB_PATH    = DATA_DIR / "weather.db"
TABLE_NAME = "weather_raw"


# ─── 1. CSV ──────────────────────────────────────────────────────────────────

def load_csv(df: pd.DataFrame, prefix: str = "weather") -> Path:
    """
    Sauvegarde le DataFrame en CSV horodaté dans DATA_DIR.

    Args:
        df     : DataFrame à exporter.
        prefix : Préfixe du nom de fichier.

    Returns:
        Path : Chemin du fichier créé.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath  = DATA_DIR / f"{prefix}_{timestamp}.csv"

    df.to_csv(filepath, index=False)
    logger.info("CSV sauvegardé : %s", filepath)
    return filepath


# ─── 2. SQLite ───────────────────────────────────────────────────────────────

def load_sqlite(df: pd.DataFrame, db_path: Path = DB_PATH) -> None:
    """
    Insère le DataFrame dans la table SQLite en mode append.

    Args:
        df      : DataFrame à insérer.
        db_path : Chemin de la base SQLite.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df_sql = _prepare_for_sqlite(df)

    with sqlite3.connect(db_path) as conn:
        _create_table_if_not_exists(conn)
        df_sql.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]

    logger.info("SQLite : %d ligne(s) insérée(s) → %s", len(df_sql), db_path)
    logger.info("SQLite : total table '%s' = %d ligne(s).", TABLE_NAME, count)


def _prepare_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convertit les types pandas non supportés par SQLite.

    Notes:
        - StringDtype → object  : to_sql() ne gère pas StringDtype
        - Int64 → float         : préserve les NaN (int natif les rejette)
        - datetimetz → str      : SQLite n'a pas de type datetime natif
    """
    df = df.copy()

    for col in df.select_dtypes(include="string").columns:
        df[col] = df[col].astype(object)

    for col in df.select_dtypes(include="Int64").columns:
        df[col] = df[col].astype(float)

    for col in df.select_dtypes(include="datetimetz").columns:
        df[col] = df[col].astype(str)

    if "date" in df.columns:
        df["date"] = df["date"].astype(str)  # date non détecté par select_dtypes

    return df


def _create_table_if_not_exists(conn: sqlite3.Connection) -> None:
    """Crée la table weather_raw si elle n'existe pas encore."""
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            batch_id      INTEGER,
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


# ─── 3. Orchestration ────────────────────────────────────────────────────────

def load(df: pd.DataFrame, batch_id: int = 1) -> None:
    """
    Point d'entrée du module — charge le DataFrame vers CSV et SQLite.

    Args:
        df       : DataFrame final issu de transform.py.
        batch_id : Identifiant du run, permet de filtrer les lignes
                   d'une même exécution (WHERE batch_id = n).
    """
    if df.empty:
        logger.warning("DataFrame vide — rien à charger.")
        return

    df = df.copy()
    df.insert(0, "batch_id", batch_id)

    logger.info("Load — début (batch_id=%d).", batch_id)
    load_csv(df)
    load_sqlite(df)
    logger.info("Load — terminé (batch_id=%d).", batch_id)
