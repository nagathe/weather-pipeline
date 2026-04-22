import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import List, Dict, Any


# ─────────────────────────────────────────────
# 1. JSON → DataFrame brut
# ─────────────────────────────────────────────

def to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convertit une liste de dicts (output d'extract.py) en DataFrame brut.
    Retourne un DataFrame vide structuré si la liste est vide.
    """
    if not records:
        print("[WARN] Aucune donnée reçue — DataFrame vide retourné.")
        return pd.DataFrame(columns=[
            "city", "temperature", "humidity",
            "weather", "timestamp", "datetime"
        ])

    df = pd.DataFrame(records)
    print(f"[INFO] DataFrame créé : {df.shape[0]} ligne(s), {df.shape[1]} colonne(s).")
    return df


# ─────────────────────────────────────────────
# 2. Nettoyage : colonnes, noms, types
# ─────────────────────────────────────────────

EXPECTED_COLUMNS = ["city", "temperature", "humidity", "weather", "timestamp", "datetime"]

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Vérifie la présence des colonnes attendues
    - Renomme proprement
    - Supprime les doublons
    - Strip les strings
    """
    if df.empty:
        return df

    # Vérification des colonnes attendues
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"[ERROR] Colonnes manquantes dans le DataFrame : {missing_cols}")

    # Garde uniquement les colonnes utiles (dans le bon ordre)
    df = df[EXPECTED_COLUMNS].copy()

    # Strip espaces sur les colonnes texte
    str_cols = ["city", "weather"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # Suppression des doublons éventuels (même ville + même timestamp)
    before = len(df)
    df = df.drop_duplicates(subset=["city", "timestamp"])
    after = len(df)
    if before != after:
        print(f"[INFO] {before - after} doublon(s) supprimé(s).")

    # Reset index propre
    df = df.reset_index(drop=True)

    print(f"[INFO] Nettoyage OK — {len(df)} ligne(s) conservée(s).")
    return df


# ─────────────────────────────────────────────
# 3. Cast des types  — CORRIGÉ
# ─────────────────────────────────────────────

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les bons types à chaque colonne.
    """
    if df.empty:
        return df

    df = df.copy()

    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").astype(float)
    df["humidity"]    = pd.to_numeric(df["humidity"],    errors="coerce").astype(float)
    df["timestamp"]   = pd.to_numeric(df["timestamp"],   errors="coerce").astype("Int64")
    df["city"]        = df["city"].astype(pd.StringDtype())
    df["weather"]     = df["weather"].astype(pd.StringDtype())

    print("[INFO] Cast des types OK.")
    return df


# ─────────────────────────────────────────────
# 4. Gestion des dates  — CORRIGÉ
# ─────────────────────────────────────────────

def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconstruit datetime_utc depuis timestamp et ajoute colonnes dérivées.
    """
    if df.empty:
        return df

    df = df.copy()

    df["datetime_utc"] = pd.to_datetime(
        df["timestamp"],
        unit="s",
        utc=True,
        errors="coerce"
    )

    df["date"]        = df["datetime_utc"].dt.date          # garde type date Python
    df["hour_utc"]    = df["datetime_utc"].dt.hour.astype("Int64")   # ← CORRIGÉ : Int64 nullable
    df["day_of_week"] = df["datetime_utc"].dt.day_name().astype(pd.StringDtype())  # ← CORRIGÉ

    df = df.drop(columns=["datetime"], errors="ignore")

    print("[INFO] Dates traitées OK.")
    return df


# ─────────────────────────────────────────────
# 5. Nulls & unités  — CORRIGÉ
# ─────────────────────────────────────────────

TEMP_MIN = -89.0
TEMP_MAX =  60.0
HUMIDITY_MIN =   0.0
HUMIDITY_MAX = 100.0

# Colonnes dérivées de datetime_utc — nulles par cascade, pas à loguer
DATETIME_DERIVED = {"date", "hour_utc", "day_of_week"}

def handle_nulls_and_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Loggue les nulls sur colonnes primaires uniquement
    - Valeurs aberrantes -> NaN (hors plages physiques, NaN exclus du test)
    - Ajoute temperature_f et data_quality
    """
    if df.empty:
        return df

    df = df.copy()

    # -- Valeurs nulles — colonnes primaires seulement --
    primary_cols = [c for c in df.columns if c not in DATETIME_DERIVED]
    null_counts  = df[primary_cols].isnull().sum()
    if null_counts.any():
        print("[WARN] Valeurs nulles sur colonnes primaires :")
        print(null_counts[null_counts > 0].to_string())

    # -- Temperatures aberrantes --
    temp_valid = df["temperature"].notna()
    temp_mask  = temp_valid & ~df["temperature"].between(
        TEMP_MIN, TEMP_MAX, inclusive="both"
    )
    if temp_mask.any():
        print(f"[WARN] {temp_mask.sum()} temperature(s) aberrante(s) -> NaN")
        df.loc[temp_mask, "temperature"] = np.nan

    # -- Humidites aberrantes --
    hum_valid = df["humidity"].notna()
    hum_mask  = hum_valid & ~df["humidity"].between(
        HUMIDITY_MIN, HUMIDITY_MAX, inclusive="both"
    )
    if hum_mask.any():
        print(f"[WARN] {hum_mask.sum()} humidite(s) aberrante(s) -> NaN")
        df.loc[hum_mask, "humidity"] = np.nan

    # -- Conversion unite --
    df["temperature_f"] = (df["temperature"] * 9 / 5 + 32).round(2)

    # -- Flag qualite — cast sur Series pandas, pas sur ndarray numpy --
    critical_cols = ["city", "temperature", "humidity", "datetime_utc"]
    df["data_quality"] = pd.Series(
        np.where(
            df[critical_cols].isnull().any(axis=1),
            "incomplete",
            "ok"
        ),
        dtype=pd.StringDtype()
    )

    print("[INFO] Gestion nulls & unites OK.")
    return df


# ─────────────────────────────────────────────
# Pipeline complet
# ─────────────────────────────────────────────

def transform(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Pipeline de transformation complet.
    Input  : List[Dict] — output brut d'extract.py
    Output : pd.DataFrame propre et typé
    """
    print("\n" + "="*50)
    print("  TRANSFORM — début du pipeline")
    print("="*50)

    df = to_dataframe(records)
    df = clean_dataframe(df)
    df = cast_types(df)
    df = process_dates(df)
    df = handle_nulls_and_units(df)

    print("\n[INFO] Transform terminé.")
    print("="*50 + "\n")
    return df


# ─────────────────────────────────────────────
# Main — test standalone
# ─────────────────────────────────────────────

if __name__ == "__main__":
    # Simule l'output d'extract.py pour tester sans appel API
    mock_records = [
        {
            "city": "Paris",
            "temperature": 18.5,
            "humidity": 72,
            "weather": "light rain",
            "timestamp": 1717000000,
            "datetime": "2024-05-29T12:26:40",
        },
        {
            "city": "London",
            "temperature": 14.2,
            "humidity": 85,
            "weather": "overcast clouds",
            "timestamp": 1717000100,
            "datetime": "2024-05-29T12:28:20",
        },
        {
            "city": "Berlin",
            "temperature": None,      # valeur manquante volontaire
            "humidity": 60,
            "weather": "clear sky",
            "timestamp": 1717000200,
            "datetime": "2024-05-29T12:30:00",
        },
        {
            "city": "Madrid",
            "temperature": 999.0,     # valeur aberrante volontaire
            "humidity": 40,
            "weather": "sunny",
            "timestamp": None,        # timestamp manquant
            "datetime": None,
        },
        # Doublon de Paris
        {
            "city": "Paris",
            "temperature": 18.5,
            "humidity": 72,
            "weather": "light rain",
            "timestamp": 1717000000,
            "datetime": "2024-05-29T12:26:40",
        },
    ]

    df_result = transform(mock_records)

    print("\n### DataFrame final :\n")
    print(df_result.to_string(index=False))

    print("\n### Types :\n")
    print(df_result.dtypes)

    print("\n### Data quality :\n")
    print(df_result[["city", "temperature", "humidity", "datetime_utc", "data_quality"]])
