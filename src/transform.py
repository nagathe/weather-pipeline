"""
transform.py
------------
Module de transformation des données météo brutes en DataFrame propre et typé.

Rôle dans le pipeline ETL :
    extract → TRANSFORM → load

Pipeline interne (dans l'ordre) :
    1. to_dataframe()           — List[Dict] → DataFrame brut
    2. clean_dataframe()        — sélection colonnes, strip, dédoublonnage
    3. cast_types()             — typage explicite de chaque colonne
    4. process_dates()          — dérivation de colonnes temporelles
    5. handle_nulls_and_units() — validation, outliers, conversions, qualité

Dépendances :
    - pandas : manipulation tabulaire
    - numpy  : opérations vectorisées (np.where, np.nan)
"""

import logging
import pandas as pd
import numpy as np
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


# ─── Constantes ──────────────────────────────────────────────────────────────
# Colonnes attendues en sortie d'extract.py (contrat d'interface ETL)
EXPECTED_COLUMNS = ["city", "temperature", "humidity", "weather", "timestamp"]

# Colonnes dérivées du timestamp — exclues du contrôle de nulls primaires
DATETIME_DERIVED = ["date", "hour_utc", "day_of_week"]

# Plages physiquement acceptables (records mondiaux NASA/WMO)
TEMP_MIN, TEMP_MAX         = -89.0,  60.0   # °C
HUMIDITY_MIN, HUMIDITY_MAX =   0.0, 100.0   # %


# ─── 1. JSON → DataFrame brut ────────────────────────────────────────────────

def to_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convertir la liste de dictionnaires issue d'extract.py en DataFrame.

    Args:
        records: Liste de dicts, un par ville (output d'extract).

    Returns:
        DataFrame brut avec autant de lignes que de villes extraites.
        Retourne un DataFrame vide (colonnes EXPECTED_COLUMNS) si records=[].
    """
    if not records:
        logger.warning("Aucune donnée reçue — DataFrame vide retourné.")
        return pd.DataFrame(columns=EXPECTED_COLUMNS)

    df = pd.DataFrame(records)
    logger.info("DataFrame créé : %d ligne(s), %d colonne(s).", df.shape[0], df.shape[1])
    return df


# ─── 2. Nettoyage des données ────────────────────────────────────────────────

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie le DataFrame brut :
        - Vérifie la présence des colonnes attendues (lève ValueError sinon)
        - Sélectionne uniquement les colonnes EXPECTED_COLUMNS
        - Normalise les chaînes (strip des espaces)
        - Supprime les doublons (city + timestamp)

    Args:
        df: DataFrame brut issu de to_dataframe().

    Returns:
        DataFrame nettoyé, réindexé.

    Raises:
        ValueError: Si une ou plusieurs colonnes attendues sont absentes.
    """
    if df.empty:
        return df

    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        # Erreur bloquante : le contrat d'interface ETL n'est pas respecté
        raise ValueError(f"Colonnes manquantes : {missing_cols}")

    # Conservations seulement des colonnes utiles (élimine les champs API non prévus)
    df = df[EXPECTED_COLUMNS].copy()

    # Normalisation des chaînes : éviter les doublons invisibles ("Paris " vs "Paris")
    for col in ["city", "weather"]:
        df[col] = df[col].astype(str).str.strip()

    # Dédoublonnage : même ville au même instant = même mesure
    before = len(df)
    df = df.drop_duplicates(subset=["city", "timestamp"]).reset_index(drop=True)
    removed = before - len(df)
    if removed:
        logger.info("%d doublon(s) supprimé(s).", removed)

    logger.info("Nettoyage OK — %d ligne(s) conservée(s).", len(df))
    return df


# ─── 3. Cast des types ───────────────────────────────────────────────────────

def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique un typage explicite à chaque colonne.

    Conversions appliquées :
        - temperature → float64        (errors="coerce" : invalide → NaN)
        - humidity    → float64        (errors="coerce" : invalide → NaN)
        - timestamp   → Int64 nullable (errors="coerce" : invalide → pd.NA)
        - city        → pd.StringDtype (nullable string, plus léger qu'object)
        - weather     → pd.StringDtype

    Notes:
        - Int64 (majuscule) est le type entier nullable de pandas,
          contrairement à int64 qui ne supporte pas les NaN.
        - errors="coerce" garantit qu'une valeur non castable devient NaN/NA
          plutôt que de lever une exception.

    Args:
        df: DataFrame nettoyé issu de clean_dataframe().

    Returns:
        DataFrame avec types explicites.
    """
    if df.empty:
        return df

    df = df.copy()
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce").astype(float)
    df["humidity"]    = pd.to_numeric(df["humidity"],    errors="coerce").astype(float)
    df["timestamp"]   = pd.to_numeric(df["timestamp"],   errors="coerce").astype("Int64")
    df["city"]        = df["city"].astype(pd.StringDtype())
    df["weather"]     = df["weather"].astype(pd.StringDtype())

    logger.info("Cast des types OK.")
    return df


# ─── 4. Dates ────────────────────────────────────────────────────────────────

def process_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dérive des colonnes temporelles à partir du timestamp Unix.

    Colonnes créées :
        - datetime_utc : Timestamp pandas timezone-aware (UTC)
        - date         : Date seule (datetime.date)
        - hour_utc     : Heure UTC (0–23), Int64 nullable
        - day_of_week  : Nom du jour en anglais (ex: "Monday")

    Notes:
        - utc=True assure que datetime_utc est toujours en UTC,
          sans ambiguïté lors d'un éventuel changement d'heure.
        - errors="coerce" transforme les timestamps invalides en NaT.

    Args:
        df: DataFrame typé issu de cast_types().

    Returns:
        DataFrame enrichi des colonnes temporelles.
    """
    if df.empty:
        return df

    df = df.copy()

    # Conversion timestamp Unix (secondes) → datetime timezone-aware UTC
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True, errors="coerce")

    # Colonnes dérivées pour faciliter les analyses temporelles en aval
    df["date"]        = df["datetime_utc"].dt.date
    df["hour_utc"]    = df["datetime_utc"].dt.hour.astype("Int64")
    df["day_of_week"] = df["datetime_utc"].dt.day_name().astype(pd.StringDtype())

    logger.info("Dates traitées OK.")
    return df


# ─── 5. Nulls, outliers, unités et qualité ───────────────────────────────────

def handle_nulls_and_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Étape finale de validation et d'enrichissement :
        1. Détection des valeurs nulles sur colonnes primaires (warning)
        2. Remplacement des températures hors plage par NaN
        3. Remplacement des humidités hors plage par NaN
        4. Ajout de la colonne temperature_f (°C → °F)
        5. Ajout de la colonne data_quality ("ok" / "incomplete")

    Plages acceptées (constantes du module) :
        - Température : [TEMP_MIN, TEMP_MAX]     = [-89.0, 60.0] °C
        - Humidité    : [HUMIDITY_MIN, HUMIDITY_MAX] = [0.0, 100.0] %

    Colonne data_quality :
        - "ok"         : toutes les colonnes critiques sont renseignées
        - "incomplete" : au moins une colonne critique est nulle
        Colonnes critiques : city, temperature, humidity, datetime_utc

    Args:
        df: DataFrame issu de process_dates().

    Returns:
        DataFrame final prêt pour le chargement (load.py).
    """
    if df.empty:
        return df

    df = df.copy()

    # ── 1. Audit des nulls (colonnes primaires uniquement) ──
    primary_cols = [c for c in df.columns if c not in DATETIME_DERIVED]
    null_counts  = df[primary_cols].isnull().sum()
    if null_counts.any():
        logger.warning(
            "Valeurs nulles sur colonnes primaires :\n%s",
            null_counts[null_counts > 0].to_string(),
        )

    # ── 2. Outliers température ──
    # notna() évite de traiter les NaN existants comme des outliers
    temp_mask = df["temperature"].notna() & ~df["temperature"].between(TEMP_MIN, TEMP_MAX, inclusive="both")
    if temp_mask.any():
        logger.warning("%d température(s) aberrante(s) → NaN", temp_mask.sum())
        df.loc[temp_mask, "temperature"] = np.nan

    # ── 3. Outliers humidité ──
    hum_mask = df["humidity"].notna() & ~df["humidity"].between(HUMIDITY_MIN, HUMIDITY_MAX, inclusive="both")
    if hum_mask.any():
        logger.warning("%d humidité(s) aberrante(s) → NaN", hum_mask.sum())
        df.loc[hum_mask, "humidity"] = np.nan

    # ── 4. Conversion °C → °F (arrondie à 2 décimales) ──
    df["temperature_f"] = (df["temperature"] * 9 / 5 + 32).round(2)

    # ── 5. Indicateur qualité ligne ──
    # np.where est plus performant qu'un .apply() ligne par ligne
    critical_cols = ["city", "temperature", "humidity", "datetime_utc"]
    df["data_quality"] = pd.Series(
        np.where(df[critical_cols].isnull().any(axis=1), "incomplete", "ok"),
        dtype=pd.StringDtype(),
    )

    logger.info("Gestion nulls & unités OK.")
    return df


# ─── Pipeline transformation complet ─────────────────────────────────────────

def transform(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Point d'entrée du module — orchestre les 5 étapes de transformation.

    Args:
        records (List[Dict[str, Any]]): Output brut d'extract.py,
            liste de dicts avec les clés : city, temperature, humidity,
            weather, timestamp, datetime.

    Returns:
        pd.DataFrame: DataFrame propre, typé et enrichi, prêt pour load.py.
        Colonnes finales :
            city, temperature, humidity, weather, timestamp,
            datetime_utc, date, hour_utc, day_of_week,
            temperature_f, data_quality

    Notes:
        - Si records est vide, retourne un DataFrame vide sans erreur.
        - Lève ValueError si les colonnes attendues sont absentes.
    """
    logger.info("Transform — début du pipeline.")

    df = to_dataframe(records)           # 1. List[Dict] → DataFrame
    df = clean_dataframe(df)             # 2. Sélection, strip, dédoublonnage
    df = cast_types(df)                  # 3. Typage explicite
    df = process_dates(df)               # 4. Colonnes temporelles
    df = handle_nulls_and_units(df)      # 5. Validation, unités, qualité

    logger.info("Transform terminé — %d ligne(s).", len(df))
    return df