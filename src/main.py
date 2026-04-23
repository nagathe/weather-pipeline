"""
main.py — Point d'entrée du pipeline ETL météo.

Pipeline : extract → transform → load
"""

import logging
from extract   import extract
from transform import transform
from load      import load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline(cities: list[str], batch_id: int = 1) -> None:
    """
    Orchestre le pipeline ETL complet.

    Args:
        cities   : Villes à interroger.
        batch_id : Identifiant du run (défaut = 1).
    """
    logger.info("Pipeline météo — démarrage (%d villes, batch_id=%d).", len(cities), batch_id)

    records = extract(cities)
    if not records:
        logger.error("Aucune donnée extraite — pipeline arrêté.")
        return
    logger.info("%d enregistrement(s) extrait(s).", len(records))

    df_clean = transform(records)
    if df_clean.empty:
        logger.error("DataFrame vide après transform — pipeline arrêté.")
        return
    logger.info("Transform OK — %d ligne(s) valide(s).", len(df_clean))

    load(df_clean, batch_id=batch_id)
    logger.info("Pipeline — terminé avec succès.")


if __name__ == "__main__":
    CITIES = [
        "Paris", "London", "Berlin", "Madrid", "Tokyo",
        "New York", "Dubai", "Sydney", "São Paulo", "Cairo",
    ]
    run_pipeline(CITIES, batch_id=1)
