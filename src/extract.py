"""
extract.py
----------
Module d'extraction des données météo depuis l'API OpenWeatherMap.

Rôle dans le pipeline ETL :
    EXTRACT → transform → load

Dépendances :
    - requests  : appels HTTP vers l'API
    - config.py : API_KEY et BASE_URL (ne pas versionner la clé !)
"""

import logging
from datetime import datetime
from typing import Any, Dict, List

import requests

from config import API_KEY, BASE_URL

# ─── Logging ─────────────────────────────────────────────────────────────────
# Le logger hérite de la configuration définie dans main.py (basicConfig).
# En exécution directe, le bloc __main__ configure son propre handler.
logger = logging.getLogger(__name__)


# ─── Extraction ──────────────────────────────────────────────────────────────
def extract(cities: List[str]) -> List[Dict[str, Any]]:
    """
    Interroge l'API OpenWeatherMap pour chaque ville de la liste.

    Args:
        cities (List[str]): Liste de noms de villes (ex: ["Paris", "London"]).

    Returns:
        List[Dict[str, Any]]: Liste de dictionnaires structurés contenant :
            - city        : nom de la ville retourné par l'API
            - temperature : température en °C (unités métriques)
            - humidity    : humidité relative en %
            - weather     : description météo (ex: "light rain")
            - timestamp   : timestamp Unix UTC fourni par l'API
            - datetime    : timestamp converti au format ISO 8601 (UTC)

    Notes:
        - Les erreurs par ville sont loggées sans interrompre le pipeline.
        - Une ville en erreur est simplement absente de la liste retournée.
        - Le timeout par requête est fixé à 5 secondes.
    """
    results: List[Dict[str, Any]] = []
    total = len(cities)

    logger.info("Début de l'extraction — %d ville(s) à traiter.", total)

    for index, city in enumerate(cities, start=1):
        logger.info("[%d/%d] Extraction en cours : %s", index, total, city)

        try:
            # Construction de l'URL avec les paramètres obligatoires :
            #   q     → nom de la ville
            #   appid → clé API personnelle
            #   units → "metric" pour obtenir °C (sinon Kelvin par défaut)
            url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=5)
            response.raise_for_status()  # Lève HTTPError si status >= 400

            data = response.json()

            # Extraction défensive : .get() évite un KeyError si le champ
            # est absent (ex: API qui change de structure, ville partielle)
            weather_list: List[Dict[str, Any]] = data.get("weather") or [{}]
            main_data: Dict[str, Any] = data.get("main") or {}
            timestamp = data.get("dt")  # Timestamp Unix UTC

            result = {
                "city":        data.get("name"),
                "temperature": main_data.get("temp"),
                "humidity":    main_data.get("humidity"),
                "weather":     weather_list[0].get("description"),
                "timestamp":   timestamp,
                # Conversion timestamp → ISO 8601, None si champ absent
                "datetime":    datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None,
            }

            results.append(result)
            logger.info(
                "[%d/%d] ✅ %s — %.1f°C, %s",
                index, total, city, result["temperature"], result["weather"],
            )

        except requests.HTTPError as e:
            # Ex: 404 ville introuvable, 401 clé API invalide
            logger.error("[%d/%d] ❌ Erreur HTTP pour '%s' : %s", index, total, city, e)
        except requests.Timeout:
            # L'API n'a pas répondu dans le délai imparti (5 s)
            logger.error("[%d/%d] ⏱️  Timeout pour '%s'", index, total, city)
        except requests.RequestException as e:
            # Erreur réseau générique (DNS, connexion refusée, etc.)
            logger.error("[%d/%d] ❌ Erreur réseau pour '%s' : %s", index, total, city, e)
        except Exception as e:
            # Filet de sécurité : erreur inattendue (parsing JSON, etc.)
            logger.exception("[%d/%d] 💥 Erreur inattendue pour '%s' : %s", index, total, city, e)

    logger.info(
        "Extraction terminée — %d/%d ville(s) récupérée(s).",
        len(results), total,
    )
    return results


# ─── Exécution modul extract (debug) ───────────────────────────────────────────────
# En cas de test du module d'extraction seul

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    cities = ["Paris", "London", "Berlin", "Madrid"]
    results = extract(cities)

    print("\n─── Résultats ───────────────────────────────────")
    for r in results:
        print(r)
    print("─────────────────────────────────────────────────")
