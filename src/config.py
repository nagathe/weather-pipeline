"""
config.py
---------
Chargement et exposition des variables d'environnement nécessaires à l'application.
Les valeurs sensibles (clé API, etc.) doivent être définies dans un fichier `.env`.
"""

import logging
import os

from dotenv import load_dotenv

# ─── Logging ────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

# ─── Chargement du .env ─────────────────────────────────────────────────────
load_dotenv()
logger.debug("Variables d'environnement chargées depuis .env")

# ─── Variables de configuration ─────────────────────────────────────────────
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

# ─── Vérification des variables de config ───────────────────────────────────
if not API_KEY:
    logger.error("API_KEY manquante — vérifiez votre fichier .env")
    raise EnvironmentError("API_KEY est requise mais n'est pas définie.")

if not BASE_URL:
    logger.error("BASE_URL manquante — vérifiez votre fichier .env")
    raise EnvironmentError("BASE_URL est requise mais n'est pas définie.")

logger.debug("Configuration validée : API_KEY ✅ | BASE_URL ✅")
