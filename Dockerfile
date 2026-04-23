# Utiliser une image Python légère
FROM python:3.11-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les requirements
COPY requirements.txt .

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier tout le code source
COPY src/ ./src/

# Créer les répertoires pour les données
RUN mkdir -p /app/data

# Commande par défaut (à adapter selon votre main.py)
CMD ["python", "src/main.py"]
