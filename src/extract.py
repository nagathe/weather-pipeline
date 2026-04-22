import requests

from datetime import datetime
from config import API_KEY, BASE_URL


def extract_weather(city: str) -> dict:
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"

    # Récupérer la réponse de la requête http GET sur le serveur de OpenWeatherMap
    response = requests.get(url)
    # Lever une exception en cas d'échec de la requête
    response.raise_for_status()

    # Parser la réponse json en dictionnaire python
    data = response.json()
    weather_list = data.get("weather") or [{}] #après le 'or' : si 'weather' est vide, alors retourner une liste avec un dict vide

    return {
        "city": data.get("name"),
        "temperature": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
        "weather": weather_list[0].get("description"),
        "timestamp": data.get("dt"),
        "datetime": datetime.utcfromtimestamp(data.get("dt")).isoformat()
    }



if __name__ == "__main__":
    cities = ["Paris", "London", "Berlin", "Madrid"]

    results = []

    for city in cities:
        print(f"[INFO] Fetching data for {city}...")

        try:
            result = extract_weather(city)
            results.append(result)
        except Exception as e:
            print(f"Erreur pour {city}: {e}")

    print("\n### Résultats:\n")
    for r in results:
        print(r)