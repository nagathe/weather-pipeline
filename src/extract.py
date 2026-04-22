import requests
from datetime import datetime
from typing import Dict, Any, List
from config import API_KEY, BASE_URL


def extract_weather(city: str) -> Dict[str, Any]:
    """
    Fetch weather data for a given city from OpenWeatherMap API
    and return a structured dictionary.
    """
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"

    response = requests.get(url, timeout=5)
    response.raise_for_status()

    data = response.json()

    # Safe extraction
    weather_list: List[Dict[str, Any]] = data.get("weather") or [{}]
    main_data: Dict[str, Any] = data.get("main") or {}

    timestamp = data.get("dt")

    return {
        "city": data.get("name"),
        "temperature": main_data.get("temp"),
        "humidity": main_data.get("humidity"),
        "weather": weather_list[0].get("description"),
        "timestamp": timestamp,
        "datetime": datetime.utcfromtimestamp(timestamp).isoformat() if timestamp else None,
    }

def extract(cities: List[str]) -> List[Dict[str, Any]]:
    """
    Wrapper appelé par main.py — itère sur une liste de villes.
    """
    results = []
    for city in cities:
        print(f"[INFO] Fetching data for {city}...")
        try:
            result = extract_weather(city)
            results.append(result)
        except requests.RequestException as e:
            print(f"[ERROR] API error for {city}: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error for {city}: {e}")
    return results


def main() -> None:
    """
    Entry point for script execution.
    """
    cities = ["Paris", "London", "Berlin", "Madrid"]
    results = []

    for city in cities:
        print(f"[INFO] Fetching data for {city}...")

        try:
            result = extract_weather(city)
            results.append(result)
        except requests.RequestException as e:
            print(f"[ERROR] API error for {city}: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error for {city}: {e}")

    print("\n### Results:\n")
    for r in results:
        print(r)


if __name__ == "__main__":
    main()
