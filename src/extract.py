import requests
from datetime import datetime
from config import API_KEY, BASE_URL


def extract_weather(city: str) -> dict:
    url = f"{BASE_URL}?q={city}&appid={API_KEY}&units=metric"

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    return {
        "city": data.get("name"),
        "temperature": data.get("main", {}).get("temp"),
        "humidity": data.get("main", {}).get("humidity"),
        "weather": data.get("weather", [{}])[0].get("main"),
        "timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    result = extract_weather("Paris")
    print(result)
