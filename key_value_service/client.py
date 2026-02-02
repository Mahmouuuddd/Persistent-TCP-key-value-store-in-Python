import requests

BASE_URL = "http://127.0.0.1:8000"

requests.post(f"{BASE_URL}/set", json={"key": "last_name", "value": "Saqr"})
requests.post(f"{BASE_URL}/set", json={"key": "name", "value": "Mahmoud"})
r = requests.get(f"{BASE_URL}/get/name")
print(r.json())
