import json
import requests

PB_URL = "https://petapaontrack.kojimena.com"
ADMIN_EMAIL = "baeimej@gmail.com"  
ADMIN_PASSWORD = "Nutella.2112"      

with open("games.json", "r", encoding="utf-8") as f:
    Juegos = json.load(f)

session = requests.Session()

auth = session.post(
    f"{PB_URL}/api/admins/auth-with-password",
    json={"identity": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
)

if auth.status_code != 200:
    print("Error autenticando admin:", auth.text)
    exit()

token = auth.json()["token"]
session.headers["Authorization"] = f"Bearer {token}"

print("Autenticado correctamente como admin.")

# 2. Subir cada juego
for juego in Juegos:
    r = session.post(
        f"{PB_URL}/api/collections/games/records",
        json=juego
    )
    
    if r.status_code == 200:
        print("Subido:", juego["name"])
    else:
        print("Error con", juego["name"], r.status_code, r.text)

print("\n--- PROCESO COMPLETO ---")
