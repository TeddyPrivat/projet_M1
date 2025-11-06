## Connexion à Bluesky et récupérer le token en json

import os
import json
import requests
from dotenv import load_dotenv

# Charger les variables d’environnement
load_dotenv()

URL = "https://bsky.social/xrpc/com.atproto.server.createSession"


def login(timeout: int = 10):
    """Se connecte à Bluesky et enregistre les tokens dans token.json."""
    identifier = os.getenv("BSKY_IDENTIFIER")
    password = os.getenv("BSKY_PASSWORD")

    if not identifier or not password:
        print("❌ Erreur: identifiant ou mot de passe manquant dans le fichier .env")
        return False

    payload = {"identifier": identifier, "password": password}

    try:
        r = requests.post(URL, json=payload, timeout=timeout)
        print("🌐 HTTP status:", r.status_code)

        if r.status_code != 200:
            print("❌ Erreur de connexion :", r.text)
            return False

        data = r.json()
        access = data.get("accessJwt")
        refresh = data.get("refreshJwt")

        if access:
            with open("/opt/airflow/src/token.json", "w", encoding="utf-8") as f:
                json.dump(
                    {"accessJwt": access, "refreshJwt": refresh},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            print("✅ Login OK — tokens sauvegardés dans token.json")
            return True
        else:
            print("⚠️ Login échoué — aucun accessJwt reçu")
            return False

    except requests.exceptions.Timeout:
        print("⏰ Erreur : timeout")
    except requests.exceptions.ConnectionError:
        print("🌐 Erreur : échec connexion réseau")
    except Exception as e:
        print("❗ Erreur inattendue :", e)

    return False


if __name__ == "__main__":
    login()
