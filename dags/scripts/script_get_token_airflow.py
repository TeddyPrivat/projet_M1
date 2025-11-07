import os
import requests
from dotenv import load_dotenv

# Charge le fichier .env de ce dossier
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

def get_token():
    url = "https://bsky.social/xrpc/com.atproto.server.createSession"
    data = {
        "identifier": os.getenv("BLUESKY_USERNAME"),
        "password": os.getenv("BLUESKY_PASSWORD")
    }

    response = requests.post(url, json=data)
    tokens = response.json()
    access_jwt = tokens.get("accessJwt")
    print("Access JWT:", access_jwt)
    return access_jwt