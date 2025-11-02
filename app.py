# simple_get_author_feed_from_env.py
# pip install atproto python-dotenv requests

import os
import time
import json
from atproto import Client
from dotenv import load_dotenv

load_dotenv() 

LOGIN = os.environ.get("LOGIN_BLUESKY")
APP_PWD = os.environ.get("PASS_BLUESKY")

if not LOGIN or not APP_PWD:
    raise SystemExit("Erreur: définis LOGIN_BLUESKY et PASS_BLUESKY dans ton .env ou variables d'env.")

if LOGIN.startswith("@"):
    LOGIN = LOGIN[1:]

TARGET = os.environ.get("TARGET_BSKY") or "marshal.dev"

print("Login utilisé :", LOGIN)
print("Target :", TARGET)

#Connexion à BlueSky avec nos identifiants
client = Client()
profile = client.login(LOGIN, APP_PWD)

# selon la version du SDK, profile peut être dict ou objet
try:
    profile_handle = profile.get("handle") if isinstance(profile, dict) else getattr(profile, "handle", None)
except Exception:
    profile_handle = None

print("Connecté en tant :", profile_handle or profile)

# --- récupération simple et pagination ---
params = {"actor": TARGET, "limit": 50}
cursor = None
total = 0

#Merci ChatGPT
print("Méthodes disponibles :", dir(client.app))
print('*' * 100)
print('*' * 100)

while True:
    #Cursor fonctionne comme un genre de marque-page, au premier tour il vaut None, et affiche donc les 50 premiers
    #Ensuite il vaut le dernier post retrouvé, et dit "jusqu'à ce post j'ai tout envoyé, j'envoie les 50 suivants"
    if cursor:
        params["cursor"] = cursor

    #Appel réel à l'API
    resp = client.app.bsky.feed.get_author_feed(params)

    # normalise la réponse (dict ou objet)
    if isinstance(resp, dict):
        items = resp.get("feed", []) or resp.get("posts", [])
        cursor = resp.get("cursor")
    else:
        items = getattr(resp, "feed", None) or getattr(resp, "posts", None) or []
        cursor = getattr(resp, "cursor", None)

    if not items:
        print("Aucun item trouvé ou fin de flux.")
        break

    for it in items:
        post = it.post
        print(post.author.handle, "→", post.record.text)
        print("-" * 60)

    total += len(items)
    print(f"Récupérés jusqu'ici: {total} posts. Cursor: {cursor}")

    if not cursor:
        break

    time.sleep(0.15)  # throttle léger

print("Total posts récupérés :", total)
