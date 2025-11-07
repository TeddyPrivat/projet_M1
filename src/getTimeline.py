import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from time import sleep

load_dotenv()

mongo_uri = f"mongodb+srv://{os.getenv('CLUSTER_NAME')}:{os.getenv('CLUSTER_PASSWORD')}@fakenewscluster.ivs71yd.mongodb.net/?retryWrites=true&w=majority&appName=FakeNewsCluster"
client = MongoClient(mongo_uri)
db = client["bluesky_db"]
collection = db["timeline"]
collection.create_index("post.uri", unique=True)

def get_timeline(token,limit=50):
    url = "https://bsky.social/xrpc/app.bsky.feed.getTimeline"
    headers = {"Authorization": f"Bearer {token}"}
    cursor = None
    new_count = 0

    while True:
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        for attempt in range(5):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=10)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"Erreur réseau ({e}), tentative {attempt + 1}/5...")
                sleep(1)
        else:
            print("Impossible de récupérer la page de la timeline après 5 tentatives.")
            break

        data = response.json()
        feed = data.get("feed", [])
        if not feed:
            print("Aucun post supplémentaire à récupérer.")
            break

        # Insertion incrémentale
        for post in feed:
            post_uri = post.get("post", {}).get("uri")
            if not post_uri:
                continue
            if not collection.find_one({"post.uri": post_uri}):
                try:
                    collection.insert_one(post)
                    new_count += 1
                except Exception as e:
                    print(f"Erreur lors de l'insertion : {e}")

        print(f"{len(feed)} posts récupérés dans cette boucle, {new_count} nouveaux ajoutés au total.")

        cursor = data.get("cursor")
        if not cursor:
            print("Fin de la timeline atteinte.")
            break

        sleep(1)  # Évite de saturer l'API

    print(f"\nTotal final : {new_count} nouveaux posts ajoutés dans MongoDB.")
    return new_count

if __name__ == "__main__":
    get_timeline()