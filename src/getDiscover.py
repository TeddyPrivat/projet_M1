import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from time import sleep

load_dotenv()

mongo_uri = f"mongodb+srv://{os.getenv('CLUSTER_NAME')}:{os.getenv('CLUSTER_PASSWORD')}@fakenewscluster.ivs71yd.mongodb.net/?retryWrites=true&w=majority&appName=FakeNewsCluster"
client = MongoClient(mongo_uri)
db = client["bluesky_db"]
collection = db["feed_discover"]
collection.create_index("post_uri", unique=True)

def get_last_post_date():
    last_post = collection.find_one(sort=[("post.record.createdAt", -1)])
    last_date = None
    if last_post:
        post_data = last_post.get("post", {})
        record_data = post_data.get("record", {})
        last_date = record_data.get("createdAt")
    return last_date

def get_with_search(token, queries, limit=100):
    url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
    headers = {"Authorization": f"Bearer {token}"}
    total_new = 0
    languages = ["en", "fr"]
    last_date_post = get_last_post_date()

    for query in queries:
        for lang in languages:
            print(f"\n=== Recherche pour le mot-clé : '{query}' ===")
            cursor = None
            new_count = 0

            while True:
                params = {
                    "q": query,
                    "limit": limit,
                    "lang": lang,
                }
                if last_date_post:
                    params["since"] = last_date_post
                if cursor:
                    params["cursor"] = cursor

                # Tentatives avec gestion d'erreurs réseau
                for attempt in range(5):
                    try:
                        response = requests.get(url, headers=headers, params=params, timeout=10)
                        response.raise_for_status()
                        break
                    except requests.exceptions.RequestException as e:
                        print(f"Erreur réseau ({e}), tentative {attempt + 1}/5...")
                        sleep(1)
                else:
                    print("Impossible de récupérer la page après 5 tentatives.")
                    break

                data = response.json()
                posts = data.get("posts", [])
                if not posts:
                    print("Aucun post supplémentaire à récupérer.")
                    break

                for post in posts:
                    post_uri = post.get("uri")
                    if not post_uri:
                        print("Post ignoré car pas d'URI:", post)
                        continue
                    post_uri = post_uri.strip()
                    doc= {
                        "post": post,
                        "post_uri": post_uri,
                        "query": query,
                        "lang": lang
                    }
                    try:
                        collection.update_one(
                            {"post_uri": post_uri},
                        {"$setOnInsert": doc},
                            upsert=True)
                        new_count += 1
                    except Exception as e:
                        if "duplicate key error" not in str(e):
                            print(f"Erreur lors de l'insertion : {e}")
                        else:
                            print("Erreur MongoDB :", e)

                print(f"{len(posts)} posts récupérés, {new_count} nouveaux ajoutés pour '{query}'.")

                cursor = data.get("cursor")
                if not cursor:
                    print(f"Fin des résultats pour '{query}'.")
                    break

                sleep(1)  # Respecter le rate limit

            total_new += new_count

    print(f"\nTotal final : {total_new} nouveaux posts ajoutés dans MongoDB.")
    return total_new

if __name__ == "__main__":
    queries = ["news", "world", "science", "tech"]
    get_with_search(os.getenv("BLUESKY_ACCESS_JWT"), queries)