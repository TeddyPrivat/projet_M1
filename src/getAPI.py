import os
from dotenv import load_dotenv
import requests
from pymongo import MongoClient
from time import sleep,time

# Charge les variables d'environnement
load_dotenv()

# Paramètres permettant à se connecter à notre cluster MongoDB
mongo_uri = f"mongodb+srv://{os.getenv('CLUSTER_NAME')}:{os.getenv('CLUSTER_PASSWORD')}@fakenewscluster.ivs71yd.mongodb.net/?retryWrites=true&w=majority&appName=FakeNewsCluster"
client = MongoClient(mongo_uri)
db = client["bluesky_db"]
collection = db["posts"]

# Création d'un index pour éviter les doublons pour faire de l'ingestion incrémentale
collection.create_index("post.uri", unique=True)

def create_session():
    url = "https://bsky.social/xrpc/com.atproto.server.createSession"
    #connexion à l'aide de nos identifiants récupérés dans le .env
    data = {
        "identifier": os.getenv("BLUESKY_USERNAME"),
        "password": os.getenv("BLUESKY_PASSWORD")
    }

    response = requests.post(url, json=data)
    tokens = response.json()

    print("Access JWT:", tokens.get("accessJwt"))
    print("Refresh JWT:", tokens.get("refreshJwt"))


def get_posts_from_an_account(actor, limit= 100):
    print(f"Récupération des données du compte : {actor}")
    url = "https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed"
    headers = {"Authorization": f"Bearer {os.getenv('BLUESKY_ACCESS_JWT')}"}

    cursor = None
    new_count = 0

    while True:
        params = {"actor": actor, "limit": limit}
        if cursor:
            params["cursor"] = cursor

        # Retry jusqu'à 10 fois si erreur réseau
        for i in range(10):
            r = requests.get(url, headers=headers, params=params)
            if r.status_code == 200:
                break
            elif r.status_code == 502:
                print(f"Upstream error, on retente après 1 seconde...")
                sleep(1)
            else:
                print(f"Erreur {r.status_code} : {r.text}")
        else:
            print(f"Impossible de récupérer les posts pour {actor} après 10 tentatives")
            return

        data = r.json()
        feed = data.get("feed", [])
        if not feed:
            print("Aucun post supplémentaire à récupérer pour ce compte.")
            break

        # Parcours des posts récupérés
        for post in feed:
            post_uri = post.get("post", {}).get("uri")
            if not post_uri:
                continue

            # Vérifie si le post existe déjà en base pour éviter les doublons
            if not collection.find_one({"post.uri": post_uri}):
                try:
                    collection.insert_one(post)  # Ajout du post en base
                    new_count += 1
                except Exception as e:
                    print(f"Erreur lors de l'insertion : {e}")

        # Affichage de debug par page de feed
        print(f"{len(feed)} posts récupérés dans cette boucle.")

        cursor = data.get("cursor")
        if not cursor:
            break  # Fin des posts

        sleep(1)  # Pour éviter de saturer l'API

    print(f"\nTotal final : {new_count} nouveaux posts ajoutés pour {actor}")

def get_timeline():
    url = "https://bsky.social/xrpc/app.bsky.feed.getTimeline"
    data = {
        "identifier": os.getenv("BLUESKY_USERNAME"),
        "password": os.getenv("BLUESKY_PASSWORD")
    }
    headers = {"Authorization": f"Bearer {os.getenv('BLUESKY_ACCESS_JWT')}"}
    response = requests.get(url, json=data, headers=headers)
    data = response.json()
    posts = data.get("feed", [])
    print(f"Nombre de posts reçus : {len(posts)}")
    return posts

def check_if_posts_exist_in_collection_and_save(feed):
    new_count = 0
    for post in feed:
        post_uri = post.get("post", {}).get("uri")
        if not post_uri:
            continue

        # Vérifie si le post existe déjà en base pour éviter les doublons
        if not collection.find_one({"post.uri": post_uri}):
            try:
                collection.insert_one(post)  # Ajout du post en base
                new_count += 1
            except Exception as e:
                print(f"Erreur lors de l'insertion : {e}")
    print(f"{new_count} posts ajoutés dans la collection.")

def get_posts_from_accounts(accounts):
    start_time = time()
    for actor in accounts:
        try:
            # Appel à la fonction optimisée
            get_posts_from_an_account(actor)
            sleep(1)  # Pour éviter de saturer l'API
        except Exception as e:
            print(f"Erreur sur le compte {actor} : {e}")
    end_time = time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"Temps d'exécution du script : {minutes} minutes et {seconds} secondes")


def purge_mongo_collection(): # permet de supprimer la collection des posts
    collection.delete_many({})
    print(f"Collection '{collection.name}' vidée.")


if __name__ == "__main__":
    create_session()
    #purge_mongo_collection()
    # accounts = ["bloomberg.com"]
    # get_posts_from_accounts(accounts)
    #get_timeline()