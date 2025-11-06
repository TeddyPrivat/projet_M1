import requests
import time
from dotenv import load_dotenv
import os
from pymongo import MongoClient
from bsky_token import load_token
from mongo_connection import connect_to_mongo

load_dotenv()

def search_posts(query, lang=None, limit=100, cursor=None):
    """
    Recherche des posts publics sur Bluesky selon un mot-clé et une langue.
    """
    access_token = load_token()

    url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"q": query, "limit": limit}
    if lang:
        params["lang"] = lang
    if cursor:
        params["cursor"] = cursor

    r = requests.get(url, headers=headers, params=params, timeout=10)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"❌ Error search_posts: {r.status_code}: {r.text}")
        return None


def get_author_feed(handle, limit=100, cursor=None):
    """
    Récupère les posts d’un auteur spécifique (compte d’actualité).
    """
    access_token = load_token()
    url = "https://bsky.social/xrpc/app.bsky.feed.getAuthorFeed"
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"actor": handle, "limit": limit}
    if cursor:
        params["cursor"] = cursor

    r = requests.get(url, headers=headers, params=params, timeout=10)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"❌ Error get_author_feed ({handle}): {r.status_code}: {r.text}")
        return None


def fetch_and_import_to_mongo(collection_name="new_posts",db_name=None, max_posts=100):
    """
    Combine la recherche par mot-clé et la récupération de comptes médias.
    Stocke directement les résultats dans MongoDB.
    """
    uri = os.getenv("MONGO_URI")
    #db = connect_to_mongo()
    if not uri:
        print("❌ Échec de connexion à MongoDB. Abandon de l’import.")
        return

    client = MongoClient(uri)
    # Si la base n'est pas précisée, on prend celle définie par défaut dans .env ou "projet_bluesky"
    if not db_name:
        db_name = os.getenv("projet_bluesky")

    db = client[db_name]

    topics = [
        "politics", "government", "election", "policy", "réforme", "war", "guerre",
        "politique", "gouvernement", "élection"
    ]
    languages = ["en", "fr"]
    news_accounts = [
        "bbcnews.bsky.social", "reuters.com", "apnews.com", "nytimes.com",
        "washingtonpost.com", "lemonde.bsky.social", "liberation.bsky.social",
        "afp.com", "cnbc.com", "usatoday.com", "theguardian.com", "latimes.com",
        "edition.cnn.com", "wsj.com", "60minutes.bsky.social", "france24.com",
        "bfmtv.com"
    ]

    seen_uris = set()

    # ✅ Utilisation correcte du paramètre sans redéfinition
    collection = db[collection_name]
    collection.create_index("uri", unique=True)

    inserted_count = 0

    # 1️⃣ Recherche par mot-clé
    for lang in languages:
        for topic in topics:
            print(f"\n🔍 Searching for '{topic}' in language '{lang}' …")
            cursor = None
            while inserted_count < max_posts:
                data = search_posts(query=topic, lang=lang, limit=100, cursor=cursor)
                if not data:
                    break
                posts = data.get("posts", [])
                if not posts:
                    break

                for p in posts:
                    uri = p.get("uri")
                    if uri and not collection.find_one({"uri": uri}):
                        clean_post = {
                            "uri": uri,
                            "author": p.get("author", {}).get("handle"),
                            "text": p.get("record", {}).get("text", ""),
                            "createdAt": p.get("record", {}).get("createdAt", ""),
                            "lang": p.get("record", {}).get("langs", []),
                            "likeCount": p.get("likeCount", 0),
                            "repostCount": p.get("repostCount", 0),
                        }

                        collection.insert_one(clean_post)
                        inserted_count += 1
                        author = p.get("author", {}).get("handle", "unknown")
                        text = p.get("record", {}).get("text", "")
                        print(f"✅ Inserted @{author}: {text[:80]}")

                    if inserted_count >= max_posts:
                        break

                cursor = data.get("cursor")
                if not cursor:
                    break
                time.sleep(1)

    # 2️⃣ Récupération des comptes d’actualité
    for handle in news_accounts:
        print(f"\n📰 Fetching feed from @{handle} …")
        cursor = None
        count_for_author = 0
        while inserted_count < max_posts:
            data = get_author_feed(handle, limit=100, cursor=cursor)
            if not data:
                break
            feed = data.get("feed", [])
            if not feed:
                break

            for item in feed:
                post = item.get("post", {})
                uri = post.get("uri")
                if uri and not collection.find_one({"uri": uri}):
                    clean_post = {
                        "uri": uri,
                        "author": post.get("author", {}).get("handle", handle),
                        "text": post.get("record", {}).get("text", ""),
                        "createdAt": post.get("record", {}).get("createdAt", ""),
                        "lang": post.get("record", {}).get("langs", []),
                        "likeCount": post.get("likeCount", 0),
                        "repostCount": post.get("repostCount", 0),
                    }

                    collection.insert_one(clean_post)
                    inserted_count += 1
                    count_for_author += 1
                    author = post.get("author", {}).get("handle", handle)
                    text = post.get("record", {}).get("text", "")
                    print(f"🆕 Inserted @{author}: {text[:80]}")

                if inserted_count >= max_posts:
                    break

            cursor = data.get("cursor")
            if not cursor:
                break
            time.sleep(1)

        print(f"✅ {count_for_author} posts collected from @{handle}")

    print(f"\n💾 Total posts: {inserted_count} saved into database collection '{collection_name}'.")
