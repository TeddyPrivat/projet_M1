#Connexion à database MongoDB
from pymongo import MongoClient
from dotenv import load_dotenv
import os


load_dotenv()



def connect_to_mongo(db_name="projet_bluesky"):

    uri = os.getenv("MONGO_URI")

    if not uri:
        print("❌ Erreur : variable MONGO_URI introuvable dans le fichier .env.")
        return None

    try:
        client = MongoClient(uri)
        client.admin.command("ping")
        #db = client[db_name]
        print("✅ Connected to MongoDB Atlas!")
        print(f"📂 Database: {db_name}")
        return db_name
    except Exception as e:
        print("❌ Connection failed:", e)
        return None


if __name__ == "__main__":
    db = connect_to_mongo()
