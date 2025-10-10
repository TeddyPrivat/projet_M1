import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Charge les variables d'environnement
load_dotenv()
# Affiche Connexion OK
def connect_database():
    uri = f"mongodb+srv://{os.getenv('CLUSTER_NAME')}:{os.getenv('CLUSTER_PASSWORD')}@fakenewscluster.ivs71yd.mongodb.net/?retryWrites=true&w=majority&appName=FakeNewsCluster"
    client = MongoClient(uri)

    try:
        client.admin.command("ping")
        print("✅ Connexion réussie à MongoDB Atlas !")
    except Exception as e:
        print("❌ Erreur :", e)

if __name__ == "__main__":
    connect_database()