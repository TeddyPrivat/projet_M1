# permet de tester l'API, d'afficher les données dans le terminal
import os
from dotenv import load_dotenv
from atproto import Client

# Charge les variables d'environnement
load_dotenv()

def create_session():
    client = Client()
    client.login(os.getenv("BLUESKY_USERNAME"), os.getenv("BLUESKY_PASSWORD"))
    return client

def create_post(client_session, text):
    post = client_session.send_post(text)
    print(post)

if __name__ == "__main__":
    create_post(create_session(), "William me manque :'(")