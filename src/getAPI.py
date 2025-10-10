# permet de tester l'API, d'afficher les données dans le terminal
import os
import requests
from dotenv import load_dotenv
from atproto import Client

# Charge les variables d'environnement
load_dotenv()

def get_access_token():
    resp = requests.post(
        "https://bsky.social/xrpc/com.atproto.server.createSession",
        json={"identifier": os.getenv("BLUESKY_USERNAME"), "password": os.getenv("BLUESKY_PASSWORD")},
    )
    resp.raise_for_status()
    session = resp.json()
    access_token = session["accessJwt"]
    print("access token : ", access_token)
    return session

def create_session():
    client = Client()
    client.login(os.getenv("BLUESKY_USERNAME"), os.getenv("BLUESKY_PASSWORD"))
    return client

def create_post(client_session, text):
    post = client_session.send_post(text)
    print(post)

if __name__ == "__main__":
    create_post(create_session(), "William me manque :'(")