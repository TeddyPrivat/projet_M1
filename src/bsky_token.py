import json

def load_token():
    """Load your Bluesky access token from token.json"""
    try:
        with open("/opt/airflow/src/token.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("accessJwt")
    except FileNotFoundError:
        print("❌ token.json not found. Run login.py first.")
    except json.JSONDecodeError:
        print("❌ Invalid token.json format.")
    return None