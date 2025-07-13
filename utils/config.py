
import os
from dotenv import load_dotenv

load_dotenv() # Load environment variables from .env file

TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

def print_config():
    print("Configuration Loaded:")
    print(f"Telegram API ID: {TELEGRAM_API_ID}")
    print(f"Telegram API Hash: {TELEGRAM_API_HASH[:5]}...") # Mask part of the hash
    print(f"Postgres DB: {POSTGRES_DB}")
    print(f"Postgres User: {POSTGRES_USER}")
    print(f"Postgres Host: {POSTGRES_HOST}")
    print(f"Postgres Port: {POSTGRES_PORT}")
    print(f"Postgres Password: {'*' * len(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else 'None'}")

if __name__ == "__main__":
    print_config()

