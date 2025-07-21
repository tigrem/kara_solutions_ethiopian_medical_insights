# orchestration/scripts/load_telegram_raw_data.py
import psycopg2
import json
import os
from dotenv import load_dotenv

# Load environment variables (POSTGRES_*)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(project_root, '.env'), override=True)

# Database connection parameters
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# Path to your raw JSON file
RAW_JSON_FILE_PATH = os.path.abspath(os.path.join(
    project_root, 'data', 'raw', 'telegram_messages.json'
))

def load_raw_data():
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        conn.autocommit = False
        cur = conn.cursor()

        print("Ensuring raw_telegram_messages table exists and is ready...")
        # Create table if it doesn't exist, with JSONB for message_data
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.raw_telegram_messages (
            id SERIAL PRIMARY KEY, -- Internal ID for the raw record
            message_data JSONB, -- The full JSON blob from Telegram
            scraped_date DATE,
            ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """)
        conn.commit()
        print("Table 'raw_telegram_messages' ensured to exist.")

        # Clear existing data if you want to re-ingest fresh data each run
        # cur.execute("TRUNCATE TABLE public.raw_telegram_messages;")
        # conn.commit()
        # print("Truncated raw_telegram_messages table.")

        with open(RAW_JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            raw_messages = json.load(f)

        # Prepare data for insertion
        data_to_insert = []
        current_scraped_date = datetime.now().date() # Use current date as scraped_date

        for msg in raw_messages:
            # Ensure 'date' is a datetime object if it's not already
            # msg_date = datetime.fromisoformat(msg['date']) if isinstance(msg['date'], str) else msg['date']
            # For simplicity, we'll insert the whole dict as JSONB
            data_to_insert.append((json.dumps(msg), current_scraped_date))

        # Use executemany for efficient batch insert
        if data_to_insert:
            print(f"Inserting {len(data_to_insert)} raw messages into raw_telegram_messages...")
            cur.executemany(
                """
                INSERT INTO public.raw_telegram_messages (message_data, scraped_date)
                VALUES (%s, %s);
                """,
                data_to_insert
            )
            conn.commit()
            print("Raw Telegram data loaded successfully.")
        else:
            print("No raw messages to insert.")

    except FileNotFoundError:
        print(f"Error: Raw JSON file not found at {RAW_JSON_FILE_PATH}")
        raise
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"Database error occurred: {e}")
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {RAW_JSON_FILE_PATH}. Is it valid JSON?")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    load_raw_data()