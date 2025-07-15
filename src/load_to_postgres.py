# src/load_to_postgres.py
import os
import json
import logging
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
# This path derivation ensures .env is loaded from the project root
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env')) # CORRECTED: Ensure .env is loaded from project root

# Database connection details from .env - CORRECTED VARIABLE NAMES
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

# Get the project root directory
# Assuming load_to_postgres.py is in kara_solutions_ethiopian_medical_insights/src/
# This correctly goes up one level to the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) # CORRECTED: Changed '..' to '..' and removed '..'

# Base directory for telegram messages
base_directory = os.path.join(project_root, 'data', 'raw', 'telegram_messages')

logging.info(f"Project Root: {project_root}")
logging.info(f"Base Directory for Raw Data: {base_directory}")

def create_raw_table(cursor):
    """
    Creates the 'raw_telegram_messages' table in the 'public' schema if it doesn't exist.
    Stores the raw JSON as JSONB for flexibility and efficient querying.
    """
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.raw_telegram_messages (
                id SERIAL PRIMARY KEY,
                message_data JSONB,
                channel_name VARCHAR(255),
                scraped_date DATE,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logging.info("Table 'public.raw_telegram_messages' ensured to exist.")
    except Exception as e:
        logging.error(f"Error creating public.raw_telegram_messages table: {e}")
        raise

def insert_message_data(cursor, message_data, channel_name, scraped_date):
    """
    Inserts a single message's JSON data into the raw_telegram_messages table.
    Checks for duplicates based on original Telegram message ID, channel name, and scraped date.
    """
    try:
        original_telegram_message_id = message_data.get('id')
        if original_telegram_message_id is not None:
            # Check for existing message to prevent duplicates
            cursor.execute(
                """
                SELECT 1 FROM public.raw_telegram_messages
                WHERE (message_data->>'id')::BIGINT = %s
                  AND channel_name = %s
                  AND scraped_date = %s;
                """,
                (original_telegram_message_id, channel_name, scraped_date)
            )
            if cursor.fetchone():
                logging.debug(f"Skipping duplicate message ID {original_telegram_message_id} for channel {channel_name} on {scraped_date}.")
                return

        cursor.execute(
            """
            INSERT INTO public.raw_telegram_messages (message_data, channel_name, scraped_date)
            VALUES (%s, %s, %s);
            """,
            (json.dumps(message_data), channel_name, scraped_date)
        )
        logging.debug(f"Inserted message ID {original_telegram_message_id} from channel {channel_name}")
    except Exception as e:
        logging.error(f"Error inserting message data for message ID {message_data.get('id', 'N/A')}: {e}")

def load_raw_data_to_postgres():
    """
    Connects to PostgreSQL and loads all scraped JSON data from the data lake
    following the <date>/<channel_name>/<message_id>.json structure.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = False # We'll manage transactions manually
        cursor = conn.cursor()

        create_raw_table(cursor)

        processed_files = 0
        if not os.path.exists(base_directory):
            logging.error(f"Base directory for raw data does not exist: {base_directory}. Please ensure data scraping has run and created files here.")
            return

        for date_dir in os.listdir(base_directory):
            date_path = os.path.join(base_directory, date_dir)
            if os.path.isdir(date_path):
                try:
                    # Date directories are named YYYY-MM-DD
                    scraped_date_obj = datetime.strptime(date_dir, '%Y-%m-%d').date()
                except ValueError:
                    logging.warning(f"Skipping malformed date directory name: {date_dir}. Expected YYYY-MM-DD format.")
                    continue

                for channel_subdir in os.listdir(date_path):
                    channel_path = os.path.join(date_path, channel_subdir)
                    if os.path.isdir(channel_path):
                        channel_name = channel_subdir
                        logging.info(f"Processing directory: {channel_path} (Channel: {channel_name}, Date: {scraped_date_obj})")

                        json_files = [f for f in os.listdir(channel_path) if f.endswith('.json')]
                        if not json_files:
                            logging.warning(f"No JSON message files found in {channel_path}.")
                            continue

                        for json_file in json_files:
                            file_path = os.path.join(channel_path, json_file)
                            try:
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    message_data = json.load(f)
                                insert_message_data(cursor, message_data, channel_name, scraped_date_obj)
                                processed_files += 1
                            except json.JSONDecodeError as e:
                                logging.error(f"Error decoding JSON from {file_path}: {e}")
                            except Exception as e:
                                logging.error(f"Error processing file {file_path}: {e}")
                conn.commit() # Commit after each date directory to reduce transaction size
                logging.info(f"Committed data for date: {scraped_date_obj}")

        logging.info(f"Successfully processed and loaded {processed_files} JSON files into PostgreSQL.")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback() # Rollback on database errors
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        if conn:
            conn.rollback() # Rollback on other unexpected errors
    finally:
        if conn:
            cursor.close()
            conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    load_raw_data_to_postgres()