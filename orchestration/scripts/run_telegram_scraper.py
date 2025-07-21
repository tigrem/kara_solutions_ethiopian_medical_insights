# orchestration/scripts/run_telegram_scraper.py
import subprocess
import os
import sys

# Adjust this path if your actual scraper is elsewhere
TELEGRAM_SCRAPER_SCRIPT = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'src', 'telegram_scraper.py'
))

# Ensure the .env file is loaded for API_ID/HASH
from dotenv import load_dotenv
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(project_root, '.env'), override=True)

def run_scraper():
    print(f"Running Telegram scraper: {TELEGRAM_SCRAPER_SCRIPT}")
    try:
        # Use sys.executable to ensure the script runs with the active venv Python
        result = subprocess.run(
            [sys.executable, TELEGRAM_SCRAPER_SCRIPT],
            capture_output=True,
            text=True,
            check=True,
            env=os.environ.copy() # Pass current environment variables
        )
        print("Scraper Stdout:\n", result.stdout)
        if result.stderr:
            print("Scraper Stderr:\n", result.stderr)
        print("Telegram scraper completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Telegram scraper failed: {e.returncode}")
        print(f"Stdout:\n{e.stdout}")
        print(f"Stderr:\n{e.stderr}")
        raise # Re-raise to indicate failure to Dagster
    except FileNotFoundError:
        print(f"Error: Telegram scraper script not found at {TELEGRAM_SCRAPER_SCRIPT}")
        raise

if __name__ == "__main__":
    run_scraper()