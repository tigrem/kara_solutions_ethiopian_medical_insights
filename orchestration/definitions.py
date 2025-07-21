# orchestration/definitions.py
import os
from dagster import op, job, ScheduleDefinition
import subprocess
import sys

# Define paths to your wrapper scripts
SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts'))
TELEGRAM_SCRAPER_SCRIPT = os.path.join(SCRIPTS_DIR, 'run_telegram_scraper.py')
RAW_DATA_LOADER_SCRIPT = os.path.join(SCRIPTS_DIR, 'load_telegram_raw_data.py')
YOLO_DETECTOR_SCRIPT = os.path.join(SCRIPTS_DIR, 'run_yolo_detection.py')
DBT_TRANSFORMATIONS_SCRIPT = os.path.join(SCRIPTS_DIR, 'run_dbt_transformations.py')

# Helper function to run Python scripts as subprocesses
def _run_python_script(script_path: str, context):
    context.log.info(f"Executing script: {script_path}")
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            check=True,
            env=os.environ.copy() # Pass current environment variables to subprocess
        )
        context.log.info(f"Script Stdout:\n{result.stdout}")
        if result.stderr:
            context.log.warning(f"Script Stderr:\n{result.stderr}")
        context.log.info(f"Script {script_path} completed successfully.")
    except subprocess.CalledProcessError as e:
        context.log.error(f"Script {script_path} failed with exit code {e.returncode}")
        context.log.error(f"Stdout:\n{e.stdout}")
        context.log.error(f"Stderr:\n{e.stderr}")
        raise  # Re-raise the exception to mark the op as failed in Dagster
    except FileNotFoundError:
        context.log.error(f"Script not found: {script_path}. Check path and permissions.")
        raise

# --- Ops Definition ---

@op
def scrape_telegram_data(context):
    """Scrapes new data from Telegram channels."""
    _run_python_script(TELEGRAM_SCRAPER_SCRIPT, context)

@op
def load_raw_to_postgres(context, start_after_scrape):
    """Loads raw scraped Telegram data (JSON) into PostgreSQL."""
    _run_python_script(RAW_DATA_LOADER_SCRIPT, context)

@op
def run_yolo_enrichment(context, start_after_scrape):
    """Runs YOLO object detection on new images and generates detections CSV."""
    _run_python_script(YOLO_DETECTOR_SCRIPT, context)

@op
def run_dbt_transformations(context, start_after_load, start_after_yolo):
    """Runs dbt transformations (run and test) to build data marts."""
    _run_python_script(DBT_TRANSFORMATIONS_SCRIPT, context)

# --- Job Definition ---

@job
def medical_insights_pipeline():
    """
    Orchestrates the entire medical insights data pipeline.
    Dependencies:
    - scrape_telegram_data runs first.
    - load_raw_to_postgres depends on scrape_telegram_data.
    - run_yolo_enrichment depends on scrape_telegram_data.
    - run_dbt_transformations depends on both load_raw_to_postgres and run_yolo_enrichment.
    """
    # Step 1: Scrape data
    scrape_result = scrape_telegram_data()

    # Step 2: Load raw data to Postgres (depends on scrape)
    load_result = load_raw_to_postgres(start_after_scrape=scrape_result)

    # Step 3: Run YOLO enrichment (depends on scrape for images)
    yolo_result = run_yolo_enrichment(start_after_scrape=scrape_result)

    # Step 4: Run dbt transformations (depends on both raw data load and YOLO enrichment)
    run_dbt_transformations(start_after_load=load_result, start_after_yolo=yolo_result)

# --- Schedule Definition ---

# Schedule the pipeline to run daily at a specific time (e.g., 2 AM UTC)
# Adjust cron_schedule as needed for your desired frequency
daily_medical_insights_schedule = ScheduleDefinition(
    job=medical_insights_pipeline,
    cron_schedule="0 2 * * *", # Every day at 2:00 AM UTC
    description="Daily run of the medical insights data pipeline."
)

# Define the repository, which contains your job and schedule
# This is how Dagster discovers your definitions
from dagster import Definitions

defs = Definitions(
    jobs=[medical_insights_pipeline],
    schedules=[daily_medical_insights_schedule]
)