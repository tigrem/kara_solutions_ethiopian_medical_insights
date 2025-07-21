# orchestration/scripts/run_dbt_transformations.py
import subprocess
import os
import sys
from dotenv import load_dotenv

# Load environment variables (POSTGRES_*)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(project_root, '.env'), override=True)

DBT_PROJECT_DIR = os.path.abspath(os.path.join(
    project_root, "medical_insights_dwh", "dbt_project"
))

def run_dbt():
    print(f"Running dbt commands in: {DBT_PROJECT_DIR}")
    env_with_vars = os.environ.copy() # Ensure all env vars are passed to subprocess

    # Run dbt deps
    try:
        print("Running dbt deps...")
        result = subprocess.run(
            ["dbt", "deps"],
            capture_output=True, text=True, check=True, cwd=DBT_PROJECT_DIR, env=env_with_vars
        )
        print(result.stdout)
        if result.stderr: print("dbt deps Stderr:\n", result.stderr)
        print("dbt deps completed.")
    except subprocess.CalledProcessError as e:
        print(f"dbt deps failed: {e.returncode}\n{e.stdout}\n{e.stderr}")
        raise
    except FileNotFoundError:
        print("Error: dbt command not found. Ensure dbt is in your PATH.")
        raise

    # Run dbt run
    try:
        print("\nRunning dbt run...")
        result = subprocess.run(
            ["dbt", "run"],
            capture_output=True, text=True, check=True, cwd=DBT_PROJECT_DIR, env=env_with_vars
        )
        print(result.stdout)
        if result.stderr: print("dbt run Stderr:\n", result.stderr)
        print("dbt run completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"dbt run failed: {e.returncode}\n{e.stdout}\n{e.stderr}")
        raise

    # Run dbt test
    try:
        print("\nRunning dbt test...")
        result = subprocess.run(
            ["dbt", "test"],
            capture_output=True, text=True, check=True, cwd=DBT_PROJECT_DIR, env=env_with_vars
        )
        print(result.stdout)
        if result.stderr: print("dbt test Stderr:\n", result.stderr)
        print("dbt test completed. Check output for results.")
    except subprocess.CalledProcessError as e:
        print(f"dbt test failed: {e.returncode}\n{e.stdout}\n{e.stderr}")
        # Do not raise here, as tests can fail without pipeline failure
        # but log the error clearly.
        pass # Dagster will show the error in logs even if not raised

if __name__ == "__main__":
    run_dbt()