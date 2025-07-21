# orchestration/scripts/run_yolo_detection.py
import subprocess
import os
import sys

# Adjust this path if your actual YOLO detector is elsewhere
YOLO_DETECTOR_SCRIPT = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'src', 'yolo_detector.py' # Assuming yolo_detector is in src
))

# Ensure the .env file is loaded for any necessary configs
from dotenv import load_dotenv
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(project_root, '.env'), override=True)

def run_yolo():
    print(f"Running YOLO object detection: {YOLO_DETECTOR_SCRIPT}")
    try:
        result = subprocess.run(
            [sys.executable, YOLO_DETECTOR_SCRIPT],
            capture_output=True,
            text=True,
            check=True,
            env=os.environ.copy()
        )
        print("YOLO Stdout:\n", result.stdout)
        if result.stderr:
            print("YOLO Stderr:\n", result.stderr)
        print("YOLO detection completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"YOLO detection failed: {e.returncode}")
        print(f"Stdout:\n{e.stdout}")
        print(f"Stderr:\n{e.stderr}")
        raise
    except FileNotFoundError:
        print(f"Error: YOLO detector script not found at {YOLO_DETECTOR_SCRIPT}")
        raise

if __name__ == "__main__":
    run_yolo()