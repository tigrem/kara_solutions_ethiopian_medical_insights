# src/yolo_image_analyzer.py - REVISED with MORE DEBUGGING PRINTS
import os
import json
import pandas as pd
from ultralytics import YOLO
from datetime import datetime
import hashlib # For generating a unique ID from file path
from dotenv import load_dotenv

# Load environment variables (assuming .env is in project root)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(os.path.join(project_root, '.env'), override=True)

# Define paths
# Point to the base directory where your 'telegram_images' folder resides
# This should resolve to D:\10academy\week7\kara_solutions_ethiopian_medical_insights\data\raw\telegram_images
BASE_IMAGES_DIR = os.path.join(project_root, 'data', 'raw', 'telegram_images')
PROCESSED_DATA_DIR = os.path.join(project_root, 'data', 'processed')
YOLO_OUTPUT_FILE = os.path.join(PROCESSED_DATA_DIR, 'yolo_detections.csv')

# Ensure processed data directory exists
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# --- DEBUGGING PRINTS ---
print(f"Project Root: {project_root}")
print(f"Calculated BASE_IMAGES_DIR: {BASE_IMAGES_DIR}")
print(f"Does BASE_IMAGES_DIR exist? {os.path.exists(BASE_IMAGES_DIR)}")
print(f"Is BASE_IMAGES_DIR a directory? {os.path.isdir(BASE_IMAGES_DIR)}")
# --- END DEBUGGING PRINTS ---

# Load a pre-trained YOLOv8 model
model = YOLO('yolov8n.pt') # 'yolov8n.pt' (nano) is good for quick testing

def get_image_files_recursive(base_dir):
    """Recursively finds all image files in a base directory and its subdirectories."""
    image_paths = []
    for root, _, files in os.walk(base_dir):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
                image_paths.append(os.path.join(root, file))
    return image_paths

def analyze_images_with_yolo():
    print("Starting YOLO image analysis...")
    detection_records = []

    image_files = get_image_files_recursive(BASE_IMAGES_DIR)

    # --- DEBUGGING PRINTS ---
    print(f"Number of image files found by recursive search: {len(image_files)}")
    if len(image_files) > 0:
        print(f"First 5 found image files (if any):")
        for i, p in enumerate(image_files[:5]):
            print(f"  {i+1}. {p}")
    # --- END DEBUGGING PRINTS ---

    if not image_files:
        print(f"No image files found in {BASE_IMAGES_DIR} or its subdirectories. Skipping YOLO analysis.")
        return

    for image_path in image_files:
        print(f"Analyzing image: {image_path}")

        unique_image_id = hashlib.sha256(image_path.encode()).hexdigest()
        message_id_for_fk = unique_image_id

        try:
            results = model(image_path)
            if results and hasattr(results[0], 'boxes') and results[0].boxes:
                for r in results:
                    boxes = r.boxes
                    for box in boxes:
                        class_id = int(box.cls[0])
                        confidence = float(box.conf[0])
                        detected_class_name = model.names[class_id]

                        detection_records.append({
                            'message_id': message_id_for_fk,
                            'detected_object_class': detected_class_name,
                            'confidence_score': confidence,
                            'detection_timestamp': datetime.now().isoformat()
                        })
            else:
                print(f"No objects detected in {image_path}")

        except Exception as e:
            print(f"Error processing image {image_path} with YOLO: {e}")
            continue

    if detection_records:
        df_detections = pd.DataFrame(detection_records)
        df_detections.to_csv(YOLO_OUTPUT_FILE, index=False)
        print(f"YOLO detections saved to {YOLO_OUTPUT_FILE}")
    else:
        print("No objects detected or no images processed from any subdirectory.")

if __name__ == "__main__":
    analyze_images_with_yolo()