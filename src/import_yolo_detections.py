import psycopg2
import csv
from io import StringIO

# Database connection parameters (ensure these match your Neon credentials EXACTLY)
POSTGRES_DB = 'neondb'
POSTGRES_USER = 'neondb_owner'
POSTGRES_PASSWORD = 'npg_3AdCgPQDvcz1'
POSTGRES_HOST = 'ep-dry-bonus-a2r95tms-pooler.eu-central-1.aws.neon.tech'
POSTGRES_PORT = '5432'

# Path to your CSV file
CSV_FILE_PATH = r'D:\10academy\week7\kara_solutions_ethiopian_medical_insights\data\processed\yolo_detections.csv'

conn = None
cur = None

try:
    # Connect to your Neon database
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    cur = conn.cursor()

    print("Attempting to import data into 'yolo_detections_csv' table using copy_expert...")

    # Prepare data for copy_expert: Read CSV, skip header, and create a StringIO object
    with open(CSV_FILE_PATH, 'r') as f:
        # Read the entire content of the file
        content = f.read()
        # Split into lines, keeping line endings, then join from the second line (skipping header)
        lines = content.splitlines(True) # 'True' keeps newline characters
        data_without_header = "".join(lines[1:])

    # Create a file-like object in memory from the processed string
    csv_file_like_object = StringIO(data_without_header)

    # Use copy_expert for efficient bulk import with a raw COPY command
    # This provides the exact COPY SQL command to PostgreSQL.
    copy_sql = """
        COPY public.yolo_detections_csv(message_id, detected_object_class, confidence_score, detection_timestamp)
        FROM STDIN WITH (FORMAT CSV);
    """
    # We specify FORMAT CSV. We are sending data via STDIN (standard input)
    # from the csv_file_like_object. 'WITH HEADER' is not used because we
    # manually skipped the header line when preparing data_without_header.

    cur.copy_expert(copy_sql, csv_file_like_object)

    conn.commit() # Commit the data insertion
    print("Data imported successfully into 'yolo_detections_csv' table using copy_expert.")

except psycopg2.Error as e:
    if conn:
        conn.rollback() # Rollback any changes in case of a database error
    print(f"Database error occurred: {e}")
    if e.diag:
        print(f"SQLSTATE: {e.diag.sqlstate}")
        print(f"Message Detail: {e.diag.message_detail}")
        print(f"Message Hint: {e.diag.message_hint}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

finally:
    # Ensure cursor and connection are closed
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("Database connection closed.")