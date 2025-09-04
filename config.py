import os

BASE_OUTPUT_FOLDER = os.path.dirname(os.path.abspath(__file__))
CSV_FOLDER = os.path.join(BASE_OUTPUT_FOLDER, 'CSV')
JSON_FOLDER = os.path.join(BASE_OUTPUT_FOLDER, 'JSON')

# Create directories if they do not exist
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(JSON_FOLDER, exist_ok=True)
