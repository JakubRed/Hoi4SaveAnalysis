import os
import sys
import sqlite3
import json
from utils.db_utils import validate_dataset_id_sequence
from datetime import datetime
function_log_tag = "[export_general_info_to_sql]"
table_name = "General_info"

def export_general_info_to_sql(db_path, json_path, dataset_id=None):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute(f"""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='{table_name}'
    """)
    if not cursor.fetchone():
        raise RuntimeError(f"Table '{table_name}' does not exist in the database.")

    # Load the JSON file
    with open(json_path, 'r', encoding='utf-16') as f:
        data = json.load(f)

    # Determine dataset_id if not given
    if dataset_id is None:
        cursor.execute(f"SELECT MAX(dataset_id) FROM {table_name}")
        result = cursor.fetchone()
        dataset_id = (result[0] or 0) + 1
    else:
        validate_dataset_id_sequence(cursor, dataset_id)
    print(f"{function_log_tag} Using dataset_id: {dataset_id}")
        
    # Extract fields
    extracted = {
        "dataset_id": dataset_id,
        "timestamp": datetime.now().isoformat(),
        "date": data.get("date"),
        "save_version": data.get("save_version"),
        "game_unique_seed": data.get("game_unique_seed"),
        "game_unique_id": data.get("game_unique_id"),
        "session": data.get("session"),
        "version": data.get("version"),
        "multiplayer_random_count": data.get("multiplayer_random_count"),
        "multiplayer_random_seed": data.get("multiplayer_random_seed"),
        "debug_current_ref_id": data.get("debug_current_ref_id")
    }

    # Insert data into db
    cursor.execute(f'''
        INSERT INTO {table_name} (
            dataset_id, timestamp, date, save_version, game_unique_seed, game_unique_id,
            session, version, multiplayer_random_count, multiplayer_random_seed, debug_current_ref_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(extracted.values()))

    conn.commit()
    conn.close()
    print(f"{function_log_tag} General info successfully exported.")

# Allow direct execution
if __name__ == "__main__":
    print(f"{function_log_tag} Running standalone export...")
    if len(sys.argv) < 4:
        print("Usage: python general_info.py <database_file> <json_file> <dataset_id>")
        sys.exit(1)

    db_path = sys.argv[1]
    json_path = sys.argv[2]
    dataset_id = int(sys.argv[3])

    export_general_info_to_sql(db_path, json_path, dataset_id)
