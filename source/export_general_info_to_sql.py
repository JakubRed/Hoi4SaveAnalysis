import os
import sys
import sqlite3
import json
from utils.db_utils import validate_dataset_id_sequence
from datetime import datetime
function_log_tag = "[export_general_info_to_sql]"
table_name = "General_info"

def export_general_info_to_sql(cursor, json_path, dataset_id=None):

    cursor.execute(f"""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='{table_name}'
    """)
    if not cursor.fetchone():
        raise RuntimeError(f"Table '{table_name}' does not exist in the database.")

    # Load the JSON file
    with open(json_path, 'r', encoding='utf-16') as f:
        data = json.load(f)
        
    gameplaysettings = data.get("gameplaysettings", {})
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
        "debug_current_ref_id": data.get("debug_current_ref_id"),
        "difficulty": gameplaysettings.get("difficulty"),
        "ironman": gameplaysettings.get("ironman"),
        "historical": gameplaysettings.get("historical")
    }

    # Insert data into db
    cursor.execute(f'''
        INSERT INTO {table_name} (
            dataset_id, timestamp, date, save_version, game_unique_seed, game_unique_id,
            session, version, multiplayer_random_count, multiplayer_random_seed, debug_current_ref_id,
            ironman, historical, difficulty
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(extracted.values()))

    print(f"{function_log_tag} General info successfully exported.")
