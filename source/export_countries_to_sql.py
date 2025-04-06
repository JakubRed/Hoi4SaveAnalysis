import os
import sys
import sqlite3
import json
from utils.db_utils import validate_dataset_id_sequence

table_name = "Country"
function_log_tag = "[export_countries_to_sql]"

def export_countries_to_sql(db_path, json_path, dataset_id=None):
    function_log_tag = "[export_countries_to_sql]"
    # Connect to database
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
        countries_data = data.get('countries', {})

    # Determine dataset_id if not given
    if dataset_id is None:
        cursor.execute(f"SELECT MAX(dataset_id) FROM {table_name}")
        result = cursor.fetchone()
        dataset_id = (result[0] or 0) + 1
    else:
        validate_dataset_id_sequence(cursor, dataset_id)

    print(f"{function_log_tag} Using dataset_id: {dataset_id}")

    for tag, country_info in countries_data.items():
        capital = country_info.get("capital")
        original_capital = country_info.get("original_capital")
        political_power = country_info.get("political_power")
        stability = country_info.get("stability")
        war_support = country_info.get("war_support")
        manpower_ratio = country_info.get("manpower_ratio")

        cursor.execute(f'''
            INSERT INTO {table_name} (
                dataset_id, tag, capital, original_capital,
                political_power, stability, war_support, manpower_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dataset_id, tag, capital, original_capital,
            political_power, stability, war_support, manpower_ratio
        ))

    conn.commit()
    conn.execute("VACUUM;")
    conn.close()
    print(f"{function_log_tag} Countries successfully exported.")

if __name__ == "__main__":
    print(f"{function_log_tag} Running standalone export...")
    if len(sys.argv) < 3:
        print("Usage: python export_countries_to_sql.py <database_file> <json_file> [<dataset_id>]")
        sys.exit(1)

    db_path = sys.argv[1]
    json_path = sys.argv[2]
    dataset_id = int(sys.argv[3]) if len(sys.argv) > 3 else None

    export_countries_to_sql(db_path, json_path, dataset_id)
