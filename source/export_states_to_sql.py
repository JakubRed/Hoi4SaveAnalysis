import os
import sys
import sqlite3
import json
import pandas as pd
from utils.db_utils import validate_dataset_id_sequence
from datetime import datetime
function_log_tag = "[export_states_to_sql]"
table_name = "States"

def format_levels(levels):
    if isinstance(levels, list):
        total_sum = sum(levels)
        count_multiplied = len(levels) * 100
        return total_sum, count_multiplied
    return 0, 0

def export_states_to_sql(db_path, json_path, dataset_id=None):
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
        states_data = data.get('states', {})

    # Determine dataset_id if not given
    if dataset_id is None:
        cursor.execute(f"SELECT MAX(dataset_id) FROM {table_name}")
        result = cursor.fetchone()
        dataset_id = (result[0] or 0) + 1
    else:
        validate_dataset_id_sequence(cursor, dataset_id)
    print(f"{function_log_tag} Using dataset_id: {dataset_id}") #to be removed

    for state_id, state_info in states_data.items():
        owner = state_info.get('owner', '')
        manpower_available = state_info.get('manpower_pool', {}).get('available')
        manpower_locked = state_info.get('manpower_pool', {}).get('locked')
        manpower_total = state_info.get('manpower_pool', {}).get('total')
        state_category = state_info.get('state_category', '')

        buildings = state_info.get('buildings', {})
        if isinstance(buildings, list):
            buildings = {}

        formatted = {}
        for key, json_key in [
            ('infrastructure', 'infrastructure'),
            ('industrial_complex', 'industrial_complex'),
            ('arms_factory', 'arms_factory'),
            ('dockyard', 'dockyard'),
            ('anti_air', 'anti_air_building'),
            ('fuel_silo', 'fuel_silo'),
            ('radar_station', 'radar_station'),
            ('synthetic_refinery', 'synthetic_refinery')
        ]:
            s, c = format_levels(buildings.get(json_key, {}).get('level', []))
            formatted[f'{key}_sum'] = s
            formatted[f'{key}_count'] = c

    # Insert data into db
        cursor.execute(f'''
            INSERT INTO {table_name} (
                state_id, dataset_id, owner, manpower_available, manpower_locked, manpower_total, state_category,
                infrastructure_sum, infrastructure_count,
                industrial_complex_sum, industrial_complex_count,
                arms_factory_sum, arms_factory_count,
                dockyard_sum, dockyard_count,
                anti_air_sum, anti_air_count,
                fuel_silo_sum, fuel_silo_count,
                radar_station_sum, radar_station_count,
                synthetic_refinery_sum, synthetic_refinery_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            state_id, dataset_id, owner, manpower_available, manpower_locked, manpower_total, state_category,
            formatted['infrastructure_sum'], formatted['infrastructure_count'],
            formatted['industrial_complex_sum'], formatted['industrial_complex_count'],
            formatted['arms_factory_sum'], formatted['arms_factory_count'],
            formatted['dockyard_sum'], formatted['dockyard_count'],
            formatted['anti_air_sum'], formatted['anti_air_count'],
            formatted['fuel_silo_sum'], formatted['fuel_silo_count'],
            formatted['radar_station_sum'], formatted['radar_station_count'],
            formatted['synthetic_refinery_sum'], formatted['synthetic_refinery_count']
        ))

    conn.commit()

    conn.execute("VACUUM;") #to be removed
    conn.close()
    print(f"{function_log_tag} {table_name} successfully exported.")

# Allow direct execution
if __name__ == "__main__":
    print(f"{function_log_tag} Running standalone export...")
    if len(sys.argv) < 3:
        print("Usage: python export_states_to_sql.py <database_file> <json_file> [<dataset_id>]")
        sys.exit(1)

    db_path = sys.argv[1]
    json_path = sys.argv[2]
    dataset_id = int(sys.argv[3]) if len(sys.argv) > 3 else None

    export_states_to_sql(db_path, json_path, dataset_id)
