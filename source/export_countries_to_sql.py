import os
import sys
import sqlite3
import json
from utils.db_utils import timeit

table_name = "Country"
function_log_tag = "[export_countries_to_sql]"

@timeit
def export_countries_to_sql(cursor, json_path, tracked_flags, dataset_id=None):
    function_log_tag = "[export_countries_to_sql]"

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

    # print(f"{function_log_tag} Using dataset_id: {dataset_id}")

    for country_tag, country_info in countries_data.items():
        if tracked_flags and tracked_flags.get(country_tag) != 1:
            continue
        capital = country_info.get("capital")
        original_capital = country_info.get("original_capital")
        political_power = country_info.get("political_power")
        stability = country_info.get("stability")
        war_support = country_info.get("war_support")
        manpower_ratio = country_info.get("manpower_ratio")

        cursor.execute(f'''
            INSERT INTO {table_name} (
                dataset_id, country_tag, capital, original_capital,
                political_power, stability, war_support, manpower_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            dataset_id, country_tag, capital, original_capital,
            political_power, stability, war_support, manpower_ratio
        ))