import json
from utils.db_utils import safe_round

function_log_tag = "[export_fuel_to_sql]"
table_name = "Fuel"

def export_fuel_to_sql(cursor, json_path, tracked_flags, dataset_id=None):
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
    # if dataset_id is None:
    #     cursor.execute(f"SELECT MAX(dataset_id) FROM {table_name}")
    #     result = cursor.fetchone()
    #     dataset_id = (result[0] or 0) + 1
    # else:
    #     validate_dataset_id_sequence(cursor, dataset_id)

    # print(f"{function_log_tag} Using dataset_id: {dataset_id}")

    countries_data = data.get("countries", {})

    for country_tag, country in countries_data.items():
        if tracked_flags and tracked_flags.get(country_tag) != 1:
            continue
        fuel_info = country.get("fuel_status", {})
        if not fuel_info:
            continue  # skip countries with no fuel info

        cursor.execute(f'''
            INSERT INTO {table_name} (
                Country_tag, dataset_id, fuel, max_fuel, fuel_gain,
                fuel_cost, fuel_gain_per_oil, fuel_gain_from_states,
                fuel_gain_from_lend_lease, fuel_consumption_from_lend_lease
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            country_tag, dataset_id,
            safe_round(fuel_info.get("fuel")),
            safe_round(fuel_info.get("max_fuel")),
            safe_round(fuel_info.get("fuel_gain")),
            safe_round(fuel_info.get("fuel_cost")),
            safe_round(fuel_info.get("fuel_gain_per_oil")),
            safe_round(fuel_info.get("fuel_gain_from_states")),
            safe_round(fuel_info.get("fuel_gain_from_lend_lease")),
            safe_round(fuel_info.get("fuel_consumption_from_lend_lease"))
        ))

    cursor.connection.commit()
    print(f"{function_log_tag} Fuel data successfully exported.")
