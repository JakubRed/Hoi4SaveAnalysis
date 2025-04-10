import json
from utils.db_utils import validate_dataset_id_sequence

function_log_tag = "[export_dataset_date]"
table_name = "Dataset_date"

def export_dataset_date(cursor, json_path, dataset_id=None):
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

    date_string = data.get("date", None)
    if not date_string:
        raise ValueError(f"{function_log_tag} 'date' not found in JSON file.")

    try:
        year, month, day, hour = map(int, date_string.split('.'))
    except Exception:
        raise ValueError(f"{function_log_tag} Failed to parse 'date': {date_string}")

    cursor.execute(f'''
        INSERT INTO {table_name} (
            dataset_id, year, month, day, hour
        ) VALUES (?, ?, ?, ?, ?)
    ''', (dataset_id, year, month, day, hour))

    cursor.connection.commit()
    print(f"{function_log_tag} {table_name} successfully exported.")
