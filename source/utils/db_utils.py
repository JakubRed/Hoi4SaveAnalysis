import subprocess
import os

def validate_dataset_id_sequence(cursor, dataset_id):
    if dataset_id != 0:
        cursor.execute("SELECT MAX(dataset_id) FROM General_info")
        previous_dataset_id = cursor.fetchone()[0]
        print(f"previous dataset: {previous_dataset_id}")
        if previous_dataset_id is not None and previous_dataset_id + 1 != dataset_id:
            print("Inconsistent dataset_id: expected {}, got {}".format(previous_dataset_id, dataset_id))
            raise ValueError("Inconsistent dataset_id: expected {}, got {}".format(previous_dataset_id, dataset_id))

def safe_round(x):
    return round(x, 1) if x is not None else None

def clear_all_tables(cursor):
    tables_to_clear = [
        "Country", "Construction", "Dataset_date", "Fuel", "Equipment_production",
        "General_info", "States"         
    ]
    for table in tables_to_clear:
        cursor.execute(f"DELETE FROM {table}")
    cursor.connection.commit()
    print("[db_utils] Cleared all data from tables.")

def load_country_tracking_flags(cursor):
    function_log_tag = "[load_country_tracking_flags]"
    # Check if the table exists
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='Start_settings_for_each_country'
    """)
    if not cursor.fetchone():
        raise RuntimeError("Table 'Start_settings_for_each_country' does not exist in the database.")

    cursor.execute("SELECT country_tag, is_tracked FROM Start_settings_for_each_country")
    results = cursor.fetchall()

    country_flags = {row[0]: row[1] for row in results}

    print(f"{function_log_tag} Loaded tracking flags for {len(country_flags)} countries.")
    return country_flags

def convert_save_to_json(hoi4save_path, save_path, output_path):
    """
    Converts a Hearts of Iron IV .hoi4 save file to JSON using the hoi4save tool.

    Parameters:
    - hoi4save_path: str — path to the compiled hoi4save CLI tool
    - save_path: str — path to the input .hoi4 save file
    - output_path: str — path to where the output JSON should be written
    """
    function_log_tag = "[convert_save_to_json]"
    if not os.path.exists(hoi4save_path):
        raise FileNotFoundError(f"{function_log_tag} hoi4save tool not found at: {hoi4save_path}")

    if not os.path.exists(save_path):
        raise FileNotFoundError(f"{function_log_tag} Save file not found: {save_path}")

    print(f"{function_log_tag} Converting {save_path} to {output_path}...")

    try:
        with open(output_path, 'w', encoding='utf-8') as outfile:
            subprocess.run([
                hoi4save_path,
                save_path
            ], check=True, stdout=outfile)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"{function_log_tag} Failed to convert save file: {e}")

    print(f"{function_log_tag} Conversion completed successfully → {output_path}")
