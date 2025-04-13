import os
import time
import shutil
import sqlite3
import yaml
from datetime import datetime
from export_states_to_sql import export_states_to_sql
from export_general_info_to_sql import export_general_info_to_sql
from export_countries_to_sql import export_countries_to_sql
from export_dataset_date_to_sql import export_dataset_date
from export_fuel_to_sql import export_fuel_to_sql
from export_construction_to_sql import export_construction_to_sql
from export_eq_production_to_sql import export_equipment_production_to_sql
import utils.db_utils as db_utils

from time import perf_counter

# Load main config file
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
config_path = os.path.join(main_dir, 'data', 'config.yaml')
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Load instance-specific config from argument
import sys
if len(sys.argv) < 2:
    print("Usage: python main.py <instance_config.yaml>")
    sys.exit(1)

instance_config_path = sys.argv[1]
with open(instance_config_path, "r", encoding="utf-8") as f:
    instance = yaml.safe_load(f)

# Merge configs (instance values override general ones)
config.update(instance)

autosave_name = config["autosave_file"]
input_folder = config["input_folder"]
poll_interval = config.get("poll_interval", 5)
watch_enabled = config.get("watch_for_changes", True)
parsed_save_file = os.path.join(main_dir, instance["processed_folder"], config["parsed_save_file"])
db_path = os.path.join(main_dir, instance["db_path"], instance["db_name"])
last_modified = None
hoi4save_parser_path = config.get("hoi4save_parser_path")
save_path = config.get("input_folder")
save_path = os.path.join(config["input_folder"], config["autosave_file"])
print(f"save Path: {save_path}")
output_path = os.path.join(instance["processed_folder"], instance.get("output_file"))

print("Processing autosave file once...")

# Process the save file 
# db_utils.convert_save_to_json(hoi4save_parser_path, save_path, output_path)

# dataset_id = 2
# export_states_to_sql(db_path, parsed_save_file, dataset_id)
# export_general_info_to_sql(db_path, parsed_save_file, dataset_id)

# Connect to database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
tracked_countries = db_utils.load_country_tracking_flags(cursor)
for key in tracked_countries:
    tracked_countries[key] = 0
tracked_countries = {"GER": 1, "ENG": 1, "USA": 1, "FRA": 1, "SOV": 1, "ITA": 1, "JAP": 1, "POL" : 1} # 8 countries

db_utils.clear_all_tables(cursor)
# export_general_info_to_sql(cursor, parsed_save_file, 1)
conn.commit()

cursor.execute(f"SELECT MAX(dataset_id) FROM Dataset_date")
dataset_id = cursor.fetchone()
dataset_id = (dataset_id[0] or 0)

start = perf_counter()

for i in range(1):
    
    loop_start = perf_counter()
    
    dataset_id = dataset_id + 1
    print(f"dataset id: {dataset_id}")
    export_general_info_to_sql(cursor, parsed_save_file, dataset_id)
    export_construction_to_sql(cursor, parsed_save_file, tracked_countries, dataset_id)
    export_countries_to_sql(cursor, parsed_save_file, tracked_countries, dataset_id)
    export_dataset_date(cursor, parsed_save_file, dataset_id)
    export_equipment_production_to_sql(cursor, parsed_save_file, tracked_countries, dataset_id)
    export_fuel_to_sql(cursor, parsed_save_file, tracked_countries, dataset_id)
    export_states_to_sql(cursor, parsed_save_file, tracked_countries, dataset_id)
    
    loop_end = perf_counter()
    print(f"[LOOP] Export nr {i+1:<31} [LOOP]  - {loop_end - loop_start:.4f} s")
        
    conn.commit()
    # print()
    
end = perf_counter()
print(f"[DATASET] Export {+1:<31} [DATASET]-{end - start:.4f} s")
    
conn.execute("VACUUM;")
conn.close()
