import os
import time
import shutil
import yaml
from datetime import datetime
from export_states_to_sql import export_states_to_sql
from export_general_info_to_sql import export_general_info_to_sql
from export_countries_to_sql import export_countries_to_sql

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

print("Processing autosave file once...")

# Process the save file 

dataset_id = 2
# export_states_to_sql(db_path, parsed_save_file, dataset_id)
# export_general_info_to_sql(db_path, parsed_save_file, dataset_id)

export_states_to_sql(db_path, parsed_save_file)
export_general_info_to_sql(db_path, parsed_save_file)
export_countries_to_sql(db_path, parsed_save_file)
