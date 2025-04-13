import time
import shutil
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime
from utils.db_utils import convert_save_to_json
from data_exctraction import run_data_extraction

import sys
import yaml

main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
config_path = os.path.join(main_dir, 'data', 'config.yaml')
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)
with open(sys.argv[1], "r", encoding="utf-8") as f:
    instance = yaml.safe_load(f)

save_filename = instance.get("autosave_file", "autosave.hoi4")
# input_folder = os.path.expanduser(instance["input_folder"])
input_folder = config.get("input_folder")
backup_path = instance["backup_path"]
hoi4save_parser_path = config.get("hoi4save_parser_path")
output_path = os.path.join(instance["processed_folder"], instance["output_file"])

class SaveChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return

        if os.path.basename(event.src_path).lower() == save_filename.lower():
            # Wait until the file stops changing (with timeout)
            last_size = -1
            unchanged_counter = 0
            timeout = 60  # maximum 60 seconds waiting time
            start_time = time.time()
            while True:
                current_size = os.path.getsize(event.src_path)
                if current_size == last_size:
                    unchanged_counter += 1
                    if unchanged_counter >= 2:
                        break
                else:
                    unchanged_counter = 0
                if time.time() - start_time > timeout:
                    print(f"[watcher] Timeout while waiting for {save_filename} to stabilize.")
                    return
                last_size = current_size
                time.sleep(1)
            print(f"[watcher] Detected change in {save_filename}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"autosave_{timestamp}.hoi4"
            backup_file_path  = os.path.join(backup_path, backup_name)

            os.makedirs(backup_file_path , exist_ok=True)
            shutil.copy2(event.src_path, backup_file_path)
            print(f"[watcher] Backup created at {backup_file_path}")

            convert_save_to_json(hoi4save_parser_path, event.src_path, output_path)
            run_data_extraction(output_path)

if __name__ == "__main__":
    print(f"[watcher] Monitoring: {input_folder}")
    observer = Observer()
    event_handler = SaveChangeHandler()
    observer.schedule(event_handler, path=input_folder, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
