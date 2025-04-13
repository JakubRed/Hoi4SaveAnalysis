import sqlite3
import json
import os
import sys

def extract_grouped_by_dataset(db_path, max_datasets=None, format_mode="list"):
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Database file '{db_path}' does not exist.")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    grouped = {}

    for table in tables:
        # Get column names
        cursor.execute(f"PRAGMA table_info({table});")
        columns_info = cursor.fetchall()
        column_names = [col[1] for col in columns_info]

        if "dataset_id" not in column_names:
            continue

        has_id = "id" in column_names

        # Load all data from table
        cursor.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        for row in rows:
            row_dict = dict(zip(columns, row))

            if "dataset_id" not in row_dict:
                continue

            try:
                dataset_id_int = int(row_dict["dataset_id"])
            except (ValueError, TypeError):
                continue

            if max_datasets is not None and dataset_id_int > max_datasets:
                continue

            dataset_id = str(dataset_id_int)

            if dataset_id not in grouped:
                grouped[dataset_id] = {}

            if format_mode == "dict":
                if not has_id or "id" not in row_dict or row_dict["id"] is None:
                    continue

                row_id = str(row_dict["id"])

                if table not in grouped[dataset_id]:
                    grouped[dataset_id][table] = {}

                grouped[dataset_id][table][row_id] = row_dict
            else:
                if table not in grouped[dataset_id]:
                    grouped[dataset_id][table] = []

                grouped[dataset_id][table].append(row_dict)

    conn.close()
    return grouped


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py path/to/database.db path/to/output.json [max_dataset_id] [format_mode]")
        print("format_mode: 'list' (default) or 'dict'")
        sys.exit(1)

    db_path = sys.argv[1]
    output_path = sys.argv[2]

    if os.path.isdir(output_path):
        print(f"Error: '{output_path}' is a directory. Please provide a full output file path like 'output.json'.")
        sys.exit(1)

    max_datasets = None
    if len(sys.argv) >= 4:
        try:
            max_datasets = int(sys.argv[3])
        except ValueError:
            print("Error: max_dataset_id must be an integer.")
            sys.exit(1)

    format_mode = "list"
    if len(sys.argv) >= 5:
        if sys.argv[4] in ["list", "dict"]:
            format_mode = sys.argv[4]
        else:
            print("Error: format_mode must be 'list' or 'dict'")
            sys.exit(1)

    try:
        result = extract_grouped_by_dataset(db_path, max_datasets, format_mode)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        print(f"✅ Data saved to '{output_path}' in '{format_mode}' mode.")
    except Exception as e:
        print(f"❌ Error: {e}")
