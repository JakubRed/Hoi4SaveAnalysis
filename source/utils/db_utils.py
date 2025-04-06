def validate_dataset_id_sequence(cursor, dataset_id):
    cursor.execute("SELECT MAX(dataset_id) FROM General_info")
    previous_dataset_id = cursor.fetchone()[0]
    if previous_dataset_id is not None and previous_dataset_id + 1 != dataset_id:
        raise ValueError(f"Inconsistent dataset_id: expected {previous_dataset_id + 1}, got {dataset_id}")
