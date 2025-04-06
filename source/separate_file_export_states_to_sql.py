import json
import sqlite3
import os
import sys
import pandas as pd

# Function returning building summary as separate values (sum, count * 100)
def format_levels(levels):
    if isinstance(levels, list):
        total_sum = sum(levels)
        count_multiplied = len(levels) * 100
        return total_sum, count_multiplied
    return 0, 0

# Get arguments: database and json filenames
if len(sys.argv) < 3:
    print("Usage: python script.py <database_file> <json_file>")
    sys.exit(1)

DB_FILENAME = sys.argv[1]
json_filename = sys.argv[2]

# Connect to database
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()

# Automatically determine next dataset_id
cursor.execute("SELECT MAX(dataset_id) FROM States")
result = cursor.fetchone()
dataset_id = (result[0] or 0) + 1
print(f"Using dataset_id: {dataset_id}")

# JSON file name is already parsed above
if not os.path.exists(json_filename):
    raise FileNotFoundError(f"File '{json_filename}' does not exist.")

with open(json_filename, 'r', encoding='utf-16') as f:
    data = json.load(f)
    states_data = data.get('states', {})

for state_id, state_info in states_data.items():
    owner = state_info.get('owner', '')
    manpower_available = state_info.get('manpower_pool', {}).get('available', None)
    manpower_locked = state_info.get('manpower_pool', {}).get('locked', None)
    manpower_total = state_info.get('manpower_pool', {}).get('total', None)
    state_category = state_info.get('state_category', '')

    buildings = state_info.get('buildings', {})
    if isinstance(buildings, list):
        buildings = {}

    # Format building data as sum and count values
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

    # Insert data into Main.db with matching 23 columns
    cursor.execute('''
        INSERT INTO States (
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
cursor.close()

# Preview imported data
print("\nPreview of last imported dataset:")
df = pd.read_sql_query("SELECT * FROM States WHERE dataset_id = ? ORDER BY state_id", conn, params=(dataset_id,))
print(df.head(10))

conn.execute("VACUUM;")
conn.close()
print(f"States data imported to {DB_FILENAME} successfully with dataset_id.")
