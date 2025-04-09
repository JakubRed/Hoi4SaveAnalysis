import json
from utils.db_utils import validate_dataset_id_sequence

function_log_tag = "[export_construction_to_sql]"
table_name = "Construction"

def export_construction_to_sql(cursor, json_path, dataset_id=None):
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

    print(f"{function_log_tag} Using dataset_id: {dataset_id}")

    countries_data = data.get("countries", {})

    for tag, country in countries_data.items():
        reports = country.get("country_reports", {})
        construction = reports.get("construction", {})
        if not construction:
            continue

        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} (
                country_tag, dataset_id,
                civilian_factory, military_factory, dockyard, port, infrastructure,
                air_base, rocket_site, gun_emplacement, radar, anti_air, refinery,
                fuel_silo, supply_node, nuclear_reactor, land_fort, naval_fort, other
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            tag, dataset_id,
            construction.get("civilian_factory"),
            construction.get("military_factory"),
            construction.get("dockyard"),
            construction.get("port"),
            construction.get("infrastructure"),
            construction.get("air_base"),
            construction.get("rocket_site"),
            construction.get("gun_emplacement"),
            construction.get("radar"),
            construction.get("anti_air"),
            construction.get("refinery"),
            construction.get("fuel_silo"),
            construction.get("supply_node"),
            construction.get("nuclear_reactor"),
            construction.get("land_fort"),
            construction.get("naval_fort"),
            construction.get("other")
        ))

    cursor.connection.commit()
    print(f"{function_log_tag} Construction data successfully exported.")
