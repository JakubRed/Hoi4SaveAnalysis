import json
from utils.db_utils import validate_dataset_id_sequence

function_log_tag = "[export_construction_to_sql]"
table_name = "Construction"

def export_construction_to_sql(cursor, json_path, tracked_flags, dataset_id=None):
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

    countries_data = data.get("countries", {})

    for country_tag, country in countries_data.items():
        if tracked_flags and tracked_flags.get(country_tag) != 1:
            continue
        reports = country.get("country_reports", {})
        construction = reports.get("construction", {})
        if not construction:
            continue
        
        country_tag, dataset_id,
        civilian_factory = construction.get("civilian_factory")
        military_factory = construction.get("military_factory")
        dockyard = construction.get("dockyard")
        port = construction.get("port")
        infrastructure = construction.get("infrastructure")
        air_base = construction.get("air_base")
        rocket_site = construction.get("rocket_site")
        gun_emplacement = construction.get("gun_emplacement")
        radar = construction.get("radar")
        anti_air = construction.get("anti_air")
        refinery = construction.get("refinery")
        fuel_silo = construction.get("fuel_silo")
        supply_node = construction.get("supply_node")
        nuclear_reactor = construction.get("nuclear_reactor")
        land_fort = construction.get("land_fort")
        naval_fort = construction.get("naval_fort")
        other = construction.get("other")

        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} (
                country_tag, dataset_id,
                civilian_factory, military_factory, dockyard, port, infrastructure,
                air_base, rocket_site, gun_emplacement, radar, anti_air, refinery,
                fuel_silo, supply_node, nuclear_reactor, land_fort, naval_fort, other
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            country_tag, dataset_id, civilian_factory, military_factory, dockyard, port,
            infrastructure, air_base, rocket_site, gun_emplacement, radar, anti_air, refinery,
            fuel_silo, supply_node, nuclear_reactor, land_fort, naval_fort, other
        ))

    cursor.connection.commit()
    print(f"{function_log_tag} Construction data successfully exported.")
