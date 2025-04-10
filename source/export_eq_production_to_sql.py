import json
from utils.db_utils import timeit

function_log_tag = "[export_equipment_production_to_sql]"
table_name = "Equipment_production"

@timeit
def export_equipment_production_to_sql(cursor, json_path, tracked_flags, dataset_id=None):
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

    # if dataset_id is None:
    #     cursor.execute(f"SELECT MAX(dataset_id) FROM {table_name}")
    #     result = cursor.fetchone()
    #     dataset_id = (result[0] or 0) + 1
    # else:
    #     validate_dataset_id_sequence(cursor, dataset_id)

    # print(f"{function_log_tag} Using dataset_id: {dataset_id}")

    countries_data = data.get("countries", {})

    for country_tag, country in countries_data.items():
        if tracked_flags and tracked_flags.get(country_tag) != 1:
            continue
        reports = country.get("country_reports", {})
        equipment = reports.get("equipment_production", {})
        if not equipment:
            continue

        cursor.execute(f'''
            INSERT OR REPLACE INTO {table_name} (
                country_tag, dataset_id,
                convoy, train, floating_harbor, railway_gun, armor, land_cruiser, motorized,
                mechanized, infantry, capital_ship, submarine, screen_ship, fighter,
                heavy_fighter, interceptor, tactical_bomber, strategic_bomber, cas,
                naval_bomber, missile, emplacement_gun_ammo, ballistic_missile, nuclear_missile,
                sam_missile, suicide, scout_plane, maritime_patrol_plane, air_transport,
                carrier, missile_launcher, support, amphibious, anti_air, artillery,
                anti_tank, rocket, flame
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        ''', (
            country_tag, dataset_id,
            equipment.get("convoy"), equipment.get("train"), equipment.get("floating_harbor"),
            equipment.get("railway_gun"), equipment.get("armor"), equipment.get("land_cruiser"),
            equipment.get("motorized"), equipment.get("mechanized"), equipment.get("infantry"),
            equipment.get("capital_ship"), equipment.get("submarine"), equipment.get("screen_ship"),
            equipment.get("fighter"), equipment.get("heavy_fighter"), equipment.get("interceptor"),
            equipment.get("tactical_bomber"), equipment.get("strategic_bomber"), equipment.get("cas"),
            equipment.get("naval_bomber"), equipment.get("missile"), equipment.get("emplacement_gun_ammo"),
            equipment.get("ballistic_missile"), equipment.get("nuclear_missile"), equipment.get("sam_missile"),
            equipment.get("suicide"), equipment.get("scout_plane"), equipment.get("maritime_patrol_plane"),
            equipment.get("air_transport"), equipment.get("carrier"), equipment.get("missile_launcher"),
            equipment.get("support"), equipment.get("amphibious"), equipment.get("anti_air"),
            equipment.get("artillery"), equipment.get("anti_tank"), equipment.get("rocket"), equipment.get("flame")
        ))

    # cursor.connection.commit()
    # print(f"{function_log_tag} Equipment production data successfully exported.")
