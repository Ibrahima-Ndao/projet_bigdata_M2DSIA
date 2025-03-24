import psycopg2
import csv
from datetime import datetime
import os
from psycopg2.extras import execute_values
from dotenv import load_dotenv
load_dotenv()


# Nom de la table pour les données d'accidents
table_name = "motor_vehicle_crashes"



# Commande SQL pour créer la table
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    Year INTEGER,                          -- Année de l'accident
    CaseVehicleID INTEGER,                 -- Identifiant du véhicule dans l'accident
    VehicleBodyType VARCHAR(250),           -- Type de carrosserie
    RegistrationClass VARCHAR(250),         -- Classe d'immatriculation
    ActionPriorToAccident VARCHAR(250),    -- Action avant l'accident
    TruckBusTypeAxles VARCHAR(250),         -- Type / Essieux des camions/bus
    DirectionOfTravel VARCHAR(250),         -- Direction de déplacement
    FuelType VARCHAR(250),                  -- Type de carburant
    VehicleYear INTEGER,                   -- Année du véhicule
    StateOfRegistration VARCHAR(250),       -- État d'immatriculation
    NumberOfOccupants INTEGER,             -- Nombre d'occupants
    EngineCylinders INTEGER,               -- Cylindres du moteur
    VehicleMake VARCHAR(250),               -- Marque du véhicule
    ContributingFactor1 VARCHAR(250),      -- Facteur contributif 1
    ContributingFactor1Description TEXT,   -- Description du facteur contributif 1
    ContributingFactor2 VARCHAR(250),      -- Facteur contributif 2
    ContributingFactor2Description TEXT,   -- Description du facteur contributif 2
    EventType VARCHAR(250),                 -- Type d'événement
    PartialVIN VARCHAR(250)                 -- Numéro partiel du véhicule (VIN)
);
"""

# Liste des colonnes pour l'insertion
table_columns = [
    "Year", "CaseVehicleID", "VehicleBodyType", "RegistrationClass", "ActionPriorToAccident",
    "TruckBusTypeAxles", "DirectionOfTravel", "FuelType", "VehicleYear", "StateOfRegistration",
    "NumberOfOccupants", "EngineCylinders", "VehicleMake", "ContributingFactor1",
    "ContributingFactor1Description", "ContributingFactor2", "ContributingFactor2Description",
    "EventType", "PartialVIN"
]

# Commande SQL pour insérer les données
insert_query = f"""
    INSERT INTO {table_name} ({', '.join(table_columns)})
    VALUES %s
    ON CONFLICT DO NOTHING;
"""


DATABASE_URL = os.getenv('DATABASE_URL')
print(DATABASE_URL)

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def create_crash_table(conn):
    cur = conn.cursor()
    print(f"Creating query: {create_table_query}")
    try:
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully or already exists.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()

# Utilitaire pour convertir les champs
def safe_cast(value, to_type):
    if value == "":
        return None
    try:
        return to_type(value)
    except ValueError:
        return None

def insert_crash_data(conn, input_file_path, batch_size=100):
    cursor = conn.cursor()
    rows_to_insert = []
    
    with open(input_file_path, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file)
        headers = next(csv_reader)

        for idx, row in enumerate(csv_reader, start=2):
            if len(row) < 19:
                print(f"Ligne {idx} ignorée (colonnes insuffisantes) : {row}")
                continue

            formatted_row = [
                safe_cast(row[0], int),
                safe_cast(row[1], int),
                row[2] or None,
                row[3] or None,
                row[4] or None,
                row[5] or None,
                row[6] or None,
                row[7] or None,
                safe_cast(row[8], int),
                row[9] or None,
                safe_cast(row[10], int),
                safe_cast(row[11], int),
                row[12] or None,
                row[13] or None,
                row[14] or None,
                row[15] or None,
                row[16] or None,
                row[17] or None,
                row[18] or None
            ]
            rows_to_insert.append(formatted_row)

            if len(rows_to_insert) >= batch_size:
                execute_values(cursor, insert_query.replace("VALUES (%s)", "VALUES %s"), rows_to_insert)
                conn.commit()
                print(f"{len(rows_to_insert)} lignes insérées.")
                rows_to_insert = []  # Resetter après chaque commit

        # Insertion du dernier batch, s'il y en a un
        if rows_to_insert:
            execute_values(cursor, insert_query.replace("VALUES (%s)", "VALUES %s"), rows_to_insert)
            conn.commit()
            print(f"{len(rows_to_insert)} dernières lignes insérées.")
    
    cursor.close()

if __name__ == '__main__':
    conn = get_conn()
    create_crash_table(conn)
    input_file_path = "Motor_Vehicle_Crashes.csv"
    print("Starting data insertion...")
    try:
        insert_crash_data(conn, input_file_path)
    except Exception as e:
        print(f"Error during data insertion: {e}")

    #insert_crash_data(conn, input_file_path)
