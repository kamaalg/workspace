from dotenv import load_dotenv
import os
import pandas as pd
import psycopg
from psycopg.rows import dict_row
load_dotenv()
from datetime import timezone,datetime
DB_URL = os.getenv("DB_URL", "postgresql+psycopg://app:app@postgres:5432/app")
# So basically what we have to do is migrate csv to postgres then run all of the data points through Google Earth :DONE
# while obtaining the the data and putting into seperate table then do it for 3 month historical data:NOT DONE
# then make a script that will make a plot out of these data points and save it as an image:NOT DONE
#then create api endpoint to serve these images based on id of the data point:NOT DONE
# then create a simple frontend to display these images based on id input:NOT DONE
def migrate_csv_to_postgres(csv_file_path: str) -> None:


    df = pd.read_csv(csv_file_path)
    dsn = DB_URL.replace("postgresql+psycopg://", "postgresql://")
    df["edidtt"] = pd.to_datetime(
    df["edidtt"], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce").dt.tz_localize("UTC")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            

            # Insert data into the table
            for _, row in df.iterrows():
                print(row["OBJECTID"])
                edidtt = None if pd.isna(row["edidtt"]) else row["edidtt"].to_pydatetime()
                if pd.isna(row["okdt"]):
                    okdt = None
                else:
                    okdt = row["okdt"]
                olcutarix = None if pd.isna(row["olcutarix"]) else row["olcutarix"]

                cur.execute("""
                INSERT INTO parcels (object_id, year_val, name, rayon, sahe, kod, olcutarix, ad, ok, fayl, importad, importdt, ediad, edidtt, okad, okdt, olcen, qeyd, agronom_name, village_name, village_elder_name, district_name, fin_kod, village, district, id_value, square, mulk, teyinat, itf, il, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (row["OBJECTID"], row["Year"], row["Name"], row["rayon"], row["sahe"], row["kod"], olcutarix, row["ad"], row["ok"], row["fayl"], row["importad"], row["importdt"], row["ediad"], edidtt, row["okad"], okdt, row["olcen"], row["qeyd"], row["AgronomName"], row["VillageName"], row["VillageElderName"], row["DistrictName"], row["FinKod"], row["Village"], row["District"], row["ID"], row["Square"], row["mulk"], row["teyinat"], row["ITF"], row["Il"], row["Geom"]))
            
            conn.commit()
    print("Data migration completed.")
migrate_csv_to_postgres("/workspace/geozona_2025_202509161646.csv")