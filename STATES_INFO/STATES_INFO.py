import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import mysql.connector
import pymysql

# === Config ===
SHAPEFILE_PATH = "./US_States_Shapefile/cb_2022_us_state_500k.shp"
OUTPUT_CSV = "States_Info.csv"
TABLE_NAME = 'location'

# === Load Shapefile ===
print("Loading shapefile...")
states_gdf = gpd.read_file(SHAPEFILE_PATH).to_crs("EPSG:4326")

# === Coordinate Grid ===
print("Generating coordinate grid...")
lat_range = range(24, 50)
lon_range = range(-125, -65)
grid_points = [(lon, lat) for lat in lat_range for lon in lon_range]

df_points = pd.DataFrame(grid_points, columns=["Longitude", "Latitude"])
df_points["geometry"] = df_points.apply(lambda row: Point(row["Longitude"], row["Latitude"]), axis=1)
gdf_points = gpd.GeoDataFrame(df_points, geometry="geometry", crs="EPSG:4326")

# === Match to States ===
print("Matching coordinates to states...")
joined = gpd.sjoin(gdf_points, states_gdf[["NAME", "geometry"]], how="left", predicate="within")
joined["State"] = joined["NAME"].fillna("Ocean Monitoring Point")
final_df = joined[["Longitude", "Latitude", "State"]]

print(f"Saving CSV to {OUTPUT_CSV}...")
final_df.to_csv(OUTPUT_CSV, index=False)

print("📡 Connecting to MySQL...")
try:
    conn = pymysql.connect(
    user='g6_qf5214_1',
    password='qf5214_1',
    host='rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com',
    port=3306,
    database='nasa_power'
)
    cursor = conn.cursor()
    print("✅ Connected.")

    print(f"🧹 Clearing table `{TABLE_NAME}`...")
    cursor.execute(f"DELETE FROM {TABLE_NAME}")
    conn.commit()

    print(f"⬆️ Inserting {len(final_df)} rows...")
    for i, row in enumerate(final_df.itertuples(index=False), 1):
        try:
            cursor.execute(
                f"INSERT INTO {TABLE_NAME} (Longitude, Latitude, State) VALUES (%s, %s, %s)",
                (row.Longitude, row.Latitude, row.State)
            )
            if i % 500 == 0:
                print(f"  ...Inserted {i} rows")
        except Exception as e:
            print(f"❌ Error on row {i}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Upload complete.")
except Exception as conn_err:
    print(f"❌ Database connection failed: {conn_err}")