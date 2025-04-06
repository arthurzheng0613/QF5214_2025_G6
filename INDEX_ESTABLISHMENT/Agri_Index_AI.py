import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine
from tqdm import tqdm

# ===== Configuration =====
latitude_min, latitude_max = 24, 49
longitude_min, longitude_max = -125, -66
start_date = "2018-01-01"
end_date = "2024-12-31"
output_table = 'agri_ai_index'

# ===== Database Info =====
user = 'g6_qf5214_1'
password = 'qf5214_1'
host = 'rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com'
port = 3306
db = 'nasa_power'

# ===== Load AI Weights =====
weight_df = pd.read_csv("AI_Assigned_Index_Weight.csv")

# ===== Connect to DB =====
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
conn = engine.connect()

# ===== Query Templates =====
query_template = """
SELECT * FROM {table}
WHERE Longitude BETWEEN {lon_min} AND {lon_max}
AND Latitude BETWEEN {lat_min} AND {lat_max}
AND Timestamp BETWEEN '{start}' AND '{end}'
"""

print("🔄 Loading data from database...")

def read_sql_with_progress(table, conn, sql_template, chunksize=20000, **kwargs):
    print("🟡 Counting total rows...")
    count_sql = f"""
        SELECT COUNT(*) FROM {table}
        WHERE Longitude BETWEEN {kwargs['lon_min']} AND {kwargs['lon_max']}
        AND Latitude BETWEEN {kwargs['lat_min']} AND {kwargs['lat_max']}
        AND Timestamp BETWEEN '{kwargs['start']}' AND '{kwargs['end']}'
    """
    total = pd.read_sql_query(count_sql, conn).iloc[0, 0]
    print(f"✅ Total rows = {total}")

    sql = sql_template.format(table=table, **kwargs)
    chunks = []

    print("📦 Starting to fetch data chunks...")
    reader = pd.read_sql_query(sql, conn, chunksize=chunksize)
    for chunk in tqdm(reader, total=(total // chunksize) + 1, desc=f"📊 Loading {table}"):
        chunks.append(chunk)

    return pd.concat(chunks, ignore_index=True)

df_meteorology = read_sql_with_progress(
    table="meteorology",
    conn=conn,
    sql_template=query_template,
    lon_min=longitude_min,
    lon_max=longitude_max,
    lat_min=latitude_min,
    lat_max=latitude_max,
    start=start_date,
    end=end_date
)

df_radiation = read_sql_with_progress(
    table="radiation",
    conn=conn,
    sql_template=query_template,
    lon_min=longitude_min,
    lon_max=longitude_max,
    lat_min=latitude_min,
    lat_max=latitude_max,
    start=start_date,
    end=end_date
)

conn.close()

# ===== Merge Datasets =====
key_cols = ['Longitude', 'Latitude', 'Timestamp']
df_all = pd.merge(df_meteorology, df_radiation, on=key_cols, how='outer')
print(f"✅ Merged {df_all.shape[0]} records.")

# ===== Standardize and Calculate AI_EXPERT_INDEX =====
def standardize_column(col, method):
    if method == 'min-max normalization':
        return (col - col.min()) / (col.max() - col.min())
    elif method == 'min-max inverse normalization':
        return 1 - ((col - col.min()) / (col.max() - col.min()))
    else:
        return col

df_all['AI_EXPERT_INDEX'] = 0
for _, row in tqdm(weight_df.iterrows(), total=len(weight_df), desc="Calculating index"):
    var = row['variable']
    weight = row['AI_Assigned_Weight']
    impact = row.get('AI_Judged_Impact', 'Positive')
    if var in df_all.columns:
        try:
            std_col = standardize_column(df_all[var], 'min-max normalization')
            if impact == 'Negative':
                std_col *= -1
            df_all['AI_EXPERT_INDEX'] += std_col * weight
        except Exception as e:
            print(f"⚠️ Issue processing {var}: {e}")

# Scale to [-100, 100]
df_all['AI_EXPERT_INDEX'] = df_all['AI_EXPERT_INDEX'] * 200 - 100

# ===== Export CSV =====
csv_path = "Agricultural_Environment_AI_Expert_Index.csv"
df_all.to_csv(csv_path, index=False)
print(f"📄 Index saved to: {csv_path}")

# ===== Generate Heatmap =====
latest_date = df_all['Timestamp'].max()
heat_df = df_all[df_all['Timestamp'] == latest_date].copy()

if not heat_df.empty:
    pivot_table = heat_df.pivot_table(index='Latitude', columns='Longitude', values='AI_EXPERT_INDEX')
    plt.figure(figsize=(12, 6))
    sns.heatmap(pivot_table.sort_index(ascending=False), cmap='RdYlGn', center=0)
    plt.title(f'AI_EXPERT_INDEX Heatmap on {latest_date}')
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    image_path = "AI_Expert_Index_Heatmap.png"
    plt.savefig(image_path, dpi=300)
    plt.close()
    print(f"🖼️ Heatmap image saved to: {image_path}")
else:
    print("⚠️ No data available for heatmap generation.")

# ===== Write Back to Database=====
summary_df = df_all[['Longitude', 'Latitude', 'Timestamp', 'AI_EXPERT_INDEX']].copy()
summary_df.to_sql(output_table, engine, if_exists='replace', index=False)
print(f"🗃️ Index data written to table: {output_table}")
