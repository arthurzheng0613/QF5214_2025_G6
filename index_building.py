import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from IPython.display import display

user = 'g6_qf5214_5'
password = 'qf5214_5'
host = 'rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com'
port = 3306
db = 'nasa_power'

def get_engine():
    return create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4')

def query_sql(sql):
    try:
        engine = get_engine()
        with engine.connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception as e:
        print(f"查询失败: {e}")
        return None

sql1 = '''
SELECT WS50M,WS50M_RANGE,WS10M,RHOA,WD50M,WS50M_MIN
FROM meteorology
WHERE timestamp BETWEEN '2020-12-01' AND '2021-01-01';
'''
sql2 = '''
SELECT MIDDAY_INSOL,AOD_55,ALLSKY_SFC_SW_DWN,ALLSKY_SFC_SW_DIRH,ALLSKY_SRF_ALB,CLOUD_AMT_DAY
FROM radiation 
WHERE timestamp BETWEEN '2020-12-01' AND '2021-01-01';
'''

df1 = query_sql(sql1)
df2 = query_sql(sql2)


if df1 is not None and df2 is not None:
    df = pd.concat([df1, df2], axis=1)

# Calculate weights of variables in period of interest
def calculate_weights(df, positive_index, negative_index, wd50m_deviation=False):
    if df is not None:
        for col in positive_index:
            df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())

        for col in negative_index:
            df[col] = (df[col].max() - df[col]) / (df[col].max() - df[col].min())

        if wd50m_deviation:
            df['WD50M'] = (df['WD50M'] - df['WD50M'].mean()) / df['WD50M'].std()
            df['WD50M'] = (df['WD50M'] - df['WD50M'].min()) / (df['WD50M'].max() - df['WD50M'].min())

        X = df[positive_index + negative_index].to_numpy()
        epsilon = 1e-6
        P = (X + epsilon) / (np.sum(X + epsilon, axis=0, keepdims=True))

        n = X.shape[0]
        k = 1 / np.log(n)
        E = -k * np.sum(P * np.log(P), axis=0)
        D = 1 - E
        W = D / np.sum(D)

        weight_df = pd.DataFrame({
            'Indicator': positive_index + negative_index,
            'Weight': W
        })

        return weight_df
    else:
        print("Data query failed. Please check the database connection or SQL query.")
        return None

# Weight of wind index variables
positive_index1 = ['WS50M', 'WS10M', 'RHOA','WS50M_MIN']
negative_index1 = ['WS50M_RANGE', 'WD50M']
weight_df1 = calculate_weights(df1, positive_index1, negative_index1, wd50m_deviation=True)

# Weight of solar index variables
positive_index2 = ['MIDDAY_INSOL', 'ALLSKY_SFC_SW_DWN','ALLSKY_SRF_ALB','ALLSKY_SFC_SW_DIRH']
negative_index2 = ['AOD_55', 'CLOUD_AMT_DAY']
weight_df2 = calculate_weights(df2, positive_index2, negative_index2)

# Display weights
if weight_df1 is not None:
    print("SQL1 Weight Results:")
    display(weight_df1)  

if weight_df2 is not None:
    print("SQL2 Weight Results:")
    display(weight_df2)  

# Filter the variables to only cover columns of our interest
weights_dict1 = dict(zip(weight_df1["Indicator"], weight_df1["Weight"]))
weights_dict2 = dict(zip(weight_df2["Indicator"], weight_df2["Weight"]))

valid_columns1 = [col for col in df1.columns if col in weights_dict1]
valid_columns2 = [col for col in df2.columns if col in weights_dict2]

filtered_data1 = df1[valid_columns1]
filtered_data2 = df2[valid_columns2]
filtered_weights1 = pd.Series(weights_dict1)[valid_columns1]
filtered_weights2 = pd.Series(weights_dict2)[valid_columns2]

# Commpute index and add into original data files
sql3 = '''
SELECT *
FROM meteorology 
WHERE timestamp BETWEEN '2020-12-01' AND '2021-01-01';
'''
sql4 = '''
SELECT *
FROM radiation 
WHERE timestamp BETWEEN '2020-12-01' AND '2021-01-01';
'''

df3 = query_sql(sql3)
df4 = query_sql(sql4)

df3["Wind_Score"] = filtered_data1.dot(filtered_weights1)
df4["Solar_Score"] = filtered_data2.dot(filtered_weights2)

# Export the results
df3.to_excel("C:/Users/26279/Desktop/Index_Wind_Resource.xlsx")
df4.to_excel("C:/Users/26279/Desktop/Index_Solar_Resource.xlsx")

    
    
    
    
    
    