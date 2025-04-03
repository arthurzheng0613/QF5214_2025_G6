import pandas as pd
from sqlalchemy import create_engine


user = 'g6_qf5214_3'
password = 'qf5214_3'
host = 'rm-zf8608n9yz8lywnj1go.mysql.kualalumpur.rds.aliyuncs.com'
port = 3306
db = 'nasa_power'

# Establish database connection
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4')

df = pd.read_sql("select * from meteorology limit 100", con = engine)
print(df)