import pandas as pd
from sqlalchemy import create_engine

# connect to your DB
# engine = create_engine("postgresql+psycopg2://app:app@postgres:5432/app")

# # load data into a DataFrame
# df = pd.read_sql("SELECT * FROM sentinelresults;", engine)

# # write to Parquet
# df.to_parquet("parcels.parquet", index=False)
print(pd.read_parquet("sentinelresults.parquet"))