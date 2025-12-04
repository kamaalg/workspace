import psycopg
import os
from psycopg.rows import dict_row
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, root_mean_squared_error, mean_absolute_error

from sklearn.ensemble import RandomForestRegressor
#ADD NDVI lag
DB_URL = os.getenv("DB_URL", "postgresql+psycopg://app:app@postgres:5432/app")
DSN = DB_URL.replace("postgresql+psycopg://", "postgresql://")

def fetch_training_dataframe():
    sql = """
        SELECT
    c.object_id,
    c.img_date            AS img_date,
    s.img_date            AS s1_date,
    c.lat,
    c.lon,
    c.ndvi AS ndvi_target,

    s.vv_mean,
    s.vh_mean,
    s.vv_vh_ratio,
    c.ndvi_mean,
    c.bsi_mean,
    c.ndvi_min,
    c.ndvi_max,
    c.bsi_min,
    c.bsi_max,
    c.gdd,

    c.meteo_temp_mean,
    c.meteo_temp_min,
    c.meteo_temp_max,
    c.meteo_rain_sum

FROM sentinelcenter c
CROSS JOIN LATERAL (
    SELECT *
    FROM sentinel1results s
    WHERE s.object_id = c.object_id
    ORDER BY ABS(s.img_date - c.img_date)
    LIMIT 1
) s;

        """


    with psycopg.connect(
        DSN,
        row_factory=dict_row,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    
    df = pd.DataFrame(rows)
    return df

def main():
    df = fetch_training_dataframe()
    
    feature_cols = [
        "vv_mean", "vh_mean", "vv_vh_ratio","gdd",
        "meteo_temp_mean",
        "meteo_rain_sum",'doy'
    ]
    df['img_date'] = pd.to_datetime(df['img_date'])

    df['doy'] = df['img_date'].dt.dayofyear
    
    


    df = df.sort_values("img_date")
    unique_timestamps = len(df['img_date'].unique())
    print(df['img_date'].unique())


    df = df.dropna(subset=feature_cols + ["ndvi_target"])

    cutoff = pd.Timestamp("2025-11-18 00:00:00")
    
    traindf = df[df["img_date"]<=cutoff]
    testdf = df[df["img_date"]>cutoff]
    unique_timestamps = len(testdf['img_date'].unique())
    print(unique_timestamps)
    print(len(testdf))
    X_train = traindf[feature_cols].values
    X_test = testdf[feature_cols].values

    y_train = traindf["ndvi_target"].values
    y_test = testdf["ndvi_target"].values



    



    rf = RandomForestRegressor(
        n_estimators=300,
        max_depth=None,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    )
    rf.fit(X_train, y_train)

    y_train_pred = rf.predict(X_train)
    y_test_pred = rf.predict(X_test)

    train_r2 = r2_score(y_train, y_train_pred)
    test_r2 = r2_score(y_test, y_test_pred)
    rmse = root_mean_squared_error(y_test, y_test_pred)
    mae = mean_absolute_error(y_test, y_test_pred)

    print("Train R²:", train_r2)
    print("Test  R²:", test_r2)
    print("Test RMSE:", rmse)
    print("Test MAE:", mae)
    print("OOB R²:", rf.oob_score)

    #TARGET:Test R² ≥ 0.4
# Test RMSE ≤ 0.06
# Test MAE ≤ 0.05
if __name__ == "__main__":
    main()
