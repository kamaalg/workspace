import psycopg
import os
from psycopg.rows import dict_row
import pandas as pd
from sklearn.metrics import roc_auc_score, f1_score, classification_report
import lightgbm as lgb

COTTON_PHASE_SPEC = [
    (("01-01","04-14"), "Off-season",   {"NDVI": (0.05, 0.20), "EVI": (0.05, 0.15), "NDWI": (-0.20, 0.05)}),
    (("04-15","05-15"), "Emergence",    {"NDVI": (0.15, 0.35), "EVI": (0.10, 0.25), "NDWI": (-0.05, 0.15)}),
    (("05-16","06-15"), "Vegetative",   {"NDVI": (0.35, 0.55), "EVI": (0.20, 0.40), "NDWI": (0.00,  0.20)}),
    (("06-16","07-10"), "Squaring",     {"NDVI": (0.50, 0.65), "EVI": (0.30, 0.45), "NDWI": (0.05,  0.25)}),
    (("07-11","08-10"), "Flowering",    {"NDVI": (0.60, 0.80), "EVI": (0.35, 0.55), "NDWI": (0.05,  0.25)}),
    (("08-11","09-15"), "Boll formation",{"NDVI": (0.55,0.75), "EVI": (0.35, 0.50), "NDWI": (0.00,  0.20)}),
    (("09-16","10-31"), "Boll opening / Harvest", {"NDVI": (0.30, 0.55), "EVI": (0.20, 0.40), "NDWI": (-0.05, 0.15)}),
    (("11-01","12-31"), "Post-harvest", {"NDVI": (0.05, 0.25), "EVI": (0.05, 0.15), "NDWI": (-0.20, 0.05)}),
]
DB_URL = os.getenv("DB_URL", "postgresql+psycopg://app:app@postgres:5432/app")
DSN = DB_URL.replace("postgresql+psycopg://", "postgresql://")
def get_phase_and_ranges(date):
    mm_dd = date.strftime("%m-%d")
    for (start, end), phase, ranges in COTTON_PHASE_SPEC:
        if start <= mm_dd <= end:
            return phase, ranges
    return None, {}
def health_from_indices(ndvi, evi, ndwi, ranges, margin=0.02):

    score = 0
    count = 0
    for name, val in [("NDVI", ndvi), ("EVI", evi), ("NDWI", ndwi)]:
        if val is None or name not in ranges:
            continue
        low, high = ranges[name]
        count += 1
        
        if low - margin <= val <= high + margin:
            score += 1  # good for this index
    
    if count == 0:
        return None  # cannot decide
    
    frac_good = score / count
    
    # you can tune these thresholds
    if frac_good >= 0.67:
        return 1   # healthy
    elif frac_good <= 0.33:
        return 0   # stressed
    else:
        return None  # ambiguous, drop from training
def label_row(row):
    phase, ranges = get_phase_and_ranges(row["img_date"])
    # adjust these keys if you have EVI/NDWI later
    ndvi = row["ndvi_target"]
    evi = row.get("EVI", None)
    ndwi = row.get("NDWI", None)

    label = health_from_indices(ndvi, evi, ndwi, ranges)
    return pd.Series({"health_label": label, "phase": phase})



def fetch_training_dataframe():
    sql = """
        SELECT
    c.object_id,
    c.img_date            AS img_date,
    s.img_date            AS s1_date,
    c.lat,
    c.lon,
    c.ndvi as ndvi_target,
    c.ndwi AS NDWI,
    c.evi AS EVI,

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
    if df.empty:
        print("No data returned from DB, aborting.")
        return

    df["img_date"] = pd.to_datetime(df["img_date"])
    df["s1_date"] = pd.to_datetime(df["s1_date"])


    numeric_cols = [
        "vv_mean",
        "vh_mean",
        "vv_vh_ratio",
        "ndvi_target",
        "ndvi_mean",
        "bsi_mean",
        "ndvi_min",
        "ndvi_max",
        "bsi_min",
        "bsi_max",
        "gdd",
        "meteo_temp_mean",
        "meteo_temp_min",
        "meteo_temp_max",
        "meteo_rain_sum",
        "lat",
        "lon",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    labels_phases = df.apply(label_row, axis=1)
    df = pd.concat([df, labels_phases], axis=1)

    df = df[df["health_label"].notna()].copy()
    df["health_label"] = df["health_label"].astype(int)

    df = df.sort_values(["object_id", "img_date"])

    df["health_label_t_plus_7"] = (
        df.groupby("object_id")["health_label"].shift(-3)
    )

    train_df = df[df["health_label_t_plus_7"].notna()].copy()
    train_df["health_label_t_plus_7"] = train_df["health_label_t_plus_7"].astype(int)

    if train_df.empty:
        print("No rows with future labels; check shift window or data coverage.")
        return

    feature_cols = [
        "vv_mean", "vh_mean", "vv_vh_ratio",
        "ndvi_target",
        "ndvi_mean", "bsi_mean",
        "ndvi_min", "ndvi_max",
        "bsi_min", "bsi_max",
        "gdd",
        "meteo_temp_mean", "meteo_temp_min", "meteo_temp_max",
        "meteo_rain_sum",
        "lat", "lon",
    ]

    train_df = train_df.dropna(subset=feature_cols)

    train_df = train_df.sort_values("img_date")
    n = len(train_df)
    split_idx = int(n * 0.7)

    train_data = train_df.iloc[:split_idx]
    valid_data = train_df.iloc[split_idx:]

    X_train = train_data[feature_cols]
    y_train = train_data["health_label_t_plus_7"]
    X_valid = valid_data[feature_cols]
    y_valid = valid_data["health_label_t_plus_7"]

    print(f"Training samples: {len(X_train)}, validation samples: {len(X_valid)}")

    train_ds = lgb.Dataset(X_train, label=y_train)
    valid_ds = lgb.Dataset(X_valid, label=y_valid, reference=train_ds)

    params = {
        "objective": "binary",
        "metric": ["auc", "binary_logloss"],
        "learning_rate": 0.05,
        "num_leaves": 64,
        "min_data_in_leaf": 200,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "lambda_l2": 1.0,
        "verbose": -1,
    }

    model = lgb.train(
        params,
        train_ds,
        num_boost_round=2000,
        valid_sets=[train_ds, valid_ds],
        valid_names=["train", "valid"],
    )

    p_valid = model.predict(X_valid, num_iteration=model.best_iteration)
    y_pred = (p_valid >= 0.5).astype(int)

    auc = roc_auc_score(y_valid, p_valid)
    f1 = f1_score(y_valid, y_pred)

    print(f"Validation AUC (t+7): {auc:.4f}")
    print(f"Validation F1  (t+7): {f1:.4f}")
    print("Validation classification report (t+7):")
    print(classification_report(y_valid, y_pred, digits=3))

    model_path = "cotton_stress_tplus7_lgbm.txt"
    model.save_model(model_path)
    print(f"Model saved to {model_path}")
    sample_idx = valid_data.index[0]  
    sample_row = valid_data.loc[sample_idx]

    X_sample = sample_row[feature_cols].to_frame().T 
    X_sample = X_sample.apply(pd.to_numeric, errors="coerce")

    y_true = sample_row["health_label_t_plus_7"]

    p_sample = model.predict(X_sample, num_iteration=model.best_iteration)[0]
    y_pred = int(p_sample >= 0.5)

    print("=== Single-sample test ===")
    print("Object:", sample_row["object_id"])
    print("Date t:", sample_row["img_date"])
    print("True label (t+7):", y_true)
    print("Predicted prob(stress in 7d):", round(p_sample, 3))
    print("Predicted class (t+7):", y_pred)
main()