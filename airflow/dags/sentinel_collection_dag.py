"""
Sentinel-2 Data Collection DAG

This DAG focuses exclusively on collecting Sentinel-2 imagery data and computing
vegetation indices (NDVI, NDWI, EVI) for agricultural parcels.

Features:
- Configurable date range for historical data collection
- Batch processing with progress tracking
- Duplicate detection (skips already processed date/parcel combinations)
- Robust error handling with retries
- Detailed logging
"""

from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
import os
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Optional, Tuple
import ee
from shapely import wkb


DAYS_BACK = int(Variable.get("sentinel_days_back", default_var=30))
MAX_CLOUD_COVERAGE = int(Variable.get("sentinel_max_cloud", default_var=90))
BATCH_SIZE = int(Variable.get("sentinel_batch_size", default_var=50))
CLOUD_PROB_THRESHOLD = int(Variable.get("sentinel_cloud_prob", default_var=40))
PROCESSED_GEOMS = '/opt/airflow/dags/manual_logging/sentinel_processed_geoms.txt'


def ensure_ee() -> None:
    import json
    CREDENTIALS_JSON_PATH = "/opt/airflow/dags/ee-cred.json"
    if os.path.exists(CREDENTIALS_JSON_PATH):
        try:
            with open(CREDENTIALS_JSON_PATH, 'r') as f:
                key_data = json.load(f)
            service_account = key_data.get('client_email')
            credentials = ee.ServiceAccountCredentials(service_account, CREDENTIALS_JSON_PATH)
            ee.Initialize(credentials=credentials)
            print(f"Earth Engine initialized with service account: {service_account}")
            return
        except Exception as e:
            print(f"Service account auth failed: {e}")
            raise
    try:
        ee.Initialize()
        print("Earth Engine initialized with default credentials")
        return
    except Exception as e:
        print(f"Earth Engine initialization failed: {e}")
        raise


def to_ee_geometry(geom_wkt: str, epsg: int = 3857) -> ee.Geometry:
    from shapely import wkt as shapely_wkt
    from shapely.ops import transform as shp_transform
    from shapely.geometry import mapping as shp_mapping
    from pyproj import Transformer
    geom_input = str(wkb.loads(bytes.fromhex(geom_wkt)))
    
    geom = shapely_wkt.loads(geom_input)
    to4326 = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True).transform
    geom4326 = shp_transform(to4326, geom)
    geojson = shp_mapping(geom4326)
    return ee.Geometry(geojson, proj=ee.Projection("EPSG:4326"), geodesic=False)


def s2_best_images(
    aoi: ee.Geometry,
    start: str,
    end: str,
    max_cloud: int = 90,
    prob_thresh: int = 40
) -> ee.ImageCollection:
    sr = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
          .filterBounds(aoi)
          .filterDate(start, end)
          .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud)))
    cp = (ee.ImageCollection("COPERNICUS/S2_CLOUD_PROBABILITY")
          .filterBounds(aoi)
          .filterDate(start, end))
    join = ee.Join.saveFirst('clouds')
    cond = ee.Filter.equals(leftField='system:index', rightField='system:index')
    joined = ee.ImageCollection(join.apply(sr, cp, cond))

    def add_aoi_cloudprob(img):
        prob = ee.Image(img.get('clouds')).select('probability')
        mean_prob = prob.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=aoi,
            scale=20,
            maxPixels=1e9
        ).get('probability')
        return ee.Image(img).set('aoi_cloud_prob', mean_prob)

    ranked = (joined
              .map(add_aoi_cloudprob)
              .filter(ee.Filter.notNull(['aoi_cloud_prob']))
              .sort('system:time_start', False))

    def apply_mask(img):
        clouds = ee.Image(img.get('clouds')).select('probability')
        scl = img.select('SCL')
        cloud_ok = clouds.lt(prob_thresh)
        scl_ok = (scl.neq(3)
                  .And(scl.neq(8))
                  .And(scl.neq(9))
                  .And(scl.neq(10))
                  .And(scl.neq(11)))
        veg_only = scl.eq(4)
        return img.updateMask(cloud_ok.And(scl_ok).And(veg_only))

    return ranked.map(apply_mask)


def compute_indices(img: ee.Image) -> ee.Image:
    scaled = img.select(["B2", "B3", "B4", "B8"]).multiply(0.0001).rename(["blue", "green", "red", "nir"])
    ndvi = scaled.normalizedDifference(["nir", "red"]).rename("NDVI")
    ndwi = scaled.normalizedDifference(["green", "nir"]).rename("NDWI")
    evi = ee.Image().expression(
        "2.5 * (N - R) / (N + 6 * R - 7.5 * B + 1)",
        {
            "N": scaled.select("nir"),
            "R": scaled.select("red"),
            "B": scaled.select("blue"),
        },
    ).rename("EVI")
    return img.addBands([ndvi, ndwi, evi])


def s1_images(aoi: ee.Geometry, start: str, end: str) -> ee.ImageCollection:
    return (ee.ImageCollection("COPERNICUS/S1_GRD")
            .filterBounds(aoi)
            .filterDate(start, end)
            .filter(ee.Filter.eq('instrumentMode', 'IW'))
    )


def get_reduce_s1_stats(img: ee.Image, geom: ee.Geometry, deadline: int = 10, tries: int = 6, tile_scales: tuple = (2, 4, 8)) -> Optional[Dict[str, Any]]:
    import time, random
    last_err = None
    base_sleep = 0.5
    jitter = 0.2
    for attempt in range(tries):
        tile_scale = tile_scales[min(attempt, len(tile_scales) - 1)]
        expr = img.select(['VV', 'VH']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=10,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=tile_scale
        )
        date_expr = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')
        out_expr = ee.Dictionary(expr).combine({'date': date_expr})
        try:
            try:
                ee.data.setDeadline(deadline)
            except Exception:
                pass
            vals = out_expr.getInfo()
            vv = vals.get('VV')
            vh = vals.get('VH')
            ratio = None
            try:
                if vv is not None and vh is not None and vh != 0:
                    ratio = float(vv) / float(vh)
            except Exception:
                ratio = None
            vals['vv_mean'] = vv
            vals['vh_mean'] = vh
            vals['vv_vh_ratio'] = ratio
            return vals
        except Exception as e:
            msg = str(e)
            if 'Invalid geometry' in msg or 'Feature has no geometry' in msg:
                raise
            last_err = e
            sleep = base_sleep * (2 ** attempt) * (1.0 + jitter * random.random())
            time.sleep(min(10.0, sleep))
    raise last_err


def get_reduce_stats_with_timeout(
    img_with_indices: ee.Image,
    geom: ee.Geometry,
    deadline: int = 10,
    tries: int = 6,
    base_sleep: float = 0.5,
    jitter: float = 0.2,
    tile_scales: tuple = (2, 4, 8)
) -> Optional[Dict[str, Any]]:
    import time
    import random
    last_err = None
    for attempt in range(tries):
        tile_scale = tile_scales[min(attempt, len(tile_scales) - 1)]
        expr = img_with_indices.select(['NDVI', 'NDWI', 'EVI']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=10,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=tile_scale
        )
        date_expr = ee.Date(img_with_indices.get("system:time_start")).format("YYYY-MM-dd")
        out_expr = ee.Dictionary(expr).combine({"date": date_expr})
        try:
            try:
                ee.data.setDeadline(deadline)
            except Exception:
                pass
            return out_expr.getInfo()
        except Exception as e:
            msg = str(e)
            if 'Invalid geometry' in msg or 'Feature has no geometry' in msg:
                raise
            last_err = e
            sleep = base_sleep * (2 ** attempt) * (1.0 + jitter * random.random())
            time.sleep(min(10.0, sleep))
    raise last_err


@dag(
    dag_id="sentinel_results_collection",
    start_date=datetime(2025, 10, 1),
    schedule= timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "data-engineering",
    },
    tags=["sentinel", "earth-engine", "vegetation-indices"],
    description="Collect Sentinel-2 imagery and compute vegetation indices for all parcels",
)
def sentinel_collection_pipeline():

    @task
    def fetch_parcels() -> List[Dict[str, Any]]:
        DB_URL = os.getenv("AIRFLOW_CONN_DATA_POSTGRES", "postgresql+psycopg2://app:app@postgres:5432/app")
       
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            #change to "OBJECT_ID"
            result = conn.execute(text("""
                SELECT object_id, ad, rayon, geom
                FROM parcels
                WHERE parcels.geom IS NOT NULL
            """))
            parcels = [dict(row._mapping) for row in result]
        if os.path.exists(PROCESSED_GEOMS):
            with open(PROCESSED_GEOMS, "r") as f:
                processed_ids = {int(line.strip()) for line in f if line.strip().isdigit()}
        else:
            processed_ids = set()
        new_parcels = [p for p in parcels if p["object_id"] not in processed_ids]
        return new_parcels[:BATCH_SIZE]


    @task
    def process_sentinel_data(parcels: List[Dict[str, Any]]) -> Dict[str, Any]:
        ensure_ee()
        DB_URL = os.getenv("AIRFLOW_CONN_DATA_POSTGRES", "postgresql+psycopg2://app:app@postgres:5432/app")

        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        stats = {
            "total_parcels": len(parcels),
            "processed": 0,
            "skipped_no_images": 0,
            "skipped_duplicates": 0,
            "new_records": 0,
            "errors": 0,
        }
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            for idx, parcel in enumerate(parcels, start=1):
                obj_id = parcel['object_id']
                try:
                    with open(PROCESSED_GEOMS, "a") as f:
                        f.write(f"{obj_id}\n")
                except Exception:
                    pass
                try:
                    geom = to_ee_geometry(parcel["geom"], epsg=3857)
                    s2_imgs = s2_best_images(
                        geom,
                        start_str,
                        end_str,
                        max_cloud=MAX_CLOUD_COVERAGE,
                        prob_thresh=CLOUD_PROB_THRESHOLD
                    )
                    # TODO: add meteo
                    # TODO
                    total_images = int(ee.Number(s2_imgs.size()).getInfo())
                    if total_images == 0:
                        stats["skipped_no_images"] += 1
                        continue
                    img_list_ee = s2_imgs.toList(total_images)
                    new_records_for_parcel = 0
                    duplicates_for_parcel = 0
                    for img_idx in range(total_images):
                        try:
                            img = ee.Image(img_list_ee.get(img_idx))
                            img_with_indices = compute_indices(img).clip(geom)
                            result = get_reduce_stats_with_timeout(
                                img_with_indices,
                                geom,
                                deadline=10,
                                tries=6,
                                tile_scales=(2, 4, 8)
                            )
                            check_result = conn.execute(
                                text("SELECT 1 FROM sentinelresults WHERE object_id = :object_id AND img_date = :img_date"),
                                {"object_id": parcel["object_id"], "img_date": result["date"]}
                            )
                            if check_result.fetchone():
                                duplicates_for_parcel += 1
                            else:
                                conn.execute(
                                    text("""
                                        INSERT INTO sentinelresults (ad, rayon, object_id, ndvi, ndwi, evi, img_date)
                                        VALUES (:ad, :rayon, :object_id, :ndvi, :ndwi, :evi, :img_date)
                                    """),
                                    {
                                        "ad": parcel["ad"],
                                        "rayon": parcel["rayon"],
                                        "object_id": parcel["object_id"],
                                        "ndvi": result.get("NDVI"),
                                        "ndwi": result.get("NDWI"),
                                        "evi": result.get("EVI"),
                                        "img_date": result.get("date")
                                    }
                                )
                                new_records_for_parcel += 1
                                pixel_centers = ee.Image.pixelLonLat().sample(region=geom, scale=10, geometries=True).limit(int(Variable.get("sentinel_max_points", default_var=1000)))
                                sampled = img.select(["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]).sampleRegions(collection=pixel_centers, scale=10, geometries=True)
                                info = sampled.getInfo()
                                features = info.get("features", [])
                                for f in features:
                                    geom_f = f.get("geometry") or {}
                                    if geom_f.get("type") != "Point":
                                        continue
                                    lon, lat = geom_f.get("coordinates", [None, None])
                                    props = f.get("properties", {}) or {}
                                    def _safe(v):
                                        try:
                                            return None if v is None else float(v) * 0.0001
                                        except Exception:
                                            return None
                                    b2 = _safe(props.get("B2"))
                                    b3 = _safe(props.get("B3"))
                                    b4 = _safe(props.get("B4"))
                                    b8 = _safe(props.get("B8"))
                                    ndvi = None
                                    if b8 is not None and b4 is not None and (b8 + b4) != 0:
                                        ndvi = (b8 - b4) / (b8 + b4)
                                    ndwi = None
                                    if b3 is not None and b8 is not None and (b3 + b8) != 0:
                                        ndwi = (b3 - b8) / (b3 + b8)
                                    evi = None
                                    if b8 is not None and b4 is not None and b2 is not None and (b8 + 6.0 * b4 - 7.5 * b2 + 1.0) != 0:
                                        evi = 2.5 * (b8 - b4) / (b8 + 6.0 * b4 - 7.5 * b2 + 1.0)
                                    try:
                                        conn.execute(
                                            text("""
                                                INSERT INTO sentinel_pixels (object_id, lon, lat, b2, b3, b4, b5, b6, b7, b8, b8a, b11, b12, img_date)
                                                VALUES (:object_id, :lon, :lat, :b2, :b3, :b4, :b5, :b6, :b7, :b8, :b8a, :b11, :b12, :img_date)
                                            """),
                                            {
                                                "object_id": parcel["object_id"],
                                                "lon": lon,
                                                "lat": lat,
                                                "b2": _safe(props.get("B2")),
                                                "b3": _safe(props.get("B3")),
                                                "b4": _safe(props.get("B4")),
                                                "b5": _safe(props.get("B5")),
                                                "b6": _safe(props.get("B6")),
                                                "b7": _safe(props.get("B7")),
                                                "b8": _safe(props.get("B8")),
                                                "b8a": _safe(props.get("B8A")),
                                                "b11": _safe(props.get("B11")),
                                                "b12": _safe(props.get("B12")),
                                                "img_date": result.get("date")
                                            }
                                        )
                                        conn.execute(
                                            text("""
                                                INSERT INTO sentinelcenters (object_id, lon, lat, ndvi, ndwi, evi, img_date)
                                                VALUES (:object_id, :lon, :lat, :ndvi, :ndwi, :evi, :img_date)
                                            """),
                                            {
                                                "object_id": parcel["object_id"],
                                                "lon": lon,
                                                "lat": lat,
                                                "ndvi": ndvi,
                                                "ndwi": ndwi,
                                                "evi": evi,
                                                "img_date": result.get("date")
                                            }
                                        )
                                    except Exception as e:
                                        print(f"Error inserting center row: {e}")
                                # Sentinel-1 ingestion: fetch S1 images in same date window and insert VV/VH means
                                try:
                                    s1_col = s1_images(geom, start_str, end_str)
                                    s1_by_day = s1_col.map(lambda i: i.set('date_ymd', ee.Date(i.get('system:time_start')).format('YYYY-MM-dd'))).distinct('date_ymd')
                                    total_s1 = int(ee.Number(s1_by_day.size()).getInfo())
                                    if total_s1 > 0:
                                        s1_list = s1_by_day.toList(total_s1)
                                        for s1_idx in range(total_s1):
                                            try:
                                                s1_img = ee.Image(s1_list.get(s1_idx))
                                                s1_stats = get_reduce_s1_stats(s1_img, geom, deadline=10, tries=4, tile_scales=(1,2,4))
                                            except Exception as e:
                                                print(f"  Error obtaining S1 stats: {e}")
                                                continue
                                            if not s1_stats:
                                                continue
                                            img_date = s1_stats.get('date')
                                            vv = s1_stats.get('vv_mean')
                                            vh = s1_stats.get('vh_mean')
                                            ratio = s1_stats.get('vv_vh_ratio')
                                            try:
                                                check_s1 = conn.execute(text("SELECT 1 FROM sentinel1results WHERE object_id = :object_id AND img_date = :img_date"), {"object_id": parcel["object_id"], "img_date": img_date})
                                                if not check_s1.fetchone():
                                                    conn.execute(text("INSERT INTO sentinel1results (object_id, ad, rayon, vv_mean, vh_mean, vv_vh_ratio, img_date) VALUES (:object_id, :ad, :rayon, :vv, :vh, :ratio, :img_date)"), {"object_id": parcel["object_id"], "ad": parcel["ad"], "rayon": parcel["rayon"], "vv": vv, "vh": vh, "ratio": ratio, "img_date": img_date})
                                            except Exception as e:
                                                print(f"  Error inserting S1 row: {e}")
                                except Exception as e:
                                    print(f"  Error fetching S1 collection: {e}")
                        except Exception as img_error:
                            print(f"Error processing image {img_idx + 1}: {img_error}")
                            continue
                    stats["processed"] += 1
                    stats["new_records"] += new_records_for_parcel
                    stats["skipped_duplicates"] += duplicates_for_parcel
                except Exception as parcel_error:
                    print(f"Error processing parcel {parcel['object_id']}: {parcel_error}")
                    stats["errors"] += 1
                    continue
        print(f"\n{'='*60}")
        print("PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"Total parcels: {stats['total_parcels']}")
        print(f"Successfully processed: {stats['processed']}")
        print(f"New records inserted: {stats['new_records']}")
        print(f"Duplicates skipped: {stats['skipped_duplicates']}")
        print(f"No images found: {stats['skipped_no_images']}")
        print(f"Errors: {stats['errors']}")
        print(f"{'='*60}\n")
        return stats

    parcels = fetch_parcels()
    results = process_sentinel_data(parcels)


dag_instance = sentinel_collection_pipeline()
