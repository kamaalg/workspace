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
import time
import requests
from fastapi import HTTPException
from collections import deque
import statistics
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
from psycopg.rows import dict_row
import psycopg
import json
import os
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Optional, Tuple
import ee
from shapely import wkb
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping as shp_mapping, Point
from pyproj import Transformer
from shapely.ops import transform as shp_transform
T_BASE=18
AIRFLOW_DB_URL = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "postgresql+psycopg2://app:app@postgres:5432/airflow")#will write data here after fetching
DB_URL = os.getenv("AIRFLOW_CONN_DATA_POSTGRES")

DAYS_BACK = int(Variable.get("sentinel_days_back", default_var=30))
MAX_CLOUD_COVERAGE = int(Variable.get("sentinel_max_cloud", default_var=90))
BATCH_SIZE = int(Variable.get("sentinel_batch_size", default_var=50))
CLOUD_PROB_THRESHOLD = int(Variable.get("sentinel_cloud_prob", default_var=40))
PROCESSED_GEOMS = '/opt/airflow/dags/manual_logging/sentinel_processed_geoms.txt'

# with open('../secrets/ee-service-account.json', 'r') as file:
CREDENTIALS_DICT = {


}
        
                    
                            
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
        credentials_json_string = json.dumps(CREDENTIALS_DICT)
        credentials = ee.ServiceAccountCredentials(
            CREDENTIALS_DICT['client_email'],
            key_data=credentials_json_string
        )
        ee.Initialize(credentials=credentials)
        print(f"✓ Earth Engine initialized with hardcoded credentials")
        return
    except Exception as e:
        print(f"❌ Hardcoded auth failed: {e}")
        raise
    

def _geojson4326_to_ee(geojson: Dict[str, Any]) -> ee.Geometry:
    return ee.Geometry(geojson, proj=ee.Projection("EPSG:4326"), geodesic=False)
def _wkt_to_geojson4326(wkt_str: str, epsg: int) -> Dict[str, Any]:
    geom = shapely_wkt.loads(wkt_str)
    to4326 = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True).transform
    geom4326 = shp_transform(to4326, geom)
    return shp_mapping(geom4326)

def to_ee_geometry(
    geom_input,
    epsg: int = 4326
) -> ee.Geometry:
    print(type(geom_input))

    geom_input = str(wkb.loads(bytes.fromhex(geom_input)))
    print(geom_input)
    if isinstance(geom_input, dict):
        return _geojson4326_to_ee(geom_input)
    if isinstance(geom_input, str):
        gj = _wkt_to_geojson4326(geom_input, epsg=epsg)
        return _geojson4326_to_ee(gj)
    raise HTTPException(status_code=400, detail="Provide GeoJSON (dict) or WKT (str).")


def today_utc() -> datetime:
    return datetime.now(timezone.utc)
def daterange(days_back: int = 10) -> Tuple[str, str]:
    end = today_utc() + timedelta(days=1)      
    start = today_utc() - timedelta(days=days_back)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
def s2_best_image(
    aoi: ee.Geometry,
    start: str,
    end: str,
    max_cloud: int = 90,
    prob_thresh: int = 30
) -> Optional[ee.Image]:
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
            scale=10,
            maxPixels=1e9
        ).get('probability')
        return ee.Image(img).set('aoi_cloud_prob', mean_prob)

    ranked = (joined
              .map(add_aoi_cloudprob)
              .filter(ee.Filter.notNull(['aoi_cloud_prob']))
                .sort('system:time_start', False)); 


    def apply_mask(img):
        clouds = ee.Image(img.get('clouds')).select('probability')
        scl = img.select('SCL')
        cloud_ok = clouds.lt(prob_thresh)
        scl_ok = (scl.neq(3)  # exclude cloud shadows()
          .And(scl.neq(8))  # exclude medium-probability clouds
          .And(scl.neq(9))  # exclude high-probability clouds
          .And(scl.neq(10)) # exclude thin cirrus
          .And(scl.neq(11)) # exclude snow/ice
)



        return img.updateMask(cloud_ok.And(scl_ok))

    ranked_masked = ranked.map(apply_mask)
    print(type(ranked_masked))
    return ranked_masked


def ee_sample_and_get_samples(img, geom: ee.Geometry, scale: int = 10):
  
    

    BANDS = ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]

    proj = img.select('B2').projection().atScale(scale)
    # buffer_dist = scale / 2  
    # geom_buffered = geom.buffer(buffer_dist)
    print("Area (m²):", geom.area(10).getInfo())
    print("Area (km²):", geom.area(10).getInfo() / 1e6)

    print("Bounds:", geom.bounds().coordinates().getInfo())

    px = (ee.Image.pixelLonLat()
        .reproject(proj)
        .sample(region=geom, projection=proj, geometries=True))
    print("pixel_centers count:", px.size().getInfo())

    pixel_centers = (ee.Image.pixelLonLat()
                     .reproject(proj)
                     .sample(region=geom, projection=proj, geometries=True)
                     .distinct(['longitude','latitude']))
    print(pixel_centers.size().getInfo())

    try:
        sampled = (img.select(BANDS)
                     .sampleRegions(collection=pixel_centers,
                                    projection=proj,
                                    geometries=True))
        info = sampled.getInfo()
        print("Sampled size",sampled.size().getInfo()) 
    except Exception as e:
        print(f"  Error sampling regions server-side: {e}")
        return []

    out = []
    for f in info.get("features", []):
        geom_f = (f.get("geometry") or {})
        if geom_f.get("type") != "Point":
            continue
        lon, lat = geom_f.get("coordinates", [None, None])

        props = f.get("properties", {}) or {}
        scaled_props = {}
        for k, v in props.items():
            if v is None:
                scaled_props[k] = None
            elif k in BANDS:
                try:
                    scaled_props[k] = float(v) * 0.0001
                except Exception:
                    scaled_props[k] = None
            else:
               
                pass

        out.append({"lon": lon, "lat": lat, "properties": scaled_props})
    return out
def get_daily_hourly_temps(
    lat: float,
    lon: float,
    date: str,
    timezone: str = "UTC",
    max_retries: int = 3,
    timeout: int = 10,
    backoff_factor: float = 2.0,
) :

    base_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m",
            "precipitation",
            "rain",
            "snowfall",
            "relativehumidity_2m",
            "shortwave_radiation",
            "soil_moisture_0_1cm",
            "soil_moisture_1_3cm",
            "soil_temperature_0cm",
            "snow_depth",
        ]),
        "start_date": date,
        "end_date": date,
        "timezone": timezone,
    }

    last_error: Optional[Exception] = None

    for attempt in range(max_retries):
        try:
            resp = requests.get(base_url, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()

            times = data["hourly"]["time"]
            temps = data["hourly"]["temperature_2m"]
            precip = data["hourly"]["precipitation"]
            rain = data["hourly"]["rain"]
            snowfall = data["hourly"]["snowfall"]
            humidity = data["hourly"]["relativehumidity_2m"]
            shortwave = data["hourly"]["shortwave_radiation"]
            soil_moisture_0_1 = data["hourly"]["soil_moisture_0_1cm"]
            soil_temp_0 = data["hourly"]["soil_temperature_0cm"]
            snow_depth = data["hourly"]["snow_depth"]
            elevation = data.get("elevation")

            out= []
            for i, t in enumerate(times):
                out.append({
                    "time": t,
                    "temperature_c": temps[i],
                    "precipitation": precip[i],
                    "rainfall": rain[i],
                    "snowfall": snowfall[i],
                    "humidity": humidity[i],
                    "shortwave": shortwave[i],
                    "soil_moisture": soil_moisture_0_1[i],
                    "soil_temp_0": soil_temp_0[i],
                    "snow_depth": snow_depth[i],
                    "elevation": elevation,
                })
            return out

        except (requests.RequestException, KeyError, ValueError,Exception) as e:
            last_error = e
            if attempt == max_retries - 1:
                break
            sleep_time = backoff_factor ** attempt
            time.sleep(sleep_time)

    
    return []
def calculate_t_min_max(temp_json):
    temp_list = deque()
    for i in range(len(temp_json)):
        temp_list.append(temp_json[i]["temperature_c"])
    if len(temp_list) != 0:
        temp_min = min(temp_list)
        temp_max = max(temp_list)
        print(temp_min,temp_max)
        
        return temp_min,temp_max
    else:
        return None,None
def s1_sample_and_get_centers(img, geom: ee.Geometry, scale: int = 10):
    """
    Sample Sentinel-1 pixel centers (VV, VH) inside/near a geometry.

    Returns a list of:
      { "lon": float, "lat": float, "properties": {"VV": value, "VH": value} }
    """

    BANDS = ["VV", "VH"]

    # Use S1 VV band projection at desired scale
    proj = img.select('VV').projection().atScale(scale)

    # Buffer so we catch edge pixels whose centers are just outside the field
    # buffer_dist = scale / 2
    # geom_buffered = geom.buffer(buffer_dist)

    # Get pixel centers on the S1 grid
    pixel_centers = (ee.Image.pixelLonLat()
                     .reproject(proj)
                     .sample(region=geom,
                             projection=proj,
                             geometries=True)
                     .distinct(['longitude', 'latitude']))

    try:
        sampled = (img.select(BANDS)
                     .sampleRegions(collection=pixel_centers,
                                    projection=proj,
                                    geometries=True))
        info = sampled.getInfo()
    except Exception as e:
        print(f"  Error sampling S1 regions server-side: {e}")
        return []

    out = []
    for f in info.get("features", []):
        geom_f = (f.get("geometry") or {})
        if geom_f.get("type") != "Point":
            continue
        lon, lat = geom_f.get("coordinates", [None, None])

        props = f.get("properties", {}) or {}
        s1_props = {}
        for k, v in props.items():
            if k in BANDS:
                if v is None:
                    s1_props[k] = None
                else:
                    # Sentinel-1 GRD in EE is sigma0 in *linear* units.
                    # Keep as-is; you can convert to dB later if you want:
                    #   10 * math.log10(v)
                    try:
                        s1_props[k] = float(v)
                    except Exception:
                        s1_props[k] = None

        out.append({"lon": lon, "lat": lat, "properties": s1_props})

    return out


def record_centers(center_list, object_id, img_date):
    ndvi_window = deque(maxlen=3)
    bsi_window = deque(maxlen=3)

    engine = create_engine(AIRFLOW_DB_URL)

    with engine.connect() as conn:
        for center in center_list:
            print(center)

            lon = center["lon"]
            lat = center["lat"]
            properties = center.get("properties", {})

            # Skip if no band data
            if not properties:
                continue

            # First insert into sentinelraw
            conn.execute(
                """
                INSERT INTO sentinelraw
                  (object_id, b2, b3, b4, b5, b6, b8a, b8, b11, b12, lat, lon, img_date)
                VALUES
                  (%s,       %s, %s, %s, %s, %s, %s,  %s, %s,  %s,  %s,  %s,  %s)
                """,
                (
                    object_id,
                    properties.get("B2"),
                    properties.get("B3"),
                    properties.get("B4"),
                    properties.get("B5"),
                    properties.get("B6"),
                    properties.get("B8A"),
                    properties.get("B8"),
                    properties.get("B11"),
                    properties.get("B12"),
                    lat,
                    lon,
                    img_date,
                ),
            )

            B2 = properties.get("B2")
            B3 = properties.get("B3")
            B4 = properties.get("B4")
            B5 = properties.get("B5")
            B6 = properties.get("B6")
            B8 = properties.get("B8")
            B11 = properties.get("B11")
            B12 = properties.get("B12")

   

            ndvi = (B8 - B4) / (B8 + B4) if (B8 + B4) != 0 else None
            ndwi = (B3 - B8) / (B8 + B3) if (B8 + B3) != 0 else None
            gndvi = (B8 - B3) / (B8 + B3) if (B8 + B3) != 0 else None
            mtci = (B6 - B5) / (B5 - B4) if (B5 - B4) != 0 else None
            re_r = B5 / B4 if B4 != 0 else None
            grvi = (B3 - B4) / (B3 + B4) if (B3 + B4) != 0 else None
            ndmi = (B8 - B11) / (B8 + B11) if (B8 + B11) != 0 else None
            ndii = (B8 - B12) / (B8 + B12) if (B8 + B12) != 0 else None
            lswi = (B8 - B11) / (B8 + B11) if (B8 + B11) != 0 else None
            bsi = ((B11 + B4) - (B8 + B2)) / ((B11 + B4) + (B8 + B2)) if ((B11 + B4) + (B8 + B2)) != 0 else None
            ndbi = (B11 - B8) / (B11 + B8) if (B11 + B8) != 0 else None
            ndti = (B11 - B12) / (B11 + B12) if (B11 + B12) != 0 else None
            swir_b11 = B11
            swir_b12 = B12
            nir = B8
            red = B4
            blue = B2

            den = (nir + 6 * red - 7.5 * blue + 1)
            evi = 2.5 * (nir - red) / den if den != 0 else None

            temp_json = get_daily_hourly_temps(lat=lat, lon=lon, date=img_date)

            if ndvi is not None:
                ndvi_window.append(ndvi)
            if bsi is not None:
                bsi_window.append(bsi)

            ndvi_mean_3 = statistics.mean(ndvi_window) if ndvi_window else None
            ndvi_min_3 = min(ndvi_window) if ndvi_window else None
            ndvi_max_3 = max(ndvi_window) if ndvi_window else None

            bsi_mean_3 = statistics.mean(bsi_window) if bsi_window else None
            bsi_min_3 = min(bsi_window) if bsi_window else None
            bsi_max_3 = max(bsi_window) if bsi_window else None

            temp_min, temp_max = calculate_t_min_max(temp_json)
            temp_mean = (temp_max + temp_min) / 2 if (temp_max is not None and temp_min is not None) else None
            gdd_raw = (temp_max + temp_min) / 2 - T_BASE if (temp_max is not None and temp_min is not None) else None
            gdd = max(0, gdd_raw) if gdd_raw is not None else None

            conn.execute(
                """
                INSERT INTO sentinelcenter
                  (ndvi, ndwi, evi,
                   gndvi, mtci, re_r, grvi,
                   ndmi, ndii, lswi,
                   bsi, ndbi, ndti,
                   swir_b11, swir_b12,
                   object_id, lon, lat, img_date,
                   meteo_data,
                   ndvi_mean, bsi_mean,
                   ndvi_min, ndvi_max,
                   bsi_min, bsi_max,
                   gdd,
                   meteo_temp_min, meteo_temp_max, meteo_temp_mean)
                VALUES
                  (%s, %s, %s,
                   %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s,
                   %s, %s,
                   %s, %s, %s, 
                   %s,
                   %s, %s,
                   %s, %s,
                   %s, %s,
                   %s,
                   %s, %s, %s)
                """,
                (
                    ndvi, ndwi, evi,
                    gndvi, mtci, re_r, grvi,
                    ndmi, ndii, lswi,
                    bsi, ndbi, ndti,
                    swir_b11, swir_b12,
                    object_id, lon, lat, img_date,
                    json.dumps(temp_json),
                    ndvi_mean_3, bsi_mean_3,
                    ndvi_min_3, ndvi_max_3,
                    bsi_min_3, bsi_max_3,
                    gdd,
                    temp_min, temp_max, temp_mean,
                ),
            )

def s1_images_in_range(aoi: ee.Geometry, start: str, end: str) -> ee.ImageCollection:
    """
    Return Sentinel-1 GRD images over AOI between start and end (YYYY-MM-DD).
    Filters to land mode (IW) and VV+VH dual-pol.
    """
    col = (ee.ImageCollection("COPERNICUS/S1_GRD")
           .filterBounds(aoi)
           .filterDate(start, end)
           .filter(ee.Filter.eq('instrumentMode', 'IW'))
           .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
           .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH'))
           .select(['VV', 'VH']))
    return col
def list_s1_images_with_dates(aoi: ee.Geometry, start: str, end: str):
    col = s1_images_in_range(aoi, start, end)
    n = col.size().getInfo()
    imgs = col.toList(n)

    out = []
    for i in range(n):
        img = ee.Image(imgs.get(i))
        date = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd').getInfo()
        out.append({
            "image": img,      # ee.Image object
            "date": date
        })
    return out   
def iterate_centers(centers,date,row):
    dsn = (AIRFLOW_DB_URL
       .replace("postgresql+psycopg2://", "postgresql://")
       .replace("postgresql+psycopg://", "postgresql://"))

    with psycopg.connect(dsn,row_factory=dict_row, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5) as conn:
        with conn.cursor() as cur:
            for center in centers:
                cur.execute(
                            "SELECT 1 FROM sentinel1results WHERE object_id = %s AND img_date = %s AND lon = %s AND lat = %s",
                            (row["object_id"], date,center['lon'],center['lat']) 
                    )
                if cur.fetchone():
                    print("[INFO] Sentinel 1 data already exists in db.")
                else:
                    print(center)
                    properties = center['properties']
                    ratio = properties['VV']/properties['VH']
                    time = datetime.now()
                    cur.execute("""
                                INSERT INTO  sentinel1results (object_id,ad, rayon, vv_mean, vh_mean,vv_vh_ratio,img_date,inserted_at,lon,lat)
                                VALUES (%s, %s,%s, %s, %s, %s,%s,%s,%s,%s);
                            """, (row["object_id"],row["ad"],row["rayon"],properties['VV'],properties['VH'],ratio,date,time,center['lon'],center['lat']))
                        
                print(date, len(centers))
                print(centers[0]["properties"])
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
        print("HERE is the db url",DB_URL)
       
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
        return new_parcels


    @task
    def process_sentinel_data(parcels: List[Dict[str, Any]]) -> Dict[str, Any]:
        ensure_ee()
        actually_processed = 0
        for i,row in enumerate(parcels):
            geom = to_ee_geometry(row["geom"], 3857)
            s2_start, s2_end = daterange(days_back=720)
            s2_imgs = s2_best_image(geom, s2_start,s2_end,max_cloud=60)
            def add_date(img):
                        return img.set('date_ymd', ee.Date(img.get('system:time_start')).format('YYYY-MM-dd'))

            s2_by_day = s2_imgs.map(add_date).distinct('date_ymd')
            total    = int(s2_by_day.size().getInfo())
            _lst     = s2_by_day.toList(total)
            img_list = [ee.Image(_lst.get(i)) for i in range(total)]
            print(img_list)
            images = list_s1_images_with_dates(geom,  s2_start, s2_end)
            print(images)
            for item in images:
                img = item["image"]
                date = item["date"]
                centers = s1_sample_and_get_centers(img, geom, scale=10)
                iterate_centers(centers=centers,date=date,row=row)
                
            for s2_img in img_list:
                    date_str = ee.Date(s2_img.get('system:time_start')) \
                        .format('YYYY-MM-dd').getInfo()
                    print(date_str)
                    

                    centers = ee_sample_and_get_samples(s2_img,geom=geom)
                    actually_processed+=len(centers)
                    record_centers(centers,row["object_id"],date_str)

                    print(centers)


    
        stats = {
            "total_parcels": len(parcels),
            "found_images": len(img_list),
            "actually_processed":actually_processed
     
       
        }
        return stats
        
       

    parcels = fetch_parcels()
    results = process_sentinel_data(parcels)
    return results


dag_instance = sentinel_collection_pipeline()
