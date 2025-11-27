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
from fastapi import HTTPException

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

AIRFLOW_DB_URL = os.getenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "postgresql+psycopg2://app:app@postgres:5432/airflow")#will write data here after fetching
DB_URL = os.getenv("AIRFLOW_CONN_DATA_POSTGRES")

DAYS_BACK = int(Variable.get("sentinel_days_back", default_var=30))
MAX_CLOUD_COVERAGE = int(Variable.get("sentinel_max_cloud", default_var=90))
BATCH_SIZE = int(Variable.get("sentinel_batch_size", default_var=50))
CLOUD_PROB_THRESHOLD = int(Variable.get("sentinel_cloud_prob", default_var=40))
PROCESSED_GEOMS = '/opt/airflow/dags/manual_logging/sentinel_processed_geoms.txt'

with open('./secrets/ee-service.account.json', 'r') as file:
        CREDENTIALS_DICT = json.load(file)
                    
                            
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
def record_centers(center_list,object_id,img_date):
    # ndvi_window = deque(maxlen=3)
    # bsi_window = deque(maxlen=3)
    engine = create_engine(AIRFLOW_DB_URL)
    with engine.connect() as conn:
        for center in center_list:
            print(center)
            
            lon = center['lon']
            lat = center['lat']
            properties = center['properties']
            
            conn.execute(
        """
        INSERT INTO sentinelraw
        (object_id, b2, b3, b4, b5, b6, b8a, b8, b11, b12, lat, lon, img_date)
        VALUES
        (%s,       %s, %s, %s, %s, %s, %s,  %s, %s,  %s,  %s,  %s,  %s)
        """,
        (object_id,
        properties.get("B2"), properties.get("B3"), properties.get("B4"),
        properties.get("B5"), properties.get("B6"), properties.get("B8A"),
        properties.get("B8"), properties.get("B11"), properties.get("B12"),
        lat, lon, img_date)
            )
        # ndvi = (properties['B8']-properties['B4'])/(properties['B8']+properties['B4'])
        # ndwi = (properties['B3']-properties['B8'])/(properties['B8']+properties['B3'])
        # gndvi = (properties['B8']-properties['B3'])/(properties['B8']+properties['B3'])
        # mtci = (properties['B6']-properties['B5'])/(properties['B5']-properties['B4'])
        # re_r = properties['B5']/properties['B4']
        # grvi = (properties['B3']-properties['B4'])/(properties['B3']+properties['B4'])
        # ndmi = (properties['B8']-properties['B11'])/(properties['B8']+properties['B11'])
        # ndii = (properties['B8']-properties['B12'])/(properties['B8']+properties['B12'])
        # lswi = (properties['B8']-properties['B11'])/(properties['B8']+properties['B11'])
        # bsi = ((properties['B11']+properties['B4'])-(properties['B8']+properties['B2']))/((properties['B11']+properties['B4'])+(properties['B8']+properties['B2']))
        # ndbi = (properties['B11']-properties['B8'])/(properties['B11']+properties['B8'])
        # ndti = (properties['B11']-properties['B12'])/(properties['B11']+properties['B12'])
        # swir_b11 = properties['B11']
        # swir_b12 = properties['B12']
        # nir  = properties['B8']
        # red  = properties['B4']
        # blue = properties['B2']

        # den = (nir + 6*red - 7.5*blue + 1)
        # evi = 2.5 * (nir - red) / den if den != 0 else None
        # temp_json = get_daily_hourly_temps(lat=lat,lon=lon,date=img_date)

        # ndvi_window.append(ndvi)
        # bsi_window.append(bsi)
        # ndvi_mean_3 = statistics.mean(ndvi_window)
        # ndvi_min_3 = min(ndvi_window)
        # ndvi_max_3 = max(ndvi_window)

        # bsi_mean_3 = statistics.mean(bsi_window)
        # bsi_min_3 = min(bsi_window)
        # bsi_max_3 = max(bsi_window)

        # temp_min, temp_max = calculate_t_min_max(temp_json)
        # gdd_raw = (temp_max + temp_min) / 2 - T_BASE
        # gdd = max(0, gdd_raw)
        #TODO: I need  to add gdd and cgdd
#         cur.execute("""
#     INSERT INTO sentinelcenter
#       (ndvi, ndwi, evi,
#        gndvi, mtci, re_r, grvi,
#        ndmi, ndii, lswi,
#        bsi, ndbi, ndti,
#        swir_b11, swir_b12,
#        object_id, lon, lat, img_date,meteo_data,ndvi_mean,bsi_mean,ndvi_min,ndvi_max,bsi_min,bsi_max,gdd)  VALUES
# (%s, %s, %s,
#        %s, %s, %s, %s,
#        %s, %s, %s,
#        %s, %s, %s,
#        %s, %s,
#        %s, %s, %s,
#         %s,%s,%s,%s,%s,%s,%s,%s,%s)
# """, (
#     ndvi, ndwi, evi,
#     gndvi, mtci, re_r, grvi,
#     ndmi, ndii, lswi,
#     bsi, ndbi, ndti,
#     swir_b11, swir_b12,
#     object_id, lon, lat, img_date,json.dumps(temp_json),ndvi_mean_3,bsi_mean_3,ndvi_min_3,ndvi_max_3,bsi_min_3,bsi_max_3,gdd
# ))
        

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
        for i,row in enumerate(parcels[-30:]):
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
