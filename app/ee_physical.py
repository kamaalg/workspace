import os,io
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Union, Tuple
import matplotlib.pyplot as plt
from fastapi.responses import StreamingResponse, JSONResponse
import matplotlib
matplotlib.use("Agg")
import ee
from fastapi import APIRouter, Body, HTTPException

from shapely import wkt as shapely_wkt
from shapely.ops import transform as shp_transform
from shapely.geometry import mapping as shp_mapping
from pyproj import Transformer

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
load_dotenv()
# =========================
# Initialization & time
# =========================
def ensure_ee() -> None:
    try:
        ee.Initialize(project=os.getenv("EE_PROJECT"))
    except Exception:
        ee.Initialize()

def today_utc() -> datetime:
    return datetime.now(timezone.utc)

def daterange(days_back: int = 10) -> Tuple[str, str]:
    end = today_utc() + timedelta(days=1)      
    start = today_utc() - timedelta(days=days_back)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


# =========================
# Geometry helpers
# =========================

def _geojson4326_to_ee(geojson: Dict[str, Any]) -> ee.Geometry:
    return ee.Geometry(geojson, proj=ee.Projection("EPSG:4326"), geodesic=False)

def _wkt_to_geojson4326(wkt_str: str, epsg: int) -> Dict[str, Any]:
    geom = shapely_wkt.loads(wkt_str)
    to4326 = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True).transform
    geom4326 = shp_transform(to4326, geom)
    return shp_mapping(geom4326)

def to_ee_geometry(
    geom_input: Union[Dict[str, Any], str],
    epsg: int = 4326
) -> ee.Geometry:
    # print(geom_input)
    if isinstance(geom_input, dict):
        return _geojson4326_to_ee(geom_input)
    if isinstance(geom_input, str):
        gj = _wkt_to_geojson4326(geom_input, epsg=epsg)
        return _geojson4326_to_ee(gj)
    raise HTTPException(status_code=400, detail="Provide GeoJSON (dict) or WKT (str).")


# =========================
# Sentinel-2 (indices)
# =========================

def _pick_first(col: ee.ImageCollection) -> Optional[ee.Image]:
    # if col.size().getInfo() == 0:
    #     return None
    # print(col.first().getInfo())
    return ee.Image(col.first())

def _fmt_date(img: Optional[ee.Image]) -> Optional[str]:
    if img is None:
        return None
    return ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')



def s2_best_image(
    aoi: ee.Geometry,
    start: str,
    end: str,
    max_cloud: int = 90,
    prob_thresh: int = 40
) -> Optional[ee.Image]:
    # S2 SR (A+B)
    sr = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
          .filterBounds(aoi)
          .filterDate(start, end)
          .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud)))

    # S2 cloud probability
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
                .sort('system:time_start"', False)); 
            
    # try:
    #     # print("scenes:", ranked.size().getInfo())
    #     # print('SR count:', sr.size().getInfo())
    #     # print('CP count:', cp.size().getInfo())
    #     # print('JOINED (with clouds):', joined.size().getInfo())
    #     # print('RANKED (has aoi_cloud_prob):', ranked.size().getInfo())

    #     # dates = ee.List(ranked.aggregate_array('system:time_start')) \
    #     #         .map(lambda t: ee.Date(t).format('YYYY-MM-dd'))
    #     # print('Dates:', dates.getInfo())

        
    # except Exception:
    #     pass



    def apply_mask(img):
        clouds = ee.Image(img.get('clouds')).select('probability')
        scl = img.select('SCL')
        cloud_ok = clouds.lt(prob_thresh)
        scl_ok = (scl.neq(3)  # cloud shadow
                .And(scl.neq(8))  # medium clouds
                .And(scl.neq(9))  # high clouds
                .And(scl.neq(10)) # thin cirrus
                .And(scl.neq(11)))# snow/ice
        veg_only = scl.eq(4);

        return img.updateMask(cloud_ok.And(scl_ok).And(veg_only))

    ranked_masked = ranked.map(apply_mask)
    print(type(ranked_masked))
    return ranked_masked


def s2_indices(img: ee.Image) -> ee.Image:
    scaled = img.select(["B2","B3","B4","B8"]).multiply(0.0001).rename(["blue","green","red","nir"])

    ndvi = scaled.normalizedDifference(["nir", "red"]).rename("NDVI")
    ndwi = scaled.normalizedDifference(["green", "nir"]).rename("NDWI")
    evi  = ee.Image().expression(
        "2.5 * (N - R) / (N + 6 * R - 7.5 * B + 1)",
        {
            "N": scaled.select("nir"),
            "R": scaled.select("red"),
            "B": scaled.select("blue"),
        },
    ).rename("EVI")

    return img.addBands([ndvi, ndwi, evi])




import time, random
from ee.ee_exception import EEException
import ee

def get_reduce_stats_with_timeout(img_idx, geom,
                                  deadline=5,          # fail fast in 5s
                                  tries=6,             # total attempts
                                  base_sleep=0.5,      # backoff base
                                  jitter=0.2,          # +/- 20% jitter
                                  tile_scales=(2, 4, 8)):  # ramp up tileScale
    last_err = None
    for attempt in range(tries):
        # ramp tileScale as attempts increase (helps with memory)
        tile_scale = tile_scales[min(attempt, len(tile_scales)-1)]

        expr = img_idx.select(['NDVI','NDWI','EVI']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=10,
            bestEffort=True,
            maxPixels=1e9,
            tileScale=tile_scale
        )
        date_expr = ee.Date(img_idx.get("system:time_start")).format("YYYY-MM-dd")

        out_expr = ee.Dictionary(expr).combine({"date": date_expr})
        try:
            # per-attempt deadline
            try:
                ee.data.setDeadline(deadline)  # seconds
            except Exception:
                pass
            return out_expr.getInfo()
        except Exception as e:
            msg = str(e)
            # Some errors aren't worth retrying
            if ('Invalid geometry' in msg or
                'Feature has no geometry' in msg):
                raise
            # If it's memory related, retries with larger tileScale may help
            last_err = e
            # Exponential backoff with jitter
            sleep = base_sleep * (2 ** attempt) * (1.0 + jitter * random.random())
            time.sleep(min(10.0, sleep))  # cap sleep
    # Exhausted retries
    raise last_err


# =========================
# FastAPI router
# =========================

router = APIRouter()
@router.get("/ee/physical/get_graphs")
def get_graphs(rayon:str,ad:str):
    ensure_ee()
    print(rayon,ad)

    with psycopg.connect(os.getenv("DB_URL").replace("postgresql+psycopg://", "postgresql://"),row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT  ad,rayon,NDVI,NDWI,EVI,object_id,img_date FROM sentinelresults WHERE rayon=%s AND ad=%s AND NDVI is NOT NULL AND NDWI IS NOT NULL AND EVI IS NOT NULL",(rayon,ad))
            rows = cur.fetchall()
            print(rows)
            ndvi = []
            ndwi = []
            evi = []
            date = []
            for row in rows:
                ndvi.append(row["ndvi"])
                ndwi.append(row["ndwi"])
                evi.append(row["evi"])
                date.append(row["img_date"])
            fig, ax = plt.subplots(figsize=(10, 5))
            x = range(len(date))
            ax.plot(x, ndvi, marker="o", label="NDVI")
            ax.plot(x, ndwi, marker="o", label="NDWI")
            ax.plot(x, evi,  marker="o", label="EVI")
            ax.set_title(f"Indices for rayon={rayon}, ad={ad} (n={len(rows)})")
            ax.set_xlabel("Date")
            ax.set_ylabel("Index value")
            ax.set_xticks(x)
            ax.set_xticklabels(date, rotation=45, ha="right")
            ax.grid(True, linestyle="--", alpha=0.3)
            ax.legend()
            fig.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format="png", dpi=150, bbox_inches="tight")
            plt.close(fig)
            buf.seek(0)
            return StreamingResponse(buf, media_type="image/png")         # print(f"  Used image from {s2_date} with stats: {s2_stats}")
         
                    
            return None

@router.post("/ee/physical/today")
def physical_today(body: Dict[str, Any] = Body(...)):
    
    ensure_ee()

    geom_geojson = body.get("geom_geojson")
    geom_wkt = body.get("geom_wkt")
    print("GGEOM_WKT",geom_wkt)
    epsg = int(body.get("epsg", 4326))

    if geom_geojson:
        aoi = to_ee_geometry(geom_geojson, epsg=4326)
    elif geom_wkt:
        aoi = to_ee_geometry(geom_wkt, epsg=epsg)
    else:
        raise HTTPException(400, "Provide 'geom_geojson' (EPSG:4326) or 'geom_wkt' with 'epsg'.")

    s2_start, s2_end = daterange(days_back=100)

    s2_img = s2_best_image(aoi, s2_start, s2_end, max_cloud=100)
    if s2_img:
        s2_with_idx = s2_indices(s2_img)
        s2_date = _fmt_date(s2_img)
        s2_stats = s2_with_idx.select(["NDVI", "NDWI", "EVI"]).reduceRegion(
            reducer=ee.Reducer.mean(), geometry=aoi, scale=10, bestEffort=True, maxPixels=1e9
        ).getInfo()
    else:
        s2_date, s2_stats = None, {"NDVI": None, "NDWI": None, "EVI": None}

   

    return {
        "region_units": "mean over AOI",
        "diagnostics": {
            "s2_window": [s2_start, s2_end],

           
        },
        "sentinel2": {
            "date_used": s2_date,
            "NDVI": s2_stats.get("NDVI"),
            "NDWI": s2_stats.get("NDWI"),
            "EVI":  s2_stats.get("EVI")
        },
      
    }

# =========================
# Batch process
# =========================
def batch_process():
        ensure_ee()
        DB_URL = os.getenv("DB_URL", "postgresql+psycopg://app:app@postgres:5432/app")
        dsn = DB_URL.replace("postgresql+psycopg://", "postgresql://")
        count = 0
        with psycopg.connect(dsn,row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT  object_id,ad,rayon,geom FROM parcels WHERE geom IS NOT NULL")
                rows = cur.fetchall()
                for row in rows[-5:]:
                    # print(row["object_id"],row["ad"])
                    geom = to_ee_geometry(row["geom"], 3857)

                    s2_start, s2_end = daterange(days_back=60)
                    s2_imgs = s2_best_image(geom, s2_start,s2_end,max_cloud=90)
                    total = int(ee.Number(s2_imgs.size()).getInfo())
                    if total == 0:
                        return []
                    _lst = s2_imgs.toList(total)
                    img_list = [ee.Image(_lst.get(i)) for i in range(total)]
                    for s2_img in img_list:
                       
                        # print(f"  Image date: {date}, cloud over AOI: {cloud}%")
                        if s2_img:
                            s2_with_idx = s2_indices(s2_img).clip(geom)
                            try:
                                
                                s2_stats = get_reduce_stats_with_timeout(
                                    s2_with_idx, geom,
                                    deadline=10,     # fail after 5s, then retry
                                    tries=6,        # total attempts
                                    tile_scales=(2,4,8)
        )
                                print(s2_stats)
                            # print(f"  Used image from {s2_date} with stats: {s2_stats}")

                                cur.execute("""
                                    INSERT INTO  sentinelresults (ad, rayon, object_id,ndvi, ndwi, evi, img_date)
                                    VALUES (%s, %s,%s, %s, %s, %s,%s);
                                """, (row["ad"],row["rayon"],row["object_id"],s2_stats.get("NDVI"), s2_stats.get("NDWI"), s2_stats.get("EVI"),s2_stats.get("date")))
                                # single row
                                

                            except Exception as e:
                                conn.commit()
                                print(f"  Error obtaining stats: {e}")
                        else:
                            print("  No suitable Sentinel-2 image found.")
                conn.commit()
if __name__ == "__main__":
    batch_process()