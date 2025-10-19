import os,io
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Union, Tuple
import matplotlib.pyplot as plt
from fastapi.responses import StreamingResponse, JSONResponse
import matplotlib
matplotlib.use("Agg")
import ee
from fastapi import APIRouter, Body, HTTPException
import matplotlib.dates as mdates
from shapely import wkt as shapely_wkt
from shapely.ops import transform as shp_transform
from shapely.geometry import mapping as shp_mapping
from pyproj import Transformer
import pandas as pd
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
        veg_only = scl.eq(4)#new change

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
            try:
                ee.data.setDeadline(deadline)  # seconds
            except Exception:
                pass
            return out_expr.getInfo()
        except Exception as e:
            msg = str(e)
            if ('Invalid geometry' in msg or
                'Feature has no geometry' in msg):
                raise
            last_err = e
            sleep = base_sleep * (2 ** attempt) * (1.0 + jitter * random.random())
            time.sleep(min(10.0, sleep))  # cap sleep
    raise last_err


# =========================
# FastAPI router
# =========================
COTTON_PHASE_SPEC = [
    # Off-season
    (("01-01","04-14"), "Off-season",   {"NDVI": (0.05, 0.20), "EVI": (0.05, 0.15), "NDWI": (-0.20, 0.05)}),
    # Growing season windows (example for temperate N. hemisphere)
    (("04-15","05-15"), "Emergence",    {"NDVI": (0.15, 0.35), "EVI": (0.10, 0.25), "NDWI": (-0.05, 0.15)}),
    (("05-16","06-15"), "Vegetative",   {"NDVI": (0.35, 0.55), "EVI": (0.20, 0.40), "NDWI": (0.00,  0.20)}),
    (("06-16","07-10"), "Squaring",     {"NDVI": (0.50, 0.65), "EVI": (0.30, 0.45), "NDWI": (0.05,  0.25)}),
    (("07-11","08-10"), "Flowering",    {"NDVI": (0.60, 0.80), "EVI": (0.35, 0.55), "NDWI": (0.05,  0.25)}),
    (("08-11","09-15"), "Boll formation",{"NDVI": (0.55,0.75), "EVI": (0.35, 0.50), "NDWI": (0.00,  0.20)}),
    (("09-16","10-31"), "Boll opening / Harvest", {"NDVI": (0.30, 0.55), "EVI": (0.20, 0.40), "NDWI": (-0.05, 0.15)}),
    # Late off-season
    (("11-01","12-31"), "Post-harvest", {"NDVI": (0.05, 0.25), "EVI": (0.05, 0.15), "NDWI": (-0.20, 0.05)}),
]
def _md(d: datetime) -> str:
    return d.strftime("%m-%d")

def _in_range(md_str: str, start_md: str, end_md: str) -> bool:
    if start_md <= end_md:
        return start_md <= md_str <= end_md
    return md_str >= start_md or md_str <= end_md

def phase_for_date(d: datetime):
    md = _md(d)
    for (win, name, ranges) in COTTON_PHASE_SPEC:
        if _in_range(md, win[0], win[1]):
            return name, ranges
    return "Unknown", {"NDVI": (0.0, 1.0), "EVI": (0.0, 1.0), "NDWI": (-1.0, 1.0)}

def classify_value(val: float, low_high: tuple[float, float]):
    low, high = low_high
    if val < low - 0.10:   
        return "bad"
    if val < low:          
        return "warn"
    return "ok"
router = APIRouter()
@router.get("/ee/physical/get_graphs")
def get_graphs(rayon: str, ad: str, object_id: str):
    ensure_ee()
    with psycopg.connect(os.getenv("DB_URL").replace("postgresql+psycopg://", "postgresql://"),
                         row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ad, rayon, NDVI, NDWI, EVI, object_id, img_date
                FROM sentinelresults
                WHERE rayon=%s AND ad=%s AND object_id=%s
                  AND NDVI IS NOT NULL AND NDWI IS NOT NULL AND EVI IS NOT NULL
                ORDER BY img_date ASC
            """, (rayon, ad, object_id))
            rows = cur.fetchall()

    if not rows:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="No data found")

    ndvi, ndwi, evi, dates = [], [], [], []
    for r in rows:
        ndvi.append(float(r["ndvi"]))
        ndwi.append(float(r["ndwi"]))
        evi.append(float(r["evi"]))
        d = r["img_date"]
       
            
        dates.append(d)

    fig, (ax1, ax2) = plt.subplots(nrows=2, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

    ax1.plot(dates, ndvi, marker="o", label="NDVI")
    ax1.plot(dates, ndwi, marker="o", label="NDWI")
    ax1.plot(dates, evi,  marker="o", label="EVI")
    ax1.set_title(f"Indices for rayon={rayon}, ad={ad}, areaID={object_id} (n={len(rows)})")
    ax1.set_ylabel("Index value")
    ax1.grid(True, linestyle="--", alpha=0.3)
    ax1.legend(loc="best")

 
  

    phases = []
    ndvi_status, evi_status, ndwi_status = [], [], []
    for d, v_ndvi, v_evi, v_ndwi in zip(dates, ndvi, evi, ndwi):
        phase_name, ranges = phase_for_date(d)
        phases.append(phase_name)
        ndvi_status.append(classify_value(v_ndvi, ranges["NDVI"]))
        evi_status.append(classify_value(v_evi, ranges["EVI"]))
        ndwi_status.append(classify_value(v_ndwi, ranges["NDWI"]))


    seg_start = 0
    for i in range(1, len(dates) + 1):
        if i == len(dates) or phases[i] != phases[seg_start]:
            start_x = dates[seg_start]
            end_x = dates[i-1]
            ax2.axvspan(start_x, end_x, alpha=0.08)
            mid_x = start_x + (end_x - start_x) / 2
            ax2.text(mid_x, 2.6, phases[seg_start], ha="center", va="center", fontsize=8, rotation=0)
            seg_start = i

    color_map = {"ok": "tab:green", "warn": "orange", "bad": "red"}

    ax2.scatter(dates, [2]*len(dates), s=35, c=[color_map[s] for s in ndvi_status], label="NDVI status", marker="o", edgecolors="none")
    ax2.scatter(dates, [1]*len(dates), s=35, c=[color_map[s] for s in evi_status],  label="EVI status",  marker="o", edgecolors="none")
    ax2.scatter(dates, [0]*len(dates), s=35, c=[color_map[s] for s in ndwi_status], label="NDWI status", marker="o", edgecolors="none")

    ax2.set_yticks([0,1,2])
    ax2.set_yticklabels(["NDWI","EVI","NDVI"])
    ax2.set_ylim(-0.6, 2.8)
    ax2.grid(True, axis="x", linestyle="--", alpha=0.3)
    ax2.set_xlabel("Date")

    from matplotlib.lines import Line2D
    proxies = [
        Line2D([0], [0], marker='o', color='none', markerfacecolor='tab:green', markersize=8, label='OK'),
        Line2D([0], [0], marker='o', color='none', markerfacecolor='orange',    markersize=8, label='Slightly low'),
        Line2D([0], [0], marker='o', color='none', markerfacecolor='red',       markersize=8, label='Very low'),
    ]
    ax2.legend(handles=proxies, loc="upper left", ncols=3, frameon=False, fontsize=8)

    span_days = (max(dates) - min(dates)).days
    if span_days < 120:
        ax2.text(1.0, 1.02, f"Note: only {span_days} days of data (not full season).",
                 transform=ax2.transAxes, ha="right", va="bottom", fontsize=8, alpha=0.8)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return StreamingResponse(buf, media_type="image/png")      
         
                    

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
        with psycopg.connect(dsn,row_factory=dict_row, keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT  object_id,ad,rayon,geom FROM parcels WHERE geom IS NOT NULL")
                rows = cur.fetchall()
                for i,row in enumerate(rows):
                    geom = to_ee_geometry(row["geom"], 3857)

                    s2_start, s2_end = daterange(days_back=360)
                    s2_imgs = s2_best_image(geom, s2_start,s2_end,max_cloud=90)
                    total = int(ee.Number(s2_imgs.size()).getInfo())
                    if total == 0:
                        conn.commit()
                        continue
                    _lst = s2_imgs.toList(total)
                    img_list = [ee.Image(_lst.get(i)) for i in range(total)]
                    for s2_img in img_list:
                       
                        if s2_img:
                            s2_with_idx = s2_indices(s2_img).clip(geom)
                            try:
                                
                                s2_stats = get_reduce_stats_with_timeout(
                                    s2_with_idx, geom,
                                    deadline=10,    
                                    tries=6,        
                                    tile_scales=(2,4,8)
        )
                                print(s2_stats)
                                cur.execute(
                                    "SELECT 1 FROM sentinelresults WHERE object_id = %s AND img_date = %s",
                                    (row["object_id"], s2_stats["date"]) 
                                )
                                if cur.cur.fetchone():
                                    print("Already exists.")
                                    pass
                                else:
                                    cur.execute("""
                                        INSERT INTO  sentinelresults (ad, rayon, object_id,ndvi, ndwi, evi, img_date)
                                        VALUES (%s, %s,%s, %s, %s, %s,%s);
                                    """, (row["ad"],row["rayon"],row["object_id"],s2_stats.get("NDVI"), s2_stats.get("NDWI"), s2_stats.get("EVI"),s2_stats.get("date")))
                        

                                

                            except Exception as e:
                                print(f"  Error obtaining stats: {e}")
                    
                        else:
                            print("  No suitable Sentinel-2 image found.")
                    conn.commit()
                    if (i + 1) % 50 == 0:
                        print(f"[progress] committed parcels: {i+1}/{len(rows)}")

if __name__ == "__main__":
    batch_process()