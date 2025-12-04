import os,io
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Union, Tuple
import matplotlib.pyplot as plt
from fastapi.responses import StreamingResponse, JSONResponse
import matplotlib
matplotlib.use("Agg")
import ee
import statistics
from collections import deque

from fastapi import APIRouter, Body, HTTPException
import matplotlib.dates as mdates
from shapely import wkt as shapely_wkt
from shapely import wkb
import json
from shapely.ops import transform as shp_transform
from shapely.geometry import mapping as shp_mapping, Point
from pyproj import Transformer
import pandas as pd
import psycopg
import requests

from psycopg.rows import dict_row
from dotenv import load_dotenv
load_dotenv()

T_BASE = 18
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





def _fmt_date(img: Optional[ee.Image]) -> Optional[str]:
    if img is None:
        return None
    return ee.Date(img.get('system:time_start')).format('YYYY-MM-dd')


def s2_best_image(
    aoi: ee.Geometry,
    start: str,
    end: str,
    max_cloud: int = 90,
    prob_thresh: int = 20
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
            scale=20,
            maxPixels=1e9
        ).get('probability')
        return ee.Image(img).set('aoi_cloud_prob', mean_prob)

    ranked = (joined
              .map(add_aoi_cloudprob)
              .filter(ee.Filter.notNull(['aoi_cloud_prob']))
                .sort('system:time_start', False)); 
            
    # try:
    #     print("scenes:", ranked.size().getInfo())
    #     print('SR count:', sr.size().getInfo())
    #     print('CP count:', cp.size().getInfo())
    #     print('JOINED (with clouds):', joined.size().getInfo())
    #     print('RANKED (has aoi_cloud_prob):', ranked.size().getInfo())

    #     dates = ee.List(ranked.aggregate_array('system:time_start')) \
    #             .map(lambda t: ee.Date(t).format('YYYY-MM-dd'))
    #     print('Dates:', dates.getInfo())

        
    # except Exception:
    #     pass



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

        # land_ok = scl.eq(4).Or(scl.eq(5))


        return img.updateMask(cloud_ok.And(scl_ok))

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


def s1_images(aoi: ee.Geometry, start: str, end: str) -> ee.ImageCollection:
    col = (ee.ImageCollection("COPERNICUS/S1_GRD")
           .filterBounds(aoi)
           .filterDate(start, end)
           .filter(ee.Filter.eq('instrumentMode', 'IW'))
    )
    return col


def get_reduce_s1_stats(img: ee.Image, geom: ee.Geometry,
                        deadline=100, tries=6, base_sleep=0.5, jitter=0.2,
                        tile_scales=(2, 4, 8)) -> Optional[Dict[str, Any]]:
    import time, random
    last_err = None
    for attempt in range(tries):
        tile_scale = tile_scales[min(attempt, len(tile_scales) - 1)]
        expr = img.select(['VV', 'VH']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=30,
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
            # compute ratio safely
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





import time, random
from ee.ee_exception import EEException
import ee

def get_reduce_stats_with_timeout(img_idx, geom,
                                  deadline=5,          
                                  tries=6,             
                                  base_sleep=0.5,      
                                  jitter=0.2,         
                                  tile_scales=(2, 4, 8)): 
    last_err = None
    for attempt in range(tries):
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
                ee.data.setDeadline(deadline)  
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


def insert_sentinelraw(cur, img: ee.Image, geom, ad: str, rayon: str, object_id: str,date: str):
   
    bands = ["B2","B3","B4","B5","B6","B8A","B8","B11","B12"]
    try:
        expr = img.select(bands).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=10,
            bestEffort=True,
            maxPixels=1e9
        )
        values = expr.getInfo() or {}
    except Exception as e:
        print(f"  Error reducing raw bands for sentinelraw: {e}")
        return

    scaled = {}
    for b in bands:
        raw = values.get(b)
        if raw is None:
            scaled[b] = None
        else:
            try:
                scaled[b] = float(raw) * 0.0001
            except Exception:
                scaled[b] = None

    try:
        cur.execute(
            """
            INSERT INTO sentinelraw (object_id, b2, b3, b4, b5, b6, b8a, b8, b11, b12,ad,rayon,date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (object_id) DO UPDATE SET
              b2 = EXCLUDED.b2,
              b3 = EXCLUDED.b3,
              b4 = EXCLUDED.b4,
              b5 = EXCLUDED.b5,
              b6 = EXCLUDED.b6,
              b8a = EXCLUDED.b8a,
              b8 = EXCLUDED.b8,
              b11 = EXCLUDED.b11,
              b12 = EXCLUDED.b12;
            """,
            (
                int(object_id),
                scaled.get("B2"), scaled.get("B3"), scaled.get("B4"), scaled.get("B5"),
                scaled.get("B6"), scaled.get("B8A"), scaled.get("B8"), scaled.get("B11"), scaled.get("B12")
            ),
        )
    except Exception as e:
        print(f"  Error inserting/updating sentinelraw row: {e}")







def ee_sample_and_get_samples(img, geom: ee.Geometry, scale: int = 10):
  
    

    BANDS = ["B2","B3","B4","B5","B6","B7","B8","B8A","B11","B12"]

    proj = img.select('B2').projection().atScale(scale)
    # buffer_dist = scale / 2  
    # geom_buffered = geom.buffer(buffer_dist)
  

    

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
    buffer_dist = scale / 2
    geom_buffered = geom.buffer(buffer_dist)

    # Get pixel centers on the S1 grid
    pixel_centers = (ee.Image.pixelLonLat()
                     .reproject(proj)
                     .sample(region=geom_buffered,
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


# =========================
# FastAPI router
# =========================
# have to create ml model that understands if cotton is growing healthy so no thesholds model needs to understand thart
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
            cur.execute("""
                SELECT lat, lon, NDVI, NDWI, EVI, object_id, img_date
                FROM sentinelcenter
                WHERE object_id=%s
                  AND NDVI IS NOT NULL AND NDWI IS NOT NULL AND EVI IS NOT NULL
                ORDER BY img_date ASC
            """, (int(object_id),))
            third_rows = cur.fetchall()

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

    fig, (ax1, ax2, ax3) = plt.subplots(
        nrows=3, figsize=(12, 11),
        sharex=False,
        gridspec_kw={"height_ratios": [3, 1.2, 2.0]}  # more space for table
    )


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

    ax3.axis('off')
    
    if third_rows:
        df = pd.DataFrame(third_rows).rename(columns={"img_date": "date"})
        df = df[["date", "lon", "lat", "ndvi", "evi", "ndwi"]].copy()

        # Format/display versions (don’t mutate source types needed elsewhere)
        df_disp = df.copy()
        # Dates → ISO (yyyy-mm-dd)
        try:
            df_disp["date"] = pd.to_datetime(df_disp["date"]).dt.date.astype(str)
        except Exception:
            df_disp["date"] = df_disp["date"].astype(str)

        # Numeric formatting
        df_disp["lon"]  = pd.to_numeric(df_disp["lon"], errors="coerce").round(6).map(lambda x: f"{x:.6f}" if pd.notna(x) else "")
        df_disp["lat"]  = pd.to_numeric(df_disp["lat"], errors="coerce").round(6).map(lambda x: f"{x:.6f}" if pd.notna(x) else "")
        for col in ["ndvi","evi","ndwi"]:
            df_disp[col] = pd.to_numeric(df_disp[col], errors="coerce").round(3).map(lambda x: f"{x:.3f}" if pd.notna(x) else "")

        # Pretty labels
        col_labels = ["Date", "Lon", "Lat", "NDVI", "EVI", "NDWI"]

        # Head / … / Tail to avoid a super tall table
        MAX_HEAD, MAX_TAIL = 12, 6
        if len(df_disp) > (MAX_HEAD + MAX_TAIL + 1):
            shown = pd.concat(
                [df_disp.head(MAX_HEAD),
                pd.DataFrame([["…"] * len(col_labels)], columns=df_disp.columns),
                df_disp.tail(MAX_TAIL)],
                ignore_index=True
            )
            subtitle = f"showing {MAX_HEAD}+{MAX_TAIL} of {len(df_disp)}"
        else:
            shown = df_disp
            subtitle = f"showing {len(df_disp)} of {len(df_disp)}"

        ax3.clear()
        ax3.axis("off")

        # Rounded white card behind the table
        from matplotlib.patches import FancyBboxPatch
        card = FancyBboxPatch(
            (0.02, 0.02), 0.96, 0.96,
            boxstyle="round,pad=0.012,rounding_size=8",
            linewidth=0.8, edgecolor="#e5e7eb", facecolor="white",
            transform=ax3.transAxes, zorder=-1
        )
        ax3.add_patch(card)

        # Build the table
        tbl = ax3.table(
            cellText=shown.values,
            colLabels=col_labels,
            bbox=[0.04, 0.06, 0.92, 0.88],  # [left, bottom, width, height] inside the card
            cellLoc="left",
            colLoc="left",
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(8)

        for c in range(len(col_labels)):
            hcell = tbl[(0, c)]
            hcell.set_text_props(weight="bold")
            hcell.set_facecolor("#f3f4f6")
            hcell.set_edgecolor("#e5e7eb")
            hcell.set_linewidth(0.8)
            hcell.PAD = 0.18

        n_rows = shown.shape[0]
        n_cols = shown.shape[1]
        for r in range(1, n_rows + 1):
            bg = "#ffffff" if (r % 2 == 1) else "#fafafa"
            for c in range(n_cols):
                cell = tbl[(r, c)]
                cell.set_facecolor(bg)
                cell.set_edgecolor("#eeeff1")
                cell.set_linewidth(0.6)
                cell.PAD = 0.14

        # Align numeric columns to the right
        numeric_cols = {1, 2, 3, 4, 5}  # Lon, Lat, NDVI, EVI, NDWI (0=Date)
        for r in range(1, n_rows + 1):
            for c in numeric_cols:
                tbl[(r, c)]._loc = "right"  # right-align text in numeric columns

        # Column width tweaks (Date slightly wider, others even)
        col_widths = [0.22, 0.15, 0.15, 0.16, 0.16, 0.16]
        for c, w in enumerate(col_widths):
            tbl.auto_set_column_width(col=list(range(n_cols)))
            tbl._cells[(0, c)].set_width(w)

        ax3.set_title(
            f"Sample points — {subtitle}",
            fontsize=10, pad=6, loc="left",
        )
    else:
        ax3.axis("off")
        ax3.text(0.5, 0.5, "No per-point samples found", ha="center", va="center", fontsize=10)

# Keep after all layout
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
    print("GEOM_WKT",geom_wkt)
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
    #TODO: test between sentinel 2A and 2B and sr harmonized
def record_centers(center_list,cur,object_id,img_date,conn):
    ndvi_window = deque(maxlen=3)
    bsi_window = deque(maxlen=3)

    for center in center_list:
        print(center)
        
        lon = center['lon']
        lat = center['lat']
        properties = center['properties']
        
        cur.execute(
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
        ndvi = (properties['B8']-properties['B4'])/(properties['B8']+properties['B4'])
        ndwi = (properties['B3']-properties['B8'])/(properties['B8']+properties['B3'])
        gndvi = (properties['B8']-properties['B3'])/(properties['B8']+properties['B3'])
        mtci = (properties['B6']-properties['B5'])/(properties['B5']-properties['B4'])
        re_r = properties['B5']/properties['B4']
        grvi = (properties['B3']-properties['B4'])/(properties['B3']+properties['B4'])
        ndmi = (properties['B8']-properties['B11'])/(properties['B8']+properties['B11'])
        ndii = (properties['B8']-properties['B12'])/(properties['B8']+properties['B12'])
        lswi = (properties['B8']-properties['B11'])/(properties['B8']+properties['B11'])
        bsi = ((properties['B11']+properties['B4'])-(properties['B8']+properties['B2']))/((properties['B11']+properties['B4'])+(properties['B8']+properties['B2']))
        ndbi = (properties['B11']-properties['B8'])/(properties['B11']+properties['B8'])
        ndti = (properties['B11']-properties['B12'])/(properties['B11']+properties['B12'])
        swir_b11 = properties['B11']
        swir_b12 = properties['B12']
        nir  = properties['B8']
        red  = properties['B4']
        blue = properties['B2']

        den = (nir + 6*red - 7.5*blue + 1)
        evi = 2.5 * (nir - red) / den if den != 0 else None
        temp_json = get_daily_hourly_temps(lat=lat,lon=lon,date=img_date)

        ndvi_window.append(ndvi)
        bsi_window.append(bsi)
        ndvi_mean_3 = statistics.mean(ndvi_window)
        ndvi_min_3 = min(ndvi_window)
        ndvi_max_3 = max(ndvi_window)

        bsi_mean_3 = statistics.mean(bsi_window)
        bsi_min_3 = min(bsi_window)
        bsi_max_3 = max(bsi_window)

        temp_min, temp_max = calculate_t_min_max(temp_json)
        if temp_min != None or temp_max != None:
            gdd_raw = (temp_max + temp_min) / 2 - T_BASE
            gdd = max(0, gdd_raw)
        else:
            gdd = 0
        #TODO:finish model for null values
        
        cur.execute("""
    INSERT INTO sentinelcenter
      (ndvi, ndwi, evi,
       gndvi, mtci, re_r, grvi,
       ndmi, ndii, lswi,
       bsi, ndbi, ndti,
       swir_b11, swir_b12,
       object_id, lon, lat, img_date,meteo_data,ndvi_mean,bsi_mean,ndvi_min,ndvi_max,bsi_min,bsi_max,gdd)  VALUES
(%s, %s, %s,
       %s, %s, %s, %s,
       %s, %s, %s,
       %s, %s, %s,
       %s, %s,
       %s, %s, %s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s)
""", (
    ndvi, ndwi, evi,
    gndvi, mtci, re_r, grvi,
    ndmi, ndii, lswi,
    bsi, ndbi, ndti,
    swir_b11, swir_b12,
    object_id, lon, lat, img_date,json.dumps(temp_json),ndvi_mean_3,bsi_mean_3,ndvi_min_3,ndvi_max_3,bsi_min_3,bsi_max_3,gdd
))
        

    conn.commit()
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

                    s2_start, s2_end = daterange(days_back=1000)
                    s2_imgs = s2_best_image(geom, s2_start,s2_end,max_cloud=80)
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
                    print(img_list,len(img_list))
                    for s2_img in img_list:
                        date_str = ee.Date(s2_img.get('system:time_start')) \
                            .format('YYYY-MM-dd').getInfo()
                        print(date_str)
                        

                        centers = ee_sample_and_get_samples(s2_img,geom=geom)
                        print(len(centers))
                        record_centers(centers,cur,row["object_id"],date_str,conn)
                        

                       
        #                 if s2_img:
        #                     s2_with_idx = s2_indices(s2_img).clip(geom)
        #                     try:
        #                         print(1)
        # #                         s2_stats = get_reduce_stats_with_timeout(
        # #                             s2_with_idx, geom,
        # #                             deadline=10,    
        # #                             tries=6,        
        # #                             tile_scales=(2,4,8)
        # # )
        # #                         print(s2_stats)
        # #                         cur.execute(
        # #                             "SELECT 1 FROM sentinelresults WHERE object_id = %s AND img_date = %s",
        # #                             (row["object_id"], s2_stats["date"]) 
        # #                         )
        #                         # if cur.fetchone():
        #                         #     print("Already exists.")
        #                         # else:
        #                         #     cur.execute("""
        #                         #         INSERT INTO  sentinelresults (ad, rayon, object_id,ndvi, ndwi, evi, img_date)
        #                         #         VALUES (%s, %s,%s, %s, %s, %s,%s);
        #                         #     """, (row["ad"],row["rayon"],row["object_id"],s2_stats.get("NDVI"), s2_stats.get("NDWI"), s2_stats.get("EVI"),s2_stats.get("date")))
                                   

        #                         # Sentinel-1 processing: fetch S1 images in same date window and store VV/VH means
                                
        #                     except Exception as e:
        #                         print("Something went wrong")
                        

                                

                            
                    
                    
                    print("Saving to db for parcel",row["object_id"])
                    conn.commit()
                    if (i + 1) % 50 == 0:
                        print(f"[progress] committed parcels: {i+1}/{len(rows)}")
            





if __name__ == "__main__":
    batch_process()