import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Union, Tuple

import ee
from fastapi import APIRouter, Body, HTTPException

from shapely import wkt as shapely_wkt
from shapely.ops import transform as shp_transform
from shapely.geometry import mapping as shp_mapping
from pyproj import Transformer


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
    if col.size().getInfo() == 0:
        return None
    return ee.Image(col.first())

def _fmt_date(img: Optional[ee.Image]) -> Optional[str]:
    if img is None:
        return None
    return ee.Date(img.get("system:time_start")).format("YYYY-MM-dd").getInfo()

def s2_best_image(aoi: ee.Geometry, start: str, end: str, max_cloud: int = 1000) -> Optional[ee.Image]:
    col = (ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
           .filterBounds(aoi)
           .filterDate(start, end)
           .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", max_cloud))
           .sort("CLOUDY_PIXEL_PERCENTAGE")) 
    count = col.size().getInfo()
    print("scenes:", count)
    img = _pick_first(col)
    if img is None:
        return None
    scl = img.select("SCL")
    mask = (scl.neq(3).And(scl.neq(9)).And(scl.neq(10))) 
    return img.updateMask(mask)

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







# =========================
# FastAPI router
# =========================

router = APIRouter()

@router.post("/ee/physical/today")
def physical_today(body: Dict[str, Any] = Body(...)):
    
    ensure_ee()

    geom_geojson = body.get("geom_geojson")
    geom_wkt = body.get("geom_wkt")
    epsg = int(body.get("epsg", 4326))

    if geom_geojson:
        aoi = to_ee_geometry(geom_geojson, epsg=4326)
    elif geom_wkt:
        aoi = to_ee_geometry(geom_wkt, epsg=epsg)
    else:
        raise HTTPException(400, "Provide 'geom_geojson' (EPSG:4326) or 'geom_wkt' with 'epsg'.")

    s2_start, s2_end = daterange(days_back=12)

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
