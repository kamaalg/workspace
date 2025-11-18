 # test_s1_ee.py
import os
import ee
from ee.ee_exception import EEException


# ---------- EE init ----------
def ensure_ee():
    project = os.getenv("EE_PROJECT")
    try:
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
        print(f"[OK] Earth Engine initialized (project={project})")
    except Exception as e:
        print("[FATAL] Could not initialize Earth Engine:", e)
        raise


# ---------- Simple AOI (change if you want) ----------
# A small box over central Azerbaijan
AOI = None

def test_s1_collection():
    AOI = ee.Geometry.Rectangle([47.2, 40.2, 47.4, 40.4])  # [minLon, minLat, maxLon, maxLat]

    """Check that COPERNICUS/S1_GRD returns images."""
    col = (ee.ImageCollection("COPERNICUS/S1_GRD")
           .filterBounds(AOI)
           .filterDate("2025-01-01", "2025-03-01")
           .filter(ee.Filter.eq("instrumentMode", "IW"))
           .filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING"))
           .filter(ee.Filter.eq("resolution_meters", 10))
           )

    count = col.size().getInfo()
    print(f"[INFO] S1 collection size in test AOI/date window: {count}")

    if count == 0:
        print("[WARN] No Sentinel-1 images found for this AOI/date. "
              "Try changing dates or AOI.")
        return None

    # Grab the first image
    img = col.first()
    info = img.getInfo()
    print("[OK] Got first S1 image:")
    print("     id:", info.get("id"))
    print("     properties:", {k: info["properties"][k]
                              for k in ("orbitProperties_pass",
                                        "relativeOrbitNumber_start",
                                        "instrumentMode",
                                        "resolution_meters")
                              if k in info["properties"]})
    return img


def test_s1_reduce_region(img):
    """Run a *lightweight* reduceRegion on VV/VH over a tiny buffer instead of full AOI."""
    print("[INFO] Testing *lightweight* reduceRegion on VV/VH...")
    AOI = ee.Geometry.Rectangle([47.2, 40.2, 47.4, 40.4])  # [minLon, minLat, maxLon, maxLat]

    # Use a very small area: centroid of AOI buffered by 500 m
    center = AOI.centroid()
    spot = center.buffer(500)  # 500 m radius → tiny polygon

    expr = img.select(["VV", "VH"]).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=spot,
        scale=30,         # coarser than native 10m → far fewer pixels
        bestEffort=True,
        maxPixels=1e7,    # not insanely huge
        tileScale=1,
    )

    # Set a modest deadline so we don't hang forever
    try:
        ee.data.setDeadline(100)
    except Exception:
        pass

    try:
        vals = expr.getInfo()
        print("[OK] reduceRegion result over tiny buffer:")
        print("    VV_mean:", vals.get("VV"))
        print("    VH_mean:", vals.get("VH"))
        return vals
    except EEException as e:
        print("[ERROR] EEException during reduceRegion:", e)
        return None
    except Exception as e:
        print("[ERROR] Unexpected error during reduceRegion:", e)
        return None


def main():
    ensure_ee()
    
    img = test_s1_collection()
    if img is None:
        print("[DONE] Collection test finished (no images).")
        return
    test_s1_reduce_region(img)


if __name__ == "__main__":
    main()
