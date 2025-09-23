import os
from typing import Optional, Union, Dict, Any
from datetime import date, datetime
from fastapi import FastAPI
from pydantic import BaseModel,Field
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import json
import ee
from shapely import wkt
from shapely.ops import transform as shp_transform
from pyproj import Transformer
from .ee_physical import router as ee_phys_router

load_dotenv()

DB_URL = os.getenv("DB_URL", "postgresql+psycopg://app:app@postgres:5432/app")

app = FastAPI(title="FastAPI + Postgres Dev")
app.include_router(ee_phys_router)


class UserAddRequest(BaseModel):
    object_id: Optional[int]                 = Field(None, alias="OBJECTID")
    year: Optional[int]                      = Field(None, alias="Year")
    name: Optional[str]                      = Field(None, alias="Name")
    rayon: Optional[str]                     = Field(None, alias="rayon")
    sahe: Optional[float]                    = Field(None, alias="sahe")          # area
    kod: Optional[str]                       = Field(None, alias="kod")
    olcutarix: Optional[date]                = Field(None, alias="olcutarix")     # measurement date
    ad: Optional[str]                        = Field(None, alias="ad")
    ok: Optional[bool]                       = Field(None, alias="ok")
    fayl: Optional[str]                      = Field(None, alias="fayl")
    importad: Optional[str]                  = Field(None, alias="importad")
    importdt: Optional[datetime]             = Field(None, alias="importdt")
    ediad: Optional[str]                     = Field(None, alias="ediad")
    edidtt: Optional[datetime]               = Field(None, alias="edidtt")
    okad: Optional[str]                      = Field(None, alias="okad")
    okdt: Optional[datetime]                 = Field(None, alias="okdt")
    olcen: Optional[str]                     = Field(None, alias="olcen")
    qeyd: Optional[str]                      = Field(None, alias="qeyd")
    agronom_name: Optional[str]              = Field(None, alias="AgronomName")
    village_name: Optional[str]              = Field(None, alias="VillageName")
    village_elder_name: Optional[str]        = Field(None, alias="VillageElderName")
    district_name: Optional[str]             = Field(None, alias="DistrictName")
    fin_kod: Optional[str]                   = Field(None, alias="FinKod")
    village: Optional[str]                   = Field(None, alias="Village")
    district: Optional[str]                  = Field(None, alias="District")
    id_value: Optional[int]                  = Field(None, alias="ID")
    square: Optional[float]                  = Field(None, alias="Square")
    mulk: Optional[str]                      = Field(None, alias="mulk")
    teyinat: Optional[str]                   = Field(None, alias="teyinat")
    itf: Optional[str]                       = Field(None, alias="ITF")
    il: Optional[int]                        = Field(None, alias="Il")
    geom: Optional[Union[str, Dict[str, Any]]] = Field(None, alias="Geom")  
@app.on_event("startup")
def init_ee():
    ee.Initialize()
@app.get("/")
def root():
    return {"hello": "world"}

def wkt_to_ee(w: str, epsg: int) -> ee.Geometry:
    poly = wkt.loads(w)
    tr = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    poly4326 = shp_transform(lambda x, y, z=None: tr.transform(x, y), poly)
    outer = [[x, y] for x, y in poly4326.exterior.coords]
    holes = [[[x, y] for x, y in r.coords] for r in poly4326.interiors]
    return ee.Geometry.Polygon([outer] + holes, proj=ee.Projection("EPSG:4326"), geodesic=False)
@app.post("/addUser")
def health(item: UserAddRequest):
    dsn = DB_URL.replace("postgresql+psycopg://", "postgresql://")
    payload = item.model_dump() if hasattr(item, "model_dump") else item.dict()

    sql = """
    INSERT INTO parcels (
      object_id, year_val, name, rayon, sahe, kod, olcutarix, ad, ok, fayl,
      importad, importdt, ediad, edidtt, okad, okdt, olcen, qeyd,
      agronom_name, village_name, village_elder_name, district_name, fin_kod,
      village, district, id_value, square, mulk, teyinat, itf, il, geom_wkt
    ) VALUES (
      %(object_id)s, %(year)s, %(name)s, %(rayon)s, %(sahe)s, %(kod)s, %(olcutarix)s, %(ad)s, %(ok)s, %(fayl)s,
      %(importad)s, %(importdt)s, %(ediad)s, %(edidtt)s, %(okad)s, %(okdt)s, %(olcen)s, %(qeyd)s,
      %(agronom_name)s, %(village_name)s, %(village_elder_name)s, %(district_name)s, %(fin_kod)s,
      %(village)s, %(district)s, %(id_value)s, %(square)s, %(mulk)s, %(teyinat)s, %(itf)s, %(il)s, %(geom)s
    )
    RETURNING id;
    """
    geom_val = payload.get("geom")
    if isinstance(geom_val, dict):
        geom_val = json.dumps(geom_val)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {**payload, "geom": geom_val})
            new_id = cur.fetchone()[0]
        conn.commit()
    return {"id": new_id}


@app.get("/batch_process")
def db_check():
    dsn = DB_URL.replace("postgresql+psycopg://", "postgresql://")
    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute()
    except Exception as e:
        return {"error": str(e)}
@app.get("/test")
def test():
    geom = wkt_to_ee("POLYGON ((5243501.342700001 4865701.495800003, 5243505.541499998 4865695.113399997, 5243514.965500001 4865686.014899999, 5243529.6613 4865683.189199999, 5243536.8926 4865680.777500004, 5243569.176800001 4865672.604699999, 5243592.8114 4865676.563199997, 5243630.358199999 4865683.2623, 5243675.901299998 4865696.307099998, 5243702.055199999 4865711.9706000015, 5243730.3365 4865718.413900003, 5243746.8145 4865733.712099999, 5243756.443799999 4865736.781499997, 5243764.3189 4865738.316200003, 5243797.713399999 4865745.417199999, 5243834.653700002 4865757.8653, 5243838.7498 4865760.946800001, 5243841.4091 4865771.117200002, 5243830.669399999 4865808.291100003, 5243823.232900001 4865840.897500001, 5243821.618700001 4865852.468699999, 5243811.420200001 4865874.380999997, 5243801.473700002 4865874.6734, 5243764.0856 4865870.312799998, 5243721.481699999 4865862.870700002, 5243707.2524 4865860.337200001, 5243677.954 4865857.0363000035, 5243648.403700002 4865851.080200002, 5243612.480500001 4865845.4529, 5243600.583900001 4865848.936499998, 5243571.668099999 4865850.921899997, 5243564.9593 4865849.801299997, 5243555.8246 4865848.376199998, 5243537.508400001 4865844.880500004, 5243531.284899998 4865834.052199997, 5243522.374000002 4865801.007399999, 5243513.23 4865771.190300003, 5243506.204 4865732.822899997, 5243503.4421 4865705.8684, 5243503.134199999 4865702.896499999, 5243501.342700001 4865701.495800003))", 3857)
    print(geom)

#Do batch processing, test api endpoint and should be done. Store the results in the seperate table.
#Stretch goal:batch processing 
#Short goal: for tomorrow understand everything and prepare maybe create a table to store all the results