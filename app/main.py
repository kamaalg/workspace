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

#Do batch processing, test api endpoint and should be done. Store the results in the seperate table.
#Stretch goal:batch processing 
#Short goal: for tomorrow understand everything and prepare maybe create a table to store all the results