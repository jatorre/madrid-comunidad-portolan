#!/usr/bin/env python3
"""Build the static multi-format Iceberg+STAC catalog for the Comunidad de Madrid, native EPSG:25830.
Driven by manifests in /tmp/comu_conv/. Per dataset:
  - vector  -> v3 Iceberg (native geometry) + remote GeoParquet (the gpio file, read_parquet in place)
  - tabular -> tab Iceberg table (no geometry) + remote parquet
  - raster  -> COG asset only (no Iceberg)
Plus a catalog.datasets stac-geoparquet index and a static Iceberg-REST surface (v1/..., prefix 'sdi').
Stages under /tmp/comu_catalog ; then `gsutil -m rsync` to the bucket (separate step).
Run with the testbed venv:  iceberg-geo-testbed/.venv/bin/python comu_build.py
"""
from __future__ import annotations
import json, struct, subprocess, sys, shutil, os
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import geoarrow.pyarrow as ga
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, LongType, FloatType,
                             DoubleType, BooleanType, BinaryType, StructType, TimestamptzType)
TESTBED = Path("/Users/jatorre/workspace/iceberg-geo-testbed"); sys.path.insert(0, str(TESTBED))
from testbed._static_catalog import write_static_catalog  # noqa: E402

CRS = "EPSG:25830"
BUCKET_URL = "https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid"
STAGING = Path("/tmp/comu_catalog"); CONV = Path("/tmp/comu_conv/_norm")
VEC_SRC = Path("/tmp/comu_conv/vector"); TAB_SRC = Path("/tmp/comu_conv/tab"); COG_SRC = Path("/tmp/comu_conv/cog")
MAN_VEC = "/tmp/comu_conv/manifest_vector.json"; MAN_TAB = "/tmp/comu_conv/manifest_tab.json"
MAN_RAS = "/tmp/comu_conv/manifest_raster.json"
IRC_PREFIX = "sdi"; DUCKDB = "duckdb"
GEOM_EXT = ga.wkb().with_crs(CRS)
PROVIDER = "Comunidad de Madrid"; LICENSE = "CC-BY-4.0"
COLLECTION = "comunidad-madrid"

def dle(v): return struct.pack("<d", float(v))
def xy(x, y): return struct.pack("<dd", float(x), float(y))
def _fmeta(i): return {"PARQUET:field_id": str(i)}
def loadman(p):
    try: return json.load(open(p))
    except Exception: return {}

def semantics_for(info):
    t = info["title"]
    return {"label": t, "describes": t, "answers": t, "unit": ""}

def _ice_field(field, fid):
    t = field.type
    if pa.types.is_boolean(t):                                  it, js = BooleanType(), "boolean"
    elif pa.types.is_int64(t):                                  it, js = LongType(), "long"
    elif pa.types.is_integer(t):                                it, js = IntegerType(), "int"
    elif pa.types.is_float64(t):                                it, js = DoubleType(), "double"
    elif pa.types.is_float32(t):                                it, js = FloatType(), "float"
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t):  it, js = BinaryType(), "binary"
    else:                                                       it, js = StringType(), "string"
    return (NestedField(fid, field.name, it, required=False),
            {"id": fid, "name": field.name, "required": False, "type": js})

def _cast_temporal_to_str(t):
    for f in list(t.schema):
        if pa.types.is_temporal(f.type):
            t = t.set_column(t.schema.get_field_index(f.name), pa.field(f.name, pa.string()),
                             pc.cast(t[f.name], pa.string()))
    return t

def _normalize_vector(did, src):
    CONV.mkdir(parents=True, exist_ok=True)
    out = CONV / f"{did}.parquet"
    sel = ("SELECT * EXCLUDE(geom, bbox), ST_AsWKB(geom) AS geom_wkb, "
           "bbox.xmin AS fp_xmin, bbox.ymin AS fp_ymin, bbox.xmax AS fp_xmax, bbox.ymax AS fp_ymax")
    sql = (f"INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
           f"COPY ({sel} FROM read_parquet('{src}')) TO '{out}' (FORMAT parquet);")
    subprocess.run([DUCKDB, "-c", sql], check=True, capture_output=True)
    q = (f"INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
         f"SELECT min(ST_XMin(g)),min(ST_YMin(g)),max(ST_XMax(g)),max(ST_YMax(g)) FROM "
         f"(SELECT ST_Transform(geom,'EPSG:25830','EPSG:4326') g FROM read_parquet('{src}'));")
    r = subprocess.run([DUCKDB, "-csv", "-noheader", "-c", q], check=True, capture_output=True, text=True)
    wgs = [float(v) for v in r.stdout.strip().splitlines()[0].split(",")]
    t = pq.read_table(out)
    if "OGC_FID" in t.column_names: t = t.drop(["OGC_FID"])
    t = _cast_temporal_to_str(t)
    return t, wgs

def build_v3_vector(did, info):
    t, wgs = _normalize_vector(did, VEC_SRC / info["src"])
    attr = [c for c in t.column_names if c not in ("geom_wkb","fp_xmin","fp_ymin","fp_xmax","fp_ymax")]
    v3_cols = attr + ["geom"]
    fid3 = {n: i for i, n in enumerate(v3_cols, 1)}
    arrays = {c: t[c] for c in attr}
    arrays["geom"] = GEOM_EXT.wrap_array(t["geom_wkb"].combine_chunks())
    ice, fields, namemap = [], [], []
    for n in v3_cols:
        if n == "geom":
            ice.append(NestedField(fid3[n], "geom", BinaryType(), required=False))
            fields.append({"id": fid3[n], "name": "geom", "required": False, "type": f"geometry({CRS})"})
        else:
            nf, jf = _ice_field(t.schema.field(n), fid3[n]); ice.append(nf); fields.append(jf)
        namemap.append({"field-id": fid3[n], "names": [n]})
    v3schema = pa.schema([pa.field(n, (GEOM_EXT if n=="geom" else t.schema.field(n).type),
                                    metadata=_fmeta(fid3[n])) for n in v3_cols])
    v3t = pa.table({n: arrays[n] for n in v3_cols}, schema=v3schema)
    root3 = STAGING/"data"/"v3"/did; (root3/"data").mkdir(parents=True, exist_ok=True)
    pq3 = root3/"data"/f"{did}.parquet"
    pq.write_table(v3t, pq3, compression="zstd", store_schema=True, write_statistics=True)
    g = fid3["geom"]
    df3 = [{"path":f"data/{did}.parquet","size":pq3.stat().st_size,"rows":t.num_rows,
            "lower":{g: xy(pc.min(t["fp_xmin"]).as_py(), pc.min(t["fp_ymin"]).as_py())},
            "upper":{g: xy(pc.max(t["fp_xmax"]).as_py(), pc.max(t["fp_ymax"]).as_py())},
            "value_counts":{g: t.num_rows}, "null_value_counts":{g: 0}}]
    props = {"theme": info["theme"], "title": info["title"], "semantics": json.dumps(semantics_for(info)),
             "crs": CRS, "provider": PROVIDER, "license": LICENSE}
    mp = write_static_catalog(table_root=root3, iceberg_schema=Schema(*ice), schema_json_fields=fields,
        name_mapping=namemap, data_files=df3, format_version_in_metadata=3,
        location_uri=f"{BUCKET_URL}/data/v3/{did}", extra_properties=props)
    # publish the gpio file as the remote GeoParquet
    pdir = STAGING/"data"/"parquet"; pdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(VEC_SRC/info["src"], pdir/f"{did}.parquet")
    return json.loads(Path(mp).read_text()), wgs, t.num_rows

def build_tab(did, info):
    t = pq.read_table(TAB_SRC / info["src"])
    if "OGC_FID" in t.column_names: t = t.drop(["OGC_FID"])
    t = _cast_temporal_to_str(t)
    cols = t.column_names
    fid = {n:i for i,n in enumerate(cols,1)}
    ice, fields, namemap = [], [], []
    for n in cols:
        nf, jf = _ice_field(t.schema.field(n), fid[n]); ice.append(nf); fields.append(jf)
        namemap.append({"field-id": fid[n], "names":[n]})
    root = STAGING/"data"/"tab"/did; (root/"data").mkdir(parents=True, exist_ok=True)
    pqp = root/"data"/f"{did}.parquet"
    t = t.replace_schema_metadata(None).cast(pa.schema(
        [pa.field(n, t.schema.field(n).type, metadata=_fmeta(fid[n])) for n in cols]))
    pq.write_table(t, pqp, compression="zstd")
    df = [{"path":f"data/{did}.parquet","size":pqp.stat().st_size,"rows":t.num_rows,"lower":{},"upper":{}}]
    props = {"theme": info["theme"], "title": info["title"], "semantics": json.dumps(semantics_for(info)),
             "provider": PROVIDER, "license": LICENSE, "geospatial":"false"}
    mp = write_static_catalog(table_root=root, iceberg_schema=Schema(*ice), schema_json_fields=fields,
        name_mapping=namemap, data_files=df, format_version_in_metadata=2,
        location_uri=f"{BUCKET_URL}/data/tab/{did}", extra_properties=props)
    pdir = STAGING/"data"/"parquet_tab"; pdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(TAB_SRC/info["src"], pdir/f"{did}.parquet")
    return json.loads(Path(mp).read_text()), t.num_rows

# ---------- stac-geoparquet index ----------
_BBOX_T = pa.struct([pa.field("xmin",pa.float64(),metadata=_fmeta(10)), pa.field("ymin",pa.float64(),metadata=_fmeta(11)),
                     pa.field("xmax",pa.float64(),metadata=_fmeta(12)), pa.field("ymax",pa.float64(),metadata=_fmeta(13))])
_IDX_SCHEMA = pa.schema([pa.field("id",pa.string(),metadata=_fmeta(1)), pa.field("collection",pa.string(),metadata=_fmeta(2)),
    pa.field("geometry",pa.binary(),metadata=_fmeta(3)), pa.field("bbox",_BBOX_T,metadata=_fmeta(4)),
    pa.field("datetime",pa.timestamp("us",tz="UTC"),metadata=_fmeta(5)), pa.field("properties",pa.string(),metadata=_fmeta(6)),
    pa.field("assets",pa.string(),metadata=_fmeta(7)), pa.field("stac_version",pa.string(),metadata=_fmeta(8)),
    pa.field("type",pa.string(),metadata=_fmeta(9))])
_IDX_ICE = Schema(NestedField(1,"id",StringType(),required=False), NestedField(2,"collection",StringType(),required=False),
    NestedField(3,"geometry",BinaryType(),required=False), NestedField(4,"bbox",StructType(
        NestedField(10,"xmin",DoubleType(),required=False), NestedField(11,"ymin",DoubleType(),required=False),
        NestedField(12,"xmax",DoubleType(),required=False), NestedField(13,"ymax",DoubleType(),required=False)),required=False),
    NestedField(5,"datetime",TimestamptzType(),required=False), NestedField(6,"properties",StringType(),required=False),
    NestedField(7,"assets",StringType(),required=False), NestedField(8,"stac_version",StringType(),required=False),
    NestedField(9,"type",StringType(),required=False))
_IDX_FIELDS = [{"id":1,"name":"id","required":False,"type":"string"},{"id":2,"name":"collection","required":False,"type":"string"},
    {"id":3,"name":"geometry","required":False,"type":"binary"},{"id":4,"name":"bbox","required":False,"type":{"type":"struct","fields":[
        {"id":10,"name":"xmin","required":False,"type":"double"},{"id":11,"name":"ymin","required":False,"type":"double"},
        {"id":12,"name":"xmax","required":False,"type":"double"},{"id":13,"name":"ymax","required":False,"type":"double"}]}},
    {"id":5,"name":"datetime","required":False,"type":"timestamptz"},{"id":6,"name":"properties","required":False,"type":"string"},
    {"id":7,"name":"assets","required":False,"type":"string"},{"id":8,"name":"stac_version","required":False,"type":"string"},
    {"id":9,"name":"type","required":False,"type":"string"}]
_IDX_NAMEMAP = [{"field-id":1,"names":["id"]},{"field-id":2,"names":["collection"]},{"field-id":3,"names":["geometry"]},
    {"field-id":4,"names":["bbox"],"fields":[{"field-id":10,"names":["xmin"]},{"field-id":11,"names":["ymin"]},
     {"field-id":12,"names":["xmax"]},{"field-id":13,"names":["ymax"]}]},{"field-id":5,"names":["datetime"]},
    {"field-id":6,"names":["properties"]},{"field-id":7,"names":["assets"]},{"field-id":8,"names":["stac_version"]},{"field-id":9,"names":["type"]}]
_IDX_GEO = {"version":"1.0","primary_column":"geometry","columns":{"geometry":{"encoding":"WKB","crs":"OGC:CRS84","edges":"planar","bbox_columns":["bbox"]}}}

def _wkb_box(x0,y0,x1,y1):
    b = struct.pack("<BIII",1,3,1,5)
    for x,y in [(x0,y0),(x1,y0),(x1,y1),(x0,y1),(x0,y0)]: b += struct.pack("<dd",float(x),float(y))
    return b

# region bbox (WGS84) for tabular/raster index rows lacking own footprint
REGION_WGS = [-4.58, 39.88, -3.05, 41.17]

def assets_vector(did):
    return {"data":{"href":f"v3.{did}","type":"application/x-iceberg","roles":["data"],
                    "title":"Iceberg v3 (native geometry, EPSG:25830) — iceberg_scan / ATTACH"},
            "data_parquet":{"href":f"{BUCKET_URL}/data/parquet/{did}.parquet","type":"application/vnd.apache.parquet",
                    "roles":["data"],"title":"Remote GeoParquet (EPSG:25830) — read_parquet in place"}}
def assets_tab(did):
    return {"data":{"href":f"tab.{did}","type":"application/x-iceberg","roles":["data"],
                    "title":"Iceberg table (tabular, no geometry) — ATTACH"},
            "data_parquet":{"href":f"{BUCKET_URL}/data/parquet_tab/{did}.parquet","type":"application/vnd.apache.parquet",
                    "roles":["data"],"title":"Remote Parquet — read_parquet in place"}}
def assets_raster(did):
    return {"data":{"href":f"{BUCKET_URL}/data/cog/{did}.tif","type":"image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles":["data"],"title":"Cloud-Optimized GeoTIFF (EPSG:25830) — /vsicurl/ , rasterio, GDAL"}}

def write_index(rows):
    ids=[r["id"] for r in rows]; colls=[COLLECTION]*len(rows)
    geom=[_wkb_box(*r["wgs"]) for r in rows]
    bbox=pa.StructArray.from_arrays([pa.array([r["wgs"][i] for r in rows],pa.float64()) for i in range(4)],fields=_BBOX_T)
    props=[]
    for r in rows:
        i=r["info"]
        p={"title":i["title"],"description":i["title"],"theme":i["theme"],"crs":CRS,"materialized":True,
           "provider":PROVIDER,"license":LICENSE,"rows":r.get("rows"),"kind":r["kind"],
           "semantics":{"spec":"Open Semantic Interchange",**semantics_for(i)}}
        if r["kind"]=="tabular": p["geospatial"]=False
        props.append(json.dumps(p, ensure_ascii=False))
    assets=[]
    for r in rows:
        a = assets_vector(r["id"]) if r["kind"]=="vector" else assets_tab(r["id"]) if r["kind"]=="tabular" else assets_raster(r["id"])
        assets.append(json.dumps(a, ensure_ascii=False))
    tbl=pa.table({"id":ids,"collection":colls,"geometry":pa.array(geom,pa.binary()),"bbox":bbox,
        "datetime":pa.array([None]*len(rows),pa.timestamp("us",tz="UTC")),"properties":props,
        "assets":assets,"stac_version":["1.1.0"]*len(rows),"type":["Feature"]*len(rows)},schema=_IDX_SCHEMA)
    root=STAGING/"data"/"catalog"/"datasets"; (root/"data").mkdir(parents=True,exist_ok=True)
    pqp=root/"data"/"datasets.parquet"; pq.write_table(tbl,pqp,compression="zstd")
    props_t={"geo":json.dumps(_IDX_GEO),"theme":"catalog-index","format":"stac-geoparquet",
             "title":"Comunidad de Madrid — STAC index (stac-geoparquet)"}
    mp=write_static_catalog(table_root=root,iceberg_schema=_IDX_ICE,schema_json_fields=_IDX_FIELDS,
        name_mapping=_IDX_NAMEMAP,data_files=[{"path":"data/datasets.parquet","size":pqp.stat().st_size,
        "rows":tbl.num_rows,"lower":{},"upper":{}}],format_version_in_metadata=2,
        location_uri=f"{BUCKET_URL}/data/catalog/datasets",extra_properties=props_t,last_column_id_override=13)
    return json.loads(Path(mp).read_text()), tbl.num_rows

def make_surface(tables):
    s={}; put=lambda k,b: s.__setitem__(k, json.dumps(b,indent=2))
    ns_tables={}
    for ns,name,meta,key in tables: ns_tables.setdefault(ns,[]).append((name,meta,key))
    put("v1/config",{"defaults":{},"overrides":{"prefix":IRC_PREFIX}})
    put(f"v1/{IRC_PREFIX}/namespaces",{"namespaces":[[n] for n in ns_tables]})
    for ns,items in ns_tables.items():
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}",{"namespace":[ns],"properties":{}})
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables",{"identifiers":[{"namespace":[ns],"name":nm} for nm,_,_ in items]})
        for nm,meta,key in items:
            put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables/{nm}",
                {"metadata-location":f"{BUCKET_URL}/data/{key}/metadata/v1.metadata.json","metadata":meta,"config":{}})
    return s

def main():
    # clean only the generated trees we fully rebuild; keep nothing stale
    if STAGING.exists(): shutil.rmtree(STAGING)
    if CONV.exists(): shutil.rmtree(CONV)
    man_vec=loadman(MAN_VEC); man_tab=loadman(MAN_TAB); man_ras=loadman(MAN_RAS)
    tables=[]; rows=[]
    nv=nt=nr=0
    for did,info in sorted(man_vec.items()):
        if not (VEC_SRC/info["src"]).exists(): continue
        try:
            meta,wgs,n=build_v3_vector(did,info)
            tables.append(("v3",did,meta,f"v3/{did}"))
            rows.append(dict(id=did,info=info,wgs=wgs,rows=n,kind="vector")); nv+=1
        except Exception as e:
            print(f"  VEC FAIL {did}: {str(e)[:140]}", flush=True)
    for did,info in sorted(man_tab.items()):
        if not (TAB_SRC/info["src"]).exists(): continue
        try:
            meta,n=build_tab(did,info)
            tables.append(("tab",did,meta,f"tab/{did}"))
            rows.append(dict(id=did,info=info,wgs=REGION_WGS,rows=n,kind="tabular")); nt+=1
        except Exception as e:
            print(f"  TAB FAIL {did}: {str(e)[:140]}", flush=True)
    for did,info in sorted(man_ras.items()):
        cog = COG_SRC/info.get("src",f"{did}.tif")
        if not cog.exists(): continue
        dst = STAGING/"data"/"cog"; dst.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(cog, dst/f"{did}.tif")
        wgs = info.get("wgs", REGION_WGS)
        rows.append(dict(id=did,info=info,wgs=wgs,rows=None,kind="raster")); nr+=1
    idx_meta,nidx=write_index(rows)
    tables.append(("catalog","datasets",idx_meta,"catalog/datasets"))
    surf=make_surface(tables)
    # flat staging: key "v1/sdi/namespaces/v3/tables/<id>" -> file "<key with / -> __>.json"
    # (real-path mirror impossible locally: `namespaces` is both a file and a dir on object storage)
    d=STAGING/"_surface"; d.mkdir(parents=True,exist_ok=True)
    keymap={}
    for k,v in surf.items():
        fn=k.replace("/","__")+".json"; (d/fn).write_text(v); keymap[k]=fn
    (STAGING/"_surface_manifest.json").write_text(json.dumps(keymap,indent=1))
    print(f"[build] vector={nv} tabular={nt} raster={nr} index_rows={nidx} surface_files={len(surf)} -> {STAGING}", flush=True)

if __name__=="__main__":
    main()
