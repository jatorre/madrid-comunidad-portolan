#!/usr/bin/env python3
"""Build the FULL Madrid city catalog from manifest + state ledger.
- materialized vector layer -> Iceberg v2 (WKB+fp_*) + v3 (native geometry, EPSG:25830) + remote GeoParquet
- materialized raster layer  -> COG asset (uploaded separately)
- materialized tabular        -> Iceberg `tab` + remote parquet
- everything else (maintenance / metadata-only / failed) -> index row, materialized=false, data_status
ONE catalog.datasets stac-geoparquet index covers ALL 967 datasets. Static Iceberg-REST surface.
Run with the iceberg-geo-testbed venv."""
from __future__ import annotations
import json, struct, subprocess, sys, shutil, re
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import geoarrow.pyarrow as ga
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, LongType, FloatType,
                             DoubleType, BooleanType, BinaryType, StructType, TimestamptzType)
TESTBED = Path("/Users/jatorre/workspace/iceberg-geo-testbed"); sys.path.insert(0, str(TESTBED))
from testbed._static_catalog import write_static_catalog

CRS = "EPSG:25830"
BASE = "https://storage.googleapis.com/carto-portolan-madrid/madrid-city"
STAGING = Path("/tmp/madrid_catalog"); CONV = Path("/tmp/madrid_conv")
IRC_PREFIX = "sdi"; DUCKDB = "duckdb"
GEOM_EXT = ga.wkb().with_crs(CRS)
PROVIDER = "Ayuntamiento de Madrid"; LICENSE = "datos.gob.es (aviso legal)"
CITY_BBOX = [-3.8890, 40.3121, -3.5181, 40.6433]
MAN = {d['id']: d for d in json.load(open('/tmp/mad_manifest.json'))}
MAN_ORDER = [d['id'] for d in json.load(open('/tmp/mad_manifest.json'))]
STATE = json.load(open('/tmp/madrid_state.json')) if Path('/tmp/madrid_state.json').exists() else {}

def dle(v): return struct.pack("<d", float(v))
def xy(x, y): return struct.pack("<dd", float(x), float(y))
def _fmeta(i): return {"PARQUET:field_id": str(i)}
def sanitize(n):
    n = re.sub(r'[^A-Za-z0-9_]', '_', n)
    return n if n and not n[0].isdigit() else 'c_' + n

def _ice_field(field, fid):
    t = field.type
    if pa.types.is_boolean(t): it, js = BooleanType(), "boolean"
    elif pa.types.is_int64(t): it, js = LongType(), "long"
    elif pa.types.is_integer(t): it, js = IntegerType(), "int"
    elif pa.types.is_float64(t): it, js = DoubleType(), "double"
    elif pa.types.is_float32(t): it, js = FloatType(), "float"
    elif pa.types.is_binary(t) or pa.types.is_large_binary(t): it, js = BinaryType(), "binary"
    else: it, js = StringType(), "string"
    return (NestedField(fid, field.name, it, required=False),
            {"id": fid, "name": field.name, "required": False, "type": js})

def _normalize(src):
    """Robust: detect geom column, emit attrs + geom_wkb + per-row fp_* (native CRS)."""
    CONV.mkdir(parents=True, exist_ok=True)
    desc = subprocess.run([DUCKDB,'-csv','-noheader','-c',
        f"INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
        f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('{src}'))"],
        capture_output=True, text=True).stdout.strip().splitlines()
    cols = {l.split(',')[0]: l.split(',',1)[1] for l in desc if ',' in l}
    gc = next((c for c,t in cols.items() if 'GEOMETRY' in t.upper()), None)
    if gc is None:
        gc = next((c for c in ('geom','geometry','wkb_geometry','geom_wkb','the_geom') if c in cols), None)
    if gc is None: raise RuntimeError(f"no geometry column in {list(cols)[:8]}")
    excl = [gc] + (['bbox'] if 'bbox' in cols else [])
    out = CONV / (Path(src).stem + ".norm.parquet")
    sel = (f"SELECT * EXCLUDE({', '.join(excl)}), ST_AsWKB({gc}) AS geom_wkb, "
           f"ST_XMin({gc}) AS fp_xmin, ST_YMin({gc}) AS fp_ymin, ST_XMax({gc}) AS fp_xmax, ST_YMax({gc}) AS fp_ymax")
    subprocess.run([DUCKDB,'-c',
        f"INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
        f"COPY (SELECT * EXCLUDE(geom_wkb), geom_wkb FROM ({sel} FROM read_parquet('{src}'))) TO '{out}' (FORMAT parquet)"],
        check=True, capture_output=True)
    t = pq.read_table(out)
    # sanitize/ dedupe field names, drop export artifacts
    newnames, seen = [], {}
    for n in t.column_names:
        s = sanitize(n);
        while s in seen: s += '_'
        seen[s]=1; newnames.append(s)
    t = t.rename_columns(newnames)
    if 'OGC_FID' in t.column_names: t = t.drop(['OGC_FID'])
    for f in list(t.schema):
        if pa.types.is_temporal(f.type):
            t = t.set_column(t.schema.get_field_index(f.name), pa.field(f.name, pa.string()), pc.cast(t[f.name], pa.string()))
    return t

def _props(title, desc, kws, theme, materialized, status, source=None, semantics=None, rows=None):
    p = {"title": title, "description": (desc or "")[:500], "keywords": kws or [], "theme": theme,
         "crs": CRS, "materialized": materialized, "data_status": status,
         "provider": PROVIDER, "license": LICENSE}
    if rows is not None: p["rows"] = rows
    if source: p["source"] = source
    if semantics: p["semantics"] = semantics
    return p

def build_v2_v3(coll, src, num_rows):
    t = _normalize(src)
    attr = [c for c in t.column_names if c not in ("geom_wkb","fp_xmin","fp_ymin","fp_xmax","fp_ymax")]
    # v2
    v2_cols = attr + ["geom_wkb","fp_xmin","fp_ymin","fp_xmax","fp_ymax"]; v2t = t.select(v2_cols)
    fid = {n:i for i,n in enumerate(v2_cols,1)}; ice=[];fields=[];nm=[]
    for n in v2_cols:
        nf,jf=_ice_field(v2t.schema.field(n),fid[n]); ice.append(nf);fields.append(jf);nm.append({"field-id":fid[n],"names":[n]})
    root=STAGING/"data"/"v2"/coll; (root/"data").mkdir(parents=True,exist_ok=True); pqp=root/"data"/f"{coll}.parquet"
    v2t=v2t.replace_schema_metadata(None).cast(pa.schema([pa.field(n,v2t.schema.field(n).type,metadata=_fmeta(fid[n])) for n in v2_cols]))
    pq.write_table(v2t,pqp,compression="zstd")
    geo={"version":"1.0","primary_column":"geom_wkb","columns":{"geom_wkb":{"encoding":"WKB","crs":CRS,"edges":"planar","bbox_columns":["fp_xmin","fp_ymin","fp_xmax","fp_ymax"]}}}
    lo={fid[c]:dle(pc.min(t[c]).as_py()) for c in ("fp_xmin","fp_ymin","fp_xmax","fp_ymax")}
    up={fid[c]:dle(pc.max(t[c]).as_py()) for c in ("fp_xmin","fp_ymin","fp_xmax","fp_ymax")}
    df=[{"path":f"data/{coll}.parquet","size":pqp.stat().st_size,"rows":t.num_rows,"lower":lo,"upper":up}]
    v2mp=write_static_catalog(table_root=root,iceberg_schema=Schema(*ice),schema_json_fields=fields,name_mapping=nm,
        data_files=df,format_version_in_metadata=2,location_uri=f"{BASE}/data/v2/{coll}",
        extra_properties={"geo":json.dumps(geo),"crs":CRS,"provider":PROVIDER})
    v2_meta=json.loads(Path(v2mp).read_text())
    # v3
    v3_cols=attr+["geom"]; fid3={n:i for i,n in enumerate(v3_cols,1)}
    arrays={c:t[c] for c in attr}; arrays["geom"]=GEOM_EXT.wrap_array(t["geom_wkb"].combine_chunks())
    ice=[];fields=[];nm=[]
    for n in v3_cols:
        if n=="geom":
            ice.append(NestedField(fid3[n],"geom",BinaryType(),required=False)); fields.append({"id":fid3[n],"name":"geom","required":False,"type":f"geometry({CRS})"})
        else:
            nf,jf=_ice_field(t.schema.field(n),fid3[n]); ice.append(nf);fields.append(jf)
        nm.append({"field-id":fid3[n],"names":[n]})
    v3schema=pa.schema([pa.field(n,(GEOM_EXT if n=="geom" else t.schema.field(n).type),metadata=_fmeta(fid3[n])) for n in v3_cols])
    v3t=pa.table({n:arrays[n] for n in v3_cols},schema=v3schema)
    root3=STAGING/"data"/"v3"/coll; (root3/"data").mkdir(parents=True,exist_ok=True); pq3=root3/"data"/f"{coll}.parquet"
    pq.write_table(v3t,pq3,compression="zstd",store_schema=True,write_statistics=True)
    g=fid3["geom"]
    df3=[{"path":f"data/{coll}.parquet","size":pq3.stat().st_size,"rows":t.num_rows,
          "lower":{g:xy(pc.min(t["fp_xmin"]).as_py(),pc.min(t["fp_ymin"]).as_py())},
          "upper":{g:xy(pc.max(t["fp_xmax"]).as_py(),pc.max(t["fp_ymax"]).as_py())},
          "value_counts":{g:t.num_rows},"null_value_counts":{g:0}}]
    v3mp=write_static_catalog(table_root=root3,iceberg_schema=Schema(*ice),schema_json_fields=fields,name_mapping=nm,
        data_files=df3,format_version_in_metadata=3,location_uri=f"{BASE}/data/v3/{coll}",extra_properties={"crs":CRS,"provider":PROVIDER})
    return v2_meta, json.loads(Path(v3mp).read_text())

def build_tab(coll, src):
    t=pq.read_table(src)
    newnames,seen=[],{}
    for n in t.column_names:
        s=sanitize(n)
        while s in seen: s+='_'
        seen[s]=1;newnames.append(s)
    t=t.rename_columns(newnames)
    for f in list(t.schema):
        if pa.types.is_temporal(f.type) or pa.types.is_decimal(f.type):
            t=t.set_column(t.schema.get_field_index(f.name),pa.field(f.name,pa.string()),pc.cast(t[f.name],pa.string()))
    cols=t.column_names; fid={n:i for i,n in enumerate(cols,1)}; ice=[];fields=[];nm=[]
    for n in cols:
        nf,jf=_ice_field(t.schema.field(n),fid[n]);ice.append(nf);fields.append(jf);nm.append({"field-id":fid[n],"names":[n]})
    t=t.cast(pa.schema([pa.field(n,t.schema.field(n).type,metadata=_fmeta(fid[n])) for n in cols]))
    root=STAGING/"data"/"tab"/coll;(root/"data").mkdir(parents=True,exist_ok=True);pqp=root/"data"/f"{coll}.parquet"
    pq.write_table(t,pqp,compression="zstd")
    df=[{"path":f"data/{coll}.parquet","size":pqp.stat().st_size,"rows":t.num_rows,"lower":{},"upper":{}}]
    mp=write_static_catalog(table_root=root,iceberg_schema=Schema(*ice),schema_json_fields=fields,name_mapping=nm,
        data_files=df,format_version_in_metadata=2,location_uri=f"{BASE}/data/tab/{coll}",extra_properties={"provider":PROVIDER})
    return json.loads(Path(mp).read_text()), t.num_rows

# ---- index schema (same as proof) ----
# stac-geoparquet index schema (shared) — exec'd into the builder namespace
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
    b=struct.pack("<BIII",1,3,1,5)
    for x,y in [(x0,y0),(x1,y0),(x1,y1),(x0,y1),(x0,y0)]: b+=struct.pack("<dd",float(x),float(y))
    return b

def assets_for(layers):
    """layers: list of dicts for one index row's data (usually one)."""
    a={}
    for L in layers:
        c=L['collection']
        if L['kind']=='vector':
            a["data"]={"href":f"v2.{c}","type":"application/x-iceberg","roles":["data"],"title":"Iceberg v2 (WKB, EPSG:25830)"}
            a["data_v3"]={"href":f"v3.{c}","type":"application/x-iceberg","roles":["data"],"title":"Iceberg v3 (native geometry, EPSG:25830)"}
            a["data_parquet"]={"href":f"{BASE}/data/parquet/{c}.parquet","type":"application/vnd.apache.parquet","roles":["data"],"title":"Remote GeoParquet (EPSG:25830)"}
        elif L['kind']=='raster':
            a["data"]={"href":f"{BASE}/data/raster/{c}.tif","type":"image/tiff; application=geotiff; profile=cloud-optimized","roles":["data"],"title":"Cloud-Optimized GeoTIFF"}
        elif L['kind']=='tabular':
            a["data"]={"href":f"tab.{c}","type":"application/x-iceberg","roles":["data"],"title":"Iceberg table (non-spatial)"}
            a["data_parquet"]={"href":f"{BASE}/data/parquet/{c}.parquet","type":"application/vnd.apache.parquet","roles":["data"],"title":"Remote Parquet"}
    return json.dumps(a)

def write_index(rows):
    geom=pa.array([r["geom"] for r in rows], pa.binary())
    bbox=pa.StructArray.from_arrays([pa.array([(r["bbox"][i] if r["bbox"] else None) for r in rows],pa.float64()) for i in range(4)],fields=_BBOX_T)
    tbl=pa.table({"id":[r["id"] for r in rows],"collection":[r["coll"] for r in rows],"geometry":geom,"bbox":bbox,
        "datetime":pa.array([None]*len(rows),pa.timestamp("us",tz="UTC")),
        "properties":[json.dumps(r["props"],ensure_ascii=False) for r in rows],"assets":[r["assets"] for r in rows],
        "stac_version":["1.1.0"]*len(rows),"type":["Feature"]*len(rows)},schema=_IDX_SCHEMA)
    root=STAGING/"data"/"catalog"/"datasets";(root/"data").mkdir(parents=True,exist_ok=True);pqp=root/"data"/"datasets.parquet"
    pq.write_table(tbl,pqp,compression="zstd")
    props={"geo":json.dumps(_IDX_GEO),"theme":"catalog-index","format":"stac-geoparquet",
           "title":"Ayuntamiento de Madrid — STAC index (stac-geoparquet)",
           "semantics":json.dumps({"describes":"STAC Items for ALL Madrid datasets. materialized=true rows have cloud-native data at assets; materialized=false rows are catalogued metadata-only (data_status explains why).","answers":["dataset discovery","what data exists","is X available"]})}
    mp=write_static_catalog(table_root=root,iceberg_schema=_IDX_ICE,schema_json_fields=_IDX_FIELDS,name_mapping=_IDX_NAMEMAP,
        data_files=[{"path":"data/datasets.parquet","size":pqp.stat().st_size,"rows":tbl.num_rows,"lower":{},"upper":{}}],
        format_version_in_metadata=2,location_uri=f"{BASE}/data/catalog/datasets",extra_properties=props,last_column_id_override=13)
    return json.loads(Path(mp).read_text()), tbl.num_rows

def make_surface(tables):
    s={}; put=lambda k,b: s.__setitem__(k,json.dumps(b,indent=2)); ns_tables={}
    for ns,name,meta,key in tables: ns_tables.setdefault(ns,[]).append((name,meta,key))
    put("v1/config",{"defaults":{},"overrides":{"prefix":IRC_PREFIX}})
    put(f"v1/{IRC_PREFIX}/namespaces",{"namespaces":[[n] for n in ns_tables]})
    for ns,items in ns_tables.items():
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}",{"namespace":[ns],"properties":{}})
        put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables",{"identifiers":[{"namespace":[ns],"name":nm} for nm,_,_ in items]})
        for nm,meta,key in items:
            put(f"v1/{IRC_PREFIX}/namespaces/{ns}/tables/{nm}",{"metadata-location":f"{BASE}/data/{key}/metadata/v1.metadata.json","metadata":meta,"config":{}})
    return s

def title_for(ds, L, multi):
    base=ds['title'] or ds['id']
    if multi and L.get('layer'): return f"{base} — {L['layer']}"
    return base

def main():
    if STAGING.exists(): shutil.rmtree(STAGING)
    if CONV.exists(): shutil.rmtree(CONV)
    (STAGING/"data"/"parquet").mkdir(parents=True,exist_ok=True)
    (STAGING/"data"/"raster").mkdir(parents=True,exist_ok=True)
    tables=[]; rows=[]; nmat=0; nmeta=0; errs=[]
    for did in MAN_ORDER:
        ds=MAN[did]; st=STATE.get(did)
        coll_layers = (st or {}).get('layers') or []
        done = st and st.get('status')=='done' and coll_layers
        if done:
            multi=len(coll_layers)>1
            for L in coll_layers:
                coll=L['collection']; sem={"spec":"Open Semantic Interchange","label":ds['title'],"describes":(ds['description'] or ds['title'])[:300]}
                try:
                    if L['kind']=='vector':
                        v2m,v3m=build_v2_v3(coll,L['file'],L.get('rows'))
                        tables+=[("v2",coll,v2m,f"v2/{coll}"),("v3",coll,v3m,f"v3/{coll}")]
                        shutil.copy(L['file'], STAGING/"data"/"parquet"/f"{coll}.parquet")
                        props=_props(title_for(ds,L,multi),ds['description'],ds['keywords'],ds['category'] or ds['theme'],True,"available",semantics=sem,rows=L.get('rows'))
                        bb=L.get('bbox') or CITY_BBOX
                        rows.append(dict(id=coll,coll=ds['category'] or ds['theme'] or "madrid",geom=_wkb_box(*bb),bbox=bb,props=props,assets=assets_for([L])))
                    elif L['kind']=='raster':
                        shutil.copy(L['file'], STAGING/"data"/"raster"/f"{coll}.tif")
                        props=_props(title_for(ds,L,multi),ds['description'],ds['keywords'],ds['category'] or ds['theme'],True,"available",semantics=sem)
                        bb=L.get('bbox') or CITY_BBOX
                        rows.append(dict(id=coll,coll=ds['category'] or ds['theme'] or "madrid",geom=_wkb_box(*bb),bbox=bb,props=props,assets=assets_for([L])))
                    elif L['kind']=='tabular':
                        tm,nr=build_tab(coll,L['file']); tables.append(("tab",coll,tm,f"tab/{coll}"))
                        shutil.copy(L['file'], STAGING/"data"/"parquet"/f"{coll}.parquet")
                        props=_props(title_for(ds,L,multi),ds['description'],ds['keywords'],ds['category'] or ds['theme'],True,"available",semantics=sem,rows=nr)
                        props["portolan:geospatial"]=False
                        rows.append(dict(id=coll,coll=ds['category'] or ds['theme'] or "madrid",geom=None,bbox=None,props=props,assets=assets_for([L])))
                    nmat+=1
                except Exception as e:
                    errs.append(f"{coll}: {type(e).__name__}: {str(e)[:160]}")
                    # fall back to metadata-only row for this layer
                    props=_props(title_for(ds,L,multi),ds['description'],ds['keywords'],ds['category'] or ds['theme'],False,"build_error",
                                 source=(ds['distributions'][0]['url'] if ds['distributions'] else None))
                    rows.append(dict(id=coll,coll=ds['category'] or ds['theme'] or "madrid",geom=_wkb_box(*CITY_BBOX),bbox=CITY_BBOX,props=props,assets="{}"))
        else:
            status=(st or {}).get('status','metadata_only')
            src=ds['distributions'][0]['url'] if ds['distributions'] else f"https://geoportal.madrid.es/IDEAM_WBGEOPORTAL/"
            props=_props(ds['title'] or ds['id'],ds['description'],ds['keywords'],ds['category'] or ds['theme'],False,status,source=src)
            rows.append(dict(id=did,coll=ds['category'] or ds['theme'] or "madrid",geom=_wkb_box(*CITY_BBOX),bbox=CITY_BBOX,props=props,assets="{}"))
            nmeta+=1
    idx_meta,nidx=write_index(rows)
    tables.append(("catalog","datasets",idx_meta,"catalog/datasets"))
    surf=make_surface(tables)
    d=STAGING/"_surface"; d.mkdir(parents=True,exist_ok=True)
    for k,v in surf.items(): (d/(k.replace("/","__")+".json")).write_text(v)
    (STAGING/"_surface_manifest.json").write_text(json.dumps(list(surf.keys()),indent=1))
    print(f"index rows: {nidx} (materialized layers: {nmat}, metadata-only: {nmeta})")
    print(f"tables in surface: {len(tables)} ; errors: {len(errs)}")
    for e in errs[:25]: print("  ERR", e)

if __name__=="__main__":
    main()
