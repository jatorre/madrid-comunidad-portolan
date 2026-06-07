#!/usr/bin/env python3
"""Convertidor GENÉRICO a Portolan v3 dual-engine (DuckDB bbox + Snowflake geom nativo).
Toma un GeoParquet fuente cualquiera y lo re-expone en <prefix>/v3/<id>/. Reusable para todos los catálogos.
Reglas (BEST_PRACTICES): geom -> tipo nativo Geometry(crs=srid:4326) CONTIGUO (antes de bbox);
bbox xmin/ymin/xmax/ymax al final (en el parquet, OCULTAS del esquema Iceberg); bounds por columna
en TODAS las columnas del esquema (placeholder en all-null); descripción + field_id por columna.
Uso: portolan_v3.py PREFIX DATASET_ID   (PREFIX = bucket/prefijo, p.ej. carto-portolan-madrid/comunidad-madrid)
"""
import os, sys, json, struct, subprocess, re
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import geoarrow.pyarrow as ga
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, LongType,
                             DoubleType, FloatType, BooleanType, BinaryType)
sys.path.insert(0, "/Users/jatorre/workspace/iceberg-geo-testbed")
from testbed._static_catalog import write_static_catalog

GEOM_EXT = ga.wkb().with_crs("srid:4326")
WORK = Path("/tmp/pv3"); WORK.mkdir(exist_ok=True)
SECRET = ("INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
  "CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '', ENDPOINT 'storage.googleapis.com', "
  "URL_STYLE 'path', USE_SSL true, REGION 'auto');")
# duckdb type -> (arrow, iceberg_type, jtype)
TM = {"VARCHAR":(pa.string(),StringType,"string"), "INTEGER":(pa.int32(),IntegerType,"int"),
      "BIGINT":(pa.int64(),LongType,"long"), "DOUBLE":(pa.float64(),DoubleType,"double"),
      "FLOAT":(pa.float32(),FloatType,"float"), "BOOLEAN":(pa.bool_(),BooleanType,"boolean")}
def mtype(dt):
    d=dt.upper().split("(")[0]
    return TM.get(d, TM["VARCHAR"])   # tipos raros (TIMESTAMP/DATE/DECIMAL/…) -> string

import math
def enc(jt,v):
    if v is None: return None
    try:
        if jt=="string": return str(v)[:32].encode("utf-8")   # trunc por CARÁCTER (UTF-8 válido)
        if jt=="int": return struct.pack("<i",int(v))
        if jt=="long": return struct.pack("<q",int(v))
        if jt in ("float","double"):
            f=float(v)
            if not math.isfinite(f): return None               # NaN/Inf -> placeholder
            return struct.pack("<f",f) if jt=="float" else struct.pack("<d",f)
        if jt=="boolean": return struct.pack("<?",bool(v))
    except Exception: return None
    return None
def ph(jt):
    return (b"",b"~") if jt=="string" else (struct.pack("<?",False),)*2 if jt=="boolean" else \
           (struct.pack("<i",0),)*2 if jt=="int" else (struct.pack("<q",0),)*2 if jt=="long" else \
           (struct.pack("<f",0.0),)*2 if jt=="float" else (struct.pack("<d",0.0),)*2
def fmeta(fid, doc): return {b"PARQUET:field_id":str(fid).encode(), b"description":doc.encode()[:300]}

def duck(sql):
    r=subprocess.run(["duckdb","-unsigned","-json","-c",SECRET+sql],capture_output=True,text=True)
    txt=r.stdout; dec=json.JSONDecoder(); arrs=[]; idx=0; n=len(txt)
    while idx<n:
        while idx<n and txt[idx] in ' \t\r\n': idx+=1
        if idx>=n: break
        try: val,end=dec.raw_decode(txt,idx); arrs.append(val); idx=end
        except Exception: idx+=1
    res=[a for a in arrs if isinstance(a,list) and a and not (len(a)==1 and isinstance(a[0],dict) and a[0].get("Success"))]
    return (res[-1] if res else []), r.stderr

def convert(prefix, ds, descs=None):
    descs = descs or {}
    src=f"s3://{prefix}/data/parquet/{ds}.parquet"
    gs=os.environ.get("PV3_OUT_GS", f"gs://{prefix}")
    # 1) esquema + CRS de geom
    schema_rows,err=duck(f"DESCRIBE SELECT * FROM read_parquet('{src}')")
    if not schema_rows: raise RuntimeError(f"describe fallo {ds}: {err[-200:]}")
    cols=[(r["column_name"], r["column_type"]) for r in schema_rows]
    geomcol=[n for n,t in cols if "GEOMETRY" in t.upper()]
    if not geomcol: raise RuntimeError(f"{ds}: sin columna geometry")
    gname=geomcol[0]; gtype=[t for n,t in cols if n==gname][0]
    m=re.search(r'EPSG:(\d+)', gtype); src_epsg=m.group(1) if m else "4326"
    attrs=[(n,t) for n,t in cols if n!=gname]
    # geom 4326
    gexpr = f'"{gname}"' if src_epsg in ("4326",) or "CRS84" in gtype.upper() else f"ST_Transform(\"{gname}\",'EPSG:{src_epsg}','EPSG:4326')"
    # 2) SELECT: atributos (cast raros->VARCHAR), geom_wkb, bbox
    sel=[]
    out_cols=[]  # (name, jtype, arrow_type)
    for n,t in attrs:
        at,it,jt=mtype(t)
        ln=n.lower()   # Snowflake aborta la poda de geom si el esquema tiene nombres en MAYÚSCULAS
        if jt=="string" and t.upper().split("(")[0] not in ("VARCHAR",):
            sel.append(f'CAST("{n}" AS VARCHAR) AS "{ln}"')
        else:
            sel.append(f'"{n}" AS "{ln}"')
        out_cols.append((ln,jt,at,it))
    sel.append(f"ST_AsWKB({gexpr}) AS __wkb")
    sel.append(f"ST_XMin({gexpr}) AS xmin"); sel.append(f"ST_YMin({gexpr}) AS ymin")
    sel.append(f"ST_XMax({gexpr}) AS xmax"); sel.append(f"ST_YMax({gexpr}) AS ymax")
    raw=WORK/f"{ds}__raw.parquet"
    # filtra geometrías nulas/vacías: una fila sin geometría es inútil para consultas espaciales,
    # y rompe la poda de Snowflake (stats NaN/null en bbox).
    where=f'WHERE "{gname}" IS NOT NULL AND NOT ST_IsEmpty("{gname}")'
    copysql=f"COPY (SELECT {', '.join(sel)} FROM read_parquet('{src}') {where}) TO '{raw}' (FORMAT parquet, COMPRESSION zstd)"
    r=subprocess.run(["duckdb","-unsigned","-c",SECRET+copysql],capture_output=True,text=True)
    if not raw.exists(): raise RuntimeError(f"{ds}: COPY fallo: {r.stderr[-200:]}")
    # 3) pyarrow: orden atributos, geom (contiguo), bbox; field_ids; descripciones
    t=pq.read_table(raw)
    fid=0; arrays={}; fields=[]; meta=[]  # meta: dicts por columna del esquema (sin bbox)
    BBOX=[("xmin",pa.float64(),DoubleType,"double"),("ymin",pa.float64(),DoubleType,"double"),
          ("xmax",pa.float64(),DoubleType,"double"),("ymax",pa.float64(),DoubleType,"double")]
    order=[]  # (name, jtype, arrow, iceberg, is_schema)
    for (n,jt,at,it) in out_cols: order.append((n,jt,at,it,True))
    order.append(("geom","geometry",GEOM_EXT,BinaryType,True))
    for (n,at,it,jt) in BBOX: order.append((n,jt,at,it,False))
    cmeta=[]
    for n,jt,at,it,insch in order:
        fid+=1
        doc=descs.get(n, f"{n}") if n!="geom" else f"Geometría (EPSG:4326, tipo Geometry nativo). {descs.get('geom','')}".strip()
        if n=="geom":
            arr=GEOM_EXT.wrap_array(t["__wkb"].combine_chunks())
            fields.append(pa.field("geom",GEOM_EXT,nullable=True,metadata=fmeta(fid,doc)))
        else:
            col=t[n].combine_chunks()
            arr=pc.cast(col,at) if col.type!=at else col
            fields.append(pa.field(n,at,nullable=True,metadata=fmeta(fid,doc)))
        arrays[n]=arr; cmeta.append(dict(fid=fid,name=n,jt=jt,it=it,insch=insch,doc=doc))
    schema=pa.schema(fields, metadata={b"dataset":ds.encode()})
    tbl=pa.table({c["name"]:arrays[c["name"]] for c in cmeta}, schema=schema)
    finp=WORK/f"{ds}.parquet"; pq.write_table(tbl,finp,compression="zstd",store_schema=True,write_statistics=True)
    raw.unlink()
    # 4) sube data
    subprocess.run(["gcloud","storage","cp",str(finp),f"{gs}/v3/{ds}/data/{ds}.parquet","-q"],capture_output=True,check=True)
    # 5) bounds por columna
    N=tbl.num_rows
    lower={};upper={};vc={};nv={}
    gx0=pc.min(tbl["xmin"]).as_py();gy0=pc.min(tbl["ymin"]).as_py();gx1=pc.max(tbl["xmax"]).as_py();gy1=pc.max(tbl["ymax"]).as_py()
    for c in cmeta:
        fidc=c["fid"]; col=tbl[c["name"]]; vc[fidc]=N; nv[fidc]=int(col.null_count)
        if c["name"]=="geom":
            if None not in (gx0,gy0,gx1,gy1): lower[fidc]=struct.pack("<dd",gx0,gy0);upper[fidc]=struct.pack("<dd",gx1,gy1)
            else: lower[fidc],upper[fidc]=struct.pack("<dd",0.0,0.0),struct.pack("<dd",0.0,0.0)
            continue
        nn=col.drop_null()
        lo=enc(c["jt"],pc.min(nn).as_py()) if len(nn) else None
        hi=enc(c["jt"],pc.max(nn).as_py()) if len(nn) else None
        if lo is None: lo,hi=ph(c["jt"])
        lower[fidc]=lo;upper[fidc]=hi
    # 6) metadata Iceberg v3 (oculta bbox del esquema, bounds en columnas del esquema)
    sch=[c for c in cmeta if c["insch"]]; keep={c["fid"] for c in sch}
    data=dict(path=f"data/{ds}.parquet", size=finp.stat().st_size, rows=N,
        lower={k:v for k,v in lower.items() if k in keep}, upper={k:v for k,v in upper.items() if k in keep},
        value_counts={k:v for k,v in vc.items() if k in keep}, null_value_counts={k:v for k,v in nv.items() if k in keep})
    ice=Schema(*[NestedField(c["fid"],c["name"],(BinaryType() if c["name"]=="geom" else c["it"]()),required=False) for c in sch])
    jf=[{"id":c["fid"],"name":c["name"],"required":False,
         "type":("geometry" if c["name"]=="geom" else c["jt"]),"doc":c["doc"]} for c in sch]
    nm=[{"field-id":c["fid"],"names":[c["name"]]} for c in sch]
    root=WORK/f"meta_{ds}";
    import shutil; shutil.rmtree(root,ignore_errors=True); root.mkdir(parents=True)
    write_static_catalog(table_root=root, iceberg_schema=ice, schema_json_fields=jf, name_mapping=nm,
        data_files=[data], format_version_in_metadata=3, location_uri=f"{gs}/v3/{ds}", meta_dir_name="metadata")
    subprocess.run(["gcloud","storage","cp","-r",str(root/"metadata"),f"{gs}/v3/{ds}/","-q"],capture_output=True)
    shutil.rmtree(root,ignore_errors=True); finp.unlink()
    return N, len(sch), src_epsg

if __name__=="__main__":
    prefix, ds = sys.argv[1], sys.argv[2]
    n, ncols, epsg = convert(prefix, ds)
    print(f"  {ds}: {n} filas, {ncols} cols esquema, src EPSG:{epsg} -> v3 OK", flush=True)
