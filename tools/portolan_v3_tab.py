#!/usr/bin/env python3
"""Convertidor TABULAR a Portolan v3 (sin geom): tabla Iceberg v3 con solo columnas + bounds + minúsculas.
Para datos NO espaciales (data/parquet_tab/). Aplica las reglas Snowflake que sí valen sin geom:
nombres en minúsculas + lower/upper bounds + value/null counts en TODAS las columnas + descripciones.
Salida: gs://<prefix>/v3/<id>/{data,metadata}/. Uso: portolan_v3_tab.py PREFIX DATASET_ID
"""
import os, sys, json, struct, subprocess, math
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
sys.path.insert(0, "/tmp")
from portolan_v3 import TM, mtype, enc, ph, fmeta, duck, SECRET, WORK
sys.path.insert(0, "/Users/jatorre/workspace/iceberg-geo-testbed")
from testbed._static_catalog import write_static_catalog

def convert_tab(prefix, ds):
    src=f"s3://{prefix}/data/parquet_tab/{ds}.parquet"
    gs=os.environ.get("PV3_OUT_GS", f"gs://{prefix}")
    out_id=os.environ.get("PV3_OUT_ID", ds)   # permite renombrar la salida (colisión con dataset espacial)
    rows,err=duck(f"DESCRIBE SELECT * FROM read_parquet('{src}')")
    if not rows: raise RuntimeError(f"describe fallo {ds}: {err[-160:]}")
    cols=[(r["column_name"], r["column_type"]) for r in rows]
    # SELECT: todas las columnas en minúsculas; tipos raros -> VARCHAR
    sel=[]; out=[]  # out: (lname, jt, arrow, iceberg)
    seen=set()
    for n,t in cols:
        at,it,jt=mtype(t); ln=n.lower()
        if ln in seen: ln=f"{ln}_{len(seen)}"   # evita colisión al minuscular
        seen.add(ln)
        if jt=="string" and t.upper().split("(")[0] not in ("VARCHAR",):
            sel.append(f'CAST("{n}" AS VARCHAR) AS "{ln}"')
        else:
            sel.append(f'"{n}" AS "{ln}"')
        out.append((ln,jt,at,it))
    raw=WORK/f"{ds}__tabraw.parquet"
    csql=f"COPY (SELECT {', '.join(sel)} FROM read_parquet('{src}')) TO '{raw}' (FORMAT parquet, COMPRESSION zstd)"
    r=subprocess.run(["duckdb","-unsigned","-c",SECRET+csql],capture_output=True,text=True)
    if not raw.exists(): raise RuntimeError(f"{ds}: COPY fallo: {r.stderr[-160:]}")
    t=pq.read_table(raw)
    fields=[]; arrays={}; cmeta=[]
    for i,(ln,jt,at,it) in enumerate(out,1):
        col=t[ln].combine_chunks(); arr=pc.cast(col,at) if col.type!=at else col
        fields.append(pa.field(ln,at,nullable=True,metadata=fmeta(i,ln)))
        arrays[ln]=arr; cmeta.append(dict(fid=i,name=ln,jt=jt,it=it))
    tbl=pa.table({c["name"]:arrays[c["name"]] for c in cmeta},
                 schema=pa.schema(fields,metadata={b"dataset":ds.encode()}))
    finp=WORK/f"{ds}__tab.parquet"; pq.write_table(tbl,finp,compression="zstd",store_schema=True,write_statistics=True)
    raw.unlink()
    subprocess.run(["gcloud","storage","cp",str(finp),f"{gs}/v3/{out_id}/data/{out_id}.parquet","-q"],capture_output=True,check=True)
    N=tbl.num_rows; lower={};upper={};vc={};nv={}
    for c in cmeta:
        fidc=c["fid"]; col=tbl[c["name"]]; vc[fidc]=N; nv[fidc]=int(col.null_count)
        nn=col.drop_null()
        lo=enc(c["jt"],pc.min(nn).as_py()) if len(nn) else None
        hi=enc(c["jt"],pc.max(nn).as_py()) if len(nn) else None
        if lo is None: lo,hi=ph(c["jt"])
        lower[fidc]=lo; upper[fidc]=hi
    data=dict(path=f"data/{out_id}.parquet", size=finp.stat().st_size, rows=N,
        lower=lower, upper=upper, value_counts=vc, null_value_counts=nv)
    ice=Schema(*[NestedField(c["fid"],c["name"],c["it"](),required=False) for c in cmeta])
    jf=[{"id":c["fid"],"name":c["name"],"required":False,"type":c["jt"]} for c in cmeta]
    nm=[{"field-id":c["fid"],"names":[c["name"]]} for c in cmeta]
    root=WORK/f"metatab_{ds}"; import shutil; shutil.rmtree(root,ignore_errors=True); root.mkdir(parents=True)
    write_static_catalog(table_root=root, iceberg_schema=ice, schema_json_fields=jf, name_mapping=nm,
        data_files=[data], format_version_in_metadata=3, location_uri=f"{gs}/v3/{out_id}", meta_dir_name="metadata")
    subprocess.run(["gcloud","storage","cp","-r",str(root/"metadata"),f"{gs}/v3/{out_id}/","-q"],capture_output=True)
    shutil.rmtree(root,ignore_errors=True); finp.unlink()
    return N, len(cmeta)

if __name__=="__main__":
    n,ncols=convert_tab(sys.argv[1], sys.argv[2])
    print(f"  {sys.argv[2]}: {n} filas, {ncols} cols (tabular, sin geom) -> v3 OK", flush=True)
