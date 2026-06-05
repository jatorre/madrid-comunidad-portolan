#!/usr/bin/env python3
"""Ingest the Comunidad de Madrid CKAN open-data portal (datos.comunidad.madrid).
  CSV  -> plain parquet (tabular, tab.* namespace)  [robust: typed; fallback all_varchar]
  SHP  -> GeoParquet (vector)                        [gpio convert via /vsizip/]
Resumable. Records manifest_tab.json (tabular) and appends SHP to manifest_vector.json.
Run:  python3 convert_ckan.py csv [workers] [limit]   |   python3 convert_ckan.py shp
"""
import json, re, os, sys, subprocess, urllib.request, threading, time
import concurrent.futures as cf
PKGS=json.load(open("/tmp/ckan/packages.json"))
TAB="/tmp/comu_conv/tab"; VEC="/tmp/comu_conv/vector"; DL="/tmp/ckan/_dl"
MAN_TAB="/tmp/comu_conv/manifest_tab.json"; MAN_VEC="/tmp/comu_conv/manifest_vector.json"
os.makedirs(TAB,exist_ok=True); os.makedirs(VEC,exist_ok=True); os.makedirs(DL,exist_ok=True)
_lock=threading.Lock()
def slug(s): return re.sub(r"[^a-z0-9]+","_",s.lower()).strip("_")[:80]
def loadman(p):
    try: return json.load(open(p))
    except Exception: return {}
def save(p,rec):
    with _lock:
        m=loadman(p); m[rec["id"]]=rec; tmp=p+".tmp"; json.dump(m,open(tmp,"w"),ensure_ascii=False,indent=1); os.replace(tmp,p)

def theme_of(p):
    g=p.get("groups") or []
    return (g[0] if g else "OpenData")

def download(url,path):
    req=urllib.request.Request(url, headers={"User-Agent":"portolan-ingest/1.0"})
    with urllib.request.urlopen(req,timeout=300) as r, open(path,"wb") as f:
        while True:
            b=r.read(1024*256)
            if not b: break
            f.write(b)

def parquet_ok(path):
    if not os.path.exists(path) or os.path.getsize(path)<200: return False
    r=subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{path}')"],
                     capture_output=True,text=True,timeout=120)
    return r.returncode==0

def _rows(path):
    r=subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{path}')"],
                     capture_output=True,text=True,timeout=120)
    try: return int(r.stdout.strip())
    except Exception: return -1

def csv_to_parquet(csvp, outp):
    # Faithful data-lake conversion: all_varchar (no type inference -> NO silently dropped rows),
    # auto delimiter + header, latin-1 first (these stats CSVs are ISO-8859-1, ';'-delimited).
    for sql in (
        f"COPY (SELECT * FROM read_csv_auto('{csvp}', all_varchar=true, sample_size=-1, encoding='latin-1')) TO '{outp}' (FORMAT parquet);",
        f"COPY (SELECT * FROM read_csv_auto('{csvp}', all_varchar=true, sample_size=-1)) TO '{outp}' (FORMAT parquet);",
        f"COPY (SELECT * FROM read_csv('{csvp}', all_varchar=true, delim=';', header=true, encoding='latin-1', ignore_errors=true)) TO '{outp}' (FORMAT parquet);",
        f"COPY (SELECT * FROM read_csv('{csvp}', all_varchar=true, sample_size=-1, encoding='latin-1', ignore_errors=true)) TO '{outp}' (FORMAT parquet);",
    ):
        r=subprocess.run(["duckdb","-c",sql],capture_output=True,text=True,timeout=300)
        if parquet_ok(outp) and _rows(outp) >= 1: return True
        if os.path.exists(outp):
            try: os.remove(outp)
            except OSError: pass
    return False

def do_csv(p):
    did=slug(p["name"]); out=f"{TAB}/{did}.parquet"
    if parquet_ok(out): return did,"skip"
    res=[x for x in p["resources"] if x["format"]=="CSV" and x.get("url")]
    if not res: return did,"no-csv"
    url=res[0]["url"]; csvp=f"{DL}/{did}.csv"
    try:
        download(url,csvp)
        if os.path.getsize(csvp)<5: return did,"empty"
        if not csv_to_parquet(csvp,out): return did,"FAIL-convert"
        n=subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{out}')"],
                         capture_output=True,text=True).stdout.strip()
        try: os.remove(csvp)
        except OSError: pass
        save(MAN_TAB,{"id":did,"title":p["title"],"theme":theme_of(p),"src":f"{did}.parquet",
                      "rows":int(n) if n.isdigit() else None,"type":"tabular","license":p.get("license","cc-by")})
        return did,f"OK({n})"
    except Exception as e:
        for x in (csvp,):
            if os.path.exists(x):
                try: os.remove(x)
                except OSError: pass
        return did,f"ERR:{str(e)[:90]}"

def do_shp(p):
    did=slug(p["name"]); out=f"{VEC}/{did}.parquet"
    if parquet_ok(out): return did,"skip"
    res=[x for x in p["resources"] if x["format"]=="SHP" and x.get("url")]
    if not res: return did,"no-shp"
    url=res[0]["url"]; zp=f"{DL}/{did}.zip"; ed=f"{DL}/{did}_x"
    try:
        download(url,zp)
        os.makedirs(ed,exist_ok=True)
        subprocess.run(["unzip","-o","-q","-j",zp,"-d",ed],capture_output=True,text=True)
        shp=[f for f in os.listdir(ed) if f.lower().endswith(".shp")]
        if not shp: return did,"no-shp-in-zip"
        cp=subprocess.run(["gpio","convert",os.path.join(ed,shp[0]),out],capture_output=True,text=True,timeout=900)
        if not parquet_ok(out): return did,f"FAIL:{(cp.stderr or cp.stdout).strip()[:90]}"
        n=subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{out}')"],
                         capture_output=True,text=True).stdout.strip()
        try: os.remove(zp)
        except OSError: pass
        save(MAN_VEC,{"id":did,"typename":"ckan:"+p["name"],"title":p["title"],"theme":theme_of(p),
                      "crs":"source","rows":int(n) if n.isdigit() else None,"want":None,
                      "type":"vector","src":f"{did}.parquet"})
        return did,f"OK({n})"
    except Exception as e:
        return did,f"ERR:{str(e)[:90]}"

def main():
    mode=sys.argv[1] if len(sys.argv)>1 else "csv"
    workers=int(sys.argv[2]) if len(sys.argv)>2 else 12
    limit=int(sys.argv[3]) if len(sys.argv)>3 else 10**9
    if mode=="shp":
        todo=[p for p in PKGS if any(x["format"]=="SHP" for x in p["resources"])]
        fn=do_shp
    else:
        todo=[p for p in PKGS if any(x["format"]=="CSV" for x in p["resources"])][:limit]
        fn=do_csv
    print(f"[ckan:{mode}] {len(todo)} datasets, workers={workers}",flush=True)
    ok=skip=fail=0; fails=[]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for i,(did,st) in enumerate(ex.map(fn,todo),1):
            if st.startswith("OK"): ok+=1
            elif st=="skip": skip+=1
            else: fail+=1; fails.append((did,st))
            if i%50==0 or mode=="shp" or not st.startswith(("OK","skip")):
                print(f"  [{i}/{len(todo)}] {st[:34]:34s} {did}",flush=True)
    print(f"[ckan:{mode}] done ok={ok} skip={skip} fail={fail}",flush=True)
    for d,s in fails[:30]: print("   FAIL",d,s,flush=True)

if __name__=="__main__": main()
