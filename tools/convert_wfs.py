#!/usr/bin/env python3
"""Convert Comunidad de Madrid geoidem WFS feature types -> GeoParquet (native EPSG:25830).
The server returns the FULL layer in one GetFeature (no maxFeatures cap) but REJECTS `startIndex`
(no primary key -> "Cannot do natural order"). So: single streaming request to disk (memory-safe even
for multi-GB giants), then `gpio convert` (standardized geom+bbox, native CRS, ZSTD, Hilbert, validated).
Resumable: skips datasets whose output parquet already has the right row count.
Run:  python3 convert_wfs.py <min_count> <max_count> [workers]
"""
import json, re, os, sys, subprocess, urllib.parse, urllib.request, shutil, threading, time
import concurrent.futures as cf

BASE   = "https://idem.comunidad.madrid/geoidem/ows"
SRSREQ = "EPSG:25830"
OUTDIR = "/tmp/comu_conv/vector"
GEOJDIR= "/tmp/comu_conv/_geojson"
MANIFEST = "/tmp/comu_conv/manifest_vector.json"
os.makedirs(OUTDIR, exist_ok=True); os.makedirs(GEOJDIR, exist_ok=True)

FT = json.load(open("/tmp/geoidem_wfs_featuretypes.json"))

def slug(name):
    return re.sub(r"[^a-z0-9]+", "_", name.split(":")[-1].lower()).strip("_")
seen = {}
for r in FT:
    r["id"] = slug(r["name"]); seen.setdefault(r["id"], []).append(r)
for s, rows in seen.items():
    if len(rows) > 1:
        for r in rows: r["id"] = r["ws"].lower() + "_" + s

_lock = threading.Lock()
def load_manifest():
    if os.path.exists(MANIFEST):
        try: return json.load(open(MANIFEST))
        except Exception: return {}
    return {}
def save_manifest_entry(rec):
    with _lock:
        m = load_manifest(); m[rec["id"]] = rec
        tmp = MANIFEST + ".tmp"; json.dump(m, open(tmp,"w"), ensure_ascii=False, indent=1); os.replace(tmp, MANIFEST)

def fetch_to_file(tn, path):
    q = urllib.parse.urlencode({"service":"WFS","version":"2.0.0","request":"GetFeature",
        "typeNames":tn,"outputFormat":"application/json","srsName":SRSREQ})
    url = f"{BASE}?{q}"
    last = ""
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=900) as resp, open(path,"wb") as f:
                first = resp.read(1)
                if first == b"<":  # XML exception
                    rest = resp.read(800).decode("utf-8","ignore")
                    raise RuntimeError("WFS exception: " + rest[:160])
                f.write(first); shutil.copyfileobj(resp, f, 1024*256)
            return
        except Exception as e:
            last = str(e)
            if attempt == 3: raise
            time.sleep(3 + attempt*4)
    raise RuntimeError(last)

def parquet_rows(path):
    try:
        r = subprocess.run(["duckdb","-noheader","-csv","-c",
            f"SELECT count(*) FROM read_parquet('{path}')"], capture_output=True, text=True, timeout=300)
        return int(r.stdout.strip())
    except Exception: return -1

def convert_one(r):
    tn=r["name"]; did=r["id"]; want=r["count"]
    out=f"{OUTDIR}/{did}.parquet"
    if os.path.exists(out) and parquet_rows(out)==want:
        save_manifest_entry({"id":did,"typename":tn,"title":r["title"],"theme":r["ws"],
            "crs":SRSREQ,"rows":want,"want":want,"type":"vector","src":f"{did}.parquet"})
        return did,"skip",want
    gj=f"{GEOJDIR}/{did}.geojson"
    try:
        fetch_to_file(tn, gj)
        cp=subprocess.run(["gpio","convert",gj,out,"--overwrite"] if False else ["gpio","convert",gj,out],
                          capture_output=True, text=True, timeout=3600)
        if not os.path.exists(out):
            return did, f"FAIL:{(cp.stderr or cp.stdout).strip()[:140]}", -1
        n=parquet_rows(out)
        try: os.remove(gj)
        except OSError: pass
        save_manifest_entry({"id":did,"typename":tn,"title":r["title"],"theme":r["ws"],
            "crs":SRSREQ,"rows":n,"want":want,"type":"vector","src":f"{did}.parquet"})
        return did,("OK" if n==want else f"OK-partial({n}/{want})"),n
    except Exception as e:
        try:
            if os.path.exists(gj): os.remove(gj)
            if os.path.exists(out): os.remove(out)
        except OSError: pass
        return did, f"ERR:{str(e)[:140]}", -1

def main():
    lo=int(sys.argv[1]) if len(sys.argv)>1 else 1
    hi=int(sys.argv[2]) if len(sys.argv)>2 else 100000
    workers=int(sys.argv[3]) if len(sys.argv)>3 else 6
    todo=[r for r in FT if lo<=r["count"]<=hi]; todo.sort(key=lambda r:r["count"])
    print(f"[convert_wfs] {len(todo)} layers count in [{lo},{hi}], workers={workers}", flush=True)
    ok=skip=fail=0; fails=[]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs={ex.submit(convert_one,r):r for r in todo}
        for i,fut in enumerate(cf.as_completed(futs),1):
            did,status,n=fut.result()
            if status.startswith("OK"): ok+=1
            elif status=="skip": skip+=1
            else: fail+=1; fails.append((did,status))
            print(f"  [{i}/{len(todo)}] {status[:40]:40s} {did} ({n})", flush=True)
    print(f"[convert_wfs] done: ok={ok} skip={skip} fail={fail}", flush=True)
    if fails:
        print("[convert_wfs] FAILURES:", flush=True)
        for did,st in fails: print(f"    {did}: {st}", flush=True)

if __name__=="__main__":
    main()
