#!/usr/bin/env python3
"""Convert Comunidad de Madrid geoidem WCS coverages -> Cloud-Optimized GeoTIFF (COG), native EPSG:25830.
Server-side DEFLATE compression on the WCS download (748MB -> ~7MB), then gdal_translate -of COG.
Resumable: skips coverages whose COG already exists. Records manifest_raster.json (id, title, theme, wgs bbox).
Run:  python3 convert_wcs.py [workers]
"""
import json, re, os, sys, subprocess, urllib.parse, threading
import concurrent.futures as cf
BASE="https://idem.comunidad.madrid/geoidem/ows"
RAW="/tmp/comu_conv/_wcs"; COG="/tmp/comu_conv/cog"; MAN="/tmp/comu_conv/manifest_raster.json"
os.makedirs(RAW,exist_ok=True); os.makedirs(COG,exist_ok=True)
COVS=json.load(open("/tmp/geoidem_wcs_coverages.json"))
def slug(cid): return re.sub(r"[^a-z0-9]+","_",cid.lower()).strip("_")
def theme(cid): return cid.split("__")[0] if "__" in cid else "raster"
_lock=threading.Lock()
def loadman():
    try: return json.load(open(MAN))
    except Exception: return {}
def save(rec):
    with _lock:
        m=loadman(); m[rec["id"]]=rec; tmp=MAN+".tmp"; json.dump(m,open(tmp,"w"),ensure_ascii=False,indent=1); os.replace(tmp,MAN)

def wgs_bbox(path):
    try:
        r=subprocess.run(["gdalinfo","-json",path],capture_output=True,text=True,timeout=120)
        j=json.loads(r.stdout); ext=j.get("wgs84Extent",{}).get("coordinates",[[]])[0]
        xs=[p[0] for p in ext]; ys=[p[1] for p in ext]
        return [min(xs),min(ys),max(xs),max(ys)]
    except Exception:
        return [-4.58,39.88,-3.05,41.17]

def one(c):
    cid=c["id"]; did=slug(cid); cog=f"{COG}/{did}.tif"
    if os.path.exists(cog) and os.path.getsize(cog)>1000:
        return did,"skip"
    raw=f"{RAW}/{did}.tif"
    try:
        q=urllib.parse.urlencode({"service":"WCS","version":"2.0.1","request":"GetCoverage",
            "coverageId":cid,"format":"image/tiff","geotiff:compression":"DEFLATE",
            "geotiff:tiling":"true","geotiff:tilewidth":"512","geotiff:tileheight":"512"})
        rr=subprocess.run(["curl","-s","--max-time","600","-o",raw,f"{BASE}?{q}"],capture_output=True,text=True)
        if not os.path.exists(raw) or os.path.getsize(raw)<1000:
            return did,"FAIL-download"
        with open(raw,"rb") as f:
            if f.read(1)==b"<": os.remove(raw); return did,"FAIL-exception"
        cp=subprocess.run(["gdal_translate",raw,cog,"-of","COG","-co","COMPRESS=DEFLATE",
            "-co","PREDICTOR=2","-co","OVERVIEWS=AUTO","-co","BIGTIFF=IF_SAFER","-q"],
            capture_output=True,text=True,timeout=1800)
        if not os.path.exists(cog):
            return did,f"FAIL-cog:{cp.stderr.strip()[:100]}"
        wgs=wgs_bbox(cog)
        try: os.remove(raw)
        except OSError: pass
        save({"id":did,"coverage":cid,"title":c.get("title") or cid,"theme":theme(cid),
              "src":f"{did}.tif","wgs":wgs,"type":"raster"})
        return did,f"OK({os.path.getsize(cog)//1024}KB)"
    except Exception as e:
        return did,f"ERR:{str(e)[:100]}"

def main():
    workers=int(sys.argv[1]) if len(sys.argv)>1 else 4
    print(f"[convert_wcs] {len(COVS)} coverages, workers={workers}",flush=True)
    ok=skip=fail=0; fails=[]
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for i,(did,st) in enumerate(ex.map(one,COVS),1):
            if st.startswith("OK"): ok+=1
            elif st=="skip": skip+=1
            else: fail+=1; fails.append((did,st))
            print(f"  [{i}/{len(COVS)}] {st[:40]:40s} {did}",flush=True)
    print(f"[convert_wcs] done ok={ok} skip={skip} fail={fail}",flush=True)
    for d,s in fails: print("   FAIL",d,s,flush=True)

if __name__=="__main__": main()
