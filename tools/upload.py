#!/usr/bin/env python3
"""Upload the staged Comunidad de Madrid catalog to GCS.
  1) rsync the data/ tree (v3 metadata+parquet, remote parquet, tab, cog, catalog index)
  2) per-file upload the extensionless Iceberg-REST surface (v1/...) with content-type application/json
Bucket is already public (allUsers:objectViewer at bucket level).
"""
import json, os, subprocess, concurrent.futures as cf
STAGING="/tmp/comu_catalog"; GS="gs://carto-portolan-madrid/comunidad-madrid"

def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)

# 1) data tree (rsync, parallel). -r recursive, -m multi-thread.
print("[upload] rsync data/ ...", flush=True)
r=run(["gcloud","storage","rsync","-r",f"{STAGING}/data",f"{GS}/data"])
print(r.stdout[-400:] if r.stdout else "", (r.stderr[-600:] if r.returncode else "")[-600:], flush=True)

# 2) surface files
keymap=json.load(open(f"{STAGING}/_surface_manifest.json"))
def up(item):
    key,fn=item
    src=f"{STAGING}/_surface/{fn}"; dst=f"{GS}/{key}"
    rr=run(["gcloud","storage","cp","--content-type=application/json",src,dst])
    return key, rr.returncode, (rr.stderr.strip()[-120:] if rr.returncode else "")
print(f"[upload] {len(keymap)} surface files ...", flush=True)
ok=fail=0; fails=[]
with cf.ThreadPoolExecutor(max_workers=16) as ex:
    for key,rc,err in ex.map(up, list(keymap.items())):
        if rc==0: ok+=1
        else: fail+=1; fails.append((key,err))
print(f"[upload] surface done ok={ok} fail={fail}", flush=True)
for k,e in fails[:20]: print("   FAIL", k, e, flush=True)
