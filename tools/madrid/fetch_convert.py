#!/usr/bin/env python3
"""Resumable: download + convert every downloadable Madrid dataset.
Vector SHP/KML -> GeoParquet (native EPSG:25830, via gpio). Raster TIF -> COG.
CSV -> parquet. Writes /tmp/madrid_state.json (per-dataset, crash-safe, skip-if-done).
Maintenance/failed downloads are recorded as status!='done' (-> metadata-only later)."""
import json, os, re, subprocess, shutil, glob
from pathlib import Path

MAN = json.load(open('/tmp/mad_manifest.json'))
STATE_P = Path('/tmp/madrid_state.json')
STATE = json.load(open(STATE_P)) if STATE_P.exists() else {}
DATA = Path('/tmp/mad_data'); (DATA/'vector').mkdir(parents=True, exist_ok=True)
(DATA/'raster').mkdir(parents=True, exist_ok=True); (DATA/'tab').mkdir(parents=True, exist_ok=True)
WORK = Path('/tmp/mad_work'); WORK.mkdir(exist_ok=True)
GPIO = os.path.expanduser('~/.local/bin/gpio')
RASTER_EXT = {'tif','tiff'}

def save(): STATE_P.write_text(json.dumps(STATE, ensure_ascii=False, indent=1))
def run(cmd, **kw): return subprocess.run(cmd, capture_output=True, text=True, **kw)

def download(url, dest, timeout=180):
    r = run(['curl','-sSL','-m',str(timeout),'-o',str(dest),'-w','%{http_code} %{url_effective}',url])
    out = r.stdout.strip()
    code = out.split()[0] if out else '000'
    final = out.split(' ',1)[1] if ' ' in out else ''
    if 'mantenimiento' in final or 'ServicioNoDisponible' in final:
        return 'maintenance'
    if not dest.exists() or dest.stat().st_size < 1024:
        return 'maintenance' if dest.exists() else 'failed'
    head = dest.read_bytes()[:64].lstrip()
    if head[:1] in (b'<',) and dest.stat().st_size < 6000:  # tiny HTML error page
        return 'maintenance'
    return 'ok' if code in ('200','0') else f'http_{code}'

def extract(arc, into):
    into.mkdir(parents=True, exist_ok=True)
    ft = run(['file','-b',str(arc)]).stdout.lower()
    if 'zip' in ft:
        if run(['unzip','-oq',str(arc),'-d',str(into)]).returncode == 0: return True
    if shutil.which('7z'):
        if run(['7z','x','-y',f'-o{into}',str(arc)]).returncode == 0: return True
    # unix compress / gzip single file
    for tool in (['gzip','-dc'],['uncompress','-c']):
        if shutil.which(tool[0]):
            o = into/(arc.stem+'.out')
            rr = run(tool+[str(arc)])
            if rr.returncode == 0 and rr.stdout:
                o.write_bytes(rr.stdout.encode('latin1','ignore'));
            try:
                with open(o,'wb') as f: f.write(subprocess.run(tool+[str(arc)],capture_output=True).stdout)
                if o.stat().st_size>1024: return True
            except Exception: pass
    # maybe it's actually a zip without extension
    if run(['unzip','-oq',str(arc),'-d',str(into)]).returncode == 0: return True
    return False

def wgs_bbox_vec(parq):
    q=(f"INSTALL spatial;LOAD spatial;SET geometry_always_xy=true;"
       f"SELECT min(ST_XMin(g)),min(ST_YMin(g)),max(ST_XMax(g)),max(ST_YMax(g)) FROM "
       f"(SELECT ST_Transform(geom,'EPSG:25830','EPSG:4326') g FROM read_parquet('{parq}'));")
    r=run(['duckdb','-csv','-noheader','-c',q])
    try: return [float(v) for v in r.stdout.strip().splitlines()[0].split(',')]
    except Exception: return None

def conv_vector(shp, coll):
    out = DATA/'vector'/f'{coll}.parquet'
    if out.exists(): return out
    r = run([GPIO,'convert',str(shp),str(out)])
    if not out.exists():  # fallback to ogr2ogr
        run(['ogr2ogr','-f','Parquet',str(out),str(shp),'-lco','GEOMETRY_ENCODING=WKB'])
    return out if out.exists() else None

def rows_of(parq):
    r=run(['duckdb','-csv','-noheader','-c',f"SELECT count(*) FROM read_parquet('{parq}')"])
    try: return int(r.stdout.strip().splitlines()[-1])
    except Exception: return None

def conv_cog(tif, coll):
    out = DATA/'raster'/f'{coll}.tif'
    if out.exists(): return out
    run(['gdal_translate',str(tif),str(out),'-of','COG','-co','COMPRESS=DEFLATE',
         '-co','PREDICTOR=2','-co','OVERVIEWS=AUTO','-co','BIGTIFF=IF_SAFER'])
    return out if out.exists() else None

def wgs_bbox_ras(tif):
    j=run(['gdalinfo','-json',str(tif)]).stdout
    try:
        d=json.loads(j); ext=d.get('wgs84Extent',{}).get('coordinates',[None])[0]
        if ext:
            xs=[p[0] for p in ext]; ys=[p[1] for p in ext]
            return [min(xs),min(ys),max(xs),max(ys)]
    except Exception: pass
    return None

def process(ds):
    did = ds['id']
    if STATE.get(did,{}).get('status')=='done': return
    dists = ds['distributions']
    arcs = [d for d in dists if d['ext'] in ('zip','z')]
    csvs = [d for d in dists if d['ext']=='csv']
    layers=[]; status='metadata_only'; err=None
    try:
        if arcs:
            d0 = arcs[0]
            arc = WORK/f"{did}.{d0['ext']}"
            dl = download(d0['url'], arc)
            if dl != 'ok':
                status = dl  # maintenance/failed
            else:
                ex = WORK/did
                if ex.exists(): shutil.rmtree(ex)
                if not extract(arc, ex):
                    status='extract_failed'
                else:
                    shps = glob.glob(str(ex/'**'/'*.shp'), recursive=True)
                    tifs = [t for t in glob.glob(str(ex/'**'/'*.tif'), recursive=True)+glob.glob(str(ex/'**'/'*.tiff'),recursive=True)]
                    kmls = glob.glob(str(ex/'**'/'*.kml'), recursive=True)+glob.glob(str(ex/'**'/'*.kmz'),recursive=True)
                    multi = len(shps)+len(tifs) > 1
                    for shp in sorted(shps):
                        nm = Path(shp).stem
                        coll = f"{did}__{re.sub(r'[^a-z0-9]+','_',nm.lower())[:30]}" if multi else did
                        p = conv_vector(shp, coll)
                        if p: layers.append(dict(collection=coll, kind='vector', file=str(p),
                                                 rows=rows_of(p), bbox=wgs_bbox_vec(p), layer=nm))
                    for tif in sorted(tifs):
                        nm = Path(tif).stem
                        coll = f"{did}__{re.sub(r'[^a-z0-9]+','_',nm.lower())[:30]}" if multi else did
                        c = conv_cog(tif, coll)
                        if c: layers.append(dict(collection=coll, kind='raster', file=str(c),
                                                 bbox=wgs_bbox_ras(c), layer=nm))
                    for k in sorted(kmls):
                        coll=did; p=conv_vector(k,coll)
                        if p: layers.append(dict(collection=coll,kind='vector',file=str(p),rows=rows_of(p),bbox=wgs_bbox_vec(p),layer=Path(k).stem))
                    status = 'done' if layers else 'no_layers'
        elif csvs:
            arc = WORK/f"{did}.csv"
            dl = download(csvs[0]['url'], arc)
            if dl!='ok': status=dl
            else:
                out=DATA/'tab'/f'{did}.parquet'
                run(['duckdb','-c',f"COPY (SELECT * FROM read_csv_auto('{arc}', sample_size=-1, ignore_errors=true)) TO '{out}' (FORMAT parquet)"])
                if out.exists(): layers.append(dict(collection=did,kind='tabular',file=str(out),rows=rows_of(out),bbox=None,layer=did)); status='done'
                else: status='csv_failed'
        else:
            status='metadata_only'  # doc/none
    except Exception as e:
        status='error'; err=str(e)[:300]
    STATE[did]=dict(status=status, kind=ds['kind'], category=ds['category'], layers=layers, error=err)
    save()
    print(f"[{status:14}] {did}  layers={len(layers)}")

def main():
    todo=[d for d in MAN if d['has_download']]
    print(f"processing {len(todo)} downloadable datasets ({len(MAN)} total)")
    for i,ds in enumerate(todo,1):
        process(ds)
    # summary
    import collections
    c=collections.Counter(v['status'] for v in STATE.values())
    print("STATUS:", dict(c))
    nlayers=sum(len(v['layers']) for v in STATE.values())
    print(f"materialized layers: {nlayers}")

if __name__=='__main__':
    main()
