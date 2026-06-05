#!/usr/bin/env python3
"""Publish this repo's catalog to an object-storage bucket as a static Apache Iceberg
REST catalog — so DuckDB / Snowflake can ATTACH it.

The git repo is the *source* (collision-free: STAC + Iceberg table files). The Iceberg
REST API responses (config / namespaces / tables / table-load) are *generated* here and
written straight to the bucket as object keys via `mc pipe`. They are never materialized
on a filesystem — which is why this works on a flat-keyspace object store but not on a
filesystem-backed host like GitHub Pages (where `namespaces` can't be both file and dir).

Config: reads `portolan.config.json` at the repo root (public_base, bucket_path, mc_alias,
s3_endpoint). Env vars PUBLIC_BASE / MC_TARGET override it if set.
Usage: python tools/publish.py   (run from the repo root; mc alias must be configured)
"""
import json, os, subprocess, sys, glob, html, tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

def _config():
    try:
        return json.load(open("portolan.config.json"))
    except FileNotFoundError:
        return {}

_C = _config()
PUBLIC_BASE = (os.environ.get("PUBLIC_BASE") or _C["public_base"]).rstrip("/")
MC_TARGET = (os.environ.get("MC_TARGET")
             or f"{_C.get('mc_alias', 'upcloud')}/{_C['bucket_path']}").rstrip("/")
PREFIX = _C.get("rest_prefix", "sdi")  # Iceberg REST catalog prefix (see /v1/config overrides)


def put(key: str, body: str):
    """Write `body` to <MC_TARGET>/<key> as a single object (no filesystem, no collision)."""
    dest = f"{MC_TARGET}/{key}"
    p = subprocess.run(["mc", "pipe", dest], input=body.encode(), capture_output=True)
    if p.returncode != 0:
        sys.exit(f"mc pipe {dest} failed: {p.stderr.decode()}")
    print(f"  put {key}")


def discover_tables():
    """Each data/<namespace>/<table>/metadata/v1.metadata.json is a table."""
    tables = {}
    for mp in glob.glob("data/*/*/metadata/v1.metadata.json"):
        _, ns, table, _, _ = mp.split("/")
        tables.setdefault(ns, {})[table] = mp
    return tables


def main():
    tables = discover_tables()

    # 1) Mirror only the Iceberg METADATA (small: metadata.json + manifests) from git.
    #    The DATA bytes (parquet) live ONLY on the bucket and are uploaded out-of-band
    #    (see README "Contributing data"), so we never push or prune them here.
    print("mirroring metadata ->", MC_TARGET + "/data/  (data/*.parquet excluded)")
    subprocess.run(["mc", "mirror", "--overwrite", "data/", f"{MC_TARGET}/data/",
                    "--exclude", "**/data/**"], check=True)

    # sanity: warn (don't fail) if a table's data bytes aren't on the bucket yet
    for ns, tbls in tables.items():
        for table in tbls:
            r = subprocess.run(["mc", "ls", f"{MC_TARGET}/data/{ns}/{table}/data/"],
                               capture_output=True, text=True)
            if r.returncode != 0 or not r.stdout.strip():
                print(f"  ::warning:: no data bytes on bucket for {ns}.{table} — "
                      f"upload the parquet to {MC_TARGET}/data/{ns}/{table}/data/")

    # 2) generate the Iceberg REST catalog responses, write them as object keys
    print("generating REST catalog tree")
    put(f"v1/config", json.dumps({
        "defaults": {}, "overrides": {"prefix": PREFIX},
        "endpoints": [
            "GET /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}",
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        ],
    }))
    put(f"v1/{PREFIX}/namespaces",
        json.dumps({"namespaces": [[ns] for ns in tables]}))
    for ns, tbls in tables.items():
        put(f"v1/{PREFIX}/namespaces/{ns}",
            json.dumps({"namespace": [ns], "properties": {}}))
        put(f"v1/{PREFIX}/namespaces/{ns}/tables",
            json.dumps({"identifiers": [{"namespace": [ns], "name": t} for t in tbls]}))
        for table, mp in tbls.items():
            metadata = json.load(open(mp))
            put(f"v1/{PREFIX}/namespaces/{ns}/tables/{table}", json.dumps({
                "metadata-location": f"{PUBLIC_BASE}/{mp}",
                "metadata": metadata,
                "config": {},
            }))

    # 3) mirror the catalogue metadata documents (STAC + OGC API - Records views).
    #    Same GeoJSON model, two vocabularies — both served as static files on the bucket.
    print("mirroring catalogue docs (STAC catalog.json/items + OGC API - Records records/)")
    subprocess.run(["mc", "cp", "catalog.json", f"{MC_TARGET}/catalog.json"], check=True)
    for d in ("items", "records"):
        if os.path.isdir(d):
            subprocess.run(["mc", "mirror", "--overwrite", f"{d}/", f"{MC_TARGET}/{d}/"], check=True)

    # 4) generate a human-readable HTML explorer from the STAC metadata
    build_explorer()

    print(f"\nPublished. ATTACH endpoint:\n  {PUBLIC_BASE}")
    print(f"  Human explorer:  {PUBLIC_BASE}/index.html")


def build_explorer():
    """Static HTML index for humans — generated from the STAC metadata, with direct
    parquet download links and copy-paste SQL for DuckDB/Snowflake. Written to the bucket."""
    cat = json.load(open("catalog.json"))
    items = [json.load(open(os.path.join("items", f)))
             for f in sorted(os.listdir("items")) if f.endswith(".json")] if os.path.isdir("items") else []

    def esc(x):
        return html.escape(str(x))

    cards = []
    for it in items:
        p = it.get("properties", {})
        asset = it.get("assets", {}).get("data", {})
        meta_href = asset.get("href", "")
        table = asset.get("portolan:iceberg_table", "")
        endpoint = asset.get("portolan:iceberg_endpoint", PUBLIC_BASE)
        sem = p.get("semantics", {}) or {}
        bbox = it.get("bbox", [])
        # find the actual parquet file(s) on the bucket (dependency-free, via mc)
        parquets = []
        if "/metadata/" in meta_href:
            data_pub = meta_href.rsplit("/metadata/", 1)[0] + "/data/"
            data_mc = data_pub.replace(PUBLIC_BASE, MC_TARGET)
            ls = subprocess.run(["mc", "ls", data_mc], capture_output=True, text=True).stdout
            parquets = [data_pub + ln.split()[-1] for ln in ls.splitlines()
                        if ln.strip().endswith(".parquet")]

        sql_attach = (f"ATTACH 'cat' (TYPE iceberg, ENDPOINT '{endpoint}',\n"
                      f"  AUTHORIZATION_TYPE 'none');\nSELECT * FROM cat.{table} LIMIT 10;")
        sql_scan = f"SELECT * FROM iceberg_scan('{meta_href}') LIMIT 10;"
        dl = "".join(f'<a class="dl" href="{esc(u)}">⬇ {esc(u.rsplit("/",1)[-1])}</a>' for u in parquets) \
             or '<span class="muted">(no parquet listed)</span>'

        cards.append(f"""
    <article class="ds">
      <h2>{esc(p.get('title', it.get('id','')))}</h2>
      <p>{esc(p.get('description',''))}</p>
      <table class="meta">
        <tr><th>Answers</th><td>{esc(sem.get('answers','—'))} <span class="muted">({esc(sem.get('unit','—'))})</span></td></tr>
        <tr><th>Provider</th><td>{esc(p.get('provider','—'))}</td></tr>
        <tr><th>License</th><td>{esc(p.get('license','—'))}</td></tr>
        <tr><th>Extent (WGS84)</th><td>{esc(bbox)}</td></tr>
        <tr><th>Iceberg table</th><td><code>{esc(table)}</code></td></tr>
      </table>
      <h3>Download</h3>
      <p class="dls">{dl}</p>
      <h3>Query — DuckDB / Snowflake (ATTACH the catalog)</h3>
      <pre>{esc(sql_attach)}</pre>
      <h3>Query — scan the table directly</h3>
      <pre>{esc(sql_scan)}</pre>
      <p class="links">
        <a href="{esc(meta_href)}">Iceberg metadata.json</a> ·
        <a href="{PUBLIC_BASE}/records/{esc(it.get('id',''))}.json">OGC API - Records</a>
      </p>
    </article>""")

    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(cat.get('title','Portolan catalog'))}</title>
<style>
  :root {{ --ink:#0b1f2a; --muted:#6b7c86; --line:#e2e8ec; --lime:#c2e812; --blue:#1a5fb4; }}
  * {{ box-sizing:border-box; }}
  body {{ font:16px/1.55 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; color:var(--ink); margin:0; background:#fbfcfd; }}
  header {{ background:var(--ink); color:#fff; padding:2.4rem 1.5rem; }}
  header .wrap, main {{ max-width:880px; margin:0 auto; }}
  header h1 {{ margin:0 0 .4rem; font-size:1.7rem; }}
  header p {{ margin:0; color:#b9c6cd; max-width:62ch; }}
  .badges {{ margin-top:1rem; }}
  .badge {{ display:inline-block; background:#13303d; color:#cfe; border:1px solid #2a4a57; border-radius:999px; padding:.2rem .7rem; font-size:.78rem; margin:.2rem .3rem .2rem 0; }}
  main {{ padding:1.5rem; }}
  .ds {{ background:#fff; border:1px solid var(--line); border-radius:12px; padding:1.3rem 1.4rem; margin:1.2rem 0; }}
  .ds h2 {{ margin:.1rem 0 .4rem; }}
  .ds h3 {{ font-size:.82rem; text-transform:uppercase; letter-spacing:.04em; color:var(--muted); margin:1.1rem 0 .35rem; }}
  table.meta {{ border-collapse:collapse; width:100%; margin:.6rem 0; font-size:.92rem; }}
  table.meta th {{ text-align:left; color:var(--muted); font-weight:600; width:11rem; padding:.25rem .6rem .25rem 0; vertical-align:top; }}
  pre {{ background:#0b1f2a; color:#e6f0f3; padding:.85rem 1rem; border-radius:8px; overflow:auto; font-size:.84rem; }}
  a {{ color:var(--blue); }}
  a.dl {{ display:inline-block; background:var(--lime); color:#1c2a00; font-weight:600; text-decoration:none; padding:.4rem .8rem; border-radius:8px; margin:.2rem .4rem .2rem 0; }}
  .muted {{ color:var(--muted); }} .links {{ font-size:.88rem; }}
  footer {{ max-width:880px; margin:0 auto; padding:1rem 1.5rem 3rem; color:var(--muted); font-size:.85rem; }}
  footer code {{ background:#eef2f4; padding:.1rem .35rem; border-radius:4px; }}
</style></head>
<body>
<header><div class="wrap">
  <h1>{esc(cat.get('title','Portolan catalog'))}</h1>
  <p>{esc(cat.get('description',''))}</p>
  <div class="badges">
    <span class="badge">STAC</span><span class="badge">OGC API - Records</span>
    <span class="badge">Apache Iceberg</span><span class="badge">GeoParquet</span>
    <span class="badge">DuckDB / Snowflake</span>
  </div>
</div></header>
<main>{''.join(cards)}</main>
<footer>
  Machine-readable views of this catalogue:
  <a href="{PUBLIC_BASE}/catalog.json">STAC</a> ·
  <a href="{PUBLIC_BASE}/records/catalog.json">OGC API - Records</a> ·
  Iceberg REST endpoint <code>{PUBLIC_BASE}</code>. Generated from the STAC metadata; served as static files.
</footer>
</body></html>"""

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False) as f:
        f.write(page)
        tmp = f.name
    subprocess.run(["mc", "cp", "--attr", "Content-Type=text/html",
                    tmp, f"{MC_TARGET}/index.html"], check=True)
    os.unlink(tmp)
    print("  put index.html (human explorer)")
    print("  ATTACH 'cat' (TYPE iceberg, ENDPOINT '%s', AUTHORIZATION_TYPE 'none');" % PUBLIC_BASE)


if __name__ == "__main__":
    main()
