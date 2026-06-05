#!/usr/bin/env python3
"""Validate the catalog definition before it is published.

Runs in CI on every pull request (no credentials needed — reads the repo + does
anonymous HTTP HEADs against the public bucket). Fails the build on any problem, so
"you can PR into it" is trustworthy: a bad PR can't reach the bucket.

Checks:
  1. Every JSON file parses.
  2. Iceberg metadata (data/**/metadata/v1.metadata.json): required keys present, and every
     absolute URL it contains points at this catalog's PUBLIC_BASE (catches stale/mis-relocated
     base URLs — the exact bug class that breaks ATTACH).
  3. Iceberg manifests (*.avro): every data-file URL points at PUBLIC_BASE AND the parquet
     actually exists on the bucket (HEAD 200) — enforces "upload data, then PR the metadata".
  4. STAC catalog.json + items: required fields; bbox is 4 numbers; each Iceberg asset's
     referenced metadata.json exists locally and its iceberg_endpoint == PUBLIC_BASE.
  5. OGC API - Records records: structural record-core fields, and the portolan:* properties
     validate against the building-block schema (bblock/portolan-record/schema.json).

Env: PUBLIC_BASE — the public HTTPS base of this catalog on the bucket.
"""
import json, os, sys, glob, io, urllib.request, urllib.error
import fastavro
import jsonschema

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)
try:
    _C = json.load(open("portolan.config.json"))
except FileNotFoundError:
    _C = {}
PUBLIC_BASE = (os.environ.get("PUBLIC_BASE") or _C.get("public_base", "")).rstrip("/")
errors, checks = [], 0


def err(msg):
    errors.append(msg)


def head_ok(url):
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        return e.code == 200
    except Exception:
        return False


def urls_in(obj):
    """Yield every string value that looks like an absolute URL."""
    if isinstance(obj, str):
        if obj.startswith("http://") or obj.startswith("https://"):
            yield obj
    elif isinstance(obj, list):
        for x in obj:
            yield from urls_in(x)
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from urls_in(v)


def load_json(path):
    global checks
    checks += 1
    try:
        return json.load(open(path))
    except Exception as e:
        err(f"{path}: invalid JSON — {e}")
        return None


# load the building-block schema (#4) used to validate portolan:* properties
EXT_SCHEMA = load_json("bblock/portolan-record/schema.json") or {}


def check_portolan(where, obj):
    global checks
    checks += 1
    try:
        jsonschema.validate(obj, EXT_SCHEMA)
    except jsonschema.ValidationError as e:
        err(f"{where}: portolan extension invalid — {e.message}")


# --- 1 & 2: every JSON parses; Iceberg metadata is consistent -----------------
for p in glob.glob("**/*.json", recursive=True):
    load_json(p)

for mp in glob.glob("data/*/*/metadata/v1.metadata.json"):
    m = load_json(mp)
    if not m:
        continue
    for k in ("format-version", "location", "schemas", "snapshots", "current-snapshot-id"):
        if k not in m:
            err(f"{mp}: Iceberg metadata missing '{k}'")
    for u in urls_in(m):
        if not u.startswith(PUBLIC_BASE):
            err(f"{mp}: URL does not point at PUBLIC_BASE → {u}")

# --- 3: manifests reference data on the bucket --------------------------------
for ap in glob.glob("data/*/*/metadata/*.avro"):
    checks += 1
    try:
        recs = list(fastavro.reader(open(ap, "rb")))
    except Exception as e:
        err(f"{ap}: unreadable avro — {e}")
        continue
    for u in urls_in(recs):
        if not u.startswith(PUBLIC_BASE):
            err(f"{ap}: URL does not point at PUBLIC_BASE → {u}")
        if u.endswith(".parquet") and not head_ok(u):
            err(f"{ap}: referenced data not on bucket (HEAD != 200) → {u} "
                f"(upload the parquet before PRing the metadata)")

# --- 4: STAC ------------------------------------------------------------------
cat = load_json("catalog.json")
if cat:
    for k in ("type", "id", "description", "links"):
        if k not in cat:
            err(f"catalog.json: missing '{k}'")

for ip in glob.glob("items/*.json"):
    it = load_json(ip)
    if not it:
        continue
    for k in ("type", "id", "geometry", "bbox", "properties", "assets", "links"):
        if k not in it:
            err(f"{ip}: STAC item missing '{k}'")
    if isinstance(it.get("bbox"), list) and len(it["bbox"]) != 4:
        err(f"{ip}: bbox must have 4 numbers, got {len(it['bbox'])}")
    check_portolan(f"{ip} properties", it.get("properties", {}))
    for name, asset in (it.get("assets") or {}).items():
        check_portolan(f"{ip} assets.{name}", asset)
        ep = asset.get("portolan:iceberg_endpoint")
        if ep and ep != PUBLIC_BASE:
            err(f"{ip} assets.{name}: iceberg_endpoint {ep} != PUBLIC_BASE {PUBLIC_BASE}")
        href = asset.get("href", "")
        # iceberg assets point at metadata.json that must be in git; raquet/remote assets point
        # at data bytes on the bucket (not in git) — only check the former locally.
        if href.startswith(PUBLIC_BASE) and href.endswith(".metadata.json"):
            local = href[len(PUBLIC_BASE):].lstrip("/")
            if not os.path.exists(local):
                err(f"{ip} assets.{name}: referenced metadata not in repo → {local}")

# --- 5: OGC API - Records -----------------------------------------------------
for rp in glob.glob("records/*.json"):
    r = load_json(rp)
    if not r:
        continue
    if r.get("type") == "Catalog":
        for k in ("id", "conformsTo", "links"):
            if k not in r:
                err(f"{rp}: Records catalogue missing '{k}'")
    else:
        for k in ("type", "id", "conformsTo", "properties"):
            if k not in r:
                err(f"{rp}: Records record missing '{k}'")
        if r.get("type") != "Feature":
            err(f"{rp}: Records record type must be 'Feature'")
        if "type" not in (r.get("properties") or {}):
            err(f"{rp}: Records record missing properties.type")
        check_portolan(f"{rp} properties", r.get("properties", {}))

# --- report -------------------------------------------------------------------
if errors:
    print(f"❌ validation FAILED — {len(errors)} problem(s) across {checks} checks:\n")
    for e in errors:
        print("  •", e)
    sys.exit(1)
print(f"✅ validation passed — {checks} checks, 0 problems.")
