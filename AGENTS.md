# AGENTS.md — guide for AI agents

This repository **is a Portolan spatial-data catalog** for the **Comunidad de Madrid** (the *region* —
sibling to the *municipal* `madrid-city-portolan`). One publisher, defined as git-tracked metadata, served as
static files on object storage. No server, no API keys. Read `catalog.json` (or `portolan.config.json` →
`public_base`) for this catalog's endpoint and datasets.

**Endpoint:** `https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid`
**Source:** Comunidad de Madrid IDEM GeoServer (`geoidem`, WFS/WCS) + regional CKAN open data
(`datos.comunidad.madrid`). CRS: native **EPSG:25830**. License: **CC-BY-4.0** (attribution: *Comunidad de Madrid*).

## How to READ the data (no credentials)

Endpoint = `public_base` in `portolan.config.json`:

- **ATTACH (DuckDB / Snowflake):** `ATTACH 'cat' (TYPE iceberg, ENDPOINT '<public_base>', AUTHORIZATION_TYPE 'none');` then `SELECT * FROM cat.v3.<table>;`
- **Scan a table directly (DuckDB):** `iceberg_scan('<public_base>/data/v3/<id>/metadata/v1.metadata.json')`
- **Direct download:** GeoParquet at `<public_base>/data/parquet/<id>.parquet` (`read_parquet`).
- **Rasters:** Cloud-Optimized GeoTIFF at `<public_base>/data/cog/<id>.tif` (`/vsicurl/`, rasterio, GDAL).
- **Discover:** STAC `catalog.json` + per-dataset `<id>/collection.json`; `catalog.datasets` stac-geoparquet index; `index.html`.

Geometry is native `GEOMETRY(EPSG:25830)` in `v3.*` tables and in the remote GeoParquet (`geom` column + `bbox`).
DuckDB distance queries run in **metres** with no `ST_Transform`. Tabular (non-geo) datasets are `tab.*`
(`portolan:geospatial:false`).

## How to CONTRIBUTE

- **Fix / extend metadata** → PR editing `portolan.config.json`, `<id>/collection.json`, or the Iceberg metadata;
  run `python tools/generate_stac.py` + `python tools/validate.py` first. On merge a GitHub Action republishes.
- **Add / update data bytes** → upload the GeoParquet/COG to the bucket (`…/data/…`), then PR the matching
  metadata. A PR cannot carry the bytes — that is deliberate (git = definition; bucket = data).
- **Report a problem / request a dataset** → open an issue.

## Provenance & dedup

This catalog publishes the **distinct regional assets** of the Comunidad de Madrid. The IDEM GeoNetwork search
portal also **harvests the City of Madrid**; those municipal records are deduped OUT here and live in
`madrid-city-portolan`. See `COMUNIDAD_INVENTORY.md` for the full source investigation, counts, and conversion recipe.

## Conventions — what NOT to do

- Git holds the **definition**; the bucket holds **data** + generated artifacts. Never commit parquet/COG/TIFF.
- Don't hand-edit generated files (`catalog.json`, `items/`, `records/`, the REST tree, `index.html`) — change
  `<id>/collection.json` / config and regenerate.
- Query is the engine's native SQL. There is no custom query API.
- This is an **open, public, anonymous** catalog. Authentication/private data is out of scope.
