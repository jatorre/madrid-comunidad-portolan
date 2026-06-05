# Portolan — Comunidad de Madrid (regional)

A **Portolan** spatial-data catalog for the **Comunidad de Madrid**: a git repo that is the *source* of a
catalog and publishes itself to object storage as a **static Apache Iceberg REST catalog** (`ATTACH` from
DuckDB / Snowflake) + **STAC** + **remote GeoParquet** + **Cloud-Optimized GeoTIFF** + a human HTML explorer —
**no server**. Git holds only the *definition* (config + STAC + small Iceberg metadata); the data bytes live on
the bucket.

Sibling of [`madrid-city-portolan`](https://github.com/jatorre/madrid-city-portolan) (the *municipal* catalog).
This one covers the **region** and is built from the Comunidad's own infrastructure (not City harvests).

## Endpoint

```
https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid
```

```sql
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;
ATTACH 'cm' (TYPE iceberg,
             ENDPOINT 'https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid',
             AUTHORIZATION_TYPE 'none');
SHOW ALL TABLES;                          -- v3.* (vector, native EPSG:25830), tab.* (tabular), catalog.datasets
SELECT id, json_extract_string(properties,'$.theme') FROM cm.catalog.datasets;
```

Direct reads (no ATTACH):
```sql
-- vector GeoParquet, native EPSG:25830 (distances in metres):
SELECT count(*) FROM read_parquet('https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid/data/parquet/comun_municipios.parquet');
```
Rasters: `/vsicurl/https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid/data/cog/<id>.tif` (COG).

## Sources

- **IDEM GeoServer `geoidem`** — WFS (248 vector feature types) + WCS (98 raster coverages), EPSG:25830.
- **`datos.comunidad.madrid`** — CKAN open-data portal (2,282 datasets; tabular statistics + a few geospatial).

See **`COMUNIDAD_INVENTORY.md`** for the full investigation, counts, dedup-vs-City notes, and conversion recipe,
and **`AGENTS.md`** for how to read/contribute.

## What's published

**2,578 datasets** (249 vector + 97 raster COGs + 2,232 tabular + the index), ~8.3 GB, native EPSG:25830,
CC-BY-4.0. Published as a static **Iceberg-REST surface** (`v1/…`) + `data/` trees — the same shape as the
City catalog (no server, no top-level `catalog.json`/HTML). Discovery is the `catalog.datasets` stac-geoparquet
index. See `COMUNIDAD_INVENTORY.md` §7 for the full breakdown and the handful of documented skips.

## Layout (git repo = definition)

```
portolan.config.json          publisher + bucket config (the only per-repo config)
COMUNIDAD_INVENTORY.md         source investigation + conversion recipe + published status
tools/                         the reproducible build pipeline (see tools/README.md) + inventory references
.github/workflows/validate.yml lints JSON + checks the live endpoint (no credentials)
                              # data bytes (parquet / COG) live ON THE BUCKET ONLY (git-ignored)
```

## Principles (don't regress)

- **Git = definition; bucket = data + generated artifacts.** Never commit parquet/COG.
- **Anonymous + static + open.** Authenticated/private data is out of scope.
- **Native CRS** EPSG:25830 (honest to source). **Standards:** STAC, GeoParquet, Apache Iceberg, COG.
- **License:** CC-BY-4.0 — attribute *Comunidad de Madrid*.
