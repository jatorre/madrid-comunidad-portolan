# Comunidad de Madrid (IDEM + datos abiertos) — source investigation (2026-06-05)

Investigation of the **regional** open geodata of the Comunidad de Madrid to scope a Portolan catalog,
sibling to `madrid-city-portolan` (the *municipal* catalog). What's published, how to pull it, and how to
convert it to cloud-native formats (GeoParquet / COG), keeping native **EPSG:25830**.

> **Relationship to the City catalog.** The IDEM cartography *search portal* (GeoNetwork) heavily **harvests
> the City of Madrid** (records pointing at `geoportal.madrid.es`, `servpub.madrid.es`, `sigma.madrid.es`).
> Those are deduped OUT here — they belong to `madrid-city-portolan`. This catalog publishes the **distinct
> regional assets**: the Comunidad's own GeoServer (`geoidem`) and the regional CKAN open-data portal.

## 1. Sources (regional, distinct from the City)

| Surface | Endpoint | What it gives | Auth |
|---|---|---|---|
| **WFS (vector)** | `https://idem.comunidad.madrid/geoidem/ows?service=WFS` (GeoServer 2.x) | **248 feature types** across 21 workspaces; **18.17M features** total | none |
| **WCS (raster)** | `https://idem.comunidad.madrid/geoidem/ows?service=WCS` | **98 coverages** (hazard/risk/vulnerability grids + PV capacity), 5 m, EPSG:25830, Byte | none |
| **WMS** | `https://idem.comunidad.madrid/geoidem/ows?service=WMS` | 392 layers (styled superset of WFS+WCS) — render only | none |
| **CSW (catalog)** | `https://idem.comunidad.madrid/catalogocartografia/srv/spa/csw` (GeoNetwork) | "Catálogo de la IDE de la Comunidad de Madrid"; ~1,380 records, **mostly City harvests → deduped** | none |
| **CKAN (open data)** | `https://datos.comunidad.madrid/api/3/action/` (CKAN 2.9.0) | **2,282 datasets**; 2,236 with CSV+JSON (tabular, mostly statistics), 5 SHP (geospatial) | none |

**CRS:** uniformly **ETRS89 / UTM 30N (EPSG:25830)** for geoidem (246/248 WFS layers; the 2 exceptions are
EPSG:4258 geographic). WCS coverages all EPSG:25830 @ 5 m. **License:** CC-BY (CKAN states *Creative Commons
Attribution*; IDEM follows the regional / `datos.gob.es` reuse notice — attribution to *Comunidad de Madrid*).

## 2. WFS — 248 vector feature types (the heart of this catalog)

All EPSG:25830 (2 in EPSG:4258). **18,167,643 features** total. Workspaces:

| Workspace | # layers | Theme |
|---|---:|---|
| ZonasRiesgo | 48 | Civil-protection **hazard** layers (`PELIGROSID_*`): floods, fire, seismic, landslide, transport, industrial, weather… |
| UsoDelSuelo | 39 | Land use & **urban planning** (SIOSE, PAC crops, vegetation maps, classification/zoning `VPLA_*`, parcels) |
| Zonas | 38 | Protected/managed **zones**: hunting & fishing, public forests, vías pecuarias, Guadarrama NP zoning, nitrate-vulnerable… |
| ServiciosPublicos | 36 | **Public services & facilities** (EIEL_23 municipal infrastructure inventory, WWTPs, water plants, recycling, visitor centres) |
| BTA | 20 | **Base topography 1:?** (BTA 2017): buildings, roads, hydrography, relief, land cover, names — *high feature counts* |
| RedesTransporte | 10 | Transport networks (rail/road BTA 2011, urban streets, Camino de Santiago, green paths) |
| UnidadesAdministrativas | 9 | Admin boundaries: limits, municipalities, census districts/sections, population entities |
| LugaresProtegidos | 9 | Protected sites: ENP, Natura 2000 (LIC/ZEC/ZEPA), RAMSAR, biosphere reserves, national/regional parks |
| InstalacionesMedioAmbiente | 8 | Environmental installations: air-quality stations & zones, GHG & pollutant inventories (2018/22/23) |
| Hidrografia | 5 | Hydrography (BTA 2011 lines/polys/points, groundwater bodies, basins) |
| Elevaciones | 4 | Elevation **vectors**: contour lines (master + 20 m), BTA relief |
| CubiertaTerrestre, Edificios, RegionesBiogeograficas | 3 each | Land cover (BTA 2011), buildings (BTA 2011), physiography/bioclimatic belts |
| Direcciones, Suelo, Habitats, Geologia, Comun, NombresGeograficos | 2 each | Postal addresses & paseillos; soils (FAO / Soil Taxonomy); ecosystems & habitats (92/43/CEE); lithology; toponyms & municipalities; geographic names |
| SistemasCuadriculas | 1 | 1:10,000 sheet grid |

**Feature-count profile** (drives the conversion tiering):

| Bucket | # layers | Notes |
|---|---:|---|
| ≤ 1k | 141 | small thematic layers — single-shot GeoJSON fetch |
| 1k–10k | 44 | single-shot |
| 10k–100k | 36 | single-shot |
| 100k–1M | 23 | paginated fetch |
| > 1M | 4 | **base-topo giants** — `BTA_EDIPOBCONS_LIN_17` (3.13M), `BTA_10M_EDIPOBCONS_LIN_11` (2.99M), `BTA_REDVIARIA_LIN_17` (1.52M), `BTA_10M_REDVIARIA_LIN_11` (1.35M) — paginate or defer |

Full machine-readable list (name, title, CRS, WGS84 bbox, feature count): `tools/inventory/geoidem_wfs_featuretypes.json`.

## 3. WCS — 98 raster coverages (regional hazard/risk archive)

GeoServer **WCS 2.0.1** at the same `geoidem/ows`. 98 coverages, almost all **5 m, Byte (categorical), EPSG:25830**:
- **48** `riesgo_*` (risk) + **~44** `vulnera_*` (vulnerability) civil-protection grids (one per hazard type:
  floods, forest fire, seismic, landslide, transport, industrial, weather, supplies…),
- `peligrosid_3_1` (forest-fire hazard), `Vulnerabilidad_Territorial_IIFF_R`, `BOPC_ZONIF_RASTER` (fire-risk zoning),
- `RecursosEnergeticos__Indice_Capacidad_Acogida_FTV` (photovoltaic siting capacity).

Full list: `tools/inventory/geoidem_wcs_coverages.json`.

**COG conversion is proven and cheap.** Region-wide grids are ~26k×28k px but compress extremely well:
- **Download:** WCS GetCoverage with server-side compression — `…&format=image/tiff&geotiff:compression=DEFLATE&geotiff:tiling=true` cuts a single coverage from **748 MB → ~7 MB**.
- **COG:** `gdal_translate in.tif out.tif -of COG -co COMPRESS=DEFLATE -co PREDICTOR=2 -co OVERVIEWS=AUTO`
  → ~**19 MB** COG with overviews, in ~11 s. All 98 ≈ ~0.7 GB downloads, ~2 GB COGs. Fully feasible.

## 4. CKAN open-data portal — 2,282 datasets (mostly tabular)

`datos.comunidad.madrid` is **CKAN 2.9.0**. Standard API works: `/api/3/action/package_search`, `package_show`.
Direct resource downloads, **CC-BY**. Format facets:

| Format | # datasets |
|---|---:|
| CSV | 2,236 |
| JSON | 2,236 |
| HTML | 48 |
| ZIP | 33 |
| PDF | 11 |
| DOCX | 7 |
| **SHP** | **5** |

Publishers: **Instituto de Estadística** (2,108) + Comunidad de Madrid (174). Groups: Salud 297, Economía 268,
Educación 214, Sociedad y bienestar 207, Empleo 190, Demografía 150, … (22 themes).

→ **Overwhelmingly non-geospatial statistical tables** (CSV). These convert trivially to **plain parquet**
(`tab.*`, `portolan:geospatial:false`) via DuckDB `read_csv_auto`. The **5 SHP** datasets are geospatial
(COVID-19 TIA by health zone / municipality+district, census sections, 60+ population TIA) → GeoParquet.

Resource URL shape: `https://datos.comunidad.madrid/dataset/<pkg-uuid>/resource/<res-uuid>/download/<file>`.

## 5. Conversion pipeline (proven locally) — same principles as the City

Toolchain present: **GDAL 3.12.2, DuckDB 1.5.3, gpio 1.1.1, mc, gsutil, portolan-cli 0.7.0**. CRS policy: **keep
native EPSG:25830** (verified: `gpio convert` preserves `GEOMETRY('EPSG:25830')`; DuckDB distance queries run in
metres without transform).

**Vector (WFS) → GeoParquet** — *the City used SHP-ZIP → `gpio convert`; the region uses WFS-GeoJSON → `gpio convert`*:
```bash
# GeoServer's gpio/ogr2ogr WFS streaming is flaky here; the reliable primitive is direct GeoJSON:
curl "…/geoidem/ows?service=WFS&version=2.0.0&request=GetFeature&typeNames=<TN>&outputFormat=application/json&srsName=EPSG:25830" -o layer.geojson
gpio convert layer.geojson out.parquet      # → standardized geom + bbox, native CRS, ZSTD, Hilbert, GeoParquet-1.1 validated
# large layers (>~100k): page with &count=50000&startIndex=N and concatenate.
```

**Raster (WCS) → COG** — §3 above.

**Tabular (CKAN CSV) → parquet**:
```bash
duckdb -c "COPY (SELECT * FROM read_csv_auto('<csv-url>', sample_size=-1)) TO 'out.parquet' (FORMAT parquet);"
```

## 6. Mapping to the Portolan catalog (multi-representation, like the City)

Published to **`gs://carto-portolan-madrid/comunidad-madrid/`** (one bucket, prefix per catalog — the City is
`…/madrid-city/`; bucket is `europe-southwest1`/Madrid and **already public**, `allUsers:objectViewer`).
Endpoint: **`https://storage.googleapis.com/carto-portolan-madrid/comunidad-madrid`**.

Per dataset, the canonical Portolan read paths (all anonymous, static, no server):
- **Iceberg v3** (`data/v3/<id>`, native geometry EPSG:25830) → `ATTACH` / `iceberg_scan`,
- **remote GeoParquet** (`data/parquet/<id>.parquet`) → `read_parquet` in place,
- **STAC `catalog.json` + per-dataset `collection.json`** (git-tracked) with the **git-backed-catalog** and
  **STAC-Iceberg** extensions,
- **`catalog.datasets`** stac-geoparquet index + static **Iceberg-REST** surface (`v1/…`, prefix `sdi`).
- **Rasters** → COG asset on the bucket (`data/cog/<id>.tif`, `image/tiff; application=geotiff` cloud-optimized).

`data_provider` = **Comunidad de Madrid**; `data_license` = **CC-BY-4.0** (attribution to Comunidad de Madrid).

## 7. Scope & status (this build)

Priority order (highest-value distinct regional geodata first):
1. **248 WFS vector layers → GeoParquet + Iceberg v3 + remote parquet** — the core regional cartography.
2. **98 WCS raster coverages → COG** — the regional hazard/risk/vulnerability archive.
3. **5 CKAN SHP datasets → GeoParquet** (health/COVID geospatial).
4. **CKAN tabular (CSV → parquet, `tab.*`)** — 2,236 statistical tables; batch-ingested + scripted for the tail.

Build is **incremental**: the bucket + catalog go live early and grow as datasets are converted. Large WFS
layers (>1M) and the full 2,282-table CKAN tail are processed last / scripted, and any skips are reported.
