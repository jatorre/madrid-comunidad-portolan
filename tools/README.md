# tools/ — the reproducible build pipeline

These scripts build the Comunidad de Madrid Portolan catalog end-to-end from the live IDEM / CKAN
sources. They are environment-specific (paths under `/tmp`, the GCS bucket, and the
`iceberg-geo-testbed` venv for the Iceberg writer) and are committed as the **reproducible definition**
of how the published bytes were produced — not as a turnkey CI job. See `../COMUNIDAD_INVENTORY.md`.

| Script | Source → output |
|---|---|
| `convert_wfs.py` | geoidem **WFS** feature types → GeoParquet (native EPSG:25830). Single streaming GetFeature (the server rejects `startIndex`), then `gpio convert`. Resumable. `python3 convert_wfs.py <min_count> <max_count> [workers]` |
| `convert_wcs.py` | geoidem **WCS** coverages → **COG**. WCS GetCoverage with server-side DEFLATE (748 MB → ~7 MB), then `gdal_translate -of COG`. Resumable. `python3 convert_wcs.py [workers]` |
| `convert_ckan.py` | **CKAN** open data. `csv` → plain parquet (all_varchar, latin-1, faithful); `shp` → GeoParquet. `python3 convert_ckan.py {csv\|shp} [workers] [limit]` |
| `build_catalog.py` | All converted parquet/COG → **v3 Iceberg** (vector) + **tab Iceberg** (tabular) + remote parquet + **stac-geoparquet index** + static **Iceberg-REST surface**. Run with the testbed venv. |
| `upload.py` | `gcloud storage rsync` the `data/` tree + per-file upload of the extensionless `v1/` REST surface to the public GCS bucket. |
| `inventory/` | machine-readable scan: `geoidem_wfs_featuretypes.json` (name, title, CRS, bbox, feature count), `geoidem_wcs_coverages.json`. |

**Pipeline order:** `convert_wfs.py` + `convert_wcs.py` + `convert_ckan.py` (populate `/tmp/comu_conv/`
manifests) → `build_catalog.py` (stage `/tmp/comu_catalog/`) → `upload.py` (publish). The catalog is
incremental: re-running build+upload republishes the index + surface over whatever has been converted.
