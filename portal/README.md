# Portal — visor estático de los catálogos Portolan v3

Static site (HTML/CSS/JS, sin servidor): lista de datasets por catálogo + página de detalle con
mapa (MapLibre), tabla y snippets DuckDB/Snowflake. Lee los parquet v3 en el navegador con DuckDB-WASM.

- **Desplegado:** https://storage.googleapis.com/carto-portolan-madrid/portal/index.html
- Índices `*.index.json` + `catalogs.json` generados desde los ledgers de conversión (vector/tabla/ráster).
- Deploy: `gcloud storage cp portal/* gs://carto-portolan-madrid/portal/` (mismo bucket que los datos → sin CORS).
