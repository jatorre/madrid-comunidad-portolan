# Best practices — catálogos geoespaciales cloud-native dual-engine (Portolan v3)

> **Estándar de procesado.** A partir de ahora, los catálogos Portolan se publican en este formato:
> un **único juego de ficheros** en object storage público que se consulta **nativamente** tanto desde
> **DuckDB** como desde **Snowflake**, con poda espacial en ambos. Validado de punta a punta con el
> **Catastro español** (`catastro-es-portolan`, ~80 M features, 3 temas × 52 gerencias).

---

## 1. Objetivo

Un solo dato, dos motores, cada uno con su **poda espacial nativa**, **sin duplicar almacenamiento**:

| | DuckDB (`read_parquet`) | Snowflake (Iceberg externo) |
|---|---|---|
| geometría | `GEOMETRY` nativo | `GEOMETRY(4326)` nativo |
| poda espacial | por columnas **`bbox`** (stats de row-group) | por **`ST_INTERSECTS(geom)`** nativo (bounds del manifest) |
| medido (caja ~2 km / 12,5 M edificios) | poda row-groups | **1 / 52 micro-particiones** |

Los ficheros de datos de una tabla Iceberg **son GeoParquet normal**; el metadata Iceberg es una *capa*
encima. Por eso el mismo byte sirve para `read_parquet` directo (DuckDB/Portolan) y para `ATTACH`/Snowflake.

---

## 2. El formato canónico

```
gs://<bucket>/v3/<dataset>/
  data/<particion>=<NN>.parquet      ← GeoParquet 2.0 (un fichero por partición)
  metadata/v1.metadata.json + *.avro ← Iceberg v3 (manifest con bounds por columna)
```

Cada parquet contiene, **en este orden de columnas**:
1. **Atributos** (tipados, con nombre limpio y descripción).
2. **`geom`** — tipo lógico **`Geometry(crs=srid:4326)` nativo de Parquet** (GeoParquet 2.0, vía
   `geoarrow.pyarrow.wkb().with_crs("srid:4326")`).
3. **`xmin, ymin, xmax, ymax`** (DOUBLE) — bounding box por fila, **al final** (después de geom).

El esquema Iceberg declara **atributos + geom**, y **oculta las 4 columnas bbox** (siguen en el parquet).

---

## 3. Las reglas duras (checklist) — y por qué

Cada una se descubrió **midiendo fallos reales**. Saltarse cualquiera rompe la poda nativa en Snowflake.

| # | Regla | Si no… |
|---|---|---|
| 1 | **`geom` = tipo lógico Geometry nativo de Parquet** (no WKB binario "a pelo" ni solo GeoParquet 1.1) | Snowflake/DuckDB no lo leen como geometría sin cast |
| 2 | **CRS del tipo Parquet = exactamente `srid:4326`** (no vacío, no `EPSG:4326`) | Snowflake: `Failed to cast variant value … to REAL` |
| 3 | **`geom` con field-id CONTIGUO, sin huecos** → escribe `geom` **antes** que `bbox` | Snowflake: error interno `300010` en la poda |
| 4 | **Manifest: `lower`/`upper` bounds + `value_counts` + `null_value_counts` en TODAS las columnas del esquema** (placeholder en columnas all-null) | Snowflake: `300010`. *El bound de geom va en encoding `packed_xy_le` (16 bytes: X LE, Y LE).* |
| 5 | **Columnas `bbox` en el parquet pero OCULTAS del esquema Iceberg** | columnas DOUBLE extra en el esquema rompen la poda de geom de Snowflake; y DuckDB necesita el bbox en el parquet |
| 6 | **Descripción + `field_id` por columna** (parquet `description` + Iceberg `doc`) | sin diccionario de datos (Snowflake lo muestra como comentario de columna en `DESCRIBE TABLE`) |
| 7 | **Particionar** por una clave natural (provincia, distrito…), **un fichero por partición**, filas ordenadas espacialmente (Hilbert) | menos poda; ficheros gigantes |
| 8 | **Bucket público** (`allUsers:objectViewer`), lectura anónima | no se puede leer sin credenciales |

> **Encoding de bounds Iceberg por tipo:** string → UTF-8 (truncado ~60 bytes); int → 4-byte little-endian;
> double → 8-byte little-endian; geom → `packed_xy_le` (X e Y como doubles LE, 16 bytes). Para columnas
> totalmente nulas, emite un placeholder (p.ej. string `""`/`"~"`, int `0`) — Snowflake aborta si falta el bound.

### Requisitos de spec V3 que Snowflake exige al lector externo
Además: el snapshot debe llevar `first-row-id` y `added-rows`; el manifest avro debe ser V3 real. El
escritor de referencia (`iceberg-geo-testbed/testbed/_static_catalog.py` + `v3_geometry.py`) ya los emite —
sus bytes de bounds coinciden **byte a byte** con los que escribe el propio Snowflake managed.

---

## 4. Recetas de consulta

**DuckDB** — lectura directa, poda por `bbox`:
```sql
INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;
CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',
  ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');
SELECT count(*) FROM read_parquet('s3://<bucket>/v3/<ds>/data/*.parquet', hive_partitioning=1)
WHERE provincia='28' AND xmin BETWEEN -3.71 AND -3.69 AND ymin BETWEEN 40.41 AND 40.43;
-- refina con ST_Intersects(geom, …) tras el prefiltro bbox
```

**Snowflake** — tabla Iceberg externa, poda **nativa por geom** (sin bbox):
```sql
CREATE OR REPLACE ICEBERG TABLE <ds> EXTERNAL_VOLUME='<vol_misma_region>'
  CATALOG='<object_store_catalog>' METADATA_FILE_PATH='v3/<ds>/metadata/v1.metadata.json';
SELECT count(*) FROM <ds>
WHERE ST_INTERSECTS(geom, ST_GEOMFROMWKT('POLYGON((…))', 4326));   -- usa SRID 4326 explícito
```

> **Región Snowflake:** el external volume debe estar en la **misma región** que la cuenta. Si el bucket
> público está en otra región (p.ej. `europe-southwest1`) y la cuenta en otra, **espeja `v3/`** a un bucket
> de la región de la cuenta y registra la tabla ahí (el dato público sigue intacto para DuckDB).

---

## 5. Estado de la poda espacial por motor (medido, jun 2026)

- **Snowflake**: poda `ST_INTERSECTS(geom)` nativo sobre Iceberg v3 externo ✅ (1/52 particiones).
- **DuckDB**: poda por predicado `bbox` ✅; **NO** por `ST_Intersects(geom)` todavía
  (`duckdb-iceberg` aún no deserializa los bounds de geom del manifest — issues
  [#1002](https://github.com/duckdb/duckdb-iceberg/issues/1002) / #1013). Por eso llevamos `bbox`.

Cuando DuckDB cierre esa brecha, el `bbox` pasa a ser opcional; hasta entonces es la vía robusta en DuckDB
**y** un buen prefiltro barato en cualquier motor.

---

## 6. Pipeline de referencia

En `catastro-es-portolan/tools/` (reutilizable):
- `cat_v3_build.py` — re-encoda por partición: lee el GeoParquet origen, `geom`→tipo nativo `srid:4326`,
  aplana bbox, inyecta `field_id`+descripción, calcula bounds completos por columna, sube. Resumable.
- `cat_v3_meta.py` — escribe el metadata Iceberg v3 (oculta bbox del esquema, bounds en todas las
  columnas del esquema, `doc` por columna).
- `cat_v3_grind.sh` — driver por temas/particiones.

---

## 7. Migración del resto de Portolans  →  TODO

Ahora que el formato está validado, **re-procesar a v3 unificado** los catálogos existentes (hoy en
GeoParquet 1.1 / Iceberg fase-1, que **no** dan poda nativa en Snowflake):

- [ ] **`catastro-es-portolan`** — ✅ hecho (referencia).
- [ ] **`madrid-comunidad-portolan`** (Comunidad de Madrid, IDEM + datos abiertos) — convertir.
- [ ] **`madrid-city-portolan`** (Ayuntamiento de Madrid) — convertir.
- [ ] **`madrid-opendata`** — convertir.
- [ ] Resto de fuentes que se vayan añadiendo — nacen ya en v3.

Por catálogo: aplicar el checklist (§3), reusar el pipeline (§6) adaptando el esquema/descripciones por
dataset, publicar bajo `…/v3/<dataset>/`, y validar en DuckDB (bbox) + Snowflake (geom nativo) antes de dar
por bueno.
