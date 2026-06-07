# Portolan v3 — Runbook de conversión (para una sesión nueva)

> **Cómo usar este doc:** en una sesión nueva, apunta al repo `jatorre/madrid-comunidad-portolan` y di:
> *"Convierte <fuente> a Portolan v3 siguiendo `PORTOLAN_V3_CONVERSION.md`."* Este runbook es autocontenido:
> contiene el formato exacto, las reglas duras (con el porqué de cada una), el convertidor reusable, las
> recetas de validación y un playbook específico para **Overture**. No hay que re-descubrir nada.

---

## 0. Objetivo
Un **único juego de ficheros** en object storage público, consultable **nativamente** desde **DuckDB**
(`read_parquet`, poda por `bbox`) y **Snowflake** (Iceberg externo, poda nativa por `ST_INTERSECTS(geom)`),
sin duplicar almacenamiento. Los data files de la tabla Iceberg v3 **son GeoParquet** legibles directos.

## 1. Formato de salida (spec exacta)
```
gs://<bucket>/v3/<dataset>/
  data/<dataset>.parquet (o particionado: <particion>=<NN>.parquet)   ← GeoParquet 2.0
  metadata/v1.metadata.json + snap-*.avro                              ← Iceberg v3
```
Orden de columnas en el parquet: **atributos → `geom` → `xmin,ymin,xmax,ymax`**.
- `geom`: tipo lógico Parquet **`Geometry(crs=srid:<EPSG>)` nativo** (geoarrow), en el **CRS nativo del dataset** (no reproyectar; reproyectar solo si hay que unir varias zonas en una tabla).
- `xmin/ymin/xmax/ymax`: DOUBLE, bounding box por fila, en unidades nativas. **En el parquet pero OCULTAS del esquema Iceberg.**
- Esquema Iceberg = atributos + `geom` (sin bbox). Tipo de geom en metadata = **`geometry(srid:<EPSG>)`**.

## 2. Reglas duras (checklist — y qué error previene cada una)
| # | Regla | Si no… |
|---|---|---|
| 1 | `geom` = tipo lógico Geometry nativo de Parquet (geoarrow `wkb().with_crs(...)`) | no se lee como geometría sin cast |
| 2 | CRS del tipo Parquet = `srid:<EPSG>` (no vacío, no `EPSG:4326`) | Snowflake: `Failed to cast variant value … to REAL` |
| 3 | Tipo Iceberg de geom = **`geometry(srid:<EPSG>)`** (no `geometry` a secas) | Snowflake asume SRID 4326 → datos no-4326 mal etiquetados, `Incompatible SRID` |
| 4 | `geom` con field-id **contiguo, sin huecos** → escribir `geom` ANTES que `bbox` | Snowflake aborta la poda: `300010` |
| 5 | `lower`/`upper` bounds + `value_counts` + `null_value_counts` en **TODAS** las columnas del esquema (placeholder en all-null) | `300010`. Bound de geom en `packed_xy_le` (16 bytes: X LE, Y LE) |
| 6 | Columnas `bbox` en el parquet pero **OCULTAS del esquema Iceberg** | columnas extra rompen la poda de geom de Snowflake; DuckDB sí las necesita en el parquet |
| 7 | **Nombres de columna en minúsculas** | Snowflake aborta la poda: `300010` |
| 8 | **Filtrar geometrías NULL/vacías** (`WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)`) | una fila sin geometría rompe la poda de Snowflake (`300010`) |
| 9 | Descripción + `field_id` por columna (parquet `description` + Iceberg `doc`) | sin diccionario (Snowflake los muestra como comentario en `DESCRIBE TABLE`) |
| 10 | Particionar por clave natural; filas ordenadas espacialmente (Hilbert); bucket público (`allUsers:objectViewer`) | menos poda; no legible anónimo |

**Encoding de bounds por tipo Iceberg:** string → UTF-8 truncado por carácter (válido); int → 4-byte LE;
long → 8-byte LE; float → 4-byte LE; double → 8-byte LE (NaN/Inf → placeholder); bool → 1 byte; geom → `packed_xy_le`.
**Requisitos spec V3 que Snowflake exige:** snapshot con `first-row-id`+`added-rows`, manifest avro V3 real
(el escritor `iceberg-geo-testbed/testbed/_static_catalog.py` ya los emite; bounds byte-a-byte iguales a Snowflake).

## 3. Convertidor reusable
`tools/portolan_v3.py PREFIX DATASET_ID` — implementa TODO lo anterior: detecta geom+CRS, NO reproyecta,
minúsculas, filtra nulas/vacías, calcula bbox + bounds por columna con guardas, escribe v3 (geom contiguo
+ bbox oculto + `geometry(srid:<EPSG>)`). `tools/portolan_v3_grind.sh PREFIX` lo recorre sobre un catálogo
(resumable). Depende del venv `iceberg-geo-testbed/.venv` (geoarrow, pyiceberg) y de
`testbed/_static_catalog.write_static_catalog`.

## 4. Validación (CRS-aware — usa el SRID del dataset)
**DuckDB** (secret S3 anónimo de GCS):
```sql
SELECT count(*) FROM read_parquet('s3://<bucket>/v3/<ds>/data/*.parquet', hive_partitioning=1)
WHERE xmin BETWEEN <x0> AND <x1> AND ymin BETWEEN <y0> AND <y1>;   -- coords en el CRS nativo
```
**Snowflake** (external volume EN LA REGIÓN de la cuenta; si el bucket está en otra región, espeja `v3/`):
```sql
CREATE OR REPLACE ICEBERG TABLE t EXTERNAL_VOLUME='<vol>' CATALOG='<object_store_cat>'
  METADATA_FILE_PATH='v3/<ds>/metadata/v1.metadata.json';
SELECT ST_SRID(geom) FROM t LIMIT 1;                       -- debe ser el EPSG nativo
SELECT count(*) FROM t WHERE ST_INTERSECTS(geom, ST_GEOMFROMWKT('POLYGON((...))', <EPSG>));
-- comprueba poda: GET_QUERY_OPERATOR_STATS(query_id) -> partitions_scanned << partitions_total
```

## 5. Playbook específico: Overture
**Fuente:** `s3://overturemaps-us-west-2/release/<YYYY-MM-DD.N>/theme=<theme>/type=<type>/*.parquet`
(público, región us-west-2, anónimo: `CREATE SECRET ov (TYPE s3, PROVIDER config, REGION 'us-west-2')`).
Temas/tipos: `buildings/building` (512 ficheros), `places/place`, `transportation/segment`, `addresses/address`,
`divisions/*`, `base/*`. **CRS = 4326** (lon/lat). Nombres ya en minúsculas. Ya trae **columna `bbox` (STRUCT
xmin/xmax/ymin/ymax)** y geometría WKB.

**Qué hacer (Overture ya cumple varias reglas; faltan pocas):**
1. **Acota por región** (Overture es de TB): filtra por `bbox` al leer, p.ej. España/Madrid
   (`WHERE bbox.xmin BETWEEN -10 AND 5 AND bbox.ymin BETWEEN 35 AND 44`). Un "dataset" v3 = un theme/type
   (o theme/type×región). Particiona si es grande.
2. **geom**: Overture la trae WKB; re-encódala al tipo nativo `Geometry(crs=srid:4326)` (geoarrow) — regla 1/2.
3. **bbox**: Overture la trae como STRUCT; **aplánala** a `xmin/ymin/xmax/ymax` DOUBLE (`bbox.xmin AS xmin`, …)
   y ponla al final, OCULTA del esquema Iceberg (regla 6).
4. **Tipo Iceberg de geom = `geometry(srid:4326)`** (regla 3).
5. Resto igual: bounds por columna en todo el esquema (regla 5), `geom` contiguo antes de bbox (regla 4),
   filtra geom nula/vacía (regla 8), descripciones (regla 9), minúsculas (ya vienen así, regla 7).
6. Sube a `gs://<tu-bucket>/v3/overture_<theme>_<type>[_<region>]/` (público).

**Adaptación del convertidor:** `portolan_v3.py` lee de `s3://<prefix>/data/parquet/<id>.parquet`. Para
Overture, parametriza la **SQL fuente** (glob de Overture + filtro bbox + `bbox.xmin AS xmin …`) en lugar de
ese path fijo; el resto (re-encode geom nativo, bounds, metadata, ocultar bbox) se reutiliza igual.

**Validación Overture:** misma receta §4 con SRID 4326. Verifica poda con una caja pequeña (debería
escanear pocas particiones).

## 6. Gotchas / errores → causa (todos vistos y resueltos)
- `Failed to cast variant value "<timestamp>" to REAL` (Snowflake) → CRS del tipo Parquet vacío (regla 2) o falta de bounds/metrics en alguna columna (regla 5).
- `300010 internal error` (Snowflake) → field-id de geom con hueco (regla 4) / falta bound en alguna columna del esquema (regla 5) / nombres MAYÚSCULAS (regla 7) / fila con geom NULL o vacía (regla 8).
- `Incompatible SRID: 4326 and <X>` → tipo Iceberg `geometry` a secas en datos no-4326; usa `geometry(srid:<X>)` (regla 3) y consulta con `ST_GEOMFROMWKT(wkt, <X>)`.
- `Query needs to be retried to setup external volume` → external volume nuevo; reintenta unos minutos; debe estar en la región de la cuenta Snowflake.
- DuckDB **no** poda por `ST_Intersects(geom)` aún (duckdb-iceberg #1002/#1013) → en DuckDB usa el predicado `bbox`.
- **NO era los DOUBLE** — Snowflake soporta columnas double; nunca fue eso.

> No-espaciales (tabular): mismo Iceberg v3 pero **sin** `geom`/`bbox` (solo columnas + bounds + minúsculas);
> aplican reglas 2-no, 3-no, 4/6/8-no; sí 5 (bounds por columna), 7 (minúsculas), 9 (descripciones). Rásters → COG (no Iceberg).
