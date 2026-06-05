# Portolan spec migration — recipe (follow exactly)

Migrate ONE publisher's catalog **in place** (its existing bucket prefix + GitHub repo) to a
spec-compliant, **git-backed** Portolan catalog, WITHOUT breaking the live `ATTACH` endpoint.

**Reference already done — mirror it:** Statistics Finland.
- Live: `https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/repo/portolan-statfi-catalog`
  (study `catalog.json` and `paavo_vaesto/collection.json`).
- Local build: `/tmp/statfi-ref` (study `catalog.json`, `paavo_vaesto/collection.json`).

**Environment:** bash, git, gh (authed jatorre), mc (alias `upcloud`), duckdb, python3. Portolan CLI:
`source /tmp/pcli/bin/activate` → `portolan` 0.7.0.

**SAFETY (critical):**
- ADD/overwrite ONLY catalog files under YOUR prefix. **NEVER delete the existing `v1/` (Iceberg REST)
  or `data/` (Iceberg tables)** on the bucket — that's the live `ATTACH` layer (#5) and must keep working.
- Touch only YOUR repo + YOUR bucket prefix. Do NOT touch the federation root or other publishers.
- If you can't cleanly migrate + verify a dataset, leave its existing bucket files intact and REPORT it.
  Never push a broken catalog.

Let `P=https://8et4c.upcloudobjects.com/carto-ogc-connect-helsinki/repo/portolan-<x>-catalog`
and `T=upcloud/carto-ogc-connect-helsinki/repo/portolan-<x>-catalog`.

## Steps

1. **Discover datasets** from the live index (source of truth for ids/metadata/type):
   ```
   duckdb -unsigned -json -c "INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;
   ATTACH 'c'(TYPE iceberg,ENDPOINT '$P',AUTHORIZATION_TYPE 'none');
   SELECT id, bbox, properties, assets FROM c.catalog.datasets;"
   ```
   Each row: `id`, `bbox` {xmin,ymin,xmax,ymax}, `properties` (JSON: title, description, semantics, license, provider, keywords), `assets.data.href`. The **href tells the type**:
   - `v2.X` / `v3.X` → **vector** (tables `v2.X`,`v3.X`). Use the **v3** table for conversion.
   - `tab.X` → **non-geospatial** (table `tab.X`).
   - a `.parquet` URL under `/data/raster/` → **raster** (raquet).
   - `s3://...` (Overture) → **remote**.

2. **Build local catalog** `/tmp/mig-<x>`. For each dataset make a collection dir named by the dataset id.
   - **vector:** `duckdb -unsigned -c "INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial; COPY (SELECT * FROM iceberg_scan('$P/data/v3/<table>/metadata/v1.metadata.json')) TO '<id>/<id>.parquet' (FORMAT PARQUET);"`
   - **non-geo:** same COPY from the `tab.<table>` v1.metadata.json → `<id>/<id>.parquet` (plain parquet, no geometry).
   - **raster / remote:** no local file (asset points at the existing raquet URL / remote URL).

3. `source /tmp/pcli/bin/activate`; `cd /tmp/mig-<x>`; `portolan init --auto --title "<Publisher>" --description "..."`.
   `portolan add <each vector/non-geo collection dir>`; `printf '\n\n\n\n' | portolan check --metadata --fix`.
   For **raster** and **remote** collections the CLI won't ingest them — **hand-author** `<id>/collection.json`
   + `<id>/versions.json` mirroring the structure of `/tmp/statfi-ref/paavo_vaesto/`.

4. **Inject extensions** (mirror `/tmp/statfi-ref`; do this AFTER `check --fix`):
   - `catalog.json`: add to `stac_extensions` the git ext URL
     `https://portolan-sdi.github.io/git-backed-catalog/v1.0.0/schema.json`; set
     `git:repository` (the repo), `git:ref":"main"`, `git:provider":"github"`; add links
     `vcs` (repo), `issues` (repo+/issues), `monitor` (repo+/commits/main.atom).
   - each `collection.json`:
     - **vector/non-geo:** add iceberg ext `https://portolan-sdi.github.io/stac-iceberg-extension/v1.0.0/schema.json`;
       `iceberg:catalog_type":"rest"`, `iceberg:catalog_uri":"$P"`, `iceberg:table_id":"v3.<table>"` (or `tab.<table>`),
       `iceberg:current_snapshot_id` (read it from that table's `v1.metadata.json` `current-snapshot-id`);
       add an asset `"iceberg"` = `{href:"$P/data/v3/<table>" (or tab), type:"application/x-iceberg", roles:["data"]}`.
       Non-geo also: `"portolan:geospatial": false`.
     - **raster:** a `data` asset `{href:"<the raquet URL>", type:"application/x-raquet", roles:["data"], title:"Raquet raster — read_raquet()"}`.
     - **remote:** a `data` asset `{href:"<remote s3 URL>", type:"application/vnd.apache.parquet", roles:["data","external"], "portolan:managed": false}`.
     - all: `git:edit_url` = repo+`/edit/main/<id>/collection.json`; a `links` entry `{rel:"via", href:<source>, type:"text/html"}`.
   - **Asset hrefs MUST be absolute** (vectors/non-geo: `$P/<id>/<id>.parquet`).

5. **Upload (ADD only)** to the bucket — `mc cp` `catalog.json`, `versions.json`, and per collection
   `<id>/collection.json`, `<id>/versions.json`, and the `.parquet` (vectors/non-geo only). **Do NOT** `mc rm`
   or `mc mirror --remove`; never touch `v1/` or `data/`.

6. **Update the GitHub repo** `jatorre/portolan-<x>-catalog`: set its tracked content to the spec definition
   (`.portolan/`, `catalog.json`, `versions.json`, `<id>/collection.json` + `<id>/versions.json`), add a
   `.gitignore` with `*.parquet` (data lives on the bucket — model 3), keep/refresh `AGENTS.md`. Commit + push to `main`.

7. **VERIFY** (report the numbers):
   - `curl $P/catalog.json` → `git:repository` present, child count.
   - one `collection.json` → `iceberg:table_id` (or `portolan:geospatial:false`, or remote href).
   - `ATTACH 'c'(TYPE iceberg,ENDPOINT '$P',AUTHORIZATION_TYPE 'none')` → count a real `v3.*`/`tab.*` table (must still work — #5).
   - vector/non-geo: `read_parquet('$P/<id>/<id>.parquet')` count. raster: `curl -sI` the raquet href → 200. remote: href present.

8. **REPORT:** repo URL, `$P`, the collections + their types, verification numbers, and ANY problems.
