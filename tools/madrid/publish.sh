#!/bin/bash
# Publish the staged Madrid catalog to GCS (data tree + remote parquet/rasters + IRC surface).
set -euo pipefail
DST="gs://carto-portolan-madrid/madrid-city"
ST="/tmp/madrid_catalog"

echo "=== upload data tree (v2/v3/tab/catalog/parquet/raster) ==="
gcloud storage cp --recursive "$ST/data" "$DST/" 2>&1 | tail -2

echo "=== upload IRC REST surface (extension-less keys, application/json) ==="
cd "$ST/_surface"
n=0
for f in *.json; do
  key=$(echo "$f" | sed 's/\.json$//; s/__/\//g')
  gcloud storage cp "$f" "$DST/$key" --content-type=application/json >/dev/null 2>&1 && n=$((n+1))
done
echo "uploaded $n surface objects"

echo "=== verify endpoint ==="
BASE="https://storage.googleapis.com/carto-portolan-madrid/madrid-city"
for u in "v1/config" "v1/sdi/namespaces" "data/catalog/datasets/metadata/v1.metadata.json"; do
  printf "%-50s %s\n" "$u" "$(curl -sS -m 15 -o /dev/null -w '%{http_code}' "$BASE/$u")"
done
