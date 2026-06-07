#!/bin/bash
# Convierte a v3 (sin geom) todos los datasets TABULARES de un catálogo (data/parquet_tab/). Resumable.
PY=/Users/jatorre/workspace/iceberg-geo-testbed/.venv/bin/python
PREFIX="$1"; mkdir -p /tmp/pv3
LEDGER="/tmp/pv3/$(echo $PREFIX | tr '/' '_')_tab_done.txt"; touch "$LEDGER"
LIST="/tmp/pv3/$(echo $PREFIX | tr '/' '_')_tab_list.txt"
gcloud storage ls "gs://${PREFIX}/data/parquet_tab/" 2>/dev/null | grep '\.parquet$' | sed -E 's#.*/([^/]+)\.parquet#\1#' > "$LIST"
TOT=$(wc -l < "$LIST" | tr -d ' ')
echo "$(date +%H:%M:%S) $PREFIX TAB: $TOT datasets"
ok=0; fail=0; i=0
for d in $(cat "$LIST"); do
  i=$((i+1)); grep -qxF "$d" "$LEDGER" && { ok=$((ok+1)); continue; }
  if $PY /tmp/portolan_v3_tab.py "$PREFIX" "$d" >/tmp/pv3/lasttab.log 2>&1; then echo "$d" >> "$LEDGER"; ok=$((ok+1))
  else fail=$((fail+1)); echo "  FAIL $d: $(tail -1 /tmp/pv3/lasttab.log | head -c 130)"; fi
  [ $((i % 100)) -eq 0 ] && echo "  ... $i/$TOT (ok=$ok fail=$fail) $(date +%H:%M:%S)"
done
echo "$(date +%H:%M:%S) $PREFIX TAB FIN: ok=$ok fail=$fail / $TOT"
