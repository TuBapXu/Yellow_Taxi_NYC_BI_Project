#!/bin/bash
# Chay ben trong container lab-mage: cac buoc S01 (Runbook S01 - Buoc 4.2)
cd /home/src
D=/home/src/external_data
echo "--- Noi dung $D:"; ls -lh "$D"
NPQ=$(find "$D" -name 'yellow_tripdata_*.parquet' 2>/dev/null | wc -l)
NCSV=$(find "$D" -maxdepth 1 -iname '*zone*lookup*.csv' 2>/dev/null | wc -l)
NSHP=$(find "$D" \( -name 'taxi_zones.shp' -o -name '*taxi_zones*.zip' \) 2>/dev/null | wc -l)
echo "--- Tim thay: parquet=$NPQ, zone_lookup_csv=$NCSV, shapefile=$NSHP"
if [ "$NCSV" -gt 0 ] && [ "$NSHP" -gt 0 ]; then
  echo "### S01-1: mage run lab_project ingest_zones"
  mage run lab_project ingest_zones; echo "[exit=$?]"
else
  echo "### SKIP ingest_zones: chua co taxi_zone_lookup.csv / taxi_zones.shp"
fi
if [ "$NPQ" -gt 0 ]; then
  echo "### S01-2: scan_and_register_manifest()"
  python3 -c "from lab_project.data_loaders.scan_parquet_manifest import scan_and_register_manifest; scan_and_register_manifest()"; echo "[exit=$?]"
  for F in yellow_tripdata_2021-01.parquet yellow_tripdata_2023-02.parquet; do
    if find "$D" -name "$F" | grep -q .; then
      echo "### S01-3: mage run lab_project ingest_raw_trips (file_pattern=$F)"
      mage run lab_project ingest_raw_trips --runtime-vars "{\"file_pattern\": \"$F\"}"; echo "[exit=$?]"
    else
      echo "### SKIP $F: khong co trong Data"
    fi
  done
else
  echo "### SKIP S01 nap Parquet: chua co file yellow_tripdata_*.parquet trong Data"
fi
