"""Create a NEW isolated acceptance database and ingest only the two agreed samples."""
import os
import re
import sys
from pathlib import Path
import psycopg2
from psycopg2 import sql

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root))
name = sys.argv[1]
if not re.fullmatch(r'taxi_acceptance_[a-z0-9_]+', name):
    raise ValueError('Database name must start with taxi_acceptance_')
params = dict(host=os.environ['POSTGRES_HOST'], port=os.environ.get('POSTGRES_PORT', '5432'),
              user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'])
conn = psycopg2.connect(dbname=os.environ['POSTGRES_DB'], **params)
conn.autocommit = True
with conn.cursor() as cur:
    # No DROP/overwrite: fail if the database already exists.
    cur.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
conn.close()
os.environ['POSTGRES_DB'] = name
with psycopg2.connect(dbname=name, **params) as conn:
    with conn.cursor() as cur:
        cur.execute((root / 'postgres/init/01_init.sql').read_text())
from lab_project.data_loaders.ingest_spatial_zones import load_and_ingest_zones
from lab_project.data_loaders.scan_parquet_manifest import scan_and_register_manifest
from lab_project.data_exporters.export_parquet_chunks import export_parquet_batches_to_staging
load_and_ingest_zones()
for filename in ['yellow_tripdata_2021-01.parquet', 'yellow_tripdata_2023-02.parquet']:
    manifest = scan_and_register_manifest(file_pattern=filename)
    export_parquet_batches_to_staging(manifest)
print('ACCEPTANCE DATABASE READY:', name, flush=True)
