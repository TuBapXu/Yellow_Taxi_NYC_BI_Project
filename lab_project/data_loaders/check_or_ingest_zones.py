import os
import subprocess
from pathlib import Path
import psycopg2

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from lab_project.data_loaders.ingest_spatial_zones import load_and_ingest_zones


@data_loader
def check_or_ingest_zones(*args, **kwargs):
    """
    Task T03: Data Loader kiểm tra xem DimZone và shapefile đã nạp vào CSDL chưa.
    Tái sử dụng loader ingest_spatial_zones của STINH (S01).
    Nếu chưa, tự động nạp taxi_zone_lookup.csv + shapefile và build DimZone.
    """
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )
    
    zone_count = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'dds' AND table_name = 'DimZone'
            """)
            exists = cur.fetchone()[0]
            if exists:
                cur.execute('SELECT COUNT(*) FROM dds."DimZone"')
                zone_count = cur.fetchone()[0]
    except Exception:
        zone_count = 0
    finally:
        conn.close()

    if zone_count >= 265:
        print(f"[CHECK] DimZone đã có sẵn {zone_count} bản ghi. Bỏ qua bước nạp zones.")
        return {'status': 'SKIPPED_ALREADY_EXISTS', 'zone_count': zone_count}

    print("[START] Đang nạp Zone Lookup CSV và Shapefile...")
    load_and_ingest_zones(*args, **kwargs)

    root = Path(__file__).resolve().parents[2]
    dbt_dir = root / 'dbt_project'
    print("[START] Đang build dds.DimZone bằng dbt...")
    subprocess.run(['dbt', 'run', '--select', 'marts.dims.DimZone'], cwd=dbt_dir, check=True)
    
    return {'status': 'INGESTED_SUCCESSFULLY', 'zone_count': 266}
