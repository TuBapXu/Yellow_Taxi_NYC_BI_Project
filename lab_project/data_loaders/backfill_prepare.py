import os
import time
from pathlib import Path
import psycopg2

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def backfill_prepare(*args, **kwargs):
    """
    Block 1: Kiem tra moi truong kho du lieu DW_DB, ket noi PostgreSQL,
    kiem tra PostGIS spatial_ref_sys va DimZone truoc khi chay backfill.
    """
    print("[BACKFILL STAGE 1] Kiem tra moi truong va ha tang kho du lieu...")
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dds.\"DimZone\";")
        zone_cnt = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM dds.\"DimDate\";")
        date_cnt = cur.fetchone()[0]
    conn.close()

    print(f"-> Ha tang san sang: DimZone co {zone_cnt} zones, DimDate co {date_cnt} ngay.")
    return {
        'status': 'READY',
        'zone_count': zone_cnt,
        'date_count': date_cnt,
        'timestamp': time.time()
    }
