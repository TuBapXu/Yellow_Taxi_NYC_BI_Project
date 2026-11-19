import sqlite3
from datetime import datetime, timezone

db_path = '/home/src/mage_data/lab_project/mage-ai.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# 1. Trigger @once nap 2024-01
cur.execute("""
    INSERT OR REPLACE INTO pipeline_schedule (
        id, name, pipeline_uuid, schedule_type, schedule_interval, status,
        variables, repo_path, description, created_at, updated_at
    ) VALUES (
        2,
        'run_2024_01_incremental',
        'master_etl_pipeline',
        'time',
        '@once',
        'active',
        '{"file_pattern": "yellow_tripdata_2024-01.parquet"}',
        '/home/src/lab_project',
        'Trigger nap thu nghiem 1 file thang 01/2024 (yellow_tripdata_2024-01.parquet)',
        datetime('now'),
        datetime('now')
    );
""")

# 2. Trigger @monthly dinh ky
cur.execute("""
    INSERT OR REPLACE INTO pipeline_schedule (
        id, name, pipeline_uuid, schedule_type, schedule_interval, status,
        variables, repo_path, description, created_at, updated_at
    ) VALUES (
        3,
        'monthly_incremental_sync',
        'master_etl_pipeline',
        'time',
        '@monthly',
        'active',
        '{}',
        '/home/src/lab_project',
        'Trigger tu dong quet va nap thang moi vao ngay 5 hang thang',
        datetime('now'),
        datetime('now')
    );
""")

conn.commit()

cur.execute("SELECT id, name, pipeline_uuid, schedule_interval, status, variables FROM pipeline_schedule;")
print("=== DANH SACH TRIGGERS TRONG MAGE.AI ===")
for r in cur.fetchall():
    print(r)
conn.close()
