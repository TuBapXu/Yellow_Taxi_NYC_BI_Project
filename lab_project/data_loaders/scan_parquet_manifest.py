import os
import hashlib
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import psycopg2

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# 18 cột bắt buộc chung giữa 2 phiên bản (cột thứ 19 là airport_fee hoặc Airport_fee)
REQUIRED_COMMON_COLS = {
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'passenger_count', 'trip_distance', 'RatecodeID',
    'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
    'payment_type', 'fare_amount', 'extra', 'mta_tax',
    'tip_amount', 'tolls_amount', 'improvement_surcharge',
    'total_amount', 'congestion_surcharge'
}


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(16 * 1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def detect_schema_version(pf: pq.ParquetFile) -> str:
    col_names = set(pf.schema_arrow.names)
    missing_common = REQUIRED_COMMON_COLS - col_names
    if missing_common:
        raise ValueError(f"MISSING_REQUIRED_COLUMNS: Thiếu các cột bắt buộc {missing_common}")

    has_lower = 'airport_fee' in col_names
    has_upper = 'Airport_fee' in col_names

    if has_lower and has_upper:
        raise ValueError("SCHEMA_ALIAS_CONFLICT: Tồn tại đồng thời 'airport_fee' và 'Airport_fee'")
    elif has_lower:
        return 'v1_2021_2022'
    elif has_upper:
        return 'v2_2023'
    else:
        raise ValueError("MISSING_AIRPORT_FEE: Không tìm thấy cột airport_fee / Airport_fee")


@data_loader
def scan_and_register_manifest(*args, **kwargs):
    """
    Task S01 (stinh): Quét danh sách tệp Parquet, kiểm tra schema version,
    tính SHA-256, lấy tổng số dòng từ footer và đăng ký vào meta.file_manifest.
    
    Runtime variables hỗ trợ:
    - data_dir: thư mục chứa file Parquet (mặc định: /home/src/external_data)
    - file_pattern: lọc tên file (mặc định: yellow_tripdata_*.parquet)
    - max_files: giới hạn số file nạp trong 1 lần chạy (mặc định: None -> tất cả, hoặc truyền 1 để test 1 file)
    """
    data_dir = Path(kwargs.get('data_dir', '/home/src/external_data'))
    file_pattern = kwargs.get('file_pattern', 'yellow_tripdata_*.parquet')
    max_files = kwargs.get('max_files', None)

    parquet_files = sorted(data_dir.glob(file_pattern))
    if not parquet_files:
        # Tìm đệ quy trong các thư mục con nếu đặt trong subfolder
        parquet_files = sorted(data_dir.rglob(file_pattern))

    if not parquet_files:
        raise FileNotFoundError(f"Không tìm thấy tệp Parquet nào khớp '{file_pattern}' tại {data_dir}")

    if max_files is not None:
        parquet_files = parquet_files[:int(max_files)]

    manifest_rows = []
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            for p_file in parquet_files:
                file_name = p_file.name
                file_size = p_file.stat().st_size
                file_sha256 = compute_sha256(p_file)

                try:
                    pf = pq.ParquetFile(p_file)
                    total_rows = pf.metadata.num_rows
                    schema_ver = detect_schema_version(pf)
                    err_msg = None
                except Exception as e:
                    total_rows = 0
                    schema_ver = 'INVALID_SCHEMA'
                    err_msg = str(e)

                # Kiểm tra xem file hoặc SHA-256 đã tồn tại trong meta.file_manifest chưa
                cur.execute("""
                    SELECT file_id, file_sha256, load_status, staged_rows
                    FROM meta.file_manifest
                    WHERE file_name = %s OR file_sha256 = %s
                """, (file_name, file_sha256))
                existing = cur.fetchone()

                if err_msg:
                    status = 'FAILED'
                elif existing and existing[1] == file_sha256 and existing[2] == 'SUCCESS' and existing[3] == total_rows:
                    status = 'SKIP_ALREADY_LOADED'
                else:
                    status = 'PENDING'

                # Upsert vào bảng meta.file_manifest
                cur.execute("""
                    INSERT INTO meta.file_manifest (
                        file_name, file_path, file_sha256, file_size_bytes,
                        total_rows, source_schema_version, load_status,
                        last_updated_at, error_message
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                    ON CONFLICT (file_name) DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        file_sha256 = EXCLUDED.file_sha256,
                        file_size_bytes = EXCLUDED.file_size_bytes,
                        total_rows = EXCLUDED.total_rows,
                        source_schema_version = EXCLUDED.source_schema_version,
                        load_status = CASE
                            WHEN meta.file_manifest.file_sha256 = EXCLUDED.file_sha256
                                 AND meta.file_manifest.load_status = 'SUCCESS'
                                 AND meta.file_manifest.staged_rows = EXCLUDED.total_rows
                            THEN 'SUCCESS'
                            ELSE EXCLUDED.load_status
                        END,
                        last_updated_at = CURRENT_TIMESTAMP,
                        error_message = EXCLUDED.error_message;
                """, (
                    file_name, str(p_file), file_sha256, file_size,
                    total_rows, schema_ver,
                    'FAILED' if status == 'FAILED' else 'PENDING',
                    err_msg
                ))

                manifest_rows.append({
                    'file_name': file_name,
                    'file_path': str(p_file),
                    'file_sha256': file_sha256,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'total_rows': total_rows,
                    'source_schema_version': schema_ver,
                    'action_status': status,
                    'error_message': err_msg
                })
        conn.commit()
    finally:
        conn.close()

    df_manifest = pd.DataFrame(manifest_rows)
    print(f"==> ĐÃ QUÉT {len(df_manifest)} FILE PARQUET VÀ CẬP NHẬT META.FILE_MANIFEST:")
    print(df_manifest[['file_name', 'total_rows', 'source_schema_version', 'action_status']].to_string(index=False))
    return df_manifest


@test
def test_manifest_not_empty(output, *args) -> None:
    assert output is not None and len(output) > 0, 'Danh sách manifest không được rỗng'
    failed_files = output[output['action_status'] == 'FAILED']
    assert len(failed_files) == 0, f"Có file lỗi schema: {failed_files[['file_name', 'error_message']].to_dict('records')}"