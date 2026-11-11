import io
import os
import gc
import csv
import uuid
from datetime import datetime, timezone
import pandas as pd
import pyarrow.parquet as pq
import psycopg2

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Danh sách 24 cột mục tiêu của bảng staging.raw_yellow_trips
TARGET_STAGING_COLS = [
    'source_file_name', 'source_file_sha256', 'source_row_number',
    'load_batch_id', 'loaded_at_utc',
    'vendor_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'passenger_count', 'trip_distance', 'rate_code_id',
    'store_and_fwd_flag', 'pu_location_id', 'do_location_id',
    'payment_type', 'fare_amount', 'extra', 'mta_tax',
    'tip_amount', 'tolls_amount', 'improvement_surcharge',
    'total_amount', 'congestion_surcharge', 'airport_fee'
]

# Câu lệnh COPY chuẩn hỗ trợ schema.table cho copy_expert
COPY_SQL = f"""
    COPY staging.raw_yellow_trips ({', '.join(TARGET_STAGING_COLS)})
    FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')
"""

# Thứ tự 18 cột đầu từ Parquet
PARQUET_SOURCE_ORDER = [
    'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
    'passenger_count', 'trip_distance', 'RatecodeID',
    'store_and_fwd_flag', 'PULocationID', 'DOLocationID',
    'payment_type', 'fare_amount', 'extra', 'mta_tax',
    'tip_amount', 'tolls_amount', 'improvement_surcharge',
    'total_amount', 'congestion_surcharge'
]


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )


def format_series_for_staging(s: pd.Series, is_int_like: bool = False) -> pd.Series:
    """
    Chuyển Series từ PyArrow/Pandas sang chuỗi an toàn cho PostgreSQL COPY,
    giữ nguyên giá trị NULL (None) và cắt đuôi '.0' cho các cột mã nguyên bị lưu dạng float (vd: 1.0 -> '1'),
    nhưng giữ nguyên số lẻ như '1.5' để đẩy vào quarantine theo đúng Data Contract v1.2.
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.strftime('%Y-%m-%d %H:%M:%S').where(s.notna(), None)

    res = s.astype(object).where(s.notna(), None)
    if is_int_like:
        res = res.map(
            lambda x: None if x is None else (
                str(int(x)) if isinstance(x, float) and x.is_integer() else str(x).removesuffix('.0')
            )
        )
    else:
        res = res.map(lambda x: None if x is None else str(x))
    return res


@data_exporter
def export_parquet_batches_to_staging(df_manifest: pd.DataFrame, *args, **kwargs):
    """
    Task S01 (stinh): Đọc từng file Parquet theo chunk 100.000 dòng bằng PyArrow,
    sinh source_row_number liên tục (1..N), nạp nhanh vào staging.raw_yellow_trips
    và đối soát 100% với total_rows trong footer.
    """
    batch_size = int(kwargs.get('batch_size', 100000))
    force_reload = bool(kwargs.get('force_reload', False))
    pipeline_batch_id = str(uuid.uuid4())
    started_ts = datetime.now(timezone.utc)

    conn = get_pg_conn()
    total_processed_all_files = 0

    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO meta.batch_log (batch_id, pipeline_name, batch_status, started_at)
                VALUES (%s, 'ingest_raw_trips', 'STARTED', %s)
            """, (pipeline_batch_id, started_ts))
        conn.commit()

        for _, row in df_manifest.iterrows():
            file_name = row['file_name']
            file_path = row['file_path']
            file_sha256 = row['file_sha256']
            expected_rows = int(row['total_rows'])
            schema_ver = row['source_schema_version']
            action_status = row['action_status']

            if action_status == 'SKIP_ALREADY_LOADED' and not force_reload:
                print(f"[SKIP] {file_name} (SHA256: {file_sha256[:10]}...) đã nạp đủ {expected_rows:,} dòng trước đó.")
                continue

            if action_status == 'FAILED':
                raise RuntimeError(f"Dừng nạp do file {file_name} vi phạm schema: {row['error_message']}")

            airport_col = 'airport_fee' if schema_ver == 'v1_2021_2022' else 'Airport_fee'
            read_cols = PARQUET_SOURCE_ORDER + [airport_col]

            print(f"[START] Đang nạp {file_name} ({expected_rows:,} dòng, schema={schema_ver}, chunk={batch_size:,})...")
            loaded_at_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE meta.file_manifest
                    SET load_status = 'PROCESSING',
                        current_batch_id = %s,
                        last_updated_at = CURRENT_TIMESTAMP,
                        error_message = NULL
                    WHERE file_name = %s
                """, (pipeline_batch_id, file_name))

                cur.execute("""
                    DELETE FROM staging.raw_yellow_trips
                    WHERE source_file_name = %s OR source_file_sha256 = %s
                """, (file_name, file_sha256))
            conn.commit()

            pf = pq.ParquetFile(file_path)
            current_row_num = 0

            try:
                with conn.cursor() as cur:
                    for record_batch in pf.iter_batches(batch_size=batch_size, columns=read_cols):
                        chunk_len = record_batch.num_rows
                        df_chunk = record_batch.to_pandas()

                        out_df = pd.DataFrame({
                            'source_file_name': file_name,
                            'source_file_sha256': file_sha256,
                            'source_row_number': range(current_row_num + 1, current_row_num + chunk_len + 1),
                            'load_batch_id': pipeline_batch_id,
                            'loaded_at_utc': loaded_at_str,
                            'vendor_id': format_series_for_staging(df_chunk['VendorID'], is_int_like=True),
                            'tpep_pickup_datetime': format_series_for_staging(df_chunk['tpep_pickup_datetime']),
                            'tpep_dropoff_datetime': format_series_for_staging(df_chunk['tpep_dropoff_datetime']),
                            'passenger_count': format_series_for_staging(df_chunk['passenger_count'], is_int_like=True),
                            'trip_distance': format_series_for_staging(df_chunk['trip_distance']),
                            'rate_code_id': format_series_for_staging(df_chunk['RatecodeID'], is_int_like=True),
                            'store_and_fwd_flag': format_series_for_staging(df_chunk['store_and_fwd_flag']),
                            'pu_location_id': format_series_for_staging(df_chunk['PULocationID'], is_int_like=True),
                            'do_location_id': format_series_for_staging(df_chunk['DOLocationID'], is_int_like=True),
                            'payment_type': format_series_for_staging(df_chunk['payment_type'], is_int_like=True),
                            'fare_amount': format_series_for_staging(df_chunk['fare_amount']),
                            'extra': format_series_for_staging(df_chunk['extra']),
                            'mta_tax': format_series_for_staging(df_chunk['mta_tax']),
                            'tip_amount': format_series_for_staging(df_chunk['tip_amount']),
                            'tolls_amount': format_series_for_staging(df_chunk['tolls_amount']),
                            'improvement_surcharge': format_series_for_staging(df_chunk['improvement_surcharge']),
                            'total_amount': format_series_for_staging(df_chunk['total_amount']),
                            'congestion_surcharge': format_series_for_staging(df_chunk['congestion_surcharge']),
                            'airport_fee': format_series_for_staging(df_chunk[airport_col])
                        })

                        buf = io.StringIO()
                        out_df.to_csv(
                            buf,
                            sep='\t',
                            header=False,
                            index=False,
                            na_rep='\\N',
                            quoting=csv.QUOTE_NONE,
                            escapechar='\\'
                        )
                        buf.seek(0)

                        # Sử dụng copy_expert thay vì copy_from để nhận diện đúng schema `staging.raw_yellow_trips`
                        cur.copy_expert(COPY_SQL, buf)

                        current_row_num += chunk_len
                        print(f"  -> Đã nạp chunk: {current_row_num:,} / {expected_rows:,} dòng...")
                        del df_chunk, out_df, buf
                        gc.collect()

                    if current_row_num != expected_rows:
                        raise RuntimeError(
                            f"ROW_COUNT_MISMATCH trên {file_name}: Đã nạp {current_row_num} != Footer {expected_rows}"
                        )

                    cur.execute("""
                        UPDATE meta.file_manifest
                        SET load_status = 'SUCCESS',
                            staged_rows = %s,
                            first_loaded_at = COALESCE(first_loaded_at, CURRENT_TIMESTAMP),
                            last_updated_at = CURRENT_TIMESTAMP,
                            error_message = NULL
                        WHERE file_name = %s
                    """, (current_row_num, file_name))

                conn.commit()
                total_processed_all_files += current_row_num
                print(f"[SUCCESS] Hoàn tất {file_name}: Đã nạp khớp 100% ({current_row_num:,} / {expected_rows:,} dòng).")

            except Exception as file_err:
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE meta.file_manifest
                        SET load_status = 'FAILED',
                            last_updated_at = CURRENT_TIMESTAMP,
                            error_message = %s
                        WHERE file_name = %s
                    """, (str(file_err), file_name))
                conn.commit()
                raise file_err

        finished_ts = datetime.now(timezone.utc)
        duration_sec = round((finished_ts - started_ts).total_seconds(), 2)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE meta.batch_log
                SET batch_status = 'COMPLETED',
                    finished_at = %s,
                    duration_seconds = %s,
                    processed_records = %s
                WHERE batch_id = %s
            """, (finished_ts, duration_sec, total_processed_all_files, pipeline_batch_id))
        conn.commit()
        print(f"==> HOÀN TẤT BATCH {pipeline_batch_id}: Tổng dòng mới nạp = {total_processed_all_files:,} trong {duration_sec}s.")

    except Exception as e:
        conn.rollback()
        finished_ts = datetime.now(timezone.utc)
        duration_sec = round((finished_ts - started_ts).total_seconds(), 2)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE meta.batch_log
                SET batch_status = 'FAILED',
                    finished_at = %s,
                    duration_seconds = %s,
                    processed_records = %s,
                    error_message = %s
                WHERE batch_id = %s
            """, (finished_ts, duration_sec, total_processed_all_files, str(e), pipeline_batch_id))
        conn.commit()
        raise e
    finally:
        conn.close()