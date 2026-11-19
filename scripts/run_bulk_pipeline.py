#!/usr/bin/env python3
"""
TASK T03/T04/S03 - ULTRA-FAST BULK ETL PIPELINE RUNNER (36 MONTHS)
Tối ưu hóa tốc độ nạp dữ liệu lớn (108.87M dòng) kết hợp:
  1. Transient Staging Buffer: TRUNCATE staging trước mỗi file -> Loại bỏ O(N^2)
  2. Decoupled Testing: Smoke Check 0.5s bằng SQL sau mỗi file, chỉ dbt test ở cuối
  3. Batch Scope dbt: Chỉ transform đúng file hiện tại
"""
import sys
import os
import time
import argparse
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess

# Thêm đường dẫn project vào sys.path
root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir))

from lab_project.data_loaders.check_or_ingest_zones import check_or_ingest_zones
from lab_project.data_loaders.scan_parquet_manifest import scan_and_register_manifest
from lab_project.data_exporters.export_parquet_chunks import export_parquet_batches_to_staging
from lab_project.transformers.dbt_staging_and_dims import dbt_staging_and_dims
from lab_project.transformers.dbt_fact_incremental import dbt_fact_incremental


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )


def run_bulk(year=None, max_files=None, force_reload=False, skip_dims=False, run_tests_at_end=False):
    print("=" * 85)
    print("🚀 [BULK PIPELINE] KHỞI CHẠY BỘ ĐIỀU PHỐI NẠP TỐC ĐỘ CAO (TRANSIENT STAGING)")
    print("=" * 85)

    start_total = time.time()
    conn = get_pg_conn()

    # 1. Khởi tạo / kiểm tra Zone và 8 Dimensions
    if not skip_dims:
        print("\n[BƯỚC 0] Kiểm tra PostGIS Zone & Build 8 Dimensions (Chỉ chạy 1 lần)...")
        check_or_ingest_zones()
        dbt_staging_and_dims()
        print("-> Đã sẵn sàng 8 Dimension tables và PostGIS geometries.")

    # 2. Quét danh sách file Parquet
    data_dir = Path('/home/src/external_data')
    all_files = sorted(list(data_dir.glob('**/*.parquet')))
    if not all_files:
        print(f"[ERROR] Không tìm thấy tệp Parquet nào trong {data_dir}!")
        return

    # Lọc theo năm nếu có
    if year:
        all_files = [f for f in all_files if f"_{year}-" in f.name or f"/{year}/" in str(f)]

    # Giới hạn số file nếu có max_files
    if max_files:
        all_files = all_files[:max_files]

    print(f"\n[DANH SÁCH] Đã tìm thấy {len(all_files)} tệp Parquet cần xử lý:")
    for idx, f in enumerate(all_files, 1):
        print(f"  {idx:2d}. {f.name}")

    # 3. Lấy thông tin các file đã nạp trong Fact để tránh nạp trùng
    with conn.cursor() as cur:
        cur.execute('SELECT "SourceFileName", count(*) FROM dds."FactTaxiTrip" GROUP BY "SourceFileName";')
        already_in_fact = {r[0]: r[1] for r in cur.fetchall()}

    results = []
    
    # 4. Vòng lặp nạp từng file theo cơ chế Transient Staging
    for idx, file_path in enumerate(all_files, 1):
        file_name = file_path.name
        print("\n" + "-" * 85)
        print(f"📦 [FILE {idx}/{len(all_files)}] XỬ LÝ: {file_name}")
        print("-" * 85)

        if file_name in already_in_fact and not force_reload:
            rows_exist = already_in_fact[file_name]
            print(f"⏩ [SKIP] {file_name} đã có trong dds.FactTaxiTrip ({rows_exist:,} dòng). Bỏ qua (dùng --force-reload để nạp lại).")
            results.append({
                'file_name': file_name,
                'status': 'SKIPPED',
                'rows': rows_exist,
                'duration_seconds': 0
            })
            continue

        t_file_start = time.time()

        # 4.1. Quét Manifest file
        df_manifest = scan_and_register_manifest(
            data_dir=str(data_dir),
            file_pattern=file_name
        )
        footer_rows = int(df_manifest['total_rows'].iloc[0]) if not df_manifest.empty else 0

        # 4.2. Ingest vào Staging (Transient: TRUNCATE staging trước khi nạp)
        t_ingest_start = time.time()
        export_parquet_batches_to_staging(
            df_manifest,
            batch_size=100000,
            force_reload=True,       # Bắt buộc nạp lại vào Staging tạm
            truncate_staging=True    # TRUNCATE Staging trước khi nạp
        )
        t_ingest = round(time.time() - t_ingest_start, 2)
        print(f"  -> Ingestion (PyArrow chunk 100k -> Staging): {t_ingest}s.")

        # 4.3. dbt Transform: Chỉ nạp Fact cho đúng file này
        t_dbt_start = time.time()
        dbt_fact_incremental(target_file=file_name)
        t_dbt = round(time.time() - t_dbt_start, 2)
        print(f"  -> dbt Transform (stg_trips_flagged -> FactTaxiTrip): {t_dbt}s.")

        # 4.4. Smoke Test bằng SQL (< 0.5s): Đối soát số dòng giữa Staging và Fact
        with conn.cursor() as cur:
            cur.execute('SELECT count(*) FROM staging.raw_yellow_trips;')
            staged_count = cur.fetchone()[0]

            cur.execute('SELECT count(*) FROM dds."FactTaxiTrip" WHERE "SourceFileName" = %s;', (file_name,))
            fact_count = cur.fetchone()[0]

            # Cập nhật meta.file_manifest
            is_match = (staged_count == fact_count == footer_rows)
            status_str = 'SUCCESS' if is_match else 'MISMATCH'
            cur.execute("""
                UPDATE meta.file_manifest
                SET load_status = %s,
                    staged_rows = %s,
                    fact_rows = %s,
                    last_updated_at = CURRENT_TIMESTAMP
                WHERE file_name = %s;
            """, (status_str, staged_count, fact_count, file_name))
        conn.commit()

        # 4.5. Dọn dẹp sạch Staging để giải phóng tài nguyên cho file tiếp theo
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE staging.raw_yellow_trips;")
        conn.commit()

        t_file_total = round(time.time() - t_file_start, 2)
        if is_match:
            print(f"  ✅ [THÀNH CÔNG] File {file_name}: Nạp đúng {fact_count:,} dòng trong {t_file_total}s (~{round(t_file_total/60, 2)} phút).")
        else:
            print(f"  ⚠️ [LỆCH SỐ LIỆU] Footer: {footer_rows:,} | Staged: {staged_count:,} | Fact: {fact_count:,}!")

        results.append({
            'file_name': file_name,
            'status': status_str,
            'rows': fact_count,
            'duration_seconds': t_file_total
        })

    # 5. Chạy Data Tests toàn diện nếu được yêu cầu
    if run_tests_at_end:
        print("\n" + "=" * 85)
        print("🧪 [KIỂM ĐỊNH TOÀN DIỆN] ĐANG CHẠY BỘ SUITE 38 DATA TESTS CHO FACT...")
        print("=" * 85)
        t_test_start = time.time()
        dbt_dir = root_dir / 'dbt_project'
        test_res = subprocess.run(['dbt', 'test', '--select', 'FactTaxiTrip'], cwd=dbt_dir, capture_output=True, text=True)
        t_test = round(time.time() - t_test_start, 2)
        if test_res.returncode == 0:
            print(f"  ✅ [PASS 100%] Toàn bộ data tests đã vượt qua trong {t_test}s.")
        else:
            print(f"  ❌ [TEST FAILED] Có lỗi trong kiểm định:")
            print(test_res.stdout[-1000:])

    # 6. Tổng kết
    total_elapsed = round(time.time() - start_total, 2)
    processed = [r for r in results if r['status'] != 'SKIPPED']
    avg_duration = round(sum(r['duration_seconds'] for r in processed) / len(processed), 2) if processed else 0

    print("\n" + "=" * 85)
    print("🏁 [TỔNG KẾT VẬN HÀNH BULK ETL PIPELINE]")
    print(f"  * Tổng số file đã xử lý đợt này: {len(processed)} / {len(all_files)}")
    print(f"  * Tổng số dòng nạp thêm vào Fact: {sum(r['rows'] for r in processed):,}")
    print(f"  * Thời gian trung bình mỗi file: {avg_duration}s (~{round(avg_duration/60, 2)} phút/file)")
    print(f"  * Tổng thời gian toàn bộ tiến trình: {total_elapsed}s (~{round(total_elapsed/60, 2)} phút)")
    print("=" * 85)

    conn.close()
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Bulk ETL Pipeline Runner (NYC Yellow Taxi)")
    parser.add_argument('--year', type=str, default=None, help="Chỉ nạp cho 1 năm cụ thể (vd: 2021)")
    parser.add_argument('--max-files', type=int, default=None, help="Giới hạn số file nạp (vd: 2)")
    parser.add_argument('--force-reload', action='store_true', help="Bắt buộc nạp lại dù file đã có trong Fact")
    parser.add_argument('--skip-dims', action='store_true', help="Bỏ qua bước build Dimensions lúc đầu")
    parser.add_argument('--run-tests', action='store_true', help="Chạy dbt test toàn diện sau khi nạp xong tất cả file")
    
    args = parser.parse_args()
    run_bulk(
        year=args.year,
        max_files=args.max_files,
        force_reload=args.force_reload,
        skip_dims=args.skip_dims,
        run_tests_at_end=args.run_tests
    )
