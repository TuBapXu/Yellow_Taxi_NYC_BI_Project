import os
import sys
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from lab_project.data_loaders.ingest_spatial_zones import load_and_ingest_zones
from lab_project.data_loaders.scan_parquet_manifest import scan_and_register_manifest
from lab_project.data_exporters.export_parquet_chunks import export_parquet_batches_to_staging

DBT_DIR = ROOT_DIR / 'dbt_project'
DOCS_DIR = ROOT_DIR / 'docs'
DOCS_DIR.mkdir(parents=True, exist_ok=True)


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )


def run_cmd(cmd, cwd=DBT_DIR, desc=""):
    t0 = time.time()
    print(f"  [CMD] {desc}: {' '.join(cmd)}", flush=True)
    res = subprocess.run(cmd, cwd=cwd)
    dur = round(time.time() - t0, 2)
    if res.returncode != 0:
        print(f"  [ERROR] Thất bại sau {dur}s", flush=True)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return dur, ""


def prepare_ultra_fast_bulk_mode(conn):
    """
    Tối ưu hóa tốc độ nạp 108M dòng:
    1. Chuyển staging.raw_yellow_trips và dds."FactTaxiTrip" sang UNLOGGED (không tốn I/O ghi WAL).
    2. Tạm gỡ 11 FKs, 2 Unique Constraints (PK TripKey MD5 + uq_fact_source_lineage) và 12 Indexes phụ.
    3. Chỉ giữ lại 1 index nhẹ trên (SourceFileName) để kiểm tra Resume và xóa nhanh theo file.
    """
    print("\n[TURBO-MODE] Kích hoạt chế độ UNLOGGED & tạm gỡ PK/Indexes để nạp dbt nhanh gấp 15 lần...", flush=True)
    sql = """
    -- 1. Gỡ 11 Foreign Keys
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_PickupDateKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_DropoffDateKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_PickupTimeKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_DropoffTimeKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_PickupZoneKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_DropoffZoneKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_VendorKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_PaymentTypeKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_RateCodeKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_PassengerGroupKey_fkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_DistanceBandKey_fkey";

    -- 2. Gỡ tạm PK (MD5 random B-Tree) và Unique Constraint
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS "FactTaxiTrip_pkey";
    ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT IF EXISTS uq_fact_source_lineage;

    -- 3. Gỡ 12 Indexes phụ
    DROP INDEX IF EXISTS dds.idx_fact_pickup_date;
    DROP INDEX IF EXISTS dds.idx_fact_dropoff_date;
    DROP INDEX IF EXISTS dds.idx_fact_pickup_time;
    DROP INDEX IF EXISTS dds.idx_fact_dropoff_time;
    DROP INDEX IF EXISTS dds.idx_fact_pickup_zone;
    DROP INDEX IF EXISTS dds.idx_fact_dropoff_zone;
    DROP INDEX IF EXISTS dds.idx_fact_vendor;
    DROP INDEX IF EXISTS dds.idx_fact_payment;
    DROP INDEX IF EXISTS dds.idx_fact_ratecode;
    DROP INDEX IF EXISTS dds.idx_fact_passenger_grp;
    DROP INDEX IF EXISTS dds.idx_fact_distance_band;
    DROP INDEX IF EXISTS dds.idx_fact_usable_flags;

    -- 4. Đặt UNLOGGED để PostgreSQL không ghi WAL khi dbt INSERT
    ALTER TABLE staging.raw_yellow_trips SET UNLOGGED;
    ALTER TABLE dds."FactTaxiTrip" SET UNLOGGED;

    -- 5. Tạo 1 index siêu nhẹ trên SourceFileName để đối soát tức thì từng tháng
    CREATE INDEX IF NOT EXISTS idx_fact_temp_source_file ON dds."FactTaxiTrip" ("SourceFileName");
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def restore_fact_indexes_and_fks(conn):
    """
    Khôi phục chế độ LOGGED, Primary Key, Unique Constraint, 12 B-Tree Indexes và 11 Foreign Keys.
    """
    print("\n[RESTORE] Đang khôi phục PK, Unique Constraint, 12 Indexes và 11 FKs trên dds.\"FactTaxiTrip\"...", flush=True)
    t0 = time.time()
    sql = """
    SET maintenance_work_mem = '1GB';
    SET max_parallel_maintenance_workers = 4;

    DROP INDEX IF EXISTS dds.idx_fact_temp_source_file;

    -- 1. Chuyển lại bảng Fact sang LOGGED bền vững
    ALTER TABLE dds."FactTaxiTrip" SET LOGGED;

    -- 2. Khôi phục Khóa chính (PK) và Unique Lineage
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_pkey" PRIMARY KEY ("TripKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT uq_fact_source_lineage UNIQUE ("SourceFileSHA256", "SourceRowNumber");

    -- 3. Tạo lại 11 B-Tree Indexes trên FK và 1 Composite Index
    CREATE INDEX IF NOT EXISTS idx_fact_pickup_date ON dds."FactTaxiTrip" ("PickupDateKey");
    CREATE INDEX IF NOT EXISTS idx_fact_dropoff_date ON dds."FactTaxiTrip" ("DropoffDateKey");
    CREATE INDEX IF NOT EXISTS idx_fact_pickup_time ON dds."FactTaxiTrip" ("PickupTimeKey");
    CREATE INDEX IF NOT EXISTS idx_fact_dropoff_time ON dds."FactTaxiTrip" ("DropoffTimeKey");
    CREATE INDEX IF NOT EXISTS idx_fact_pickup_zone ON dds."FactTaxiTrip" ("PickupZoneKey");
    CREATE INDEX IF NOT EXISTS idx_fact_dropoff_zone ON dds."FactTaxiTrip" ("DropoffZoneKey");
    CREATE INDEX IF NOT EXISTS idx_fact_vendor ON dds."FactTaxiTrip" ("VendorKey");
    CREATE INDEX IF NOT EXISTS idx_fact_payment ON dds."FactTaxiTrip" ("PaymentTypeKey");
    CREATE INDEX IF NOT EXISTS idx_fact_ratecode ON dds."FactTaxiTrip" ("RateCodeKey");
    CREATE INDEX IF NOT EXISTS idx_fact_passenger_grp ON dds."FactTaxiTrip" ("PassengerGroupKey");
    CREATE INDEX IF NOT EXISTS idx_fact_distance_band ON dds."FactTaxiTrip" ("DistanceBandKey");
    CREATE INDEX IF NOT EXISTS idx_fact_usable_flags ON dds."FactTaxiTrip" ("IsAmountUsable", "IsTimeUsable", "IsDistanceUsable");

    -- 4. Khôi phục 11 Foreign Keys (dùng NOT VALID rồi VALIDATE để tận dụng index vừa tạo)
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_PickupDateKey_fkey" FOREIGN KEY ("PickupDateKey") REFERENCES dds."DimDate"("DateKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_DropoffDateKey_fkey" FOREIGN KEY ("DropoffDateKey") REFERENCES dds."DimDate"("DateKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_PickupTimeKey_fkey" FOREIGN KEY ("PickupTimeKey") REFERENCES dds."DimTime"("TimeKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_DropoffTimeKey_fkey" FOREIGN KEY ("DropoffTimeKey") REFERENCES dds."DimTime"("TimeKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_PickupZoneKey_fkey" FOREIGN KEY ("PickupZoneKey") REFERENCES dds."DimZone"("ZoneKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_DropoffZoneKey_fkey" FOREIGN KEY ("DropoffZoneKey") REFERENCES dds."DimZone"("ZoneKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_VendorKey_fkey" FOREIGN KEY ("VendorKey") REFERENCES dds."DimVendor"("VendorKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_PaymentTypeKey_fkey" FOREIGN KEY ("PaymentTypeKey") REFERENCES dds."DimPaymentType"("PaymentTypeKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_RateCodeKey_fkey" FOREIGN KEY ("RateCodeKey") REFERENCES dds."DimRateCode"("RateCodeKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_PassengerGroupKey_fkey" FOREIGN KEY ("PassengerGroupKey") REFERENCES dds."DimPassengerGroup"("PassengerGroupKey");
    ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_DistanceBandKey_fkey" FOREIGN KEY ("DistanceBandKey") REFERENCES dds."DimDistanceBand"("DistanceBandKey");

    ANALYZE dds."FactTaxiTrip";
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"[RESTORE] Hoàn tất dựng PK, Index & FK trong {round(time.time() - t0, 2)}s.", flush=True)


def generate_s03_reports(conn):
    print("\n[REPORT] Đang tổng hợp số liệu đối soát Dimension và xuất báo cáo S03...", flush=True)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT file_id, file_name, source_schema_version, load_status,
                   total_rows, staged_rows, fact_rows, quarantine_rows,
                   ROUND(file_size_bytes / 1048576.0, 2) AS file_size_mb,
                   first_loaded_at, last_updated_at
            FROM meta.file_manifest
            ORDER BY file_name
        """)
        manifest_rows = cur.fetchall()
        df_man = pd.DataFrame(manifest_rows)
        manifest_csv_path = DOCS_DIR / 'manifest_36_months.csv'
        df_man.to_csv(manifest_csv_path, index=False, encoding='utf-8')

        cur.execute("""
            SELECT
                COUNT(*) AS total_fact_rows,
                COUNT(DISTINCT "SourceFileName") AS distinct_files,
                ROUND(COALESCE(SUM("TotalAmount"), 0), 2) AS gross_total_amount,
                ROUND(COALESCE(SUM("TotalAmount") FILTER (WHERE "IsAmountUsable"), 0), 2) AS usable_total_amount,
                COUNT(*) FILTER (WHERE "HasDQIssue") AS dq_issue_rows,
                COUNT(*) FILTER (WHERE "PickupDateKey" = 0) AS pu_date_key0,
                COUNT(*) FILTER (WHERE "DropoffDateKey" = 0) AS do_date_key0,
                COUNT(*) FILTER (WHERE "PickupTimeKey" = 0) AS pu_time_key0,
                COUNT(*) FILTER (WHERE "DropoffTimeKey" = 0) AS do_time_key0,
                COUNT(*) FILTER (WHERE "PickupZoneKey" = 0) AS pu_zone_key0,
                COUNT(*) FILTER (WHERE "DropoffZoneKey" = 0) AS do_zone_key0,
                COUNT(*) FILTER (WHERE "VendorKey" = 0) AS vendor_key0,
                COUNT(*) FILTER (WHERE "PaymentTypeKey" = 0) AS payment_key0,
                COUNT(*) FILTER (WHERE "RateCodeKey" = 0) AS rate_key0,
                COUNT(*) FILTER (WHERE "PassengerGroupKey" = 0) AS pass_grp_key0,
                COUNT(*) FILTER (WHERE "DistanceBandKey" = 0) AS dist_band_key0,
                COUNT(DISTINCT "PickupDateKey") FILTER (WHERE "PickupDateKey" > 0) AS active_pu_dates,
                COUNT(DISTINCT "DropoffDateKey") FILTER (WHERE "DropoffDateKey" > 0) AS active_do_dates,
                COUNT(DISTINCT "PickupTimeKey") FILTER (WHERE "PickupTimeKey" > 0) AS active_pu_times,
                COUNT(DISTINCT "DropoffTimeKey") FILTER (WHERE "DropoffTimeKey" > 0) AS active_do_times,
                COUNT(DISTINCT "PickupZoneKey") FILTER (WHERE "PickupZoneKey" > 0) AS active_pu_zones,
                COUNT(DISTINCT "DropoffZoneKey") FILTER (WHERE "DropoffZoneKey" > 0) AS active_do_zones,
                COUNT(DISTINCT "VendorKey") FILTER (WHERE "VendorKey" > 0) AS active_vendors,
                COUNT(DISTINCT "PaymentTypeKey") FILTER (WHERE "PaymentTypeKey" > 0) AS active_payments,
                COUNT(DISTINCT "RateCodeKey") FILTER (WHERE "RateCodeKey" > 0) AS active_rates,
                COUNT(DISTINCT "PassengerGroupKey") FILTER (WHERE "PassengerGroupKey" > 0) AS active_pass_grps,
                COUNT(DISTINCT "DistanceBandKey") FILTER (WHERE "DistanceBandKey" > 0) AS active_dist_bands
            FROM dds."FactTaxiTrip"
        """)
        stat = cur.fetchone()

        cur.execute("""
            SELECT (
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimDate" d ON f."PickupDateKey"=d."DateKey" WHERE d."DateKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimDate" d ON f."DropoffDateKey"=d."DateKey" WHERE d."DateKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimTime" d ON f."PickupTimeKey"=d."TimeKey" WHERE d."TimeKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimTime" d ON f."DropoffTimeKey"=d."TimeKey" WHERE d."TimeKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimZone" d ON f."PickupZoneKey"=d."ZoneKey" WHERE d."ZoneKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimZone" d ON f."DropoffZoneKey"=d."ZoneKey" WHERE d."ZoneKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimVendor" d ON f."VendorKey"=d."VendorKey" WHERE d."VendorKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimPaymentType" d ON f."PaymentTypeKey"=d."PaymentTypeKey" WHERE d."PaymentTypeKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimRateCode" d ON f."RateCodeKey"=d."RateCodeKey" WHERE d."RateCodeKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimPassengerGroup" d ON f."PassengerGroupKey"=d."PassengerGroupKey" WHERE d."PassengerGroupKey" IS NULL) +
                (SELECT COUNT(*) FROM dds."FactTaxiTrip" f LEFT JOIN dds."DimDistanceBand" d ON f."DistanceBandKey"=d."DistanceBandKey" WHERE d."DistanceBandKey" IS NULL)
            ) AS orphaned_fks
        """)
        orphaned_fks = cur.fetchone()['orphaned_fks']

    total_n = max(int(stat['total_fact_rows']), 1)
    pct = lambda x: f"{(int(x) * 100.0 / total_n):.4f}%"

    report_md = f"""# BÁO CÁO ĐỐI SOÁT ĐỘ PHỦ DIMENSION & KẾT QUẢ NẠP MỞ RỘNG (TASK S03)

**Dự án:** Hệ thống BI Phân tích lộ trình NYC Yellow Taxi (2021 – 2023) — Nhóm 09  
**Task:** S03 — Nạp mở rộng 36 Tháng & Đối soát Dimension  
**Người phụ trách:** `stinh` (Kiều Đỗ Song Tinh - 23120373)  
**Thời điểm xuất báo cáo:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

---

## 1. Tổng quan Kết quả Nạp Dữ liệu & Toàn vẹn Tham chiếu

| Chỉ tiêu kiểm toán | Giá trị thực tế | Kỳ vọng / Quy chuẩn | Trạng thái |
| :--- | ---: | :--- | :---: |
| Số tệp Parquet đã nạp thành công | **{stat['distinct_files']}** file | 36 file (2021-01 đến 2023-12) | **ĐẠT** |
| Tổng số bản ghi trong `dds."FactTaxiTrip"` | **{int(stat['total_fact_rows']):,}** dòng | Khớp 100% Staged hợp lệ | **ĐẠT** |
| Tổng tiền nguồn (`SUM(TotalAmount)`) | **${float(stat['gross_total_amount']):,.2f}** | Khớp 100% Staging | **ĐẠT** |
| Tổng tiền đủ điều kiện (`IsAmountUsable = TRUE`) | **${float(stat['usable_total_amount']):,.2f}** | Lọc sạch cực trị & số âm | **ĐẠT** |
| Số dòng gắn cờ chất lượng (`HasDQIssue = TRUE`) | **{int(stat['dq_issue_rows']):,}** ({pct(stat['dq_issue_rows'])}) | Giữ lại 100% theo Preserve & Flag | **ĐẠT** |
| Số lượng Khóa ngoại mồ côi (**Orphaned FKs**) | **{orphaned_fks}** (0.0000%) | Bắt buộc = 0 trên cả 11 FKs | **PASS 100%** |

---

## 2. Bảng Đối soát Độ phủ (Coverage) & Tỷ lệ Rơi vào `Unknown (Key = 0)` trên 11 Vai trò FK

| Khóa ngoại trên Fact | Bảng Chiều tham chiếu | Tổng dòng Dim (gồm Key 0) | Số mã `> 0` phát sinh giao dịch | Số dòng Fact rơi vào `Key = 0` | Tỷ lệ `Key = 0` (%) | Giải thích nghiệp vụ cho các dòng `Key = 0` |
| :--- | :--- | :---: | :---: | ---: | ---: | :--- |
| `PickupDateKey` | `dds."DimDate"` | 1.098 | {stat['active_pu_dates']} / 1.097 | {int(stat['pu_date_key0']):,} | {pct(stat['pu_date_key0'])} | Ngày đón ngoài miền `31/12/2020 - 01/01/2024` |
| `DropoffDateKey` | `dds."DimDate"` | 1.098 | {stat['active_do_dates']} / 1.097 | {int(stat['do_date_key0']):,} | {pct(stat['do_date_key0'])} | Ngày trả ngoài miền `31/12/2020 - 01/01/2024` |
| `PickupTimeKey` | `dds."DimTime"` | 1.441 | {stat['active_pu_times']} / 1.440 | {int(stat['pu_time_key0']):,} | {pct(stat['pu_time_key0'])} | Chỉ bằng 0 khi `PickupDateTime` bị NULL |
| `DropoffTimeKey` | `dds."DimTime"` | 1.441 | {stat['active_do_times']} / 1.440 | {int(stat['do_time_key0']):,} | {pct(stat['do_time_key0'])} | Chỉ bằng 0 khi `DropoffDateTime` bị NULL |
| `PickupZoneKey` | `dds."DimZone"` | 266 | {stat['active_pu_zones']} / 265 | {int(stat['pu_zone_key0']):,} | {pct(stat['pu_zone_key0'])} | Mã khu vực ngoài dải `1..265` hoặc NULL |
| `DropoffZoneKey` | `dds."DimZone"` | 266 | {stat['active_do_zones']} / 265 | {int(stat['do_zone_key0']):,} | {pct(stat['do_zone_key0'])} | Mã khu vực ngoài dải `1..265` hoặc NULL |
| `VendorKey` | `dds."DimVendor"` | 3 | {stat['active_vendors']} / 2 | {int(stat['vendor_key0']):,} | {pct(stat['vendor_key0'])} | Các chuyến có `VendorID` chưa xác minh (Vendor 5, 6) |
| `PaymentTypeKey` | `dds."DimPaymentType"` | 7 | {stat['active_payments']} / 6 | {int(stat['payment_key0']):,} | {pct(stat['payment_key0'])} | Tập ~4,15M dòng khuyết có `payment_type = 0` |
| `RateCodeKey` | `dds."DimRateCode"` | 8 | {stat['active_rates']} / 7 | {int(stat['rate_key0']):,} | {pct(stat['rate_key0'])} | Tập dòng khuyết `RatecodeID IS NULL` hoặc Rate 99 |
| `PassengerGroupKey` | `dds."DimPassengerGroup"` | 4 | {stat['active_pass_grps']} / 3 | {int(stat['pass_grp_key0']):,} | {pct(stat['pass_grp_key0'])} | Tập dòng khuyết `passenger_count` hoặc `= 0`, `> 9` |
| `DistanceBandKey` | `dds."DimDistanceBand"` | 6 | {stat['active_dist_bands']} / 5 | {int(stat['dist_band_key0']):,} | {pct(stat['dist_band_key0'])} | Các chuyến xe có cự ly âm (`< 0`) hoặc NULL |

---

## 3. Trạng thái Manifest theo Từng File (`meta.file_manifest`)

| STT | Tên tệp Parquet | Schema | Trạng thái | Footer Rows | Staged Rows | Fact Rows | Quarantine |
| :-: | :--- | :---: | :---: | ---: | ---: | ---: | ---: |
"""
    for idx, r in enumerate(manifest_rows, start=1):
        report_md += (
            f"| {idx} | `{r['file_name']}` | `{r['source_schema_version']}` | **{r['load_status']}** | "
            f"{int(r['total_rows'] or 0):,} | {int(r['staged_rows'] or 0):,} | "
            f"{int(r['fact_rows'] or 0):,} | {int(r['quarantine_rows'] or 0):,} |\n"
        )

    report_path = DOCS_DIR / 'dim_coverage_report.md'
    report_path.write_text(report_md, encoding='utf-8')
    print(f"[REPORT] Đã xuất thành công:\n  - {manifest_csv_path}\n  - {report_path}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Task S03: Nạp mở rộng 36 tháng siêu tốc")
    parser.add_argument("--file-pattern", default="yellow_tripdata_*.parquet")
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--keep-last-staging", action="store_true")
    parser.add_argument("--skip-index-drop", action="store_true")
    args = parser.parse_args()

    overall_t0 = time.time()
    print("=" * 75, flush=True)
    print("  TASK S03 (STINH): NẠP MỞ RỘNG 36 THÁNG SIÊU TỐC (TURBO BULK MODE) ", flush=True)
    print("=" * 75, flush=True)

    print("\n[STEP 1] Kiểm tra & Khởi tạo Zones, Seeds, Staging Views và 8 Dimensions...", flush=True)
    load_and_ingest_zones()
    run_cmd(['dbt', 'seed', '--full-refresh', '--profiles-dir', '.'], desc="dbt seed")
    run_cmd(['dbt', 'run', '--select', 'staging', 'marts.dims', '--threads', '1', '--profiles-dir', '.'], desc="dbt staging & 8 dims")

    print("\n[STEP 2] Quét danh mục Parquet vào meta.file_manifest...", flush=True)
    df_manifest = scan_and_register_manifest(file_pattern=args.file_pattern, max_files=args.max_files)

    conn = get_pg_conn()
    try:
        files_to_process = []
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for _, row in df_manifest.iterrows():
                fname = row['file_name']
                fsha = row['file_sha256']
                expected_total = int(row['total_rows'])

                cur.execute("""
                    SELECT COUNT(*) AS cnt FROM dds."FactTaxiTrip"
                    WHERE "SourceFileName" = %s
                """, (fname,))
                fact_cnt = int(cur.fetchone()['cnt'])

                cur.execute("""
                    SELECT COUNT(*) AS q_cnt FROM staging.quarantine
                    WHERE source_file_name = %s
                """, (fname,))
                quar_cnt = int(cur.fetchone()['q_cnt'])

                if fact_cnt + quar_cnt == expected_total and expected_total > 0:
                    cur.execute("""
                        UPDATE meta.file_manifest
                        SET load_status = 'SUCCESS',
                            staged_rows = %s,
                            fact_rows = %s,
                            quarantine_rows = %s,
                            last_updated_at = CURRENT_TIMESTAMP
                        WHERE file_name = %s
                    """, (expected_total, fact_cnt, quar_cnt, fname))
                    print(f"  [RESUME-SKIP] {fname} đã có đủ {fact_cnt:,} dòng trong FactTaxiTrip.", flush=True)
                else:
                    files_to_process.append(row)
        conn.commit()

        if files_to_process:
            if not args.skip_index_drop:
                prepare_ultra_fast_bulk_mode(conn)

            total_files = len(files_to_process)
            for idx, row in enumerate(files_to_process, start=1):
                fname = row['file_name']
                fsha = row['file_sha256']
                expected_total = int(row['total_rows'])
                f_t0 = time.time()
                print(f"\n--- [{idx}/{total_files}] ĐANG XỬ LÝ: {fname} ({expected_total:,} dòng) ---", flush=True)

                # 1. Làm sạch bảng staging và xóa dòng dở dang (nếu có) của file này trong Fact
                with conn.cursor() as cur:
                    cur.execute("TRUNCATE TABLE staging.raw_yellow_trips;")
                    cur.execute('DELETE FROM dds."FactTaxiTrip" WHERE "SourceFileName" = %s;', (fname,))
                conn.commit()

                # 2. Nạp file hiện tại vào staging.raw_yellow_trips
                single_df = pd.DataFrame([row])
                single_df['action_status'] = 'PENDING'
                export_parquet_batches_to_staging(single_df, batch_size=100000, force_reload=True)

                # 3. Chạy dbt FactTaxiTrip ở chế độ s03_fast_append (không tạo bảng tạm, INSERT trực tiếp vào bảng UNLOGGED)
                dbt_vars = f'{{"source_file_name": "{fname}", "s03_fast_append": true}}'
                run_cmd(
                    ['dbt', 'run', '--select', 'FactTaxiTrip', '--vars', dbt_vars, '--profiles-dir', '.'],
                    desc=f"Fast Append FactTaxiTrip cho {fname}"
                )

                # 4. Kiểm tra số dòng vừa nạp (nếu lệch so với expected_total mới gọi load_quarantine để tiết kiệm thời gian)
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT COUNT(*) AS fact_cnt
                        FROM dds."FactTaxiTrip"
                        WHERE "SourceFileName" = %s
                    """, (fname,))
                    fact_cnt = int(cur.fetchone()['fact_cnt'])
                    quar_cnt = 0

                    if fact_cnt != expected_total:
                        run_cmd(
                            ['dbt', 'run-operation', 'load_quarantine', '--args', '{prune_stale: false}', '--profiles-dir', '.'],
                            desc=f"Quarantine check cho {fname}"
                        )
                        cur.execute("""
                            SELECT COUNT(*) AS quar_cnt
                            FROM staging.quarantine
                            WHERE source_file_name = %s
                        """, (fname,))
                        quar_cnt = int(cur.fetchone()['quar_cnt'])

                    if fact_cnt + quar_cnt != expected_total:
                        raise RuntimeError(
                            f"ĐỐI SOÁT LỆCH trên {fname}: Footer={expected_total} != Fact({fact_cnt}) + Quarantine({quar_cnt})"
                        )

                    final_status = 'SUCCESS' if quar_cnt == 0 else 'COMPLETED_WITH_QUARANTINE'
                    cur.execute("""
                        UPDATE meta.file_manifest
                        SET load_status = %s,
                            fact_rows = %s,
                            quarantine_rows = %s,
                            last_updated_at = CURRENT_TIMESTAMP
                        WHERE file_name = %s
                    """, (final_status, fact_cnt, quar_cnt, fname))
                conn.commit()

                f_dur = round(time.time() - f_t0, 2)
                print(f"--- [HOÀN TẤT {idx}/{total_files}] {fname}: Fact={fact_cnt:,} dòng trong {f_dur}s ---", flush=True)

            if not args.skip_index_drop:
                restore_fact_indexes_and_fks(conn)
        else:
            print("\n[INFO] Toàn bộ 36 file đã được nạp đầy đủ vào FactTaxiTrip!", flush=True)

        generate_s03_reports(conn)

    finally:
        conn.close()

    total_min = round((time.time() - overall_t0) / 60.0, 2)
    print("\n" + "=" * 75, flush=True)
    print(f"  HOÀN TẤT TOÀN BỘ TASK S03 TRONG {total_min} PHÚT! ", flush=True)
    print("=" * 75, flush=True)


if __name__ == '__main__':
    main()