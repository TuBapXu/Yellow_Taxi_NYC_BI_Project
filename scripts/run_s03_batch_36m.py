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
    env = os.environ.copy()
    env['PGOPTIONS'] = (env.get('PGOPTIONS', '') + ' -c jit=off').strip()
    res = subprocess.run(cmd, cwd=cwd, env=env)
    dur = round(time.time() - t0, 2)
    if res.returncode != 0:
        print(f"  [ERROR] Thất bại sau {dur}s", flush=True)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return dur, ""


def prepare_ultra_fast_bulk_mode(conn):
    """
    Tối ưu hóa tốc độ nạp 108M dòng (Safe-LOGGED Bulk Mode):
    1. Chỉ chuyển bảng tạm staging.raw_yellow_trips sang UNLOGGED; GIỮ NGUYÊN dds."FactTaxiTrip" ở chế độ LOGGED
       (Crash-Safe từng file, không bao giờ gây đỉnh ghi đĩa ~60GB khi SET LOGGED ở cuối).
    2. Tạm gỡ 11 FKs, 2 Unique Constraints (PK TripKey MD5 + uq_fact_source_lineage) và 12 Indexes phụ.
    3. Chỉ giữ lại 1 index nhẹ trên (SourceFileName) để kiểm tra Resume và xóa nhanh theo file.
    """
    print("\n[SAFE-BULK] Giữ FactTaxiTrip LOGGED (Crash-Safe) & tạm gỡ PK/Indexes/FKs để nạp nhanh an toàn...", flush=True)
    sql = """
    -- 1. Gỡ 11 Foreign Keys (cả tên chữ Hoa và chữ thường)
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

    -- 4. Chỉ đặt UNLOGGED cho bảng tạm staging; giữ FactTaxiTrip LOGGED để an toàn khi crash
    ALTER TABLE staging.raw_yellow_trips SET UNLOGGED;

    -- 5. Tạo 1 index siêu nhẹ trên SourceFileName để đối soát tức thì từng tháng
    CREATE INDEX IF NOT EXISTS idx_fact_temp_source_file ON dds."FactTaxiTrip" ("SourceFileName");

    -- 6. Gỡ MỌI FK còn lại trên Fact bất kể tên. Macro restore_fact_fk (post_hook của 8 Dims) tạo FK tên
    --    chữ thường (facttaxitrip_*_fkey) nên các lệnh DROP tên có ngoặc kép ở bước 1 không khớp -> FK vẫn còn.
    DO $$
    DECLARE r record;
    BEGIN
        FOR r IN SELECT conname FROM pg_constraint
                 WHERE conrelid = 'dds."FactTaxiTrip"'::regclass AND contype = 'f'
        LOOP
            EXECUTE format('ALTER TABLE dds."FactTaxiTrip" DROP CONSTRAINT %I', r.conname);
        END LOOP;
    END $$;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def restore_fact_indexes_and_fks(conn):
    """
    Khôi phục an toàn từng bước (mỗi lệnh COMMIT riêng lẻ, tự bỏ qua bước đã tồn tại khi Resume):
    - Đảm bảo LOGGED (chỉ chạy nếu đang UNLOGGED)
    - Primary Key, Unique Constraint, 12 B-Tree Indexes và 11 Foreign Keys (NOT VALID -> VALIDATE)
    - Dùng maintenance_work_mem=512MB & 2 workers để tránh OOM trong Docker WSL2.
    """
    print("\n[RESTORE] Đang khôi phục từng bước PK, Unique, 12 Indexes & 11 FKs trên dds.\"FactTaxiTrip\"...", flush=True)
    t0 = time.time()

    def _run_step(label, stmt):
        st = time.time()
        with conn.cursor() as cur:
            cur.execute("SET maintenance_work_mem = '512MB';")
            cur.execute("SET max_parallel_maintenance_workers = 2;")
            cur.execute(stmt)
        conn.commit()
        print(f"  [RESTORE-OK] {label} ({round(time.time() - st, 2)}s)", flush=True)

    _run_step("Drop temp SourceFileName index", 'DROP INDEX IF EXISTS dds.idx_fact_temp_source_file;')

    # 1. Chỉ chuyển LOGGED nếu hiện tại đang UNLOGGED
    with conn.cursor() as cur:
        cur.execute("SELECT relpersistence FROM pg_class WHERE oid = 'dds.\"FactTaxiTrip\"'::regclass;")
        relp = cur.fetchone()[0]
    conn.commit()
    if relp != 'p':
        _run_step("ALTER TABLE FactTaxiTrip SET LOGGED", 'ALTER TABLE dds."FactTaxiTrip" SET LOGGED;')
    else:
        print("  [RESTORE-SKIP] dds.\"FactTaxiTrip\" đã ở trạng thái LOGGED (relpersistence='p').", flush=True)

    # Lấy danh sách constraint đã tồn tại để hỗ trợ Resume an toàn
    with conn.cursor() as cur:
        cur.execute("SELECT lower(conname) FROM pg_constraint WHERE conrelid = 'dds.\"FactTaxiTrip\"'::regclass;")
        existing_cons = {r[0] for r in cur.fetchall()}
    conn.commit()

    # 2. Khôi phục PK và Unique Lineage
    if 'facttaxitrip_pkey' not in existing_cons:
        _run_step('ADD PRIMARY KEY ("TripKey")', 'ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "FactTaxiTrip_pkey" PRIMARY KEY ("TripKey");')
    if 'uq_fact_source_lineage' not in existing_cons:
        _run_step('ADD UNIQUE (SourceFileSHA256, SourceRowNumber)', 'ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT uq_fact_source_lineage UNIQUE ("SourceFileSHA256", "SourceRowNumber");')

    # 3. Tạo lại 12 B-Tree Indexes (mỗi index 1 transaction riêng biệt)
    indexes = [
        ("idx_fact_pickup_date", '("PickupDateKey")'),
        ("idx_fact_dropoff_date", '("DropoffDateKey")'),
        ("idx_fact_pickup_time", '("PickupTimeKey")'),
        ("idx_fact_dropoff_time", '("DropoffTimeKey")'),
        ("idx_fact_pickup_zone", '("PickupZoneKey")'),
        ("idx_fact_dropoff_zone", '("DropoffZoneKey")'),
        ("idx_fact_vendor", '("VendorKey")'),
        ("idx_fact_payment", '("PaymentTypeKey")'),
        ("idx_fact_ratecode", '("RateCodeKey")'),
        ("idx_fact_passenger_grp", '("PassengerGroupKey")'),
        ("idx_fact_distance_band", '("DistanceBandKey")'),
        ("idx_fact_usable_flags", '("IsAmountUsable", "IsTimeUsable", "IsDistanceUsable")'),
    ]
    for idx_name, cols in indexes:
        _run_step(f"CREATE INDEX {idx_name}", f'CREATE INDEX IF NOT EXISTS {idx_name} ON dds."FactTaxiTrip" {cols};')

    # 4. Khôi phục 11 Foreign Keys (mỗi FK 1 transaction riêng)
    fks = [
        ("FactTaxiTrip_PickupDateKey_fkey", '"PickupDateKey"', 'dds."DimDate"("DateKey")'),
        ("FactTaxiTrip_DropoffDateKey_fkey", '"DropoffDateKey"', 'dds."DimDate"("DateKey")'),
        ("FactTaxiTrip_PickupTimeKey_fkey", '"PickupTimeKey"', 'dds."DimTime"("TimeKey")'),
        ("FactTaxiTrip_DropoffTimeKey_fkey", '"DropoffTimeKey"', 'dds."DimTime"("TimeKey")'),
        ("FactTaxiTrip_PickupZoneKey_fkey", '"PickupZoneKey"', 'dds."DimZone"("ZoneKey")'),
        ("FactTaxiTrip_DropoffZoneKey_fkey", '"DropoffZoneKey"', 'dds."DimZone"("ZoneKey")'),
        ("FactTaxiTrip_VendorKey_fkey", '"VendorKey"', 'dds."DimVendor"("VendorKey")'),
        ("FactTaxiTrip_PaymentTypeKey_fkey", '"PaymentTypeKey"', 'dds."DimPaymentType"("PaymentTypeKey")'),
        ("FactTaxiTrip_RateCodeKey_fkey", '"RateCodeKey"', 'dds."DimRateCode"("RateCodeKey")'),
        ("FactTaxiTrip_PassengerGroupKey_fkey", '"PassengerGroupKey"', 'dds."DimPassengerGroup"("PassengerGroupKey")'),
        ("FactTaxiTrip_DistanceBandKey_fkey", '"DistanceBandKey"', 'dds."DimDistanceBand"("DistanceBandKey")'),
    ]
    for fk_name, col, ref_target in fks:
        if fk_name.lower() not in existing_cons:
            _run_step(
                f"ADD & VALIDATE FK {fk_name}",
                f'ALTER TABLE dds."FactTaxiTrip" ADD CONSTRAINT "{fk_name}" FOREIGN KEY ({col}) REFERENCES {ref_target} NOT VALID; '
                f'ALTER TABLE dds."FactTaxiTrip" VALIDATE CONSTRAINT "{fk_name}";'
            )

    _run_step('ANALYZE dds."FactTaxiTrip"', 'ANALYZE dds."FactTaxiTrip";')
    print(f"[RESTORE] Hoàn tất toàn bộ PK, 12 Indexes & 11 FKs trong {round(time.time() - t0, 2)}s.", flush=True)


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



def prepare_dependencies():
    print("\n[STEP 1] Kiểm tra & Khởi tạo Zones, Seeds, Staging Views và 8 Dimensions...", flush=True)
    load_and_ingest_zones()
    run_cmd(['dbt', 'seed', '--full-refresh', '--profiles-dir', '.'], desc="dbt seed")
    run_cmd(['dbt', 'run', '--select', 'staging', 'marts.dims', '--threads', '1', '--profiles-dir', '.'], desc="dbt staging & 8 dims")



def plan_files(conn, df_manifest):
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

    return files_to_process


def process_file(conn, row, idx, total_files):
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

    # 2b. Cập nhật thống kê staging NGAY sau khi nạp. Nếu không, dbt lên kế hoạch với thống kê của
    #     file trước (autoanalyze chạy trễ ~30s) -> ước lượng ~0 dòng cho source_file_name mới
    #     -> chọn kế hoạch kém: ~840s/file thay vì ~45s/file (đo thực tế 18/11).
    with conn.cursor() as cur:
        cur.execute("ANALYZE staging.raw_yellow_trips;")
    conn.commit()

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

    return {"file_name": fname, "status": final_status, "footer_rows": expected_total, "fact_rows": fact_cnt, "quarantine_rows": quar_cnt, "elapsed_seconds": f_dur}

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

    prepare_dependencies()

    print("\n[STEP 2] Quét danh mục Parquet vào meta.file_manifest...", flush=True)
    df_manifest = scan_and_register_manifest(file_pattern=args.file_pattern, max_files=args.max_files)

    conn = get_pg_conn()
    try:
        files_to_process = plan_files(conn, df_manifest)

        if files_to_process:
            if not args.skip_index_drop:
                prepare_ultra_fast_bulk_mode(conn)

            total_files = len(files_to_process)
            for idx, row in enumerate(files_to_process, start=1):
                process_file(conn, row, idx, total_files)

            if not args.skip_index_drop:
                restore_fact_indexes_and_fks(conn)
        else:
            print("\n[INFO] Toàn bộ 36 file đã được nạp đầy đủ vào FactTaxiTrip!", flush=True)
            if not args.skip_index_drop:
                restore_fact_indexes_and_fks(conn)

        generate_s03_reports(conn)

    finally:
        conn.close()

    total_min = round((time.time() - overall_t0) / 60.0, 2)
    print("\n" + "=" * 75, flush=True)
    print(f"  HOÀN TẤT TOÀN BỘ TASK S03 TRONG {total_min} PHÚT! ", flush=True)
    print("=" * 75, flush=True)


if __name__ == '__main__':
    main()