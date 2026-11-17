import os
import time
import subprocess
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def run_dbt_test_and_audit(*args, **kwargs):
    """
    Task T03: Kiểm toán chất lượng và thực thi data tests cho Fact.
    - Chạy dbt test cho FactTaxiTrip và các test kiểm tra toàn vẹn
    - Đối soát trực tiếp số dòng và tổng tiền giữa Staging và Fact
    - Kiểm tra số lượng FK mồ côi (yêu cầu = 0)
    """
    root = Path(__file__).resolve().parents[2]
    dbt_dir = root / 'dbt_project'

    t0 = time.time()
    print("[START] Đang chạy dbt test cho Fact và Data Tests toàn vẹn...")
    cmd = ['dbt', 'test', '--select', 'FactTaxiTrip', 'test_type:singular']
    res = subprocess.run(cmd, cwd=dbt_dir, capture_output=True, text=True)
    test_duration = round(time.time() - t0, 2)

    # Truy vấn đối soát CSDL thực tế
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        dbname=os.getenv('POSTGRES_DB', 'DW_DB'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'admin123')
    )

    audit_metrics = {
        'test_duration_seconds': test_duration,
        'dbt_test_passed': (res.returncode == 0)
    }

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Fact summary
            cur.execute("""
                SELECT 
                    COUNT(*) as fact_rows,
                    COUNT(DISTINCT "TripKey") as distinct_trips,
                    ROUND(COALESCE(SUM("TotalAmount"), 0), 2) as total_amount,
                    COUNT(*) FILTER (WHERE "HasDQIssue") as dq_issues
                FROM dds."FactTaxiTrip"
            """)
            f_res = cur.fetchone()
            audit_metrics['fact_rows'] = f_res['fact_rows']
            audit_metrics['distinct_trips'] = f_res['distinct_trips']
            audit_metrics['total_amount'] = float(f_res['total_amount'])
            audit_metrics['dq_issues'] = f_res['dq_issues']

            # 2. Staged summary
            cur.execute("""
                SELECT 
                    COUNT(*) as staged_rows,
                    ROUND(COALESCE(SUM(total_amount), 0), 2) as staged_amount
                FROM staging.stg_trips_flagged
            """)
            s_res = cur.fetchone()
            audit_metrics['staged_rows'] = s_res['staged_rows']
            audit_metrics['staged_amount'] = float(s_res['staged_amount'])

            # 3. Check orphaned FKs
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
                ) as orphaned_fks
            """)
            audit_metrics['orphaned_fks'] = cur.fetchone()['orphaned_fks']

    finally:
        conn.close()

    print(f"[AUDIT] Fact rows: {audit_metrics['fact_rows']:,} | Staged: {audit_metrics['staged_rows']:,} | Orphaned FKs: {audit_metrics['orphaned_fks']}")
    return audit_metrics
