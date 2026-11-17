import os
import csv
from pathlib import Path
import psutil

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_exporter
def record_benchmark_metrics(*args, **kwargs):
    """
    Task T03: Data Exporter thu thập các chỉ số hiệu năng và lưu trữ ra file benchmark_1m.csv.
    """
    audit_metrics = args[0] if args else {}

    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    ram_mb = round(mem_info.rss / (1024 * 1024), 2)

    root = (
        Path(__file__).resolve().parents[2]
        if '__file__' in globals()
        else (Path('/home/src') if Path('/home/src/dbt_project').exists() else Path.cwd())
    )
    benchmark_csv = root / 'benchmark_1m.csv'

    file_name = kwargs.get('file_pattern', 'yellow_tripdata_2021-01.parquet')

    rows = [
        ['metric_name', 'metric_value', 'unit', 'note'],
        ['file_name', file_name, 'text', 'Tệp Parquet chạy thử nghiệm 1 tháng mẫu'],
        ['staged_row_count', audit_metrics.get('staged_rows', 0), 'rows', 'Số dòng nạp thành công vào staging.raw_yellow_trips'],
        ['fact_row_count', audit_metrics.get('fact_rows', 0), 'rows', 'Số dòng nạp thành công vào dds.FactTaxiTrip'],
        ['distinct_trips', audit_metrics.get('distinct_trips', 0), 'rows', 'Số lượng TripKey duy nhất trong Fact'],
        ['fact_orphaned_fks', audit_metrics.get('orphaned_fks', 0), 'rows', 'Số lượng khóa ngoại mồ côi (yêu cầu = 0)'],
        ['has_dq_issue_rows', audit_metrics.get('dq_issues', 0), 'rows', 'Số dòng bị gắn cờ dị thường chất lượng'],
        ['total_amount_usd', audit_metrics.get('total_amount', 0.0), 'USD', 'Tổng doanh thu giao dịch ghi nhận trên Fact'],
        ['dbt_test_status', 'PASS' if audit_metrics.get('dbt_test_passed', False) else 'WARN/FAIL', 'text', 'Trạng thái kiểm thử dbt tests'],
        ['time_dbt_tests_seconds', audit_metrics.get('test_duration_seconds', 0.0), 'seconds', 'Thời gian thực thi dbt test'],
        ['process_memory_rss_mb', ram_mb, 'MB', 'Mức tiêu thụ RAM của tiến trình Mage']
    ]

    with open(benchmark_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"[SUCCESS] Đã xuất file benchmark hiệu năng tại {benchmark_csv}")
    return {
        'status': 'BENCHMARK_RECORDED',
        'benchmark_path': str(benchmark_csv),
        'metrics': audit_metrics
    }
