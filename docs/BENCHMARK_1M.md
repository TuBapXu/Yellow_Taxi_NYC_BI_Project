# BÁO CÁO BENCHMARK HIỆU NĂNG MASTER ETL PIPELINE (TASK T03)

- **Tệp thử nghiệm:** `yellow_tripdata_2021-01.parquet`
- **Thời điểm chạy:** 2026-11-17 03:36:49 UTC
- **Tổng thời gian thực thi (bao gồm Ingestion):** **883.17 giây** (~14.72 phút)

| Chỉ số hiệu năng & đối soát | Giá trị thực tế | Đơn vị | Đánh giá |
|---|---:|:---:|:---:|
| Footer Row Count | 1,369,769 | dòng | Khớp chuẩn nguồn |
| Staged Row Count | 1,369,769 | dòng | Khớp 100% footer |
| Fact Row Count | 1,369,769 | dòng | **Khớp 100% Staging** |
| Khóa ngoại mồ côi (Orphaned FKs) | **0** | dòng | **Zero Orphaned FKs** |
| Dòng có cảnh báo DQ (HasDQIssue) | 32,136 | dòng | Preserve & Flag |
| Tổng doanh thu TotalAmount | $23,935,896.09 | USD | Đối soát khớp 100% |
| Thời gian Ingestion (PyArrow) | 71.24s | giây | Chunk 100k dòng |
| Thời gian dbt Dims & Views | 28.69s | giây | 8 Dims + Views |
| Thời gian dbt Fact Incremental | 525.11s | giây | delete+insert |
| Thời gian dbt Tests & Audit | 258.13s | giây | 39 tests + Audit |
| **Tổng thời gian Pipeline** | **883.17s** | giây | ~14.7 phút cho 1,37M dòng |
| Bộ nhớ RAM tiêu thụ | 7.95 MB | MB | Đạt chỉ tiêu < 2 GB |
