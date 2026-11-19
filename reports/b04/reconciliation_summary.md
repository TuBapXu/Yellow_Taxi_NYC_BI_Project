# B04 - Đối soát tài chính & sản lượng: Nguồn (Parquet) ↔ Staging ↔ Fact

- Thời gian chạy: 302 giây
- Phạm vi: 36 file, 108,870,632 dòng theo footer Parquet

## Kết quả kiểm tra

| Kiểm tra | Kết quả | Chi tiết |
|---|---|---|
| Footer Parquet = số dòng đọc được (36 file) | PASS | 36 file |
| Footer = Fact + Quarantine, tổng tiền 9 cột Nguồn = Fact (từng file, đến từng cent) | PASS | 0 file lệch |
| Số giá trị NULL từng cột tiền: Nguồn = Fact | PASS | khớp 9/9 cột |
| Đối soát 3 chiều Nguồn = Raw = Staging (flagged + reject) = Fact (số dòng + 9 cột tiền) | PASS | yellow_tripdata_2021-01.parquet, yellow_tripdata_2023-12.parquet |

## Đối soát theo năm

| Năm | File | Dòng nguồn (footer) | Dòng Fact | Quarantine | Lệch dòng | TotalAmount nguồn | TotalAmount Fact | Lệch |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2021 | 12 | 30,904,308 | 30,904,308 | 0 | 0 | 608,700,670.92 | 608,700,670.92 | 0.00 |
| 2022 | 12 | 39,656,098 | 39,656,098 | 0 | 0 | 859,397,945.19 | 859,397,945.19 | 0.00 |
| 2023 | 12 | 38,310,226 | 38,310,226 | 0 | 0 | 1,090,383,411.89 | 1,090,383,411.89 | 0.00 |
| TONG | 36 | 108,870,632 | 108,870,632 | 0 | 0 | 2,558,482,028.00 | 2,558,482,028.00 | 0.00 |

Chi tiết 9 cột tiền và số giá trị NULL / bị làm tròn: `reconciliation.csv` (theo năm), `reconciliation_by_file.csv` (36 file), `reconciliation_staging_3way.csv` (đối soát 3 chiều qua staging: file mẫu schema v1 & v2, nạp thử rồi ROLLBACK).
