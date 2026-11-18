# B03 - Kiểm chứng độc lập từ Parquet gốc

Tính lại bằng pandas trực tiếp từ 2 file Parquet (4,283,724 dòng), không qua SQL/dbt.

| Chỉ số | Pandas (Parquet) | SQL B03 | Kết quả |
|---|---:|---:|---|
| CB08 tổng số chuyến | 4,283,724.00 | 4,283,724.00 | PASS |
| CB08 doanh thu gộp (USD) | 102,316,869.49 | 102,316,869.49 | PASS |
| CB08 doanh thu thuần (USD) | 102,992,854.62 | 102,992,854.62 | PASS |
| CB08 số chuyến bị loại khỏi thuần | 32,703.00 | 32,703.00 | PASS |
| CB01 số chuyến 2020-12 | 16.00 | 16.00 | PASS |
| CB01 số chuyến 2021-01 | 1,369,745.00 | 1,369,745.00 | PASS |
| CB01 số chuyến 2021-02 | 4.00 | 4.00 | PASS |
| CB01 số chuyến 2023-01 | 8.00 | 8.00 | PASS |
| CB01 số chuyến 2023-02 | 2,913,900.00 | 2,913,900.00 | PASS |
| CB01 số chuyến 2023-03 | 42.00 | 42.00 | PASS |
| CB01 số chuyến DateKey 0 (ngoài miền / thiếu ngày) | 9.00 | 9.00 | PASS |
| MR01 tỷ lệ tip % rate 0 | 0.02 | 0.02 | PASS |
| MR01 vận tốc mph rate 0 | 15.62 | 15.62 | PASS |
| MR01 tỷ lệ tip % rate 1 | 23.63 | 23.63 | PASS |
| MR01 vận tốc mph rate 1 | 11.08 | 11.08 | PASS |
| MR01 tỷ lệ tip % rate 2 | 19.60 | 19.60 | PASS |
| MR01 vận tốc mph rate 2 | 23.73 | 23.73 | PASS |
| MR01 tỷ lệ tip % rate 3 | 19.02 | 19.02 | PASS |
| MR01 vận tốc mph rate 3 | 26.37 | 26.37 | PASS |
| MR01 tỷ lệ tip % rate 4 | 14.43 | 14.43 | PASS |
| MR01 vận tốc mph rate 4 | 29.36 | 29.36 | PASS |
| MR01 tỷ lệ tip % rate 5 | 13.38 | 13.38 | PASS |
| MR01 vận tốc mph rate 5 | 24.76 | 24.76 | PASS |
| MR01 tỷ lệ tip % rate 6 | 242.71 | 242.71 | PASS |
| MR01 vận tốc mph rate 6 | 18.79 | 18.79 | PASS |
| CB07 hạng 1 tuyến 237->236 | 31,613.00 | 31,613.00 | PASS |
| CB07 hạng 2 tuyến 236->237 | 27,028.00 | 27,028.00 | PASS |
| CB07 hạng 3 tuyến 236->236 | 22,384.00 | 22,384.00 | PASS |
| CB07 hạng 4 tuyến 237->237 | 20,402.00 | 20,402.00 | PASS |
| CB07 hạng 5 tuyến 239->238 | 12,673.00 | 12,673.00 | PASS |
| CB07 hạng 6 tuyến 141->236 | 12,206.00 | 12,206.00 | PASS |
| CB07 hạng 7 tuyến 161->237 | 12,021.00 | 12,021.00 | PASS |
| CB07 hạng 8 tuyến 239->142 | 11,674.00 | 11,674.00 | PASS |
| CB07 hạng 9 tuyến 237->161 | 11,546.00 | 11,546.00 | PASS |
| CB07 hạng 10 tuyến 142->239 | 11,347.00 | 11,347.00 | PASS |
| CB07 số chuyến tuyến hạng 1 = lớn nhất toàn bộ | 31,613.00 | 31,613.00 | PASS |
| DQ03 dòng khuyết đồng loạt | 175,169.00 | 175,169.00 | PASS |
| DQ01 dòng HasDQIssue | 95,686.00 | 95,686.00 | PASS |
| DQ01 dòng time không dùng được | 7,858.00 | 7,858.00 | PASS |
| DQ01 dòng cự ly không dùng được | 61,197.00 | 61,197.00 | PASS |
| DQ01 dòng tiền không dùng được | 32,703.00 | 32,703.00 | PASS |
